import atexit
import copy
import datetime
import functools
import glob
import os
import papermill as pm
import re
import shutil
import threading
import time
import traceback
import tempfile
import uuid
from ahl.concurrent.futures import hpc_pool
from collections import defaultdict
from flask import Flask, render_template, request, jsonify, url_for, send_from_directory, abort, Response
from ahl.logging import get_logger
from typing import Dict

from idi.datascience.one_click_notebooks import tasks, results
from idi.datascience.one_click_notebooks.results import JobStatus, NotebookResultError

flask_app = Flask(__name__)
spark_pool = None
running_jobs = {}  # Maps jobid --> Future
complete_jobs = {}  # Maps report_name --> jobid --> results.NotebookResultBase
# TODO: Figure out how we can get the below to work without expanduser, which is required by Spark.
OUTPUT_BASE_DIR = os.path.join(os.getenv('OUTPUT_DIR', tempfile.mkdtemp(dir=os.path.expanduser('~'))), 'results')
TEMPLATE_BASE_DIR = os.path.join(os.getenv('OUTPUT_DIR', tempfile.mkdtemp(dir=os.path.expanduser('~'))), 'templates')
MONGO_HOST = 'research'
MONGO_LIBRARY = 'NOTEBOOK_OUTPUT'
result_serializer = None
alive = True
logger = get_logger(__name__)


# ----------------- Main page -------------------- #

@flask_app.route('/', methods=['GET'])
def index():
    return render_template('index.html', all_jobs=all_available_results())


# ------------------ Running checks ---------------- #

@flask_app.route('/run_report/<report_name>', methods=['GET'])
def run_report(report_name):
    path = tasks.generate_ipynb_from_py(TEMPLATE_BASE_DIR, report_name)
    nb = pm.read_notebook(path)
    metadata = [cell for cell in nb.node.cells if 'parameters' in cell.get('metadata', {}).get('tags', [])]
    parameters_as_html = ''
    if metadata:
        parameters_as_html = metadata[0]['source'].strip()

    return render_template('run_report.html', parameters_as_html=parameters_as_html, report_name=report_name)


def _get_job_result_future(job_id):
    if job_id not in running_jobs:
        return None
    job_result = running_jobs[job_id]
    return job_result


def _run_report(report_name, overrides):
    job_id = str(uuid.uuid4())
    job_start_time = datetime.datetime.now()
    result_serializer.save_check_stub(job_id, report_name,
                                      input_json=overrides, job_start_time=job_start_time,
                                      status=JobStatus.SUBMITTED)
    logger.info('Calculating a new {} ipynb with parameters: {}'.format(report_name, overrides))
    try:
        spark_future = spark_pool.submit(functools.partial(tasks.run_checks,
                                                           job_id,
                                                           job_start_time,
                                                           report_name,
                                                           OUTPUT_BASE_DIR,
                                                           TEMPLATE_BASE_DIR,
                                                           MONGO_HOST,
                                                           MONGO_LIBRARY,
                                                           overrides))
    except Exception as e:
        import traceback
        error_info = traceback.format_exc()
        notebook_result = NotebookResultError(job_id=job_id,
                                              job_start_time=job_start_time,
                                              input_json=overrides,
                                              report_name=report_name,
                                              error_info=error_info)
        logger.info('Saving error result to mongo library {}@{}...'.format(MONGO_LIBRARY, MONGO_HOST))
        result_serializer.save_check_result(notebook_result)
        logger.info('Error result saved to mongo successfully.')
        return job_id
    global running_jobs
    running_jobs[job_id] = spark_future
    return job_id


class NotebookRunError(Exception):
    pass


@flask_app.route('/run_checks/<report_name>', methods=['POST', 'PUT'])
def run_checks_http(report_name):
    overrides = request.values.get('overrides')
    override_dict = {}
    issues = []
    for s in overrides.split('\n'):
        s = s.strip()
        match = re.match('(?P<variable_name>[a-zA-Z_]+) *= *(?P<value>.+)', s)
        if match:
            try:
                # This is dirty but we trust our users...
                override_dict[match.group('variable_name')] = eval(match.group('value'))
            except Exception as e:
                issues.append('Failed to parse input: {}: {}'.format(s, str(e)))
    if issues:
        return jsonify({'status': 'Failed', 'content':('\n'.join(issues))})
    job_id = _run_report(report_name, override_dict)
    return jsonify({'id': job_id}), 202, {'Location': url_for('task_status', report_name=report_name, task_id=job_id)}


# ------------------- Serving results -------------------- #


def _get_job_results(job_id, report_name):
    global complete_jobs

    if report_name in complete_jobs and job_id in complete_jobs[report_name]:
        notebook_result = complete_jobs.get(report_name, {}).get(job_id)
    else:
        notebook_result = result_serializer.get_check_result(job_id)
        if notebook_result:
            report_name = notebook_result.report_name
            if report_name not in complete_jobs:
                complete_jobs[report_name] = {}
            complete_jobs[report_name][job_id] = notebook_result

    if notebook_result is None:
        err_info = 'Job results not found for report name={} / job id={}. ' \
                 'Did you use an invalid job ID?'.format(report_name, job_id)
        return results.NotebookResultError(job_id, error_info=err_info, report_name=report_name,
                                           job_start_time=datetime.datetime.now())
    return notebook_result


def all_available_results():
    complete = copy.deepcopy(complete_jobs)
    for report_name, reports in complete.items():
        for job_id, result in reports.items():
            reports[job_id].result_url = url_for('task_results', task_id=job_id, report_name=report_name)
            reports[job_id].ipynb_url = url_for('download_ipynb_result', task_id=job_id, report_name=report_name)
    return complete


@flask_app.route('/results/<report_name>/<task_id>')
def task_results(task_id, report_name):
    result = _get_job_results(task_id, report_name)
    return render_template('results.html',
                           task_id=task_id,
                           report_name=report_name,
                           result=result,
                           donevalue=JobStatus.DONE.value,
                           html_render=url_for('task_results_html', report_name=report_name, task_id=task_id),
                           ipynb_url=url_for('download_ipynb_result', report_name=report_name, task_id=task_id))


@flask_app.route('/result_html_render/<report_name>/<task_id>')
def task_results_html(task_id, report_name):
    result = _get_job_results(task_id, report_name)
    if isinstance(result, results.NotebookResultError):
        return '<p>This job resulted in an error: <br/><code>{}</code></p>'.format(result.error_info)
    if isinstance(result, results.NotebookResultPending):
        return task_loading(report_name, task_id)
    return result.raw_html


@flask_app.route('/result_html_render/<report_name>/<task_id>/resources/<path:resource>')
def task_result_resources_html(task_id, resource, report_name):
    result = _get_job_results(task_id, report_name)
    html_resources = result.raw_html_resources
    resource_path = os.path.join(task_id, 'resources', resource)
    if resource_path in html_resources.get('outputs', {}):
        return html_resources['outputs'][resource_path]
    else:
        return abort(404)


@flask_app.route('/result_download_ipynb/<report_name>/<task_id>')
def download_ipynb_result(task_id, report_name):
    result = _get_job_results(task_id, report_name)
    if isinstance(result, results.NotebookResultComplete):
        return Response(result.raw_ipynb_json,
                        mimetype="application/vnd.jupyter",
                        headers={"Content-Disposition": "attachment;filename={}.ipynb".format(task_id)})
    else:
        abort(404)


# ---------------- Loading -------------------- #


def task_loading(report_name, task_id):
    return render_template('loading.html', task_id=task_id, location=url_for('task_status',
                                                                             report_name=report_name,
                                                                             task_id=task_id))


def _get_job_status(task_id, report_name):
    job_result = _get_job_results(task_id, report_name)
    if job_result is None:
        return {'status': 'Job not found. Did you use an old job ID?'}
    if job_result.status == JobStatus.DONE.value:
        response = {'status': job_result.status,
                    'results_url': url_for('task_results', report_name=report_name, task_id=task_id)}
    elif job_result.status == JobStatus.ERROR.value:
        response = {'status': job_result.status,
                    'results_url': url_for('task_results', report_name=report_name, task_id=task_id)}
    else:
        response = {'status': job_result.status}
    return response


@flask_app.route('/status/<report_name>/<task_id>')
def task_status(report_name, task_id):
    return jsonify(_get_job_status(task_id, report_name))


# ----------------- Flask admin ---------------- #

def _cleanup_on_exit():
    global alive
    alive = False
    logger.info('Cleaning up jobs')
    for job_id, job_future in running_jobs.items():
        if job_future.running():
            logger.info('Cancelling job {}'.format(job_id))
            cancel_result = job_future.cancel()
            logger.info('Job cancel: {}'.format('SUCCESS' if cancel_result else 'FAILED'))
            result_serializer.update_check_status(job_id, 'CANCELLED')
    logger.info('Job cleanup done. Stopping spark pool.')
    if spark_pool:
        global spark_pool
        spark_pool.shutdown()
    shutil.rmtree(OUTPUT_BASE_DIR)
    shutil.rmtree(TEMPLATE_BASE_DIR)


def _report_hunter():
    serializer = results.NotebookResultSerializer()
    global alive, complete_jobs
    while alive:
        all_existing_results = defaultdict(dict)
        for result in serializer.get_all_results():
            all_existing_results[result.report_name][result.job_id] = result
        if complete_jobs != all_existing_results:
            logger.info('Report-hunter found a status change')
            complete_jobs = all_existing_results
        time.sleep(10)
    logger.info('Report-hunting job successfully killed.')


def start_app():
    global spark_pool, result_serializer
    logger.info('Creating {}'.format(OUTPUT_BASE_DIR))
    os.makedirs(OUTPUT_BASE_DIR)
    logger.info('Creating {}'.format(TEMPLATE_BASE_DIR))
    os.makedirs(TEMPLATE_BASE_DIR)
    result_serializer = results.NotebookResultSerializer()
    spark_pool = hpc_pool('SPARK', local_thread_count=8)
    port = int(os.getenv('OCN_PORT', 11828))
    atexit.register(_cleanup_on_exit)
    all_report_refresher = threading.Thread(target=_report_hunter)
    all_report_refresher.daemon = True
    all_report_refresher.start()
    flask_app.run('0.0.0.0', port, threaded=True, debug=True)


if __name__ == '__main__':
    start_app()
