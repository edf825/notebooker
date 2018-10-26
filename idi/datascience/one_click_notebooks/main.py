import atexit
import copy
import datetime
import functools
import glob
import os
import traceback
import uuid
from ahl.concurrent.futures import hpc_pool
from concurrent.futures import Future
from flask import Flask, render_template, request, jsonify, url_for, send_from_directory, abort, Response
from ahl.logging import get_logger

from idi.datascience.one_click_notebooks import tasks, results

flask_app = Flask(__name__)
spark_pool = None
running_jobs = {}  # Maps jobid --> Future
complete_jobs = {}  # Maps report_name --> jobid --> results.NotebookResultBase
OUTPUT_BASE_DIR = os.getenv('OUTPUT_DIR', os.path.join(os.path.dirname(os.path.realpath(__file__)), 'results'))
MONGO_HOST = 'research'
MONGO_LIBRARY = 'NOTEBOOK_OUTPUT'
result_serializer = None
logger = get_logger(__name__)


# ----------------- Main page -------------------- #

@flask_app.route('/', methods=['GET'])
def index():
    return render_template('index.html', all_jobs=all_available_results(), running_jobs=get_all_running_jobs())


@flask_app.route('/run_report', methods=['GET'])
def run_report():
    return render_template('run_report.html')


# ------------------ Running checks ---------------- #

def _get_job_result_future(job_id):
    if job_id not in running_jobs:
        return None
    job_result = running_jobs[job_id]
    return job_result


def _run_report(report_name, *args):
    job_id = str(uuid.uuid4())
    logger.info('Calculating a new {} ipynb'.format(report_name))
    spark_future = spark_pool.submit(functools.partial(tasks.run_checks,
                                                       job_id,
                                                       report_name,
                                                       OUTPUT_BASE_DIR,
                                                       MONGO_HOST,
                                                       MONGO_LIBRARY,
                                                       *args))
    global running_jobs
    running_jobs[job_id] = spark_future
    return job_id


@flask_app.route('/run_checks', methods=['POST', 'PUT'])
def run_checks_http():
    input_json = request.get_json()
    job_id = _run_report('watchdog_checks', input_json)
    return jsonify({'id': job_id}), 202, {'Location': url_for('task_status', task_id=job_id)}


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
        return results.NotebookResultError(job_id, error_info=err_info, report_name=report_name)
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
    return render_template('results.html',
                           task_id=task_id,
                           report_name=report_name,
                           html_render=url_for('task_results_html', report_name=report_name, task_id=task_id),
                           ipynb_url=url_for('download_ipynb_result', report_name=report_name, task_id=task_id))


@flask_app.route('/result_html_render/<report_name>/<task_id>')
def task_results_html(task_id, report_name):
    result = _get_job_results(task_id, report_name)
    if isinstance(result, results.NotebookResultError):
        abort(404)
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
    return Response(result.raw_ipynb_json,
                    mimetype="application/vnd.jupyter",
                    headers={"Content-Disposition": "attachment;filename={}.ipynb".format(task_id)})


# ---------------- Loading -------------------- #


def get_all_running_jobs():
    jobs = []
    for job_id, future in running_jobs.items():
        status = _get_job_status(job_id)
        jobs.append({'status': status.get('status'), 'tracker_url': url_for('task_loading', task_id=job_id)})
    return jobs


@flask_app.route('/task_loading/<task_id>')
def task_loading(task_id):
    return render_template('loading.html', task_id=task_id, location=url_for('task_status', task_id=task_id))


def _get_job_status(task_id):
    job_future = _get_job_result_future(task_id)
    if job_future is None:
        return {'status': 'Job not found. Did you use an old job ID?'}
    if job_future.done():
        try:
            notebook_result = job_future.result()
            report_name = notebook_result.report_name
            # Now save the job to "complete_jobs"
            global complete_jobs
            if report_name not in complete_jobs:
                complete_jobs[report_name] = {}
            complete_jobs[report_name][task_id] = notebook_result
            response = {'status': 'Done!',
                        'results_url': url_for('task_results', report_name=report_name, task_id=task_id)}

        except KeyboardInterrupt:
            raise
        except Exception as e:
            response = {'status': 'Error: {}'.format(str(e)),
                        'exception_info': traceback.format_exc()}
            return response
    elif job_future.running():
        response = {'status': 'Running...'}
    else:
        response = {'status': 'Submitted for execution...'}
    return response


@flask_app.route('/status/<task_id>')
def task_status(task_id):
    return jsonify(_get_job_status(task_id))


# ----------------- Flask admin ---------------- #

def _cleanup_on_exit():
    logger.info('Cleaning up jobs')
    for job_id, job_future in running_jobs.items():
        if job_future.running():
            logger.info('Cancelling job {}'.format(job_id))
            cancel_result = job_future.cancel()
            logger.info('Job cancel: {}'.format('SUCCESS' if cancel_result else 'FAILED'))
            result_serializer.update_check_status(job_id, 'CANCELLED')
    logger.info('Job cleanup done. Stopping spark pool.')
    global spark_pool
    spark_pool.shutdown()


def _find_completed_jobs():
    all_existing_results = {}
    # TODO: Read from Mongo
    # for path, dirs, files in os.walk(OUTPUT_BASE_DIR):
    #     If there is an HTML render, the check has finished
        # if any(fname.endswith('.html') for fname in files):
        #     The basename of the path is the check uuid
            # results_dir, report_name, job_id = path.rsplit(os.path.sep, 2)
            # check_result = tasks._output_status(job_id, report_name, results_dir)
            # if report_name not in all_existing_results:
            #     all_existing_results[report_name] = {}
            # all_existing_results[report_name][job_id] = check_result
    return all_existing_results


def start_app():
    global spark_pool, complete_jobs, result_serializer
    result_serializer = results.NotebookResultSerializer()
    complete_jobs = _find_completed_jobs()
    spark_pool = hpc_pool('SPARK', local_thread_count=8)
    port = int(os.getenv('OCN_PORT', 11828))
    atexit.register(_cleanup_on_exit)
    flask_app.run('0.0.0.0', port, threaded=True, debug=False)


if __name__ == '__main__':
    start_app()
