import atexit
import datetime
import functools

import click
import os
import papermill as pm
import re
import shutil
import threading
import uuid
from ahl.concurrent.futures import hpc_pool
from ahl.logging import get_logger
from flask import Flask, render_template, request, jsonify, url_for, abort, Response
from typing import Dict, Any, Tuple, List

from man.notebooker import tasks, results
from man.notebooker.caching import set_cache
from man.notebooker.constants import OUTPUT_BASE_DIR, \
    TEMPLATE_BASE_DIR, MONGO_HOST, MONGO_LIBRARY, JobStatus
from man.notebooker.handle_overrides import _handle_overrides
from man.notebooker.report_hunter import _report_hunter
from man.notebooker.results import NotebookResultError, _get_job_results, all_available_results
from man.notebooker.utils import get_all_possible_checks

flask_app = Flask(__name__)
logger = get_logger(__name__)
result_serializer = None  # type: results.NotebookResultSerializer
spark_pool = None
all_report_refresher = None


# ----------------- Main page -------------------- #

@flask_app.route('/', methods=['GET'])
def index():
    return render_template('index.html',
                           all_jobs=all_available_results(result_serializer),
                           all_reports=get_all_possible_checks())


# ------------------ Running checks ---------------- #


@flask_app.route('/run_report/<report_name>', methods=['GET'])
def run_report(report_name):
    path = tasks.generate_ipynb_from_py(TEMPLATE_BASE_DIR, report_name)
    nb = pm.read_notebook(path)
    metadata = [cell for cell in nb.node.cells if 'parameters' in cell.get('metadata', {}).get('tags', [])]
    parameters_as_html = ''
    if metadata:
        parameters_as_html = metadata[0]['source'].strip()

    return render_template('run_report.html',
                           parameters_as_html=parameters_as_html,
                           report_name=report_name,
                           all_reports=get_all_possible_checks())


def _run_report(report_name, overrides):
    job_id = str(uuid.uuid4())
    job_start_time = datetime.datetime.now()
    result_serializer.save_check_stub(job_id, report_name,
                                      job_start_time=job_start_time,
                                      status=JobStatus.SUBMITTED)
    logger.info('Calculating a new {} ipynb with parameters: {}'.format(report_name, overrides))
    try:
        spark_pool.submit(functools.partial(tasks.run_checks,
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
                                              report_name=report_name,
                                              error_info=error_info)
        logger.info('Saving error result to mongo library {}@{}...'.format(MONGO_LIBRARY, MONGO_HOST))
        result_serializer.save_check_result(notebook_result)
        logger.info('Error result saved to mongo successfully.')
        return job_id
    logger.info('Successfully submitted job to spark.')
    return job_id


@flask_app.route('/run_checks/<report_name>', methods=['POST', 'PUT'])
def run_checks_http(report_name):
    overrides = request.values.get('overrides')
    override_dict, issues = _handle_overrides(overrides)
    if issues:
        return jsonify({'status': 'Failed', 'content':('\n'.join(issues))})
    job_id = _run_report(report_name, override_dict)
    return (jsonify({'id': job_id}),
            202,  # HTTP Accepted code
            {'Location': url_for('task_status', report_name=report_name, task_id=job_id)})


# ------------------- Serving results -------------------- #


@flask_app.route('/results/<report_name>/<task_id>')
def task_results(task_id, report_name):
    result = _get_job_results(task_id, report_name, result_serializer)
    return render_template('results.html',
                           task_id=task_id,
                           report_name=report_name,
                           result=result,
                           donevalue=JobStatus.DONE,
                           html_render=url_for('task_results_html', report_name=report_name, task_id=task_id),
                           ipynb_url=url_for('download_ipynb_result', report_name=report_name, task_id=task_id),
                           all_reports=get_all_possible_checks())


@flask_app.route('/result_html_render/<report_name>/<task_id>')
def task_results_html(task_id, report_name):
    result = _get_job_results(task_id, report_name, result_serializer)
    if isinstance(result, results.NotebookResultError):
        return '<p>This job resulted in an error: <br/><code style="white-space: pre-wrap;">{}</code></p>'.format(result.error_info)
    if isinstance(result, results.NotebookResultPending):
        return task_loading(report_name, task_id)
    return result.raw_html


@flask_app.route('/result_html_render/<report_name>/<task_id>/resources/<path:resource>')
def task_result_resources_html(task_id, resource, report_name):
    result = _get_job_results(task_id, report_name, result_serializer)
    if isinstance(result, results.NotebookResultComplete):
        html_resources = result.raw_html_resources
        resource_path = os.path.join(task_id, 'resources', resource)
        if resource_path in html_resources.get('outputs', {}):
            return html_resources['outputs'][resource_path]
    return abort(404)


@flask_app.route('/result_download_ipynb/<report_name>/<task_id>')
def download_ipynb_result(task_id, report_name):
    result = _get_job_results(task_id, report_name, result_serializer)
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
    job_result = _get_job_results(task_id, report_name, result_serializer)
    if job_result is None:
        return {'status': 'Job not found. Did you use an old job ID?'}
    if job_result.status == JobStatus.DONE:
        response = {'status': job_result.status.value,
                    'results_url': url_for('task_results', report_name=report_name, task_id=task_id)}
    elif job_result.status == JobStatus.ERROR:
        response = {'status': job_result.status.value,
                    'results_url': url_for('task_results', report_name=report_name, task_id=task_id)}
    else:
        response = {'status': job_result.status.value}
    return response


@flask_app.route('/status/<report_name>/<task_id>')
def task_status(report_name, task_id):
    return jsonify(_get_job_status(task_id, report_name))


# ----------------- Flask admin ---------------- #

def _cleanup_on_exit():
    global spark_pool, all_report_refresher
    set_cache('_STILL_ALIVE', False)
    logger.info('Stopping spark pool.')
    if spark_pool:
        spark_pool.shutdown()
    shutil.rmtree(OUTPUT_BASE_DIR)
    shutil.rmtree(TEMPLATE_BASE_DIR)
    if all_report_refresher:
        # Wait until it terminates.
        logger.info('Stopping "report hunter" thread.')
        all_report_refresher.join()


@click.command()
@click.option('--mongo-host', default='research')
@click.option('--database-name', default='mongoose_restech')
@click.option('--result-collection-name', default='NOTEBOOK_OUTPUT')
@click.option('--debug/--no-debug', default=False)
@click.option('--port', default=int(os.getenv('OCN_PORT', 11828)))
def start_app(mongo_host, database_name, result_collection_name, debug, port):
    logger.info('Running man.notebooker with params: '
                'mongo-host=%s, database-name=%s, '
                'result-collection-name=%s, debug=%s, '
                'port=%s', mongo_host, database_name, result_collection_name, debug, port)
    set_cache('_STILL_ALIVE', True)
    global spark_pool, result_serializer, all_report_refresher
    logger.info('Creating {}'.format(OUTPUT_BASE_DIR))
    os.makedirs(OUTPUT_BASE_DIR)
    logger.info('Creating {}'.format(TEMPLATE_BASE_DIR))
    os.makedirs(TEMPLATE_BASE_DIR)
    result_serializer = results.NotebookResultSerializer(mongo_host=mongo_host,
                                                         database_name=database_name,
                                                         result_collection_name=result_collection_name)
    spark_pool = hpc_pool('SPARK', local_thread_count=8)
    atexit.register(_cleanup_on_exit)
    all_report_refresher = threading.Thread(target=_report_hunter, args=(mongo_host, database_name, result_collection_name))
    all_report_refresher.daemon = True
    all_report_refresher.start()
    flask_app.run('0.0.0.0', port, threaded=True, debug=debug)


if __name__ == '__main__':
    start_app()
