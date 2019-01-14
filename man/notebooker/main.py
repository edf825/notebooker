import atexit
import datetime
import functools
import json
import sys
import time
import os
import shutil
import threading
import traceback
import uuid

import click
import papermill as pm
import requests
from ahl.logging import get_logger
from flask import Flask, render_template, request, jsonify, url_for, abort, Response
from typing import Dict, Any, Tuple, List

from man.notebooker import execute_notebook, results
from man.notebooker.caching import set_cache, get_cache
from man.notebooker.constants import OUTPUT_BASE_DIR, \
    TEMPLATE_BASE_DIR, JobStatus, CANCEL_MESSAGE
from man.notebooker.handle_overrides import handle_overrides
from man.notebooker.report_hunter import _report_hunter
from man.notebooker.results import _get_job_results, all_available_results, _pdf_filename
from man.notebooker.execute_notebook import run_report_worker
from man.notebooker.utils import get_all_possible_checks

flask_app = Flask(__name__)
logger = get_logger(__name__)
result_serializer = None  # type: results.NotebookResultSerializer
all_report_refresher = None  # type: threading.Thread


# ----------------- Main page -------------------- #

@flask_app.route('/', methods=['GET'])
def index():
    limit = int(request.args.get('limit', 50))
    return render_template('index.html',
                           all_jobs=all_available_results(result_serializer, limit),
                           all_reports=get_all_possible_checks(),
                           n_results_available=result_serializer.n_all_results(),
                           donevalue=JobStatus.DONE,  # needed so we can check if a result is available  # needed so we can check if a result is available
                           )


# ------------------ Running checks ---------------- #

@flask_app.route('/run_report/<report_name>', methods=['GET'])
def run_report_http(report_name):
    path = execute_notebook.generate_ipynb_from_py(TEMPLATE_BASE_DIR, report_name)
    nb = pm.read_notebook(path)
    metadata = [cell for cell in nb.node.cells if 'parameters' in cell.get('metadata', {}).get('tags', [])]
    parameters_as_html = ''
    if metadata:
        parameters_as_html = metadata[0]['source'].strip()

    return render_template('run_report.html',
                           parameters_as_html=parameters_as_html,
                           report_name=report_name,
                           all_reports=get_all_possible_checks())


def run_report(report_name, overrides):
    job_id = str(uuid.uuid4())
    job_start_time = datetime.datetime.now()
    result_serializer.save_check_stub(job_id, report_name,
                                      job_start_time=job_start_time,
                                      status=JobStatus.SUBMITTED)
    import subprocess
    subprocess.Popen([sys.executable,
                      '-m', 'man.notebooker.execute_notebook',
                      '--job-id', job_id,
                      '--output-base-dir', OUTPUT_BASE_DIR,
                      '--template-base-dir', TEMPLATE_BASE_DIR,
                      '--report-name', report_name,
                      '--overrides-as-json', json.dumps(overrides),
                      '--mongo-db-name', result_serializer.database_name,
                      '--mongo-host', result_serializer.mongo_host,
                      '--result-collection-name', result_serializer.result_collection_name,
                      ])
    return job_id


@flask_app.route('/run_checks/<report_name>', methods=['POST', 'PUT'])
def run_checks_http(report_name):
    overrides = request.values.get('overrides')
    override_dict, issues = handle_overrides(overrides)
    if issues:
        return jsonify({'status': 'Failed', 'content': ('\n'.join(issues))})
    job_id = run_report(report_name, override_dict)
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
                           donevalue=JobStatus.DONE,  # needed so we can check if a result is available
                           html_render=url_for('task_results_html', report_name=report_name, task_id=task_id),
                           ipynb_url=url_for('download_ipynb_result', report_name=report_name, task_id=task_id),
                           pdf_url=url_for('download_pdf_result', report_name=report_name, task_id=task_id),
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


@flask_app.route('/result_download_pdf/<report_name>/<task_id>')
def download_pdf_result(task_id, report_name):
    result = _get_job_results(task_id, report_name, result_serializer)
    if isinstance(result, results.NotebookResultComplete):
        return Response(result.pdf,
                        mimetype="application/pdf",
                        headers={"Content-Disposition": "attachment;filename={}".format(_pdf_filename(task_id))})
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
    if job_result.status in (JobStatus.DONE, JobStatus.ERROR, JobStatus.TIMEOUT, JobStatus.CANCELLED):
        response = {'status': job_result.status.value,
                    'results_url': url_for('task_results', report_name=report_name, task_id=task_id)}
    else:
        response = {'status': job_result.status.value}
    return response


@flask_app.route('/status/<report_name>/<task_id>')
def task_status(report_name, task_id):
    return jsonify(_get_job_status(task_id, report_name))


# ----------------- Flask admin ---------------- #

def _cancel_all_jobs():
    all_pending = result_serializer.get_all_results(mongo_filter={'status': {'$in': [JobStatus.SUBMITTED.value,
                                                                                     JobStatus.PENDING.value]}})
    for result in all_pending:
        result_serializer.update_check_status(result.job_id, JobStatus.CANCELLED, error_info=CANCEL_MESSAGE)


@atexit.register
def _cleanup_on_exit():
    global all_report_refresher, result_serializer
    set_cache('_STILL_ALIVE', False)
    _cancel_all_jobs()
    shutil.rmtree(OUTPUT_BASE_DIR)
    shutil.rmtree(TEMPLATE_BASE_DIR)
    if all_report_refresher:
        # Wait until it terminates.
        logger.info('Stopping "report hunter" thread.')
        all_report_refresher.join()
    # Allow all clients looking for task results to get the bad news...
    time.sleep(2)


@flask_app.before_first_request
def start_app(mongo_host='research', database_name='mongoose_restech', result_collection_name='NOTEBOOK_OUTPUT',
              debug=False, port=8000):
    # TODO: Get the above from env
    # import gunicorn, logging
    # gunicorn_logger = logging.getLogger('gunicorn.error')
    # flask_app.logger.handlers = gunicorn_logger.handlers
    # logger.handlers = gunicorn_logger.handlers
    logger.info('Running man.notebooker with params: '
                'mongo-host=%s, database-name=%s, '
                'result-collection-name=%s, debug=%s, '
                'port=%s', mongo_host, database_name, result_collection_name, debug, port)
    set_cache('_STILL_ALIVE', True)
    global result_serializer, all_report_refresher
    logger.info('Creating %s', OUTPUT_BASE_DIR)
    os.makedirs(OUTPUT_BASE_DIR)
    logger.info('Creating %s', TEMPLATE_BASE_DIR)
    os.makedirs(TEMPLATE_BASE_DIR)
    result_serializer = results.NotebookResultSerializer(mongo_host=mongo_host,
                                                         database_name=database_name,
                                                         result_collection_name=result_collection_name)
    all_report_refresher = threading.Thread(target=_report_hunter, args=(mongo_host, database_name, result_collection_name))
    all_report_refresher.daemon = True
    all_report_refresher.start()


def _kick_off(host, port):
    def start_loop():
        not_started = True
        while not_started:
            try:
                url = 'http://{}:{}/'.format(host, port)
                r = requests.get(url)
                logger.info('Url %s gave status %s', url, r.status_code)
                if r.status_code == 200:
                    logger.info('Server started, quitting start_loop')
                    not_started = False
            except:
                logger.info('Server not yet started')
            time.sleep(1)
    thread = threading.Thread(target=start_loop)
    thread.daemon = True
    thread.start()


def main():
    host = '0.0.0.0'
    port = 8000
    _kick_off(host, port)
    flask_app.run(host=host, port=port, threaded=True, debug=True)


if __name__ == '__main__':
    main()
