import atexit
import datetime
import json
import sys
import time

import nbformat
import os
import shutil
import threading
import traceback
import uuid

import click
from ahl.logging import get_logger
from flask import Flask, render_template, request, jsonify, url_for, abort, Response

import man.notebooker.utils.notebook_execution
from man.notebooker import execute_notebook, results
from man.notebooker.caching import set_cache, get_cache
from man.notebooker.constants import OUTPUT_BASE_DIR, \
    TEMPLATE_BASE_DIR, JobStatus, CANCEL_MESSAGE
from man.notebooker.handle_overrides import handle_overrides
from man.notebooker.report_hunter import _report_hunter
from man.notebooker.results import _get_job_results, all_available_results, _pdf_filename, get_all_result_keys
from man.notebooker.utils.templates import get_all_possible_checks, _get_metadata_cell_idx, _get_preview
from man.notebooker.utils.web import validate_mailto, validate_title

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


@flask_app.route('/delete_report/<job_id>', methods=['POST'])
def delete_report(job_id):
    try:
        result_serializer.delete_result(job_id)
        get_all_result_keys(result_serializer, limit=50, force_reload=True)
        result = {'status': 'ok'}
    except:
        error_info = traceback.format_exc()
        result = {'status': 'error', 'error': error_info}
    return jsonify(result)


# ------------------ Running checks ---------------- #


@flask_app.route('/run_report/get_preview/<report_name>', methods=['GET'])
def run_report_get_preview(report_name):
    # Handle the case where a rendered ipynb asks for "custom.css"
    if '.css' in report_name:
        return ''
    return _get_preview(report_name)


@flask_app.route('/run_report/<report_name>', methods=['GET'])
def run_report_http(report_name):
    path = man.notebooker.utils.notebook_execution.generate_ipynb_from_py(TEMPLATE_BASE_DIR, report_name)
    nb = nbformat.read(path, as_version=nbformat.v4.nbformat)
    metadata_idx = _get_metadata_cell_idx(nb)
    parameters_as_html = ''
    has_prefix = has_suffix = False
    if metadata_idx is not None:
        metadata = nb['cells'][metadata_idx]
        parameters_as_html = metadata['source'].strip()
        has_prefix, has_suffix = bool(nb['cells'][:metadata_idx]), bool(nb['cells'][metadata_idx+1:])

    return render_template('run_report.html',
                           parameters_as_html=parameters_as_html,
                           has_prefix=has_prefix,
                           has_suffix=has_suffix,
                           report_name=report_name,
                           all_reports=get_all_possible_checks())


def _monitor_stderr(process, job_id):
    stderr = []
    while True:
        line = process.stderr.readline()
        stderr.append(line)
        logger.info(line)  # So that we have it in the log, not just in memory.
        set_cache(('run_output', job_id), ''.join(stderr))
        if line == '' and process.poll() is not None:
            break
    return ''.join(stderr)


def run_report(report_name, report_title, mailto, overrides):
    job_id = str(uuid.uuid4())
    job_start_time = datetime.datetime.now()
    result_serializer.save_check_stub(job_id, report_name,
                                      report_title=report_title,
                                      job_start_time=job_start_time,
                                      status=JobStatus.SUBMITTED)
    import subprocess
    p = subprocess.Popen([sys.executable,
                          '-m', execute_notebook.__name__,
                          '--job-id', job_id,
                          '--output-base-dir', OUTPUT_BASE_DIR,
                          '--template-base-dir', TEMPLATE_BASE_DIR,
                          '--report-name', report_name,
                          '--report-title', report_title,
                          '--mailto', mailto,
                          '--overrides-as-json', json.dumps(overrides),
                          '--mongo-db-name', result_serializer.database_name,
                          '--mongo-host', result_serializer.mongo_host,
                          '--result-collection-name', result_serializer.result_collection_name,
                          ], stderr=subprocess.PIPE)
    stderr_thread = threading.Thread(target=_monitor_stderr, args=(p, job_id, ))
    stderr_thread.daemon = True
    stderr_thread.start()
    return job_id


@flask_app.route('/run_checks/<report_name>', methods=['POST', 'PUT'])
def run_checks_http(report_name):
    issues = []
    # Get and process override script
    override_dict = handle_overrides(request.values.get('overrides'), issues)
    # Find and cleanse the title of the report
    report_title = validate_title(request.values.get('report_title'), issues)
    # Get mailto email address
    mailto = validate_mailto(request.values.get('mailto'), issues)
    if issues:
        return jsonify({'status': 'Failed', 'content': ('\n'.join(issues))})
    job_id = run_report(report_name, report_title, mailto, override_dict)
    return (jsonify({'id': job_id}),
            202,  # HTTP Accepted code
            {'Location': url_for('task_status', report_name=report_name, task_id=job_id)})


# ------------------- Serving results -------------------- #


@flask_app.route('/results/<report_name>/<task_id>')
def task_results(task_id, report_name):
    result = _get_job_results(task_id, report_name, result_serializer, ignore_cache=True)
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
    # In this method, we either:
    # - present the HTML results, if the job has finished
    # - present the error, if the job has failed
    # - present the user with some info detailing the progress of the job, if it is still running.
    result = _get_job_results(task_id, report_name, result_serializer)
    if isinstance(result, results.NotebookResultError):
        return result.raw_html
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
    # Loaded once, when the user queries /results/<report_name>/<job_id> and it is pending.
    return render_template('loading.html',
                           task_id=task_id,
                           location=url_for('task_status', report_name=report_name, task_id=task_id),
                           )


def _get_job_status(task_id, report_name):
    # Continuously polled for updates by the user client, until the notebook has completed execution (or errored).
    job_result = _get_job_results(task_id, report_name, result_serializer, ignore_cache=True)
    output = get_cache(('run_output', task_id)) or ''
    if job_result is None:
        return {'status': 'Job not found. Did you use an old job ID?'}
    if job_result.status in (JobStatus.DONE, JobStatus.ERROR, JobStatus.TIMEOUT, JobStatus.CANCELLED):
        response = {'status': job_result.status.value,
                    'results_url': url_for('task_results', report_name=report_name, task_id=task_id)}
    else:
        response = {'status': job_result.status.value, 'run_output': output}
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


def start_app(mongo_host, database_name, result_collection_name, debug, port):
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


@click.command()
@click.option('--mongo-host', default='research')
@click.option('--database-name', default='mongoose_restech')
@click.option('--result-collection-name', default='NOTEBOOK_OUTPUT')
@click.option('--debug/--no-debug', default=False)
@click.option('--port', default=int(os.getenv('OCN_PORT', 11828)))
def main(mongo_host, database_name, result_collection_name, debug, port):
    host = '0.0.0.0'
    start_app(mongo_host, database_name, result_collection_name, debug, port)
    flask_app.run(host=host, port=port, threaded=True, debug=debug)


if __name__ == '__main__':
    main()
