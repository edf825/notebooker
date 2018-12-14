import atexit
import copy
import datetime
import functools
import pkgutil

import os
import papermill as pm
import re
import retrying
import shutil
import threading
import time
import tempfile
import uuid
from ahl.concurrent.futures import hpc_pool
from flask import Flask, render_template, request, jsonify, url_for, abort, Response
from ahl.logging import get_logger
from typing import List, Tuple
from urllib3.connection import ConnectionError
from werkzeug.contrib.cache import MemcachedCache, SimpleCache

from idi.datascience.one_click_notebooks import tasks, results
from idi.datascience.one_click_notebooks.results import JobStatus, NotebookResultError
from idi.datascience.one_click_notebooks.utils import _cache_key, cache_key_to_dict

SUBMISSION_TIMEOUT = 3
RUNNING_TIMEOUT = 60
flask_app = Flask(__name__)
spark_pool = None
# cache = MemcachedCache(['127.0.0.1:11211'])
cache = SimpleCache()
cache_expiries = {}
# TODO: Figure out how we can get the below to work without expanduser, which is required by Spark.
OUTPUT_BASE_DIR = os.path.join(os.getenv('OUTPUT_DIR', tempfile.mkdtemp(dir=os.path.expanduser('~'))), 'results')
TEMPLATE_BASE_DIR = os.path.join(os.getenv('OUTPUT_DIR', tempfile.mkdtemp(dir=os.path.expanduser('~'))), 'templates')
MONGO_HOST = 'research'
MONGO_LIBRARY = 'NOTEBOOK_OUTPUT'
TEMPLATE_MODULE_NAME = 'notebook_templates'
result_serializer = None  # type: results.NotebookResultSerializer
alive = True
logger = get_logger(__name__)


@retrying.retry(stop_max_attempt_number=3)
def _get_cache(key):
    global cache
    # if cache_expiries.get(key) and datetime.datetime.now() > cache_expiries.get(key):
    #     return None
    return cache.get(key)


def get_cache(report_name, job_id):
    return _get_cache(_cache_key(report_name, job_id))


@retrying.retry(stop_max_attempt_number=3)
def _set_cache(key, value, timeout=0):
    global cache
    # if timeout_seconds:
    #     cache_expiries[key] = datetime.datetime.now() + datetime.timedelta(seconds=timeout_seconds)
    # cache[key] = value
    cache.set(key, value, timeout=timeout)


def set_cache(report_name, job_id, value):
    if value:
        return _set_cache(_cache_key(report_name, job_id), value)


# ----------------- Main page -------------------- #

@flask_app.route('/', methods=['GET'])
def index():
    return render_template('index.html',
                           all_jobs=all_available_results(),
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
    logger.info('Successfully submitted job to spark.')
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


def get_all_possible_checks():
    return list({x.rsplit('.', 1)[1]
                 for (_, x, _)
                 in pkgutil.walk_packages('idi.datascience')
                 if TEMPLATE_MODULE_NAME in x
                 and not x.endswith(TEMPLATE_MODULE_NAME)})


# ------------------- Serving results -------------------- #


def _get_job_results(job_id, report_name, retrying=False):
    current_result = get_cache(report_name, job_id)
    if current_result:
        notebook_result = current_result
    else:
        notebook_result = result_serializer.get_check_result(job_id)
        set_cache(report_name, job_id, notebook_result)

    if not notebook_result:
        err_info = 'Job results not found for report name={} / job id={}. ' \
                 'Did you use an invalid job ID?'.format(report_name, job_id)
        return results.NotebookResultError(job_id, error_info=err_info, report_name=report_name,
                                           job_start_time=datetime.datetime.now())
    if isinstance(notebook_result, str):
        if not retrying:
            return _get_job_results(job_id, report_name, retrying=True)
        raise NotebookRunError('An unexpected string was found as a result. Please run your request again.')

    return notebook_result


def _get_all_result_keys():
    # type: () -> List[Tuple[str, str]]
    all_keys = _get_cache('all_result_keys')
    if not all_keys:
        all_keys = result_serializer.get_all_result_keys()
        _set_cache('all_result_keys', all_keys, timeout=1)
    return all_keys


def all_available_results():
    all_keys = _get_all_result_keys()
    complete_jobs = {}
    for report_name, job_id in all_keys:
        result = _get_job_results(job_id, report_name)
        report_name, job_id = result.report_name, result.job_id
        result.result_url = url_for('task_results', task_id=job_id, report_name=report_name)
        result.ipynb_url = url_for('download_ipynb_result', task_id=job_id, report_name=report_name)
        complete_jobs[(report_name, job_id)] = result
    return complete_jobs


@flask_app.route('/results/<report_name>/<task_id>')
def task_results(task_id, report_name):
    result = _get_job_results(task_id, report_name)
    return render_template('results.html',
                           task_id=task_id,
                           report_name=report_name,
                           result=result,
                           donevalue=JobStatus.DONE.value,
                           html_render=url_for('task_results_html', report_name=report_name, task_id=task_id),
                           ipynb_url=url_for('download_ipynb_result', report_name=report_name, task_id=task_id),
                           all_reports=get_all_possible_checks())


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
    if isinstance(result, results.NotebookResultComplete):
        html_resources = result.raw_html_resources
        resource_path = os.path.join(task_id, 'resources', resource)
        if resource_path in html_resources.get('outputs', {}):
            return html_resources['outputs'][resource_path]
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
    logger.info('Stopping spark pool.')
    if spark_pool:
        global spark_pool
        spark_pool.shutdown()
    shutil.rmtree(OUTPUT_BASE_DIR)
    shutil.rmtree(TEMPLATE_BASE_DIR)


def _report_hunter():
    serializer = results.NotebookResultSerializer()
    last_query = None
    global alive
    while alive:
        try:
            ct = 0
            # First, check we have all keys that are available and populate their entries
            all_keys = _get_all_result_keys()
            for report_name, job_id in all_keys:
                # This method loads from db and saves into the store.
                _get_job_results(job_id, report_name)

            # Now, get all pending requests and check they haven't timed out...
            all_pending = serializer.get_all_results(mongo_filter={'status': {'$in': [JobStatus.SUBMITTED.value,
                                                                                      JobStatus.PENDING.value]}})
            now = datetime.datetime.now()
            cutoff = {JobStatus.SUBMITTED.value: now - datetime.timedelta(SUBMISSION_TIMEOUT),
                      JobStatus.PENDING.value: now - datetime.timedelta(RUNNING_TIMEOUT)}
            for result in all_pending:
                if result.job_start_time < cutoff.get(result.status):
                    delta_seconds = (cutoff.get(result.status) - now).total_seconds()
                    serializer.update_check_status(result.job_id, JobStatus.TIMEOUT,
                                                   error_info='This request timed out while being submitted to Spark. '
                                                              'Please try again! Timed out after {:.0f} minutes '
                                                              '{:.0f} seconds.'.format(delta_seconds/60,
                                                                                       delta_seconds % 60))
            # Finally, check we have the latest updates
            _last_query = datetime.datetime.now() - datetime.timedelta(minutes=1)
            query_results = serializer.get_all_results(since=last_query)
            for result in query_results:
                ct += 1
                existing = get_cache(result.report_name, result.job_id)
                if not existing or result.status != existing.status:  # Only update the cache when the status changes
                    set_cache(result.report_name, result.job_id, result)
                    logger.info('Report-hunter found a change for {} (status: {}->{})'.format(
                        result.job_id, existing.status if existing else None, result.status))
            logger.info('Found {} updates since {}.'.format(ct, last_query))
            last_query = _last_query
        except Exception as e:
            logger.exception(str(e))
        time.sleep(10)
    logger.info('Report-hunting thread successfully killed.')


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
