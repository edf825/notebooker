import os
import traceback
from ahl.logging import get_logger
from flask import render_template, jsonify, url_for, Blueprint, Response, abort, request
from typing import Union, Any
from man.notebooker.utils.caching import get_cache
from man.notebooker.constants import JobStatus, NotebookResultError, NotebookResultPending, NotebookResultComplete, NotebookResultBase
from man.notebooker.serialization.mongoose import NotebookResultSerializer, _pdf_filename
from man.notebooker.utils.results import _get_job_results, get_all_result_keys, get_latest_job_results
from man.notebooker.utils.templates import get_all_possible_templates

serve_results_bp = Blueprint('serve_results_bp', __name__)
logger = get_logger(__name__)


# ------------------- Serving results -------------------- #

def _result_serializer():
    return NotebookResultSerializer(mongo_host=os.environ['MONGO_HOST'],
                                    database_name=os.environ['DATABASE_NAME'],
                                    result_collection_name=os.environ['RESULT_COLLECTION_NAME'])


@serve_results_bp.route('/results/<path:report_name>/<task_id>')
def task_results(task_id, report_name):
    result = _get_job_results(task_id, report_name, _result_serializer(), ignore_cache=True)
    return render_template('results.html',
                           task_id=task_id,
                           report_name=report_name,
                           result=result,
                           donevalue=JobStatus.DONE,  # needed so we can check if a result is available
                           html_render=url_for('serve_results_bp.task_results_html', report_name=report_name, task_id=task_id),
                           ipynb_url=url_for('serve_results_bp.download_ipynb_result', report_name=report_name, task_id=task_id),
                           pdf_url=url_for('serve_results_bp.download_pdf_result', report_name=report_name, task_id=task_id),
                           rerun_url=url_for('run_report_bp.rerun_report', report_name=report_name, task_id=task_id),
                           all_reports=get_all_possible_templates())


def _process_result_or_abort(result):
    # type: (NotebookResultBase) -> Union[str, Any]
    if isinstance(result, (NotebookResultError, NotebookResultComplete)):
        return result.raw_html
    if isinstance(result, NotebookResultPending):
        return task_loading(result.report_name, result.job_id)
    abort(404)


@serve_results_bp.route('/result_html_render/<path:report_name>/<task_id>')
def task_results_html(task_id, report_name):
    # In this method, we either:
    # - present the HTML results, if the job has finished
    # - present the error, if the job has failed
    # - present the user with some info detailing the progress of the job, if it is still running.
    return _process_result_or_abort(_get_job_results(task_id, report_name, _result_serializer()))


@serve_results_bp.route('/result_html_render/<path:report_name>/latest')
def latest_parameterised_task_results_html(report_name):
    # In this method, we either:
    # - present the HTML results, if the job has finished
    # - present the error, if the job has failed
    # - present the user with some info detailing the progress of the job, if it is still running.
    params = {k: (v[0] if len(v) == 1 else v) for k, v in request.args.iterlists()}
    result = get_latest_job_results(report_name, params, _result_serializer())
    return _process_result_or_abort(result)


@serve_results_bp.route('/result_html_render/<path:report_name>/latest-all')
def latest_task_results_html(report_name):
    # In this method, we either:
    # - present the HTML results, if the job has finished
    # - present the error, if the job has failed
    # - present the user with some info detailing the progress of the job, if it is still running.
    # Will ignore all paramterisation of the report and get the latest of any run for a given report name
    return _process_result_or_abort(get_latest_job_results(report_name, None, _result_serializer()))


@serve_results_bp.route('/result_html_render/<path:report_name>/<task_id>/resources/<path:resource>')
def task_result_resources_html(task_id, resource, report_name):
    result = _get_job_results(task_id, report_name, _result_serializer())
    if isinstance(result, NotebookResultComplete):
        html_resources = result.raw_html_resources
        resource_path = os.path.join(task_id, 'resources', resource)
        if resource_path in html_resources.get('outputs', {}):
            return html_resources['outputs'][resource_path]
    abort(404)


@serve_results_bp.route('/result_download_ipynb/<path:report_name>/<task_id>')
def download_ipynb_result(task_id, report_name):
    result = _get_job_results(task_id, report_name, _result_serializer())
    if isinstance(result, NotebookResultComplete):
        return Response(result.raw_ipynb_json,
                        mimetype="application/vnd.jupyter",
                        headers={"Content-Disposition": "attachment;filename={}.ipynb".format(task_id)})
    else:
        abort(404)


@serve_results_bp.route('/result_download_pdf/<path:report_name>/<task_id>')
def download_pdf_result(task_id, report_name):
    result = _get_job_results(task_id, report_name, _result_serializer())
    if isinstance(result, NotebookResultComplete):
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
                           location=url_for('serve_results_bp.task_status', report_name=report_name, task_id=task_id),
                           )


def _get_job_status(job_id, report_name):
    # Continuously polled for updates by the user client, until the notebook has completed execution (or errored).
    job_result = _get_job_results(job_id, report_name, _result_serializer(), ignore_cache=True)
    key = u'run_output_{}'.format(job_id)
    output = get_cache(key) or ''
    if job_result is None:
        return {'status': 'Job not found. Did you use an old job ID?'}
    if job_result.status in (JobStatus.DONE, JobStatus.ERROR, JobStatus.TIMEOUT, JobStatus.CANCELLED):
        response = {'status': job_result.status.value,
                    'results_url': url_for('serve_results_bp.task_results', report_name=report_name, task_id=job_id)}
    else:
        response = {'status': job_result.status.value, 'run_output': output}
    return response


@serve_results_bp.route('/status/<path:report_name>/<task_id>')
def task_status(report_name, task_id):
    return jsonify(_get_job_status(task_id, report_name))


@serve_results_bp.route('/delete_report/<job_id>', methods=['POST'])
def delete_report(job_id):
    try:
        _result_serializer().delete_result(job_id)
        get_all_result_keys(_result_serializer(), limit=50, force_reload=True)
        result = {'status': 'ok'}
    except:
        error_info = traceback.format_exc()
        result = {'status': 'error', 'error': error_info}
    return jsonify(result)
