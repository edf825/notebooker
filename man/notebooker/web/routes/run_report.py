from __future__ import unicode_literals
import datetime
import json
from typing import List, Tuple, Any, Dict

import os
import subprocess
import sys
import threading
import uuid

import nbformat
from logging import getLogger
from flask import render_template, request, jsonify, url_for, Blueprint, abort

from man.notebooker import execute_notebook
from man.notebooker.constants import TEMPLATE_BASE_DIR, JobStatus, OUTPUT_BASE_DIR
from man.notebooker.serialization.serialization import get_serializer, get_fresh_serializer
from man.notebooker.web.handle_overrides import handle_overrides
from man.notebooker.utils.conversion import generate_ipynb_from_py
from man.notebooker.utils.templates import _get_preview, _get_parameters_cell_idx, get_all_possible_templates
from man.notebooker.utils.web import json_to_python, validate_title, validate_mailto

run_report_bp = Blueprint('run_report_bp', __name__)
logger = getLogger(__name__)


@run_report_bp.route('/run_report/get_preview/<path:report_name>', methods=['GET'])
def run_report_get_preview(report_name):
    # Handle the case where a rendered ipynb asks for "custom.css"
    if '.css' in report_name:
        return ''
    return _get_preview(report_name)


@run_report_bp.route('/run_report/<path:report_name>', methods=['GET'])
def run_report_http(report_name):
    json_params = request.args.get('json_params')
    initial_python_parameters = json_to_python(json_params) or ""
    try:
        path = generate_ipynb_from_py(TEMPLATE_BASE_DIR, report_name)
    except FileNotFoundError as e:
        logger.exception(e)
        return "", 404
    nb = nbformat.read(path, as_version=nbformat.v4.nbformat)
    metadata_idx = _get_parameters_cell_idx(nb)
    parameters_as_html = ''
    has_prefix = has_suffix = False
    if metadata_idx is not None:
        metadata = nb['cells'][metadata_idx]
        parameters_as_html = metadata['source'].strip()
        has_prefix, has_suffix = bool(nb['cells'][:metadata_idx]), bool(nb['cells'][metadata_idx+1:])
    logger.info("initial_python_parameters = {}".format(initial_python_parameters))
    return render_template('run_report.html',
                           parameters_as_html=parameters_as_html,
                           has_prefix=has_prefix,
                           has_suffix=has_suffix,
                           report_name=report_name,
                           all_reports=get_all_possible_templates(),
                           initialPythonParameters=initial_python_parameters)


def _monitor_stderr(process, job_id):
    stderr = []
    # Unsure whether flask app contexts are thread-safe; just reinitialise the serializer here.
    result_serializer = get_fresh_serializer()
    while True:
        line = process.stderr.readline().decode('utf-8')
        if line == '' and process.poll() is not None:
            break
        stderr.append(line)
        logger.info(line)  # So that we have it in the log, not just in memory.
        result_serializer.update_stdout(job_id, new_lines=[line])
    return ''.join(stderr)


def run_report(report_name, report_title, mailto, overrides, hide_code=False, generate_pdf_output=True, prepare_only=False):
    # Actually start the job in earnest.
    job_id = str(uuid.uuid4())
    job_start_time = datetime.datetime.now()
    result_serializer = get_serializer()
    result_serializer.save_check_stub(job_id, report_name,
                                      report_title=report_title,
                                      job_start_time=job_start_time,
                                      status=JobStatus.SUBMITTED,
                                      overrides=overrides,
                                      mailto=mailto,
                                      generate_pdf_output=generate_pdf_output,
                                      hide_code=hide_code,
                                      )
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
                          '--pdf-output' if generate_pdf_output else '--no-pdf-output',
                          '--hide-code' if hide_code else '--show-code',
                          '--serializer-cls', result_serializer.__class__.__name__
                          ] +
                         (['--prepare-notebook-only'] if prepare_only else []),
                         stderr=subprocess.PIPE)
    stderr_thread = threading.Thread(target=_monitor_stderr, args=(p, job_id, ))
    stderr_thread.daemon = True
    stderr_thread.start()
    return job_id


def _handle_run_report(report_name, overrides_dict, issues):
    # type: (str, Dict[str, Any], List[str]) -> Tuple[str, int, Dict[str, str]]
    # Find and cleanse the title of the report
    report_title = validate_title(request.values.get('report_title'), issues)
    # Get mailto email address
    mailto = validate_mailto(request.values.get('mailto'), issues)
    hide_code = request.values.get('hide_code') == 'on'
    if issues:
        return jsonify({'status': 'Failed', 'content': ('\n'.join(issues))})
    job_id = run_report(report_name, report_title, mailto, overrides_dict, hide_code=hide_code)
    return (jsonify({'id': job_id}),
            202,  # HTTP Accepted code
            {'Location': url_for('serve_results_bp.task_status', report_name=report_name, job_id=job_id)})


@run_report_bp.route('/run_report_json/<path:report_name>', methods=['POST'])
def run_report_json(report_name):
    issues = []
    # Get JSON overrides
    overrides_dict = json.loads(request.values.get('overrides'))
    return _handle_run_report(report_name, overrides_dict, issues)


@run_report_bp.route('/run_report/<path:report_name>', methods=['POST'])
def run_checks_http(report_name):
    issues = []
    # Get and process raw python overrides
    overrides_dict = handle_overrides(request.values.get('overrides'), issues)
    return _handle_run_report(report_name, overrides_dict, issues)


def _rerun_report(job_id, prepare_only=False):
    result = get_serializer().get_check_result(job_id)
    if not result:
        abort(404)
    prefix = 'Rerun of '
    title = result.report_title if result.report_title.startswith(prefix) else (prefix + result.report_title)
    new_job_id = run_report(
        result.report_name,
        title,
        result.mailto,
        result.overrides,
        generate_pdf_output=result.generate_pdf_output,
        prepare_only=prepare_only,
    )
    return new_job_id


@run_report_bp.route('/rerun_report/<job_id>/<path:report_name>', methods=['POST'])
def rerun_report(job_id, report_name):
    new_job_id = _rerun_report(job_id)
    return jsonify({'results_url': url_for('serve_results_bp.task_results',
                                           report_name=report_name, job_id=new_job_id)})
