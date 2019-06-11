from __future__ import unicode_literals
import datetime
import json
import os
import subprocess
import sys
import threading
import uuid

import nbformat
from ahl.logging import get_logger
from flask import render_template, request, jsonify, url_for, Blueprint, abort

from man.notebooker import execute_notebook
from man.notebooker.constants import TEMPLATE_BASE_DIR, JobStatus, OUTPUT_BASE_DIR
from man.notebooker.serialization.serialization import get_serializer, get_fresh_serializer
from man.notebooker.web.handle_overrides import handle_overrides
from man.notebooker.utils.conversion import generate_ipynb_from_py
from man.notebooker.utils.templates import _get_preview, _get_parameters_cell_idx, get_all_possible_templates
from man.notebooker.utils.web import validate_title, validate_mailto

run_report_bp = Blueprint('run_report_bp', __name__)
logger = get_logger(__name__)


@run_report_bp.route('/run_report/get_preview/<path:report_name>', methods=['GET'])
def run_report_get_preview(report_name):
    # Handle the case where a rendered ipynb asks for "custom.css"
    if '.css' in report_name:
        return ''
    return _get_preview(report_name)


@run_report_bp.route('/run_report/<path:report_name>', methods=['GET'])
def run_report_http(report_name):
    path = generate_ipynb_from_py(TEMPLATE_BASE_DIR, report_name)
    nb = nbformat.read(path, as_version=nbformat.v4.nbformat)
    metadata_idx = _get_parameters_cell_idx(nb)
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
                           all_reports=get_all_possible_templates())


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


def run_report(report_name, report_title, mailto, overrides, generate_pdf_output=True, prepare_only=False):
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
                          ] +
                         (['--prepare-notebook-only'] if prepare_only else []),
                         stderr=subprocess.PIPE)
    stderr_thread = threading.Thread(target=_monitor_stderr, args=(p, job_id, ))
    stderr_thread.daemon = True
    stderr_thread.start()
    return job_id


@run_report_bp.route('/run_report/<path:report_name>', methods=['POST'])
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
            {'Location': url_for('serve_results_bp.task_status', report_name=report_name, task_id=job_id)})


def _rerun_report(task_id, prepare_only=False):
    result = get_serializer().get_check_result(task_id)
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


@run_report_bp.route('/rerun_report/<task_id>/<path:report_name>', methods=['POST'])
def rerun_report(task_id, report_name):
    new_job_id = _rerun_report(task_id)
    return jsonify({'results_url': url_for('serve_results_bp.task_results',
                                           report_name=report_name, task_id=new_job_id)})
