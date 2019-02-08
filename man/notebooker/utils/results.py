import datetime

from ahl.logging import get_logger
from flask import url_for
from typing import Optional, Dict, Union, List, Tuple

from man.notebooker.serialization.mongoose import NotebookResultSerializer
from man.notebooker.utils.caching import get_cache, get_report_cache, set_cache, set_report_cache
from man.notebooker.exceptions import NotebookRunException
from man.notebooker import constants

logger = get_logger(__name__)


def _get_job_results(job_id,              # type: str
                     report_name,         # type: str
                     serializer,          # type: NotebookResultSerializer
                     retrying=False,      # type: Optional[bool]
                     ignore_cache=False,  # type: Optional[bool]
                     ):
    # type: (...) -> constants.NotebookResultBase
    current_result = get_report_cache(report_name, job_id)
    if current_result and not ignore_cache:
        notebook_result = current_result
    else:
        notebook_result = serializer.get_check_result(job_id)
        set_report_cache(report_name, job_id, notebook_result)

    if not notebook_result:
        err_info = 'Job results not found for report name={} / job id={}. ' \
                 'Did you use an invalid job ID?'.format(report_name, job_id)
        return constants.NotebookResultError(job_id,
                                             error_info=err_info,
                                             report_name=report_name,
                                             job_start_time=datetime.datetime.now())
    if isinstance(notebook_result, str):
        if not retrying:
            return _get_job_results(job_id, report_name, serializer, retrying=True)
        raise NotebookRunException('An unexpected string was found as a result. Please run your request again.')

    return notebook_result


def get_all_result_keys(serializer, limit=0, force_reload=False):
    # type: (NotebookResultSerializer, Optional[int], Optional[bool]) -> List[Tuple[str, str]]
    all_keys = get_cache(('all_result_keys', limit))
    if not all_keys or force_reload:
        all_keys = serializer.get_all_result_keys(limit=limit)
        set_cache(('all_result_keys', limit), all_keys, timeout=1)
    return all_keys


def all_available_results(serializer,  # type: NotebookResultSerializer
                          limit=50,  # type: Optional[int]
                          ):
    # type: (...) -> Dict[Tuple[str, str], constants.NotebookResultBase]
    all_keys = get_all_result_keys(serializer, limit=limit)
    complete_jobs = {}
    for report_name, job_id in all_keys:
        result = _get_job_results(job_id, report_name, serializer)
        report_name, job_id = result.report_name, result.job_id
        result.result_url = url_for('serve_results_bp.task_results', task_id=job_id, report_name=report_name)
        result.ipynb_url = url_for('serve_results_bp.download_ipynb_result', task_id=job_id, report_name=report_name)
        result.pdf_url = url_for('serve_results_bp.download_pdf_result', task_id=job_id, report_name=report_name)
        result.rerun_url = url_for('run_report_bp.rerun_report', task_id=job_id, report_name=report_name)
        complete_jobs[(report_name, job_id)] = result
    return complete_jobs
