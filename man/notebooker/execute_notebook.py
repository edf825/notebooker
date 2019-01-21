import datetime
import json
import subprocess
import sys
import uuid

import click
import os
import papermill as pm
import traceback
from typing import Any, Dict

from ahl.logging import get_logger

from man.notebooker.caching import get_cache
from man.notebooker.constants import JobStatus, CANCEL_MESSAGE, OUTPUT_BASE_DIR, TEMPLATE_BASE_DIR, \
    MONGO_HOST, MONGO_LIBRARY
from man.notebooker.results import NotebookResultComplete, NotebookResultSerializer, \
    NotebookResultError
from man.notebooker.utils.notebook_execution import ipython_to_html, ipython_to_pdf, _output_ipynb_name, \
    generate_ipynb_from_py, _output_dir

logger = get_logger(__name__)


def run_checks(job_id,              # type: str
               job_start_time,      # type: datetime.datetime
               report_name,         # type: str
               output_base_dir,     # type: str
               template_base_dir,   # type: str
               result_serializer,   # type: NotebookResultSerializer
               input_json,          # type: Dict[Any, Any]
               ):
    # type: (...) -> NotebookResultComplete
    result_serializer.update_check_status(job_id,
                                          report_name=report_name,
                                          job_start_time=job_start_time,
                                          status=JobStatus.PENDING)

    output_dir = _output_dir(output_base_dir, report_name, job_id)
    output_ipynb = _output_ipynb_name(report_name)

    if not os.path.isdir(output_dir):
        logger.info('Making dir @ {}'.format(output_dir))
        os.makedirs(output_dir)

    ipynb_raw_path = generate_ipynb_from_py(template_base_dir, report_name)
    ipynb_executed_path = os.path.join(output_dir, output_ipynb)

    logger.info('Executing notebook at {} using parameters {} --> {}'.format(ipynb_raw_path, input_json, output_ipynb))
    pm.execute_notebook(ipynb_raw_path,
                        ipynb_executed_path,
                        parameters=input_json,
                        log_output=True)
    with open(ipynb_executed_path, 'r') as f:
        raw_executed_ipynb = f.read()

    logger.info('Saving output notebook as HTML from {}'.format(ipynb_executed_path))
    html, resources = ipython_to_html(ipynb_executed_path, job_id)
    pdf = ipython_to_pdf(raw_executed_ipynb)

    notebook_result = NotebookResultComplete(job_id=job_id,
                                             job_start_time=job_start_time,
                                             job_finish_time=datetime.datetime.now(),
                                             raw_html_resources=resources,
                                             raw_ipynb_json=raw_executed_ipynb,
                                             raw_html=html,
                                             pdf=pdf,
                                             report_name=report_name)
    result_serializer.save_check_result(notebook_result)
    logger.info('Saved result to mongo successfully.')
    return notebook_result


def run_report_worker(job_submit_time,
                      report_name,
                      overrides,
                      result_serializer,
                      job_id=None,
                      output_base_dir=OUTPUT_BASE_DIR,
                      template_base_dir=TEMPLATE_BASE_DIR,
                      attempts_remaining=2
                      ):
    job_id = job_id or str(uuid.uuid4())
    still_alive = get_cache('_STILL_ALIVE')
    if still_alive is False:
        logger.info('Aborting attempt to run %s, jobid=%s as app is shutting down.', report_name, job_id)
        result_serializer.update_check_status(job_id, JobStatus.CANCELLED, error_info=CANCEL_MESSAGE)
        return
    try:
        logger.info('Calculating a new %s ipynb with parameters: %s (attempts remaining: %s)', report_name, overrides,
                    attempts_remaining)
        result = run_checks(
            job_id,
            job_submit_time,
            report_name,
            output_base_dir,
            template_base_dir,
            result_serializer,
            overrides)
        logger.info('Successfully got result.')
    except Exception:
        error_info = traceback.format_exc()
        logger.exception('%s report failed! (job id=%s)', report_name, job_id)
        notebook_result = NotebookResultError(job_id=job_id,
                                              job_start_time=job_submit_time,
                                              report_name=report_name,
                                              error_info=error_info)
        logger.error('Report run failed. Saving error result to mongo library %s@%s...', MONGO_LIBRARY, MONGO_HOST)
        result_serializer.save_check_result(notebook_result)
        logger.info('Error result saved to mongo successfully.')
        if attempts_remaining == 0:
            logger.info('Abandoning attempt to run report. It failed too many times.')
            return notebook_result
        logger.info('Retrying report.')
        return run_report_worker(job_submit_time,
                                 report_name,
                                 overrides,
                                 result_serializer,
                                 job_id=job_id,
                                 output_base_dir=output_base_dir,
                                 template_base_dir=template_base_dir,
                                 attempts_remaining=attempts_remaining - 1,
                                 )
    return result


@click.command()
@click.option('--report-name')
@click.option('--overrides-as-json', default='{}')
@click.option('--n-retries', default=3)
@click.option('--mongo-db-name', default='mongoose_restech')
@click.option('--mongo-host', default='research')
@click.option('--result-collection-name', default='NOTEBOOK_OUTPUT')
@click.option('--job-id', default=str(uuid.uuid4()))
@click.option('--output-base-dir', default=OUTPUT_BASE_DIR)
@click.option('--template-base-dir', default=TEMPLATE_BASE_DIR)
def main(report_name,
         overrides_as_json,
         n_retries,
         mongo_db_name,
         mongo_host,
         result_collection_name,
         job_id,
         output_base_dir,
         template_base_dir,
         ):
    if report_name is None:
        raise ValueError('Error! Please provide a --report-name.')
    logger.info('Creating %s', OUTPUT_BASE_DIR)
    os.makedirs(OUTPUT_BASE_DIR)
    logger.info('Creating %s', TEMPLATE_BASE_DIR)
    os.makedirs(TEMPLATE_BASE_DIR)
    overrides = json.loads(overrides_as_json) if overrides_as_json else {}
    start_time = datetime.datetime.now()
    result_serializer = NotebookResultSerializer(database_name=mongo_db_name,
                                                 mongo_host=mongo_host, 
                                                 result_collection_name=result_collection_name)
    result = run_report_worker(
        start_time,
        report_name,
        overrides,
        result_serializer,
        job_id=job_id,
        output_base_dir=output_base_dir,
        template_base_dir=template_base_dir,
        attempts_remaining=n_retries-1
    )
    if isinstance(result, NotebookResultError):
        logger.warn('Notebook execution failed! Output was:')
        logger.warn(repr(result))
        raise Exception(result.error_info)
    return result


def docker_compose_entrypoint():
    """
    Sadness. This is required because of https://github.com/jupyter/jupyter_client/issues/154
    Otherwise we will get "RuntimeError: Kernel died before replying to kernel_info"
    The suggested fix to use sh -c "command" does not work for our use-case, sadly.

    Examples
    --------
    man_execute_notebook --report-name watchdog_checks --mongo-host mktdatad
Recieved a request to run a report with the following parameters:
['/users/is/jbannister/pyenvs/notebooker/bin/python', '-m', 'man.notebooker.execute_notebook', '--report-name', 'watchdog_checks', '--mongo-host', 'mktdatad']
...

    man_execute_notebook
Recieved a request to run a report with the following parameters:
['/users/is/jbannister/pyenvs/notebooker/bin/python', '-m', 'man.notebooker.execute_notebook']
ValueError: Error! Please provide a --report-name.
    """
    args_to_execute = [sys.executable, '-m', __name__] + sys.argv[1:]
    logger.info('Recieved a request to run a report with the following parameters:')
    logger.info(args_to_execute)
    return subprocess.Popen(args_to_execute).wait()


if __name__ == '__main__':
    main()
