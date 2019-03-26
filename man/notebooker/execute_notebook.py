import datetime
import json
import uuid

import click
import os
import papermill as pm
import traceback
from typing import Any, Dict, Optional

from ahl.logging import get_logger

from man.notebooker.constants import JobStatus, CANCEL_MESSAGE, OUTPUT_BASE_DIR, TEMPLATE_BASE_DIR, NotebookResultError, \
    NotebookResultComplete
from man.notebooker.serialization.mongoose import NotebookResultSerializer
from man.notebooker.utils.notebook_execution import _output_dir, send_result_email, mkdir_p, _cleanup_dirs
from man.notebooker.utils.conversion import ipython_to_html, ipython_to_pdf, _output_ipynb_name, generate_ipynb_from_py

logger = get_logger(__name__)


def run_checks(job_id,  # type: str
               job_start_time,  # type: datetime.datetime
               template_name,  # type: str
               report_title,  # type: str
               output_base_dir,  # type: str
               template_base_dir,  # type: str
               overrides,  # type: Dict[Any, Any]
               generate_pdf_output=True,  # type: Optional[bool]
               mailto='',  # type: Optional[str]
               prepare_only=False,  # type: Optional[bool]
               ):
    # type: (...) -> NotebookResultComplete

    output_dir = _output_dir(output_base_dir, template_name, job_id)
    output_ipynb = _output_ipynb_name(template_name)

    if not os.path.isdir(output_dir):
        logger.info('Making dir @ {}'.format(output_dir))
        os.makedirs(output_dir)

    ipynb_raw_path = generate_ipynb_from_py(template_base_dir, template_name)
    ipynb_executed_path = os.path.join(output_dir, output_ipynb)

    logger.info('Executing notebook at {} using parameters {} --> {}'.format(ipynb_raw_path, overrides, output_ipynb))
    pm.execute_notebook(ipynb_raw_path,
                        ipynb_executed_path,
                        parameters=overrides,
                        log_output=True,
                        prepare_only=prepare_only,
                        )
    with open(ipynb_executed_path, 'r') as f:
        raw_executed_ipynb = f.read()

    logger.info('Saving output notebook as HTML from {}'.format(ipynb_executed_path))
    html, resources = ipython_to_html(ipynb_executed_path, job_id)
    pdf = ipython_to_pdf(raw_executed_ipynb, report_title) if generate_pdf_output else ''

    notebook_result = NotebookResultComplete(job_id=job_id,
                                             job_start_time=job_start_time,
                                             job_finish_time=datetime.datetime.now(),
                                             raw_html_resources=resources,
                                             raw_ipynb_json=raw_executed_ipynb,
                                             raw_html=html,
                                             mailto=mailto,
                                             pdf=pdf,
                                             generate_pdf_output=generate_pdf_output,
                                             report_name=template_name,
                                             report_title=report_title,
                                             overrides=overrides,
                                             )
    return notebook_result


def run_report_worker(job_submit_time,
                      report_name,
                      overrides,
                      result_serializer,
                      report_title='',
                      job_id=None,
                      output_base_dir=OUTPUT_BASE_DIR,
                      template_base_dir=TEMPLATE_BASE_DIR,
                      attempts_remaining=2,
                      mailto='',
                      generate_pdf_output=True,
                      prepare_only=False,
                      ):
    job_id = job_id or str(uuid.uuid4())
    stop_execution = os.getenv('NOTEBOOKER_APP_STOPPING')
    if stop_execution:
        logger.info('Aborting attempt to run %s, jobid=%s as app is shutting down.', report_name, job_id)
        result_serializer.update_check_status(job_id, JobStatus.CANCELLED, error_info=CANCEL_MESSAGE)
        return
    try:
        logger.info('Calculating a new %s ipynb with parameters: %s (attempts remaining: %s)', report_name, overrides,
                    attempts_remaining)
        result_serializer.update_check_status(job_id,
                                              report_name=report_name,
                                              job_start_time=job_submit_time,
                                              status=JobStatus.PENDING)
        result = run_checks(job_id,
                            job_submit_time,
                            report_name,
                            report_title,
                            output_base_dir,
                            template_base_dir,
                            overrides,
                            mailto=mailto,
                            generate_pdf_output=generate_pdf_output,
                            prepare_only=prepare_only,
                            )
        logger.info('Successfully got result.')
        result_serializer.save_check_result(result)
        logger.info('Saved result to mongo successfully.')
    except Exception:
        error_info = traceback.format_exc()
        logger.exception('%s report failed! (job id=%s)', report_name, job_id)
        result = NotebookResultError(job_id=job_id,
                                     job_start_time=job_submit_time,
                                     report_name=report_name,
                                     report_title=report_title,
                                     error_info=error_info,
                                     overrides=overrides,
                                     mailto=mailto,
                                     generate_pdf_output=generate_pdf_output,
                                     )
        logger.error('Report run failed. Saving error result to mongo library %s@%s...',
                     result_serializer.database_name, result_serializer.mongo_host)
        result_serializer.save_check_result(result)
        logger.info('Error result saved to mongo successfully.')
        if attempts_remaining > 0:
            logger.info('Retrying report.')
            return run_report_worker(job_submit_time,
                                     report_name,
                                     overrides,
                                     result_serializer,
                                     report_title=report_title,
                                     job_id=job_id,
                                     output_base_dir=output_base_dir,
                                     template_base_dir=template_base_dir,
                                     attempts_remaining=attempts_remaining - 1,
                                     mailto=mailto,
                                     generate_pdf_output=generate_pdf_output,
                                     prepare_only=prepare_only,
                                     )
        else:
            logger.info('Abandoning attempt to run report. It failed too many times.')
    return result


@click.command()
@click.option('--report-name',
              help='The name of the template to execute, relative to the template directory.')
@click.option('--overrides-as-json',
              default='{}',
              help='The parameters to inject into the notebook template, in JSON format.')
@click.option('--report-title',
              default='',
              help='A custom title for this notebook. The default is the report_name.')
@click.option('--n-retries',
              default=3,
              help='The number of times to retry when executing this notebook.')
@click.option('--mongo-db-name',
              default='mongoose_restech',
              help='The mongo database name to which we will save the notebook result.')
@click.option('--mongo-host',
              default='research',
              help='The Mongoose cluster to which we are saving notebook results.')
@click.option('--result-collection-name',
              default='NOTEBOOK_OUTPUT',
              help='The name of the BSONStore to which we are saving notebook results.')
@click.option('--job-id',
              default=str(uuid.uuid4()),
              help='The unique job ID for this notebook. Can be non-unique, but note that you will overwrite history.')
@click.option('--output-base-dir',
              default=OUTPUT_BASE_DIR,
              help='The base directory to which we will save our notebook output temporarily. Required by Papermill.')
@click.option('--template-base-dir',
              default=TEMPLATE_BASE_DIR,
              help='The base directory to which we will save our notebook templates which have been converted '
                   'from .py to .ipynb.')
@click.option('--mailto',
              default='',
              help='A comma-separated list of email addresses which will receive results.')
@click.option('--pdf-output/--no-pdf-output',
              default=True,
              help='Whether we generate PDF output or not.')
@click.option('--prepare-notebook-only',
              is_flag=True,
              help='Used for debugging and testing. Whether to actually execute the notebook or just "prepare" it.')
def main(report_name,
         overrides_as_json,
         report_title,
         n_retries,
         mongo_db_name,
         mongo_host,
         result_collection_name,
         job_id,
         output_base_dir,
         template_base_dir,
         mailto,
         pdf_output,
         prepare_notebook_only,
         ):
    try:
        if report_name is None:
            raise ValueError('Error! Please provide a --report-name.')
        report_title = report_title or report_name
        logger.info('Creating %s', output_base_dir)
        mkdir_p(output_base_dir)
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
            report_title=report_title,
            job_id=job_id,
            output_base_dir=output_base_dir,
            template_base_dir=template_base_dir,
            attempts_remaining=n_retries-1,
            mailto=mailto,
            generate_pdf_output=pdf_output,
        )
        if mailto:
            send_result_email(result, mailto)
        if isinstance(result, NotebookResultError):
            logger.warning('Notebook execution failed! Output was:')
            logger.warning(repr(result))
            raise Exception(result.error_info)
        return result
    finally:
        _cleanup_dirs()


if __name__ == '__main__':
    main()
