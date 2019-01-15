import datetime
import json
import uuid

import click
import jupytext
import nbformat
import os
import papermill as pm
import pkg_resources
import traceback
from nbconvert import HTMLExporter, PDFExporter
from traitlets.config import Config
from typing import Any, Dict, AnyStr

from ahl.logging import get_logger

from man.notebooker.caching import get_cache, set_cache
from man.notebooker.constants import JobStatus, KERNEL_SPEC, CANCEL_MESSAGE, OUTPUT_BASE_DIR, TEMPLATE_BASE_DIR, \
    MONGO_HOST, MONGO_LIBRARY
from man.notebooker.results import NotebookResultComplete, NotebookResultSerializer, \
    NotebookResultError
from man.notebooker.utils import _output_dir


logger = get_logger(__name__)


def ipython_to_html(ipynb_path, job_id):
    # type: (str, str) -> (nbformat.NotebookNode, Dict[str, Any])
    c = Config()
    c.HTMLExporter.preprocessors = ['nbconvert.preprocessors.ExtractOutputPreprocessor']
    html_exporter_with_figs = HTMLExporter(config=c)

    with open(ipynb_path, 'r') as nb_file:
        nb = nbformat.reads(nb_file.read(), as_version=nbformat.v4.nbformat)
    resources_dir = '{}/resources'.format(job_id)
    html, resources = html_exporter_with_figs.from_notebook_node(nb, resources={'output_files_dir': resources_dir})
    return html, resources


def ipython_to_pdf(raw_executed_ipynb):
    # type: (str) -> AnyStr
    pdf_exporter = PDFExporter(Config())
    pdf, _ = pdf_exporter.from_notebook_node(nbformat.reads(raw_executed_ipynb, as_version=nbformat.v4.nbformat))
    return pdf


def _output_ipynb_name(report_name):
    # type: (str) -> str
    return '{}.ipynb'.format(report_name)


def _python_template(report_name):
    # type: (str) -> str
    return os.path.join('notebook_templates', '{}.py'.format(report_name))


def _ipynb_output_path(template_base_dir, report_name):
    # type: (str, str) -> str
    return os.path.join(template_base_dir, '{}.ipynb'.format(report_name))


def generate_ipynb_from_py(template_base_dir, report_name):
    # type: (str, str) -> str
    python_input_filename = _python_template(report_name)
    output_template = _ipynb_output_path(template_base_dir, report_name)
    python_template = pkg_resources.resource_filename(__name__, python_input_filename)

    try:
        with open(output_template, 'r') as f:
            if f.read():
                logger.info('Loading ipynb from cached location: %s', output_template)
                return output_template
    except IOError:
        pass

    # "touch" the output file
    logger.info('Creating ipynb at: {}'.format(output_template))
    with open(output_template, 'w') as f:
        os.utime(output_template, None)

    jupytext_nb = jupytext.readf(python_template)
    jupytext_nb['metadata']['kernelspec'] = KERNEL_SPEC  # Override the kernel spec since we want to run it..
    jupytext.writef(jupytext_nb, output_template)
    return output_template


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
            return None
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
    return result


if __name__ == '__main__':
    main()
