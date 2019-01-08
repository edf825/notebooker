# This module is meant to be used primarily by Spark nodes, hence local imports aplenty...
import datetime
import jupytext
import nbformat
import os
import pkg_resources
import traceback
from nbconvert import HTMLExporter
from traitlets.config import Config
from typing import Any, Dict

from man.notebooker.constants import KERNEL_SPEC
from man.notebooker.results import NotebookResultComplete, NotebookResultSerializer, \
    NotebookResultError


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
    from ahl.logging import get_logger
    logger = get_logger(__name__)

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
               mongo_host,          # type: str
               mongo_library,       # type: str
               input_json,          # type: Dict[Any, Any]
               ):
    # type: (...) -> NotebookResultComplete
    from ahl.logging import get_logger
    logger = get_logger(__name__)

    # Save initial state to mongo
    serializer = NotebookResultSerializer(mongo_host=mongo_host, result_collection_name=mongo_library)
    try:
        import papermill as pm
        from man.notebooker.constants import JobStatus
        from man.notebooker.utils import _output_dir

        serializer.update_check_status(job_id,
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

        notebook_result = NotebookResultComplete(job_id=job_id,
                                                 job_start_time=job_start_time,
                                                 job_finish_time=datetime.datetime.now(),
                                                 raw_html_resources=resources,
                                                 raw_ipynb_json=raw_executed_ipynb,
                                                 raw_html=html,
                                                 report_name=report_name)
        logger.info('Saving result to mongo library {}@{}...'.format(mongo_library, mongo_host))
        serializer.save_check_result(notebook_result)
        logger.info('Saved result to mongo successfully.')
        return notebook_result
    except Exception as e:
        error_info = traceback.format_exc()
        notebook_result = NotebookResultError(job_id=job_id,
                                              job_start_time=job_start_time,
                                              report_name=report_name,
                                              error_info=error_info)
        logger.info('Saving error result to mongo library {}@{}...'.format(mongo_library, mongo_host))
        serializer.save_check_result(notebook_result)
        logger.info('Error result saved to mongo successfully.')
        raise
