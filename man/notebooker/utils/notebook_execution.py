import jupytext
import nbformat
import os
import pkg_resources
from nbconvert import HTMLExporter, PDFExporter
from traitlets.config import Config
from typing import Any, Dict, AnyStr

from ahl.logging import get_logger
from man.notebooker.constants import KERNEL_SPEC

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
    return os.path.join('..', 'notebook_templates', '{}.py'.format(report_name))


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


def _output_dir(output_base_dir, report_name, job_id):
    return os.path.join(output_base_dir, report_name, job_id)
