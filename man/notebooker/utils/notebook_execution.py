import shutil

import jupytext
import nbformat
import os
import pkg_resources
import tempfile

import re
from ahl.mail import mail
from nbconvert import HTMLExporter, PDFExporter
from nbconvert.exporters.exporter import ResourcesDict
from traitlets.config import Config
from typing import Any, Dict, AnyStr, Union

from ahl.logging import get_logger
from man.notebooker.constants import KERNEL_SPEC
from man.notebooker.results import NotebookResultComplete, NotebookResultError

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


def ipython_to_pdf(raw_executed_ipynb, report_title):
    # type: (str, str) -> AnyStr
    pdf_exporter = PDFExporter(Config())
    resources = ResourcesDict()
    resources['metadata'] = ResourcesDict()
    resources['metadata']['name'] = report_title
    pdf, _ = pdf_exporter.from_notebook_node(nbformat.reads(raw_executed_ipynb, as_version=nbformat.v4.nbformat),
                                             resources=resources)
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


def send_result_email(result, mailto):
    # type: (Union[NotebookResultComplete, NotebookResultError], str) -> None
    from_email = 'man.notebooker@man.com'
    to_email = mailto
    subject = 'Notebooker: {} report completed with status: {}'.format(result.report_title, result.status.value)
    body = result.raw_html
    attachments = []
    tmp_dir = tempfile.mkdtemp(dir=os.path.expanduser('~'))

    if isinstance(result, NotebookResultComplete):
        # Attach PDF output to the email. Has to be saved to disk temporarily for the mail API to work.
        pdf_path = os.path.join(tmp_dir, '{}_{}.pdf'.format(result.report_name,
                                                            result.job_start_time.strftime('%Y-%m-%dT%H%M%S')))
        with open(pdf_path, 'w') as f:
            f.write(result.pdf)
        attachments.append(pdf_path)

        # Embed images into the email as attachments with "cid" links.
        for resource_path, resource in result.raw_html_resources['outputs'].items():
            resource_path_short = resource_path.rsplit(os.sep, 1)[1]
            new_path = os.path.join(tmp_dir, resource_path_short)
            with open(new_path, 'w') as f:
                f.write(resource)

            body = re.sub(r'<img src="{}"'.format(resource_path),
                          r'<img src="cid:{}"'.format(resource_path_short),
                          body)
            attachments.append(new_path)

    msg = ['Please either activate HTML emails, or see the PDF attachment.', body]

    logger.info('Sending email to %s with %d attachments', mailto, len(attachments))
    mail(from_email, to_email, subject, msg, attachments=attachments)

    shutil.rmtree(tmp_dir)
