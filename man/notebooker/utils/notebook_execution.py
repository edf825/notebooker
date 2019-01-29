import errno
import shutil

import git
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
from man.notebooker.utils.caching import get_cache, set_cache
from man.notebooker.constants import KERNEL_SPEC, PYTHON_TEMPLATE_DIR, REPORT_NAME_SEPARATOR, NotebookResultError, \
    NotebookResultComplete

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
    return '{}.ipynb'.format(report_name.replace(os.sep, REPORT_NAME_SEPARATOR))


def _git_pull_templates():
    repo = git.repo.Repo(os.environ['PY_TEMPLATE_DIR'])
    repo.git.fetch()
    repo.git.pull('origin', 'master')
    return repo.commit('HEAD').hexsha


def _python_template(report_path):
    # type: (AnyStr) -> AnyStr
    file_name = '{}.py'.format(report_path)
    return os.path.join(
        PYTHON_TEMPLATE_DIR,
        file_name,
    )


def _ipynb_output_path(template_base_dir, report_path, git_hex):
    # type: (AnyStr, AnyStr, AnyStr) -> AnyStr
    file_name = '{}.ipynb'.format(report_path)
    return os.path.join(
        template_base_dir,
        git_hex,
        file_name,
    )


def generate_ipynb_from_py(template_base_dir, report_name):
    # type: (str, str) -> str
    # This method EITHER:
    # Pulls the latest version of the notebook templates from git, and regenerates templates if there is a new HEAD
    # OR: finds the local templates from the repository using a relative path
    report_path = report_name.replace(REPORT_NAME_SEPARATOR, os.path.sep)
    if PYTHON_TEMPLATE_DIR:
        logger.info('Pulling latest notebook templates from git.')
        try:
            latest_sha = _git_pull_templates()
            if get_cache('latest_sha') != latest_sha:
                logger.info('Change detected in notebook template master!')
                set_cache('latest_sha', latest_sha)
            logger.info('Git pull done.')
        except Exception as e:
            logger.exception(e)
        python_template_path = _python_template(report_path)
        sha = get_cache('latest_sha') or 'OLD'
        output_template_path = _ipynb_output_path(template_base_dir, report_path, sha)
    else:
        logger.warn('Loading from local location. This is only expected if you are running locally.')
        python_template_path = pkg_resources.resource_filename(__name__,
                                                               '../../../notebook_templates/{}.py'.format(report_path))
        output_template_path = _ipynb_output_path(template_base_dir, report_path, '')

    try:
        with open(output_template_path, 'r') as f:
            if f.read():
                logger.info('Loading ipynb from cached location: %s', output_template_path)
                return output_template_path
    except IOError:
        pass

    # "touch" the output file
    logger.info('Creating ipynb at: %s', output_template_path)
    mkdir_p(os.path.dirname(output_template_path))
    with open(output_template_path, 'w') as f:
        os.utime(output_template_path, None)

    jupytext_nb = jupytext.readf(python_template_path)
    jupytext_nb['metadata']['kernelspec'] = KERNEL_SPEC  # Override the kernel spec since we want to run it..
    jupytext.writef(jupytext_nb, output_template_path)
    return output_template_path


def _output_dir(output_base_dir, report_name, job_id):
    return os.path.join(output_base_dir, report_name, job_id)


def send_result_email(result, mailto):
    # type: (Union[NotebookResultComplete, NotebookResultError], AnyStr) -> None
    from_email = 'man.notebooker@man.com'
    to_email = mailto
    report_title = result.report_title.decode('utf-8') if isinstance(result.report_title, bytes) else result.report_title
    subject = u'Notebooker: {} report completed with status: {}'.format(report_title, result.status.value)
    body = result.raw_html
    attachments = []
    tmp_dir = tempfile.mkdtemp(dir=os.path.expanduser('~'))

    if isinstance(result, NotebookResultComplete):
        # Attach PDF output to the email. Has to be saved to disk temporarily for the mail API to work.
        report_name = result.report_name.replace(os.sep, REPORT_NAME_SEPARATOR)
        if isinstance(report_name, bytes):
            report_name = report_name.decode('utf-8')
        pdf_name = u'{}_{}.pdf'.format(report_name, result.job_start_time.strftime('%Y-%m-%dT%H%M%S'))
        pdf_path = os.path.join(tmp_dir, pdf_name)
        with open(pdf_path, 'w') as f:
            f.write(result.pdf)
        attachments.append(pdf_path)

        # Embed images into the email as attachments with "cid" links.
        for resource_path, resource in result.raw_html_resources.get('outputs', {}).items():
            resource_path_short = resource_path.rsplit(os.sep, 1)[1]
            new_path = os.path.join(tmp_dir, resource_path_short)
            with open(new_path, 'w') as f:
                f.write(resource)

            body = re.sub(r'<img src="{}"'.format(resource_path),
                          r'<img src="cid:{}"'.format(resource_path_short),
                          body)
            attachments.append(new_path)

    msg = ['Please either activate HTML emails, or see the PDF attachment.', body]

    logger.info(u'Sending email to %s with %d attachments', mailto, len(attachments))
    mail(from_email, to_email, subject, msg, attachments=attachments)

    shutil.rmtree(tmp_dir)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
