import os
import uuid

import git
import jupytext
import nbformat
import pkg_resources
from nbconvert import HTMLExporter, PDFExporter
from nbconvert.exporters.exporter import ResourcesDict
from traitlets.config import Config
from typing import AnyStr, Dict, Any, Optional

from man.notebooker.constants import REPORT_NAME_SEPARATOR, PYTHON_TEMPLATE_DIR, KERNEL_SPEC, NOTEBOOKER_DISABLE_GIT
from man.notebooker.utils.caching import get_cache, set_cache
from man.notebooker.utils.notebook_execution import logger, mkdir_p


def ipython_to_html(ipynb_path, job_id):
    # type: (str, str) -> (nbformat.NotebookNode, Dict[str, Any])
    c = Config()
    c.HTMLExporter.preprocessors = ['nbconvert.preprocessors.ExtractOutputPreprocessor']
    c.HTMLExporter.template_file = pkg_resources.resource_filename(__name__, '../web/templates/notebooker_html_output.tpl')
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


def _get_python_template_path(report_path, warn_on_local):
    # type: (str, bool) -> str
    if PYTHON_TEMPLATE_DIR:
        return _python_template(report_path)
    else:
        if warn_on_local:
            logger.warning('Loading from local location. This is only expected if you are running locally.')
        return pkg_resources.resource_filename(__name__, '../../../notebook_templates/{}.py'.format(report_path))


def _get_output_path_hex():
    # type: () -> str
    if PYTHON_TEMPLATE_DIR and not NOTEBOOKER_DISABLE_GIT:
        logger.info('Pulling latest notebook templates from git.')
        try:
            latest_sha = _git_pull_templates()
            if get_cache('latest_sha') != latest_sha:
                logger.info('Change detected in notebook template master!')
                set_cache('latest_sha', latest_sha)
            logger.info('Git pull done.')
        except Exception as e:
            logger.exception(e)
        return get_cache('latest_sha') or 'OLD'
    else:
        return str(uuid.uuid4())


def generate_ipynb_from_py(template_base_dir, report_name, warn_on_local=True):
    # type: (str, str, bool) -> str
    # This method EITHER:
    # Pulls the latest version of the notebook templates from git, and regenerates templates if there is a new HEAD
    # OR: finds the local templates from the repository using a relative path
    report_path = report_name.replace(REPORT_NAME_SEPARATOR, os.path.sep)
    python_template_path = _get_python_template_path(report_path, warn_on_local)
    output_template_path = _ipynb_output_path(template_base_dir, report_path, _get_output_path_hex())

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


def generate_py_from_ipynb(ipynb_path, output_dir='.'):
    if not ipynb_path.endswith('.ipynb'):
        logger.error('Did not expect file extension. Expected .ipynb, got %s', os.path.splitext(ipynb_path)[1])
        return None
    mkdir_p(output_dir)
    filename_no_extension = os.path.basename(os.path.splitext(ipynb_path)[0])
    output_path = os.path.join(output_dir, filename_no_extension + '.py')
    ipynb = jupytext.readf(ipynb_path)
    jupytext.writef(ipynb, output_path)
    logger.info('Successfully converted %s -> %s', ipynb_path, output_path)
    return output_path
