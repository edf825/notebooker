import os
import pkgutil

import nbformat
import pkg_resources
from ahl.logging import get_logger
from nbconvert import HTMLExporter
from traitlets.config import Config
from typing import Optional, Dict, Union

from man.notebooker.utils.caching import get_cache, set_cache
from man.notebooker.constants import TEMPLATE_BASE_DIR, PYTHON_TEMPLATE_DIR, REPORT_NAME_SEPARATOR
from man.notebooker.utils.notebook_execution import generate_ipynb_from_py

logger = get_logger(__name__)


def get_directory_structure(starting_point=PYTHON_TEMPLATE_DIR):
    # type: (Optional[str]) -> Dict[str, Union[Dict, None]]
    """
    Creates a nested dictionary that represents the folder structure of rootdir
    """
    all_dirs = {}
    rootdir = starting_point.rstrip(os.sep)
    start = rootdir.rfind(os.sep) + 1
    for path, dirs, files in os.walk(rootdir):
        folders = path[start:].split(os.sep)
        subdir = {os.sep.join(folders[1:] + [f.replace('.py', '')]): None
                  for f in files
                  if '.py' in f and '__init__' not in f}
        parent = reduce(dict.get, folders[:-1], all_dirs)
        parent[folders[-1]] = subdir
    return all_dirs[rootdir[start:]]


def get_all_possible_checks():
    if PYTHON_TEMPLATE_DIR:
        all_checks = get_directory_structure()
    else:
        logger.warn('Fetching all possible checks from local repo. New updates will not be retrieved from git.')
        import notebook_templates
        all_checks = get_directory_structure(os.path.abspath(notebook_templates.__path__[0]))
    return all_checks


def _get_metadata_cell_idx(notebook):
    # type: (nbformat.NotebookNode) -> Optional[int]
    for idx, cell in enumerate(notebook['cells']):
        tags = cell.get('metadata', {}).get('tags', [])
        if 'parameters' in tags:
            return idx
    return None


def _get_preview(report_name):
    # type: (str) -> str
    """ Returns an HTML render of a report template, with parameters highlighted. """
    cached = get_cache(('preview', report_name))
    if cached:
        logger.info('Getting %s preview from cache.', report_name)
        return cached
    path = generate_ipynb_from_py(TEMPLATE_BASE_DIR, report_name)
    nb = nbformat.read(path, as_version=nbformat.v4.nbformat)
    metadata_idx = _get_metadata_cell_idx(nb)
    conf = Config()
    conf.HTMLExporter.template_file = pkg_resources.resource_filename(__name__, '../web/templates/notebook_preview.tpl')
    exporter = HTMLExporter(config=conf)
    html = ''
    if metadata_idx is not None:
        html, _ = exporter.from_notebook_node(nb) if nb['cells'] else ('', '')
    set_cache(('preview', report_name), html, timeout=30)
    return html
