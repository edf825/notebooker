import os
import pkgutil

import nbformat
import pkg_resources
from ahl.logging import get_logger
from nbconvert import HTMLExporter
from traitlets.config import Config
from typing import Optional

from man.notebooker.caching import get_cache, set_cache
from man.notebooker.constants import TEMPLATE_BASE_DIR, PYTHON_TEMPLATE_DIR
from man.notebooker.utils.notebook_execution import generate_ipynb_from_py

logger = get_logger(__name__)


def find_templates():
    all_templates = []
    to_inspect = [PYTHON_TEMPLATE_DIR]
    while to_inspect:
        curr = to_inspect.pop(0)
        try:
            to_inspect += [os.path.join(curr, f) for f in os.listdir(curr)]
        except OSError:
            if os.path.splitext(curr)[1] == '.py' and os.path.basename(curr) != '__init__.py':
                all_templates.append(os.path.relpath(curr, PYTHON_TEMPLATE_DIR).replace('.py', ''))
    return all_templates


def get_all_possible_checks():
    if PYTHON_TEMPLATE_DIR:
        return find_templates()
    logger.warn('Fetching all possible checks from local repo. New updates will not be retrieved from git.')
    import notebook_templates
    pkg_path = notebook_templates.__path__
    return [module for (_, module, _) in pkgutil.iter_modules(pkg_path)]


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
    conf.HTMLExporter.template_file = pkg_resources.resource_filename(__name__, '../templates/notebook_preview.tpl')
    exporter = HTMLExporter(config=conf)
    html = ''
    if metadata_idx is not None:
        html, _ = exporter.from_notebook_node(nb) if nb['cells'] else ('', '')
    set_cache(('preview', report_name), html, timeout=30)
    return html
