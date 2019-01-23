import pkgutil

import nbformat
import pkg_resources
from nbconvert import HTMLExporter
from traitlets.config import Config
from typing import Optional

import man.notebooker.notebook_templates
from man.notebooker.caching import get_cache, set_cache
from man.notebooker.constants import TEMPLATE_BASE_DIR
from man.notebooker.utils.notebook_execution import generate_ipynb_from_py


def get_all_possible_checks():
    pkg_path = man.notebooker.notebook_templates.__path__
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
    set_cache(('preview', report_name), html, timeout=120)
    return html
