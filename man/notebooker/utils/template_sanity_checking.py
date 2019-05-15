import os

import click

from man.notebooker.constants import TEMPLATE_BASE_DIR
from man.notebooker.utils.conversion import generate_ipynb_from_py
from man.notebooker.utils.notebook_execution import _cleanup_dirs
from man.notebooker.utils.templates import template_name_to_notebook_node, \
    _get_metadata_cell_idx, _get_preview, _all_templates


@click.command()
@click.option('--template-dir', default='notebook_templates')
def main(template_dir):
    os.environ['PY_TEMPLATE_DIR'] = template_dir
    try:
        for template_name in _all_templates():
            print('Testing template: {}'.format(template_name))
            # Test conversion to ipynb - this will throw if stuff goes wrong
            generate_ipynb_from_py(TEMPLATE_BASE_DIR, template_name, warn_on_local=False)

            # Test that each template has parameters as expected
            nb = template_name_to_notebook_node(template_name, warn_on_local=False)
            metadata_idx = _get_metadata_cell_idx(nb)
            assert metadata_idx is not None, 'Template {} does not have a "parameters"-tagged cell.'.format(template_name)

            # Test that we can generate a preview from the template
            preview = _get_preview(template_name, warn_on_local=False)
            # Previews in HTML are gigantic since they include all jupyter css and js.
            assert len(preview) > 1000, 'Preview was not properly generated for {}'.format(template_name)
    finally:
        _cleanup_dirs()


if __name__ == '__main__':
    main()
