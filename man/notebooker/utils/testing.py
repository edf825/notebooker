import datetime
import os

import click
import uuid
from ahl.logging import get_logger
from man.notebooker.exceptions import NotebookRunException

from man.notebooker.constants import OUTPUT_BASE_DIR, TEMPLATE_BASE_DIR
from man.notebooker.execute_notebook import run_checks
from man.notebooker.utils.conversion import generate_ipynb_from_py
from man.notebooker.utils.notebook_execution import _cleanup_dirs
from man.notebooker.utils.templates import (
    template_name_to_notebook_node,
    _get_parameters_cell_idx,
    _get_preview,
    _all_templates,
)

logger = get_logger(__name__)


@click.command()
@click.option("--template-dir", default="notebook_templates")
def sanity_check(template_dir):
    logger.info("Starting sanity check")
    os.environ["PY_TEMPLATE_DIR"] = template_dir
    try:
        for template_name in _all_templates():
            logger.info("========================[ Sanity checking {} ]========================".format(template_name))
            # Test conversion to ipynb - this will throw if stuff goes wrong
            generate_ipynb_from_py(TEMPLATE_BASE_DIR, template_name, warn_on_local=False)

            # Test that each template has parameters as expected
            nb = template_name_to_notebook_node(template_name, warn_on_local=False)
            param_idx = _get_parameters_cell_idx(nb)
            if param_idx is None:
                logger.warn('Template {} does not have a "parameters"-tagged cell.'.format(template_name))

            # Test that we can generate a preview from the template
            preview = _get_preview(template_name, warn_on_local=False)
            # Previews in HTML are gigantic since they include all jupyter css and js.
            assert len(preview) > 1000, "Preview was not properly generated for {}".format(template_name)
            logger.info("========================[ PASSED ]========================".format(template_name))
    finally:
        _cleanup_dirs()


@click.command()
@click.option("--template-dir", default="notebook_templates")
def regression_test(template_dir):
    logger.info("Starting regression test")
    os.environ["PY_TEMPLATE_DIR"] = template_dir
    try:
        attempted_templates, failed_templates = [], set()
        for template_name in _all_templates():
            logger.info("============================[ Testing {} ]============================".format(template_name))
            try:
                attempted_templates.append(template_name)
                run_checks(
                    "job_id_{}".format(str(uuid.uuid4())[:6]),
                    datetime.datetime.now(),
                    template_name,
                    template_name,
                    OUTPUT_BASE_DIR,
                    TEMPLATE_BASE_DIR,
                    {},
                    generate_pdf_output=False,
                )
                logger.info("===============================[ SUCCESS ]==============================")
            except Exception as e:
                failed_templates.add(template_name)
                logger.info("===============================[ FAILED ]===============================")
                logger.exception("Failed to execute template {}".format(template_name))

        for template in attempted_templates:
            logger.info("{}: {}".format(template, "FAILED" if template in failed_templates else "PASSED"))
        if len(failed_templates) > 0:
            raise NotebookRunException(
                "The following templates failed to execute with no parameters:\n{}".format("\n".join(failed_templates))
            )
    finally:
        _cleanup_dirs()


if __name__ == "__main__":
    # sanity_check()
    regression_test()
