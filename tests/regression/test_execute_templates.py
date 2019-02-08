import datetime
import uuid

import git
import os
import pytest

from man.notebooker.constants import OUTPUT_BASE_DIR, TEMPLATE_BASE_DIR

from man.notebooker.execute_notebook import run_checks
from man.notebooker.utils.notebook_execution import _cleanup_dirs
from ..utils import _all_templates


@pytest.mark.parametrize('template_name', _all_templates())
def test_execution_of_templates(template_name):
    try:
        run_checks('job_id_{}'.format(str(uuid.uuid4())[:6]),
                   datetime.datetime.now(),
                   template_name,
                   template_name,
                   OUTPUT_BASE_DIR,
                   TEMPLATE_BASE_DIR,
                   {},
                   generate_pdf_output=False)
    finally:
        _cleanup_dirs()
