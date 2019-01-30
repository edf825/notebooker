# -*- coding: utf-8 -*-
import datetime
import shutil
import tempfile

import mock
import os

from man.notebooker.caching import set_cache, get_cache
from man.notebooker.constants import TEMPLATE_BASE_DIR

from man.notebooker.results import NotebookResultComplete
from man.notebooker.utils import notebook_execution
from tests.utils import cache_blaster


def test_send_result_email_unicode_overload():
    body_in = u'<body><h1>hello  😆 😉 😊 😋 😎</h1></body>'
    result = NotebookResultComplete(job_id=u'aåß∂å∂',
                                    job_start_time=datetime.datetime.now(),
                                    job_finish_time=datetime.datetime.now(),
                                    raw_html_resources={},
                                    raw_ipynb_json={},
                                    raw_html=body_in,
                                    pdf='',
                                    report_name=u'®eπºr† ñaµé',
                                    report_title=u'😒 😓 😔 ',
                                    )
    to_email = u'∫åññîsté®@ahl.com'
    with mock.patch('man.notebooker.utils.notebook_execution.mail') as mail:
        notebook_execution.send_result_email(result, to_email)
    email_sent = mail.mock_calls[0][1]
    assert len(email_sent) == 4, 'mail() was not called with the correct number of args'
    from_address = email_sent[0]
    to_address = email_sent[1]
    title = email_sent[2]
    body = email_sent[3]

    assert from_address == 'man.notebooker@man.com'
    assert to_address == to_email
    assert title == u'Notebooker: 😒 😓 😔  report completed with status: Checks done!'
    assert body == ['Please either activate HTML emails, or see the PDF attachment.', body_in]


@cache_blaster
def test_generate_ipynb_from_py():
    set_cache('latest_sha', 'fake_sha_early')

    python_dir = tempfile.mkdtemp()

    os.mkdir(python_dir + '/extra_path')
    with open(os.path.join(python_dir, 'extra_path', 'test_report.py'), 'w') as f:
        f.write('#hello world\n')

    with mock.patch('man.notebooker.utils.notebook_execution._git_pull_templates') as pull:
        notebook_execution.PYTHON_TEMPLATE_DIR = python_dir
        pull.return_value = 'fake_sha_early'
        notebook_execution.generate_ipynb_from_py(TEMPLATE_BASE_DIR, 'extra_path/test_report')
        pull.return_value = 'fake_sha_later'
        notebook_execution.generate_ipynb_from_py(TEMPLATE_BASE_DIR, 'extra_path/test_report')
        notebook_execution.generate_ipynb_from_py(TEMPLATE_BASE_DIR, 'extra_path/test_report')

    assert get_cache('latest_sha') == 'fake_sha_later'
    expected_ipynb_path = os.path.join(
        TEMPLATE_BASE_DIR,
        'fake_sha_early',
        'extra_path',
        'test_report.ipynb'
    )
    assert os.path.exists(expected_ipynb_path), '.ipynb was not generated as expected!'
    expected_ipynb_path = os.path.join(
        TEMPLATE_BASE_DIR,
        'fake_sha_later',
        'extra_path',
        'test_report.ipynb'
    )
    assert os.path.exists(expected_ipynb_path), '.ipynb was not generated as expected!'

    shutil.rmtree(TEMPLATE_BASE_DIR)
    shutil.rmtree(python_dir)
