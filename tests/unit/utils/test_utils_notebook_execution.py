# -*- coding: utf-8 -*-
import datetime

import mock
import pytest

from man.notebooker.constants import NotebookResultComplete

from man.notebooker.utils import notebook_execution


@pytest.mark.parametrize('test_name, job_id, report_name, report_title, expected_title, utf8encode', [
    (
        'unicode_overload',
        u'aåß∂å∂',
        u'®eπºr† ñaµé',
        u'😒 😓 😔 ',
        u'Notebooker: 😒 😓 😔  report completed with status: Checks done!',
        False,
    ), (
        'unicode overload, encoded',
        u'aåß∂å∂',
        u'®eπºr† ñaµé',
        u'😒 😓 😔 ',
        u'Notebooker: 😒 😓 😔  report completed with status: Checks done!',
        True,
    ), (
        'ascii only',
        'my job id',
        'my report name',
        'my report title',
        u'Notebooker: my report title report completed with status: Checks done!',
        False,
    ), (
        'ascii only, encoded',
        'my job id',
        'my report name',
        'my report title',
        u'Notebooker: my report title report completed with status: Checks done!',
        True,
    )
])
def test_send_result_email(test_name, job_id, report_name, report_title, expected_title, utf8encode):
    body_in = u'<body><h1>hello  😆 😉 😊 😋 😎</h1></body>'
    job_id = job_id.encode('utf-8') if utf8encode else job_id
    report_name = report_name.encode('utf-8') if utf8encode else report_name
    report_title = report_title.encode('utf-8') if utf8encode else report_title
    result = NotebookResultComplete(job_id=job_id,
                                    job_start_time=datetime.datetime.now(),
                                    job_finish_time=datetime.datetime.now(),
                                    raw_html_resources={},
                                    raw_ipynb_json={},
                                    raw_html=body_in,
                                    pdf='',
                                    report_name=report_name,
                                    report_title=report_title,
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
    assert title == expected_title
    assert body == ['Please either activate HTML emails, or see the PDF attachment.', body_in]


