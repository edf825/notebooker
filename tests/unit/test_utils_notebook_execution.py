# -*- coding: utf-8 -*-
import datetime

import mock

from man.notebooker.results import NotebookResultComplete
from man.notebooker.utils.notebook_execution import send_result_email


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
        send_result_email(result, to_email)
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
    # (
    #     from_address='man.notebooker@man.com',
    #     to_address=to_email,
    #     subject=u'Notebooker: 😒 😓 😔  report completed with status: Checks done!',
    #     msg=
    # )
