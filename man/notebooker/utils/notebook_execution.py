import errno
import shutil

import os
import tempfile

import re
from ahl.mail import mail
from typing import AnyStr, Union

from ahl.logging import get_logger
from man.notebooker.constants import REPORT_NAME_SEPARATOR, NotebookResultError, \
    NotebookResultComplete

logger = get_logger(__name__)


def _output_dir(output_base_dir, report_name, job_id):
    return os.path.join(output_base_dir, report_name, job_id)


def send_result_email(result, mailto):
    # type: (Union[NotebookResultComplete, NotebookResultError], AnyStr) -> None
    from_email = 'man.notebooker@man.com'
    to_email = mailto
    report_title = result.report_title.decode('utf-8') if isinstance(result.report_title, bytes) else result.report_title
    subject = u'Notebooker: {} report completed with status: {}'.format(report_title, result.status.value)
    body = result.raw_html
    attachments = []
    tmp_dir = tempfile.mkdtemp(dir=os.path.expanduser('~'))

    if isinstance(result, NotebookResultComplete):
        # Attach PDF output to the email. Has to be saved to disk temporarily for the mail API to work.
        report_name = result.report_name.replace(os.sep, REPORT_NAME_SEPARATOR)
        if isinstance(report_name, bytes):
            report_name = report_name.decode('utf-8')
        pdf_name = u'{}_{}.pdf'.format(report_name, result.job_start_time.strftime('%Y-%m-%dT%H%M%S'))
        pdf_path = os.path.join(tmp_dir, pdf_name)
        with open(pdf_path, 'w') as f:
            f.write(result.pdf)
        attachments.append(pdf_path)

        # Embed images into the email as attachments with "cid" links.
        for resource_path, resource in result.raw_html_resources.get('outputs', {}).items():
            resource_path_short = resource_path.rsplit(os.sep, 1)[1]
            new_path = os.path.join(tmp_dir, resource_path_short)
            with open(new_path, 'w') as f:
                f.write(resource)

            body = re.sub(r'<img src="{}"'.format(resource_path),
                          r'<img src="cid:{}"'.format(resource_path_short),
                          body)
            attachments.append(new_path)

    msg = ['Please either activate HTML emails, or see the PDF attachment.', body]

    logger.info(u'Sending email to %s with %d attachments', mailto, len(attachments))
    mail(from_email, to_email, subject, msg, attachments=attachments)

    shutil.rmtree(tmp_dir)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
