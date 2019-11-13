import datetime

import attr
import os
import tempfile

from enum import unique, Enum
from typing import AnyStr, Optional

SUBMISSION_TIMEOUT = 3
RUNNING_TIMEOUT = 60
OUTPUT_BASE_DIR = os.getenv('OUTPUT_DIR') or tempfile.mkdtemp(dir=os.path.expanduser('~'))
TEMPLATE_BASE_DIR = os.getenv('TEMPLATE_DIR') or tempfile.mkdtemp(dir=os.path.expanduser('~'))
CACHE_DIR = tempfile.mkdtemp()
NOTEBOOKER_TEMPLATE_GIT_URL = os.getenv('NOTEBOOKER_TEMPLATE_GIT_URL')
NOTEBOOKER_DISABLE_GIT = os.getenv('NOTEBOOKER_DISABLE_GIT')
CANCEL_MESSAGE = 'The webapp shut down while this job was running. Please resubmit with the same parameters.'
REPORT_NAME_SEPARATOR = '|'


def kernel_spec():
    return {'display_name': os.getenv('NOTEBOOK_KERNEL_NAME', 'man_notebooker_kernel'),
            'language': 'python',
            'name': os.getenv('NOTEBOOK_KERNEL_NAME', 'man_notebooker_kernel')}


def python_template_dir():
    # type: () -> Optional[str]
    if os.getenv('PY_TEMPLATE_DIR'):
        return os.path.join(
            os.environ['PY_TEMPLATE_DIR'],
            os.environ.get('GIT_REPO_TEMPLATE_DIR', '')
        )
    return None


@unique
class JobStatus(Enum):
    DONE = 'Checks done!'
    ERROR = 'Error'
    CANCELLED = 'CANCELLED'
    PENDING = 'Running...'
    SUBMITTED = 'Submitted to run'
    TIMEOUT = 'Report timed out. Please try again!'
    DELETED = 'This report has been deleted.'

    @staticmethod
    def from_string(s):
        # type: (AnyStr) -> JobStatus
        mapping = {
            x.value: x
            for x
            in (JobStatus.DONE, JobStatus.ERROR, JobStatus.CANCELLED, JobStatus.PENDING, JobStatus.SUBMITTED,
                JobStatus.TIMEOUT, JobStatus.DELETED)
        }.get(s)
        return mapping


# Variables for inputs from web
EMAIL_SPACE_ERR_MSG = 'The email address specified had whitespace! Please fix this before resubmitting.'
FORBIDDEN_INPUT_CHARS = list('"')
FORBIDDEN_CHAR_ERR_MSG = 'This report has an invalid input ({}) - it must not contain any of {}.'


@attr.s()
class NotebookResultBase(object):
    job_id = attr.ib()
    job_start_time = attr.ib()
    report_name = attr.ib()
    status = attr.ib(default=JobStatus.ERROR)
    overrides = attr.ib(default=attr.Factory(dict))
    mailto = attr.ib(default='')
    generate_pdf_output = attr.ib(default=True)
    stdout = attr.ib(default=attr.Factory(list))

    def saveable_output(self):
        out = attr.asdict(self)
        out['status'] = self.status.value
        return out


@attr.s()
class NotebookResultPending(NotebookResultBase):
    status = attr.ib(default=JobStatus.PENDING)
    update_time = attr.ib(default=datetime.datetime.now())
    report_title = attr.ib(default='')
    overrides = attr.ib(default=attr.Factory(dict))
    mailto = attr.ib(default='')
    generate_pdf_output = attr.ib(default=True)


@attr.s()
class NotebookResultError(NotebookResultBase):
    status = attr.ib(default=JobStatus.ERROR)
    error_info = attr.ib(default="")
    update_time = attr.ib(default=datetime.datetime.now())
    report_title = attr.ib(default='')
    overrides = attr.ib(default=attr.Factory(dict))
    mailto = attr.ib(default='')
    generate_pdf_output = attr.ib(default=True)

    @property
    def raw_html(self):
        return """<p>This job resulted in an error: <br/><code style="white-space: pre-wrap;">{}</code></p>""".format(
            self.error_info)


@attr.s(repr=False)
class NotebookResultComplete(NotebookResultBase):
    job_start_time = attr.ib()
    job_finish_time = attr.ib()
    raw_html_resources = attr.ib(attr.Factory(dict))
    status = attr.ib(default=JobStatus.DONE)
    raw_ipynb_json = attr.ib(default="")
    raw_html = attr.ib(default="")
    update_time = attr.ib(default=datetime.datetime.now())
    pdf = attr.ib(default="")
    report_title = attr.ib(default='')
    overrides = attr.ib(default=attr.Factory(dict))
    mailto = attr.ib(default='')
    generate_pdf_output = attr.ib(default=True)
    stdout = attr.ib(default=attr.Factory(list))

    def html_resources(self):
        # We have to save the raw images using Mongo GridFS - figure out where they will go here
        resources = {}
        for k, v in self.raw_html_resources.items():
            if k == 'outputs':
                resources[k] = list(v)
            else:
                resources[k] = v
        return resources

    def saveable_output(self):
        return {'raw_ipynb_json': self.raw_ipynb_json,
                'status': self.status.value,
                'report_name': self.report_name,
                'report_title': self.report_title,
                'raw_html': self.raw_html,
                'raw_html_resources': self.html_resources(),
                'job_id': self.job_id,
                'job_start_time': self.job_start_time,
                'job_finish_time': self.job_finish_time,
                'mailto': self.mailto,
                'overrides': self.overrides,
                'generate_pdf_output': self.generate_pdf_output,
                'update_time': self.update_time,
                }

    def __repr__(self):
        return 'NotebookResultComplete(job_id={job_id}, status={status}, report_name={report_name}, ' \
               'job_start_time={job_start_time}, job_finish_time={job_finish_time}, update_time={update_time}, ' \
               'report_title={report_title}, overrides={overrides}, mailto={mailto}, ' \
               'generate_pdf_output={generate_pdf_output})'.format(
            job_id=self.job_id, status=self.status, report_name=self.report_name, job_start_time=self.job_start_time,
            job_finish_time=self.job_finish_time, update_time=self.update_time, report_title=self.report_title,
            overrides=self.overrides, mailto=self.mailto, generate_pdf_output=self.generate_pdf_output
        )
