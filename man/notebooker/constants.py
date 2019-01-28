import os
import tempfile

from enum import unique, Enum
from typing import AnyStr

SUBMISSION_TIMEOUT = 3
RUNNING_TIMEOUT = 60
OUTPUT_BASE_DIR = os.path.join(os.getenv('OUTPUT_DIR', tempfile.mkdtemp(dir=os.path.expanduser('~'))), 'results')
TEMPLATE_BASE_DIR = os.path.join(os.getenv('TEMPLATE_DIR', tempfile.mkdtemp(dir=os.path.expanduser('~'))), 'templates')
PYTHON_TEMPLATE_DIR = os.path.join(os.environ['PY_TEMPLATE_DIR'],
                                   os.environ['GIT_REPO_TEMPLATE_DIR']
                                   ) if os.getenv('PY_TEMPLATE_DIR') else None  # If not None, we are likely in docker

# NB: These env vars should come from the docker image.
NOTEBOOKER_TEMPLATE_GIT_URL = os.getenv('NOTEBOOKER_TEMPLATE_GIT_URL')

KERNEL_SPEC = {'display_name': os.getenv('NOTEBOOK_KERNEL_NAME', 'man_notebooker_kernel'),
               'language': 'python',
               'name': os.getenv('NOTEBOOK_KERNEL_NAME', 'man_notebooker_kernel')}
CANCEL_MESSAGE = 'The webapp shut down while this job was running. Please resubmit with the same parameters.'


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
