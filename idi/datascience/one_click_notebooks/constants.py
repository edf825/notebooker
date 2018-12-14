import os
import tempfile

from enum import unique, Enum


SUBMISSION_TIMEOUT = 3
RUNNING_TIMEOUT = 60
# TODO: Figure out how we can get the below to work without expanduser, which is required by Spark.
OUTPUT_BASE_DIR = os.path.join(os.getenv('OUTPUT_DIR', tempfile.mkdtemp(dir=os.path.expanduser('~'))), 'results')
TEMPLATE_BASE_DIR = os.path.join(os.getenv('OUTPUT_DIR', tempfile.mkdtemp(dir=os.path.expanduser('~'))), 'templates')
MONGO_HOST = 'research'
MONGO_LIBRARY = 'NOTEBOOK_OUTPUT'
TEMPLATE_MODULE_NAME = 'notebook_templates'
_IS_ALIVE = True


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
