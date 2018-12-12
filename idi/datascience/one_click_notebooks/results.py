import attr
import datetime
import gridfs
import pymongo
from bson import Binary
from enum import Enum, unique

from ahl.logging import get_logger
from ahl.mongo import Mongoose
from ahl.mongo.auth import get_auth, authenticate
from nbconvert.writers import FilesWriter
from typing import AnyStr, Optional, Generator, Any, Dict, Union

from idi.datascience.one_click_notebooks.utils import _output_dir

logger = get_logger(__name__)


@unique
class JobStatus(Enum):
    DONE = 'Checks done!'
    ERROR = 'Error'
    CANCELLED = 'CANCELLED'
    PENDING = 'Running...'
    SUBMITTED = 'Submitted to run'

    @staticmethod
    def from_string(s):
        # type: (AnyStr) -> JobStatus
        mapping = {
            x.value: x
            for x
            in (JobStatus.DONE, JobStatus.ERROR, JobStatus.CANCELLED, JobStatus.PENDING, JobStatus.SUBMITTED)
        }.get(s)
        return mapping


@attr.s()
class NotebookResultBase(object):
    job_id = attr.ib()
    job_start_time = attr.ib()
    report_name = attr.ib()
    status = attr.ib(default=JobStatus.ERROR)

    def saveable_output(self):
        out = attr.asdict(self)
        out['status'] = self.status.value
        return out


@attr.s()
class NotebookResultPending(NotebookResultBase):
    input_json = attr.ib(attr.Factory(dict))
    status = attr.ib(default=JobStatus.PENDING)
    update_time = attr.ib(default=datetime.datetime.now())


@attr.s()
class NotebookResultError(NotebookResultBase):
    input_json = attr.ib(attr.Factory(dict))
    status = attr.ib(default=JobStatus.ERROR)
    error_info = attr.ib(default="")
    update_time = attr.ib(default=datetime.datetime.now())


@attr.s(repr=False)
class NotebookResultComplete(NotebookResultBase):
    job_start_time = attr.ib()
    job_finish_time = attr.ib()
    input_json = attr.ib(attr.Factory(dict))
    raw_html_resources = attr.ib(attr.Factory(dict))
    status = attr.ib(default=JobStatus.DONE)
    raw_ipynb_json = attr.ib(default="")
    raw_html = attr.ib(default="")
    update_time = attr.ib(default=datetime.datetime.now())

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
                'raw_html': self.raw_html,
                'raw_html_resources': self.html_resources(),
                'input_json': self.input_json,
                'job_id': self.job_id,
                'job_start_time': self.job_start_time,
                'job_finish_time': self.job_finish_time,
                'update_time': self.update_time}

    def __repr__(self):
        return 'NotebookResultComplete(job_id={job_id}, status={status}, report_name={report_name}, ' \
               'job_start_time={job_start_time}, job_finish_time={job_finish_time}, update_time={update_time})'.format(
            job_id=self.job_id, status=self.status, report_name=self.report_name, job_start_time=self.job_start_time,
            job_finish_time=self.job_finish_time, update_time=self.update_time
        )


class NotebookResultSerializer(object):
    # This class is the interface between Mongo and the rest of the application
    def __init__(self,
                 database_name='mongoose_restech',
                 mongo_host='research',
                 result_collection_name='NOTEBOOK_OUTPUT'):
        self.result_collection_name = result_collection_name
        self.mongo = Mongoose(mongo_host)._conn[database_name]
        self.library = self.mongo[self.result_collection_name]
        self.result_data_store = gridfs.GridFS(self.mongo, "notebook_data")

        user_creds = get_auth(mongo_host, 'mongoose', database_name)

        authenticate(self.mongo, user_creds.user, user_creds.password)
        self.database = database_name
        self.mongo_host = mongo_host

    def _save_raw_to_db(self, out_data):
        out_data['update_time'] = datetime.datetime.now()
        existing = self.library.find_one({'job_id': out_data['job_id']})
        if existing:
            self.library.replace_one({'_id': existing['_id']}, out_data)
        else:
            self.library.insert_one(out_data)
        # Ensure that the job_id index exists
        self.library.create_index([('job_id', pymongo.ASCENDING)], background=True)

    def _save_to_db(self, notebook_result):
        out_data = notebook_result.saveable_output()
        self._save_raw_to_db(out_data)

    def update_check_status(self, job_id, status, **extra):
        existing = self.library.find_one({'job_id': job_id})
        if not existing:
            logger.warn("Couldn't update check status to {} for job id {} since it is not in the database.".format(
                status, job_id
            ))
        else:
            existing['status'] = status
            for k, v in extra.items():
                existing[k] = v
            self._save_raw_to_db(existing)

    def save_check_stub(self, job_id, report_name, input_json=None, job_start_time=None, status=JobStatus.PENDING):
        # type: (str, str, Optional[Dict[Any, Any]], Optional[datetime.datetime], Optional[JobStatus]) -> None
        # Call this when we are just starting a check
        pending_result = NotebookResultPending(job_id=job_id,
                                               status=status,
                                               input_json=input_json,
                                               job_start_time=job_start_time,
                                               report_name=report_name)
        self._save_to_db(pending_result)

    def save_check_result(self, notebook_result):
        # type: (Union[NotebookResultComplete, NotebookResultError]) -> None
        # Save to mongo
        logger.info('Saving {}'.format(notebook_result.job_id))
        self._save_to_db(notebook_result)

        # Save to gridfs
        if notebook_result.status == JobStatus.DONE and \
                notebook_result.raw_html_resources and \
                'outputs' in notebook_result.raw_html_resources:
            for filename, binary_data in notebook_result.raw_html_resources['outputs'].items():
                self.result_data_store.put(binary_data, filename=filename)

    def get_check_result(self, job_id):
        # type: (AnyStr) -> Optional[NotebookResultBase]
        result = self.library.find_one({'job_id': job_id}, {'_id': 0})
        if not result:
            return None

        status = result.get('status')
        job_status = JobStatus.from_string(status)
        cls = {JobStatus.CANCELLED: NotebookResultError,
               JobStatus.DONE: NotebookResultComplete,
               JobStatus.PENDING: NotebookResultPending,
               JobStatus.ERROR: NotebookResultError,
               JobStatus.SUBMITTED: NotebookResultPending,
               }.get(job_status)
        if job_status == JobStatus.DONE:
            outputs = {}
            for filename in result.get('raw_html_resources', {}).get('outputs', []):
                outputs[filename] = self.result_data_store.get_last_version(filename).read()
            result['raw_html_resources']['outputs'] = outputs

        notebook_result = cls(**result)

        return notebook_result

    def get_all_results(self, since=None, limit=100):
        # type: (Optional[datetime.datetime], Optional[int]) -> Generator[NotebookResultBase]
        if since:
            results = self.library.find({'updated_time': {'$gt': since}}, {'job_id': 1}).limit(limit)
        else:
            results = self.library.find({}, {'job_id': 1}).limit(limit)
        for res in results:
            if res:
                yield self.get_check_result(res['job_id'])

    def delete_result(self, job_id):
        # type: (AnyStr) -> int
        existing = self.library.find_one({'job_id': job_id}, {'_id': 1})
        if existing:
            delete_result = self.library.delete_one({'job_id': job_id})
            return delete_result.deleted_count
        else:
            return 0


def save_output_to_mongo(mongo_host,
                         mongo_library,
                         notebook_result):
    # type: (str, str, NotebookResultComplete) -> None
    serializer = NotebookResultSerializer(mongo_host=mongo_host, result_collection_name=mongo_library)
    serializer.save_check_result(notebook_result)



if __name__ == '__main__':
    import pprint
    pprint.pprint(list(NotebookResultSerializer().get_all_results()))
