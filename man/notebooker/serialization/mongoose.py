import datetime
import functools
import threading

import gridfs
import pymongo
from ahl.logging import get_logger
from ahl.mongo import Mongoose
from ahl.mongo.auth import authenticate
from ahl.mongo.decorators import mongo_retry
from gridfs import NoFile
from mkd.auth.mongo import get_auth

from man.notebooker.constants import JobStatus, NotebookResultPending, NotebookResultError, NotebookResultComplete

logger = get_logger(__name__)
lock = threading.Lock()


def synchronized(lock):
    """ Synchronization decorator """
    def wrapper(f):
        @functools.wraps(f)
        def inner_wrapper(*args, **kw):
            with lock:
                return f(*args, **kw)
        return inner_wrapper
    return wrapper


class Singleton(type):
    _instances = {}

    @synchronized(lock)
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class NotebookResultSerializer(object):
    __metaclass__ = Singleton
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
        self.database_name = database_name
        self.mongo_host = mongo_host

    @mongo_retry
    def _save_raw_to_db(self, out_data):
        out_data['update_time'] = datetime.datetime.now()
        existing = self.library.find_one({'job_id': out_data['job_id']})
        if existing:
            self.library.replace_one({'_id': existing['_id']}, out_data)
        else:
            self.library.insert_one(out_data)
        # Ensure that the job_id index exists
        self.library.create_index([('job_id', pymongo.ASCENDING)], background=True)
        self.library.create_index([('update_time', pymongo.DESCENDING)], background=True)

    def _save_to_db(self, notebook_result):
        out_data = notebook_result.saveable_output()
        self._save_raw_to_db(out_data)

    @mongo_retry
    def update_check_status(self, job_id, status, **extra):
        # type: (str, JobStatus, **Any) -> None
        existing = self.library.find_one({'job_id': job_id})
        if not existing:
            logger.warn("Couldn't update check status to {} for job id {} since it is not in the database.".format(
                status, job_id
            ))
        else:
            existing['status'] = status.value
            for k, v in extra.items():
                existing[k] = v
            self._save_raw_to_db(existing)

    def save_check_stub(self, job_id, report_name, report_title='',
                        job_start_time=None, status=JobStatus.PENDING):
        # type: (str, str, Optional[str], Optional[datetime.datetime], Optional[JobStatus]) -> None
        # Call this when we are just starting a check
        job_start_time = job_start_time or datetime.datetime.now()
        report_title = report_title or report_name
        pending_result = NotebookResultPending(job_id=job_id,
                                               status=status,
                                               report_title=report_title,
                                               job_start_time=job_start_time,
                                               report_name=report_name)
        self._save_to_db(pending_result)

    def save_check_result(self, notebook_result):
        # type: (Union[NotebookResultComplete, NotebookResultError]) -> None
        # Save to mongo
        logger.info('Saving {}'.format(notebook_result.job_id))
        self._save_to_db(notebook_result)

        # Save to gridfs
        if notebook_result.status == JobStatus.DONE:
            if notebook_result.raw_html_resources and 'outputs' in notebook_result.raw_html_resources:
                for filename, binary_data in notebook_result.raw_html_resources['outputs'].items():
                    self.result_data_store.put(binary_data, filename=filename)
            if notebook_result.pdf:
                self.result_data_store.put(notebook_result.pdf, filename=_pdf_filename(notebook_result.job_id))

    @mongo_retry
    def get_check_result(self, job_id):
        # type: (AnyStr) -> Optional[Union[NotebookResultError, NotebookResultComplete, NotebookResultPending]]
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
               JobStatus.TIMEOUT: NotebookResultError,
               JobStatus.DELETED: None
               }.get(job_status)
        if cls is None:
            return None
        if job_status == JobStatus.DONE:
            outputs = {}
            for filename in result.get('raw_html_resources', {}).get('outputs', []):
                outputs[filename] = self.result_data_store.get_last_version(filename).read()
            result['raw_html_resources']['outputs'] = outputs
            pdf_filename = _pdf_filename(job_id)
            try:
                result['pdf'] = self.result_data_store.get_last_version(pdf_filename).read()
            except NoFile:
                pass

        if cls == NotebookResultComplete:
            notebook_result = NotebookResultComplete(
                job_id=result['job_id'],
                job_start_time=result['job_start_time'],
                report_name=result['report_name'],
                status=job_status,
                update_time=result['update_time'],
                job_finish_time=result['job_finish_time'],
                raw_html_resources=result['raw_html_resources'],
                raw_ipynb_json=result['raw_ipynb_json'],
                raw_html=result['raw_html'],
                pdf=result.get('pdf', ''),
                report_title=result.get('report_title', result['report_name']),
            )
        elif cls == NotebookResultPending:
            notebook_result = NotebookResultPending(
                job_id=result['job_id'],
                job_start_time=result['job_start_time'],
                report_name=result['report_name'],
                status=job_status,
                update_time=result['update_time'],
                report_title=result.get('report_title', result['report_name']),
            )

        elif cls == NotebookResultError:
            notebook_result = NotebookResultError(
                job_id=result['job_id'],
                job_start_time=result['job_start_time'],
                report_name=result['report_name'],
                status=job_status,
                update_time=result['update_time'],
                error_info=result['error_info'],
                report_title=result.get('report_title', result['report_name']),
            )
        else:
            raise ValueError('Could not deserialise {} into result object.'.format(result))

        return notebook_result

    @mongo_retry
    def get_all_results(self, since=None, limit=100, mongo_filter=None):
        # type: (Optional[datetime.datetime], Optional[int], Optional[Dict]) -> Generator[NotebookResultBase]
        base_filter = {'status': {'$ne': JobStatus.DELETED.value}}
        if mongo_filter:
            base_filter.update(mongo_filter)
        if since:
            base_filter.update({'update_time': {'$gt': since}})
        results = self.library.find(base_filter, {'job_id': 1}).limit(limit)
        for res in results:
            if res:
                yield self.get_check_result(res['job_id'])

    @mongo_retry
    def get_all_result_keys(self, limit=0):
        # type: (Optional[int]) -> List[Tuple[str, str]]
        keys = []
        query = {'status': {'$ne': JobStatus.DELETED.value}}
        projection = {'report_name': 1, 'job_id': 1, '_id': 0}
        for result in self.library.find(query, projection).sort('update_time', -1).limit(limit):
            keys.append((result['report_name'], result['job_id']))
        return keys

    @mongo_retry
    def n_all_results(self):
        return self.library.find({'status': {'$ne': JobStatus.DELETED.value}}).count()

    def delete_result(self, job_id):
        # type: (AnyStr) -> None
        self.update_check_status(job_id, JobStatus.DELETED)


def _pdf_filename(job_id):
    # type: (str) -> str
    return '{}.pdf'.format(job_id)
