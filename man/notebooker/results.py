import attr
import datetime
import gridfs
import pymongo

from ahl.logging import get_logger
from ahl.mongo import Mongoose
from ahl.mongo.auth import get_auth, authenticate
from ahl.mongo.decorators import mongo_retry
from flask import url_for
from gridfs import NoFile
from typing import AnyStr, Optional, Generator, Any, Dict, Union, List, Tuple

from man.notebooker.caching import get_report_cache, set_report_cache, get_cache, set_cache
from man.notebooker.constants import JobStatus
from man.notebooker.exceptions import NotebookRunException


logger = get_logger(__name__)


@attr.s()
class NotebookResultBase(object):
    job_id = attr.ib()
    job_start_time = attr.ib()
    report_name = attr.ib()
    status = attr.ib(default=JobStatus.ERROR.value)

    def saveable_output(self):
        out = attr.asdict(self)
        out['status'] = self.status.value
        return out


@attr.s()
class NotebookResultPending(NotebookResultBase):
    status = attr.ib(default=JobStatus.PENDING)
    update_time = attr.ib(default=datetime.datetime.now())


@attr.s()
class NotebookResultError(NotebookResultBase):
    status = attr.ib(default=JobStatus.ERROR)
    error_info = attr.ib(default="")
    update_time = attr.ib(default=datetime.datetime.now())


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


def _pdf_filename(job_id):
    # type: (str) -> str
    return '{}.pdf'.format(job_id)


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

    def save_check_stub(self, job_id, report_name, job_start_time=None, status=JobStatus.PENDING):
        # type: (str, str,Optional[datetime.datetime], Optional[JobStatus]) -> None
        # Call this when we are just starting a check
        job_start_time = job_start_time or datetime.datetime.now()
        pending_result = NotebookResultPending(job_id=job_id,
                                               status=status,
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
            )
        elif cls == NotebookResultPending:
            notebook_result = NotebookResultPending(
                job_id=result['job_id'],
                job_start_time=result['job_start_time'],
                report_name=result['report_name'],
                status=job_status,
                update_time=result['update_time'],
            )

        elif cls == NotebookResultError:
            notebook_result = NotebookResultError(
                job_id=result['job_id'],
                job_start_time=result['job_start_time'],
                report_name=result['report_name'],
                status=job_status,
                update_time=result['update_time'],
                error_info=result['error_info'],
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
        return self.library.find().count()

    def delete_result(self, job_id):
        # type: (AnyStr) -> None
        self.update_check_status(job_id, JobStatus.DELETED)


def _get_job_results(job_id,            # type: str
                     report_name,       # type: str
                     serializer,        # type: NotebookResultSerializer
                     retrying=False     # type: Optional[bool]
                     ):
    # type: (...) -> Union[NotebookResultError, NotebookResultComplete, NotebookResultPending]
    current_result = get_report_cache(report_name, job_id)
    if current_result:
        logger.debug('Cache hit for job_id=%s, report_name=%s', job_id, report_name)
        notebook_result = current_result
    else:
        notebook_result = serializer.get_check_result(job_id)
        set_report_cache(report_name, job_id, notebook_result)

    if not notebook_result:
        err_info = 'Job results not found for report name={} / job id={}. ' \
                 'Did you use an invalid job ID?'.format(report_name, job_id)
        return NotebookResultError(job_id, error_info=err_info, report_name=report_name,
                                   job_start_time=datetime.datetime.now())
    if isinstance(notebook_result, str):
        if not retrying:
            return _get_job_results(job_id, report_name, serializer, retrying=True)
        raise NotebookRunException('An unexpected string was found as a result. Please run your request again.')

    return notebook_result


def _get_all_result_keys(serializer, limit=0):
    # type: (NotebookResultSerializer, Optional[int]) -> List[Tuple[str, str]]
    all_keys = get_cache(('all_result_keys', limit))
    if not all_keys:
        all_keys = serializer.get_all_result_keys(limit=limit)
        set_cache(('all_result_keys', limit), all_keys, timeout=1)
    return all_keys


def all_available_results(serializer,  # type: NotebookResultSerializer
                          limit=50,
                          ):
    # type: (...) -> Dict[Tuple[str, str], Union[NotebookResultError, NotebookResultComplete, NotebookResultPending]]
    all_keys = _get_all_result_keys(serializer, limit=limit)
    complete_jobs = {}
    for report_name, job_id in all_keys:
        result = _get_job_results(job_id, report_name, serializer)
        report_name, job_id = result.report_name, result.job_id
        result.result_url = url_for('task_results', task_id=job_id, report_name=report_name)
        result.ipynb_url = url_for('download_ipynb_result', task_id=job_id, report_name=report_name)
        result.pdf_url = url_for('download_pdf_result', task_id=job_id, report_name=report_name)
        complete_jobs[(report_name, job_id)] = result
    return complete_jobs


def save_output_to_mongo(mongo_host,
                         mongo_library,
                         notebook_result):
    # type: (str, str, NotebookResultComplete) -> None
    serializer = NotebookResultSerializer(mongo_host=mongo_host, result_collection_name=mongo_library)
    serializer.save_check_result(notebook_result)
