import attr
import datetime
import gridfs
import pymongo
from bson import Binary

from ahl.logging import get_logger
from ahl.mongo import Mongoose
from ahl.mongo.auth import get_auth, authenticate
from nbconvert.writers import FilesWriter

from idi.datascience.one_click_notebooks.utils import _output_dir

logger = get_logger(__name__)


@attr.s()
class NotebookResultBase(object):
    job_id = attr.ib()
    status = attr.ib(default="Not found")

    def saveable_output(self):
        return attr.asdict(self)


@attr.s()
class NotebookResultPending(NotebookResultBase):
    job_start_time = attr.ib()
    input_json = attr.ib(attr.Factory(dict))
    status = attr.ib(default="Running...")
    report_name = attr.ib(default="")


@attr.s()
class NotebookResultError(NotebookResultBase):
    input_json = attr.ib(attr.Factory(dict))
    status = attr.ib(default="ERROR")
    error_info = attr.ib(default="")
    report_name = attr.ib(default="")


@attr.s()
class NotebookResultComplete(NotebookResultBase):
    job_start_time = attr.ib()
    job_finish_time = attr.ib()
    input_json = attr.ib(attr.Factory(dict))
    raw_html_resources = attr.ib(attr.Factory(dict))
    status = attr.ib(default="Done!")
    raw_ipynb_json = attr.ib(default="")
    raw_html = attr.ib(default="")
    report_name = attr.ib(default="")

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
                'status': 'Checks done!',
                'report_name': self.report_name,
                'raw_html': self.raw_html,
                'raw_html_resources': self.html_resources(),
                'input_json': self.input_json,
                'job_id': self.job_id,
                'job_start_time': self.job_start_time,
                'job_finish_time': self.job_finish_time}


class NotebookResultSerializer(object):
    def __init__(self,
                 database_name='mongoose_restech',
                 mongo_host='research',
                 result_collection_name='NOTEBOOK_OUTPUT'):
        self.mongo = Mongoose(mongo_host)._conn[database_name]
        self.result_data_store = gridfs.GridFS(self.mongo, "notebook_data")

        user_creds = get_auth(mongo_host, 'mongoose', database_name)

        authenticate(self.mongo, user_creds.user, user_creds.password)
        self.database = database_name
        self.mongo_host = mongo_host

        self.result_collection_name = result_collection_name

    def _save_raw_to_db(self, out_data):
        out_data['_update_time'] = datetime.datetime.now()
        existing = self.mongo[self.result_collection_name].find_one({'job_id': out_data['job_id']})
        if existing:
            self.mongo[self.result_collection_name].update_one({'_id': existing['_id']},
                                                               {'$set': out_data})
        else:
            self.mongo[self.result_collection_name].insert_one(out_data)
        # Ensure that the job_id index exists
        self.mongo[self.result_collection_name].create_index([('job_id', pymongo.ASCENDING)])

    def _save_to_db(self, notebook_result):
        out_data = notebook_result.saveable_output()
        return self._save_raw_to_db(out_data)

    def update_check_status(self, job_id, status, **extra):
        existing = self.mongo[self.result_collection_name].find_one({'job_id': job_id})
        if not existing:
            logger.warn("Couldn't update check status to {} for job id {} since it is not in the database.".format(
                status, job_id
            ))
        else:
            existing['status'] = status
            for k, v in extra.items():
                existing[k] = v
            self._save_raw_to_db(existing)

    def save_check_stub(self, job_id, report_name, input_json=None, job_start_time=None):
        # Call this when we are just starting a check
        pending_result = NotebookResultPending(job_id=job_id,
                                               status='Running...',
                                               input_json=input_json,
                                               job_start_time=job_start_time,
                                               report_name=report_name)
        self._save_to_db(notebook_result=pending_result)

    def save_check_result(self, notebook_result):
        # Save to mongo
        logger.info('Saving {}'.format(notebook_result.job_id))
        self._save_to_db(notebook_result=notebook_result)

        # Save to gridfs
        if notebook_result.raw_html_resources and 'outputs' in notebook_result.raw_html_resources:
            for filename, binary_data in notebook_result.raw_html_resources['outputs'].items():
                self.result_data_store.put(binary_data, filename=filename)

    def get_check_result(self, job_id, output_base_dir='results/'):
        result = self.mongo[self.result_collection_name].find_one({'job_id': job_id})
        if not result:
            return None

        outputs = {}
        for filename in result.get('raw_html_resources', {}).get('outputs', []):
            outputs[filename] = self.result_data_store.get_last_version(filename).read()
        result['raw_html_resources']['outputs'] = outputs

        notebook_result = NotebookResultComplete(job_id=result['job_id'],
                                                 job_start_time=result['job_start_time'],
                                                 job_finish_time=result['job_finish_time'],
                                                 input_json=result['input_json'],
                                                 raw_html_resources=result['raw_html_resources'],
                                                 raw_ipynb_json=result['raw_ipynb_json'],
                                                 raw_html=result['raw_html'],
                                                 report_name=result['report_name'])

        # writer = FilesWriter()
        # writer.build_directory = _output_dir(output_base_dir, notebook_result.report_name, notebook_result.job_id)
        # html, resources = notebook_result.raw_html, notebook_result.raw_html_resources
        # writer.write(html, resources, notebook_name=notebook_result.report_name+'.ipynb')

        return notebook_result


def save_output_to_mongo(mongo_host,
                         mongo_library,
                         notebook_result):
    serializer = NotebookResultSerializer(mongo_host=mongo_host, result_collection_name=mongo_library)
    serializer.save_check_result(notebook_result)
    return notebook_result


if __name__ == '__main__':
    print NotebookResultSerializer().get_check_result('asdasda')
