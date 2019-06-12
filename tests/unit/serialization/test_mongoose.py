from mock import patch
from man.notebooker.serialization.mongoose import NotebookResultSerializer, JobStatus


def test_mongo_filter():
    mongo_filter = NotebookResultSerializer._mongo_filter('report')
    assert mongo_filter == {'report_name': 'report'}


def test_mongo_filter_overrides():
    mongo_filter = NotebookResultSerializer._mongo_filter('report', overrides={'b': 1, 'a': 2})
    assert mongo_filter == {'report_name': 'report', 'overrides.a': 2, 'overrides.b': 1}


def test_mongo_filter_status():
    mongo_filter = NotebookResultSerializer._mongo_filter('report', status=JobStatus.DONE)
    assert mongo_filter == {'report_name': 'report', 'status': JobStatus.DONE.value}


@patch('man.notebooker.serialization.mongoose.gridfs')
@patch('man.notebooker.serialization.mongoose.Mongoose')
@patch('man.notebooker.serialization.mongoose.NotebookResultSerializer._get_all_job_ids')
def test_get_latest_job_id_for_name_and_params(_get_all_job_ids, Mongoose, gridfs):
    serializer = NotebookResultSerializer()
    serializer.get_latest_job_id_for_name_and_params('report_name', None)
    _get_all_job_ids.assert_called_once_with('report_name', None, as_of=None, limit=1)


@patch('man.notebooker.serialization.mongoose.gridfs')
@patch('man.notebooker.serialization.mongoose.Mongoose')
def test__get_all_job_ids(Mongoose, gridfs):
    serializer = NotebookResultSerializer()
    serializer._get_all_job_ids('report_name', None, limit=1)
    serializer.library.find.assert_called_once_with({'status': {'$ne': JobStatus.DELETED.value}, 'report_name': 'report_name'},
                                                    {'_id': 0, 'job_id': 1, 'report_name': 1})
