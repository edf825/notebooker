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
