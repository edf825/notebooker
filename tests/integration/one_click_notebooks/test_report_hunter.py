import datetime
import decorator
import freezegun
import pytest
import uuid

from ahl.mongo import Mongoose
from arctic.store.bson_store import BSON_STORE_TYPE
from werkzeug.contrib.cache import SimpleCache

from idi.datascience.one_click_notebooks.caching import get_cache, cache
from idi.datascience.one_click_notebooks.constants import JobStatus
from idi.datascience.one_click_notebooks.results import NotebookResultSerializer, NotebookResultPending, \
    NotebookResultError, NotebookResultComplete

from idi.datascience.one_click_notebooks.report_hunter import _report_hunter

pytest_plugins = ['ahl.testing.pytest.mongo_server']

TEST_DB_NAME = 'mongoose_restest'
TEST_LIB = 'NB_OUTPUT'


@pytest.fixture
def bson_library(mongo_server, mongo_host):
    m = Mongoose(mongo_host)
    m.initialize_library(TEST_LIB, BSON_STORE_TYPE)
    l = m.get_library(TEST_LIB)
    l.create_index('_id')
    return l


def cache_blaster(f):
    def do_it(func, *args, **kwargs):
        cache.clear()
        result = func(*args, **kwargs)
        print "Clearing cache"
        cache.clear()
        return result
    return decorator.decorator(do_it, f)

@pytest.fixture(scope="function")
def job_id():
    return


@cache_blaster
def test_report_hunter_with_nothing(bson_library, mongo_host):
    _report_hunter(mongo_host, TEST_DB_NAME, TEST_LIB, run_once=True)


@cache_blaster
@freezegun.freeze_time(datetime.datetime(2018, 1, 12))
def test_report_hunter_with_one(bson_library, mongo_host):
    serializer = NotebookResultSerializer(database_name=TEST_DB_NAME,
                                          mongo_host=mongo_host,
                                          result_collection_name=TEST_LIB)

    job_id = str(uuid.uuid4())
    report_name = str(uuid.uuid4())
    serializer.save_check_stub(job_id, report_name)
    _report_hunter(mongo_host, TEST_DB_NAME, TEST_LIB, run_once=True)
    expected = NotebookResultPending(job_id=job_id,
                                     report_name=report_name,
                                     update_time=datetime.datetime(2018, 1, 12),
                                     job_start_time=datetime.datetime(2018, 1, 12))
    assert expected == get_cache(report_name, job_id)


@cache_blaster
def test_report_hunter_with_status_change(bson_library, mongo_host):
    serializer = NotebookResultSerializer(database_name=TEST_DB_NAME,
                                          mongo_host=mongo_host,
                                          result_collection_name=TEST_LIB)

    job_id = str(uuid.uuid4())
    report_name = str(uuid.uuid4())
    with freezegun.freeze_time(datetime.datetime(2018, 1, 12, 2, 30)):
        serializer.save_check_stub(job_id, report_name)
        _report_hunter(mongo_host, TEST_DB_NAME, TEST_LIB, run_once=True)
    expected = NotebookResultPending(job_id=job_id,
                                     report_name=report_name,
                                     update_time=datetime.datetime(2018, 1, 12, 2, 30),
                                     job_start_time=datetime.datetime(2018, 1, 12, 2, 30))
    assert expected == get_cache(report_name, job_id)

    with freezegun.freeze_time(datetime.datetime(2018, 1, 12, 2, 32)):
        serializer.update_check_status(job_id, JobStatus.CANCELLED, error_info='This was cancelled!')
        _report_hunter(mongo_host, TEST_DB_NAME, TEST_LIB, run_once=True)

    expected = NotebookResultError(job_id=job_id,
                                   report_name=report_name,
                                   status=JobStatus.CANCELLED,
                                   update_time=datetime.datetime(2018, 1, 12, 2, 32),
                                   job_start_time=datetime.datetime(2018, 1, 12, 2, 30),
                                   error_info='This was cancelled!'
                                   )
    assert expected == get_cache(report_name, job_id)


@cache_blaster
@pytest.mark.parametrize('status, time_later, should_timeout',
                         [
                             (JobStatus.SUBMITTED, datetime.timedelta(minutes=1), False),
                             (JobStatus.SUBMITTED, datetime.timedelta(minutes=4), True),
                             (JobStatus.PENDING, datetime.timedelta(minutes=4), False),
                             (JobStatus.PENDING, datetime.timedelta(minutes=61), True),
                         ])
def test_report_hunter_timeout(bson_library, mongo_host, status, time_later, should_timeout):
    job_id = str(uuid.uuid4())
    report_name = str(uuid.uuid4())

    serializer = NotebookResultSerializer(database_name=TEST_DB_NAME,
                                          mongo_host=mongo_host,
                                          result_collection_name=TEST_LIB)
    start_time = time_now = datetime.datetime(2018, 1, 12, 2, 30)
    with freezegun.freeze_time(time_now):
        serializer.save_check_stub(job_id, report_name, status=status)
        _report_hunter(mongo_host, TEST_DB_NAME, TEST_LIB, run_once=True)
    expected = NotebookResultPending(job_id=job_id,
                                     report_name=report_name,
                                     status=status,
                                     update_time=time_now,
                                     job_start_time=start_time)
    assert expected == get_cache(report_name, job_id)

    time_now += time_later
    with freezegun.freeze_time(time_now):
        _report_hunter(mongo_host, TEST_DB_NAME, TEST_LIB, run_once=True)

    if should_timeout:
        mins = (time_later.total_seconds() / 60) - 1
        expected = NotebookResultError(job_id=job_id,
                                       report_name=report_name,
                                       status=JobStatus.TIMEOUT,
                                       update_time=time_now,
                                       job_start_time=start_time,
                                       error_info='This request timed out while being submitted to Spark. '
                                                  'Please try again! '
                                                  'Timed out after {:.0f} minutes 0 seconds.'.format(mins)
                                       )
    else:
        # expected does not change
        pass
    assert expected == get_cache(report_name, job_id)


@cache_blaster
def test_report_hunter_pending_to_done(bson_library, mongo_host):
    job_id = str(uuid.uuid4())
    report_name = str(uuid.uuid4())
    serializer = NotebookResultSerializer(database_name=TEST_DB_NAME,
                                          mongo_host=mongo_host,
                                          result_collection_name=TEST_LIB)

    with freezegun.freeze_time(datetime.datetime(2018, 1, 12, 2, 30)):
        serializer.save_check_stub(job_id, report_name, status=JobStatus.SUBMITTED)
        _report_hunter(mongo_host, TEST_DB_NAME, TEST_LIB, run_once=True)
    expected = NotebookResultPending(job_id=job_id,
                                     report_name=report_name,
                                     status=JobStatus.SUBMITTED,
                                     update_time=datetime.datetime(2018, 1, 12, 2, 30),
                                     job_start_time=datetime.datetime(2018, 1, 12, 2, 30))
    assert expected == get_cache(report_name, job_id)

    with freezegun.freeze_time(datetime.datetime(2018, 1, 12, 2, 32)):
        serializer.update_check_status(job_id, JobStatus.PENDING)
        _report_hunter(mongo_host, TEST_DB_NAME, TEST_LIB, run_once=True)

    expected = NotebookResultPending(job_id=job_id,
                                     report_name=report_name,
                                     status=JobStatus.PENDING,
                                     update_time=datetime.datetime(2018, 1, 12, 2, 32),
                                     job_start_time=datetime.datetime(2018, 1, 12, 2, 30))
    assert expected == get_cache(report_name, job_id)

    with freezegun.freeze_time(datetime.datetime(2018, 1, 12, 2, 37)):
        serializer.update_check_status(job_id,
                                       JobStatus.DONE,
                                       raw_html_resources={'outputs':{}},
                                       job_finish_time=datetime.datetime.now(),
                                       raw_ipynb_json='[]',
                                       raw_html='')
        _report_hunter(mongo_host, TEST_DB_NAME, TEST_LIB, run_once=True)

    expected = NotebookResultComplete(job_id=job_id,
                                      report_name=report_name,
                                      status=JobStatus.DONE,
                                      update_time=datetime.datetime(2018, 1, 12, 2, 37),
                                      job_start_time=datetime.datetime(2018, 1, 12, 2, 30),
                                      job_finish_time=datetime.datetime(2018, 1, 12, 2, 37),
                                      raw_html='',
                                      raw_html_resources={'outputs':{}},
                                      raw_ipynb_json='[]'
                                      )
    assert expected == get_cache(report_name, job_id)
