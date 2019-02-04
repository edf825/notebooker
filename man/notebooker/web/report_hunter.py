import datetime
import os
import time

from ahl.logging import get_logger

import man.notebooker.serialization.mongoose
from man.notebooker.constants import JobStatus, SUBMISSION_TIMEOUT, RUNNING_TIMEOUT
from man.notebooker.utils.results import _get_job_results, get_all_result_keys
from man.notebooker.utils.caching import get_cache, get_report_cache, set_cache, set_report_cache

logger = get_logger(__name__)


def _report_hunter(mongo_host, database_name, result_collection_name, run_once=False):
    # This is a function designed to run in a thread separately from the webapp. It updates the cache which the
    # web app reads from and performs some admin on pending/running jobs.
    serializer = man.notebooker.serialization.mongoose.NotebookResultSerializer(mongo_host=mongo_host,
                                                                                database_name=database_name,
                                                                                result_collection_name=result_collection_name)
    last_query = None
    while not os.getenv('NOTEBOOKER_APP_STOPPING'):
        try:
            ct = 0
            # First, check we have all keys that are available and populate their entries
            all_keys = get_all_result_keys(serializer)
            for report_name, job_id in all_keys:
                # This method loads from db and saves into the store.
                _get_job_results(job_id, report_name, serializer)

            # Now, get all pending requests and check they haven't timed out...
            all_pending = serializer.get_all_results(mongo_filter={'status': {'$in': [JobStatus.SUBMITTED.value,
                                                                                      JobStatus.PENDING.value]}})
            now = datetime.datetime.now()
            cutoff = {JobStatus.SUBMITTED: now - datetime.timedelta(minutes=SUBMISSION_TIMEOUT),
                      JobStatus.PENDING: now - datetime.timedelta(minutes=RUNNING_TIMEOUT)}
            for result in all_pending:
                this_cutoff = cutoff.get(result.status)
                if result.job_start_time <= this_cutoff:
                    delta_seconds = (now - this_cutoff).total_seconds()
                    serializer.update_check_status(result.job_id, JobStatus.TIMEOUT,
                                                   error_info='This request timed out while being submitted to run. '
                                                              'Please try again! Timed out after {:.0f} minutes '
                                                              '{:.0f} seconds.'.format(delta_seconds/60,
                                                                                       delta_seconds % 60))
            # Finally, check we have the latest updates
            _last_query = datetime.datetime.now() - datetime.timedelta(minutes=1)
            query_results = serializer.get_all_results(since=last_query)
            for result in query_results:
                ct += 1
                existing = get_report_cache(result.report_name, result.job_id)
                if not existing or result.status != existing.status:  # Only update the cache when the status changes
                    set_report_cache(result.report_name, result.job_id, result)
                    logger.info('Report-hunter found a change for {} (status: {}->{})'.format(
                        result.job_id, existing.status if existing else None, result.status))
            logger.info('Found {} updates since {}.'.format(ct, last_query))
            last_query = _last_query
        except Exception as e:
            logger.exception(str(e))
        if run_once:
            break
        time.sleep(10)
    logger.info('Report-hunting thread successfully killed.')
