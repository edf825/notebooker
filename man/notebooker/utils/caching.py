import retrying
from werkzeug.contrib.cache import SimpleCache

cache = SimpleCache()


def _cache_key(report_name, job_id):
    return 'report_name={}&job_id={}'.format(report_name, job_id)


@retrying.retry(stop_max_attempt_number=3)
def get_cache(key):
    global cache
    return cache.get(key)


def get_report_cache(report_name, job_id):
    return get_cache(_cache_key(report_name, job_id))


@retrying.retry(stop_max_attempt_number=3)
def set_cache(key, value, timeout=0):
    global cache
    cache.set(key, value, timeout=timeout)


def set_report_cache(report_name, job_id, value):
    if value:
        return set_cache(_cache_key(report_name, job_id), value)
