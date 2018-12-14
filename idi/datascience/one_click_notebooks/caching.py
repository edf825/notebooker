import retrying
from werkzeug.contrib.cache import SimpleCache

from idi.datascience.one_click_notebooks.utils import _cache_key

cache = SimpleCache()


@retrying.retry(stop_max_attempt_number=3)
def _get_cache(key):
    global cache
    # if cache_expiries.get(key) and datetime.datetime.now() > cache_expiries.get(key):
    #     return None
    return cache.get(key)


def get_cache(report_name, job_id):
    return _get_cache(_cache_key(report_name, job_id))


@retrying.retry(stop_max_attempt_number=3)
def _set_cache(key, value, timeout=0):
    global cache
    # if timeout_seconds:
    #     cache_expiries[key] = datetime.datetime.now() + datetime.timedelta(seconds=timeout_seconds)
    # cache[key] = value
    cache.set(key, value, timeout=timeout)


def set_cache(report_name, job_id, value):
    if value:
        return _set_cache(_cache_key(report_name, job_id), value)
