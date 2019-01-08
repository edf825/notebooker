import retrying
from werkzeug.contrib.cache import SimpleCache

from man.notebooker.utils import _cache_key

cache = SimpleCache()


@retrying.retry(stop_max_attempt_number=3)
def _get_cache(key):
    global cache
    return cache.get(key)


def get_cache(report_name, job_id):
    return _get_cache(_cache_key(report_name, job_id))


@retrying.retry(stop_max_attempt_number=3)
def _set_cache(key, value, timeout=0):
    global cache
    cache.set(key, value, timeout=timeout)


def set_cache(report_name, job_id, value):
    if value:
        return _set_cache(_cache_key(report_name, job_id), value)
