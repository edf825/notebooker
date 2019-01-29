import decorator

from man.notebooker.utils.caching import cache


def cache_blaster(f):
    def do_it(func, *args, **kwargs):
        cache.clear()
        result = func(*args, **kwargs)
        print "Clearing cache"
        cache.clear()
        return result
    return decorator.decorator(do_it, f)
