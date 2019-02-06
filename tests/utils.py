import decorator

from man.notebooker.utils.caching import cache
from man.notebooker.utils.templates import get_all_possible_templates


def cache_blaster(f):
    def blast_it(func, *args, **kwargs):
        cache.clear()
        result = func(*args, **kwargs)
        print "Clearing cache"
        try:
            cache.clear()
        except (IOError, OSError):
            # If the cache has already been blasted, never mind!
            pass
        return result
    return decorator.decorator(blast_it, f)


def _gen_all_templates(template_dict):
    for template_name, children in template_dict.items():
        if children:
            for x in _gen_all_templates(children):  # Replace with "yield from" when we have py3
                yield x
        else:
            yield template_name


def _all_templates():
    return list(_gen_all_templates(get_all_possible_templates(warn_on_local=False)))
