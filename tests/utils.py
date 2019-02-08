import decorator

from werkzeug.contrib.cache import SimpleCache

from man.notebooker.utils import caching
from man.notebooker.utils.templates import get_all_possible_templates


def cache_blaster(f):
    def blast_it(func, *args, **kwargs):
        caching.cache = SimpleCache()
        result = func(*args, **kwargs)
        caching.cache.clear()
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
