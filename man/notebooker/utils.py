import pkgutil

from man.notebooker.constants import TEMPLATE_MODULE_NAME


def _output_dir(output_base_dir, report_name, job_id):
    import os
    return os.path.join(output_base_dir, report_name, job_id)


def _cache_key(report_name, job_id):
    return 'report_name={}&job_id={}'.format(report_name, job_id)


def get_all_possible_checks():
    return list({x.rsplit('.', 1)[1]
                 for (_, x, _)
                 in pkgutil.walk_packages('idi.datascience')
                 if TEMPLATE_MODULE_NAME in x
                 and not x.endswith(TEMPLATE_MODULE_NAME)})
