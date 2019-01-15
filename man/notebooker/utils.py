import pkgutil

import man.notebooker.notebook_templates


def _output_dir(output_base_dir, report_name, job_id):
    import os
    return os.path.join(output_base_dir, report_name, job_id)


def _cache_key(report_name, job_id):
    return 'report_name={}&job_id={}'.format(report_name, job_id)


def get_all_possible_checks():
    pkg_path = man.notebooker.notebook_templates.__path__
    return [module for (_, module, _) in pkgutil.iter_modules(pkg_path)]
