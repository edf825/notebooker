def _output_dir(output_base_dir, report_name, job_id):
    import os
    return os.path.join(output_base_dir, report_name, job_id)


def _cache_key(report_name, job_id):
    return 'report_name={}&job_id={}'.format(report_name, job_id)


def cache_key_to_dict(cache_key):
    out = {}
    for assignment in cache_key.split('&'):
        k, v = assignment.split('=')
        out[k] = v
    return out



