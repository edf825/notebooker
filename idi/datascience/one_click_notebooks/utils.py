def _output_dir(output_base_dir, report_name, job_id):
    import os
    return os.path.join(output_base_dir, report_name, job_id)
