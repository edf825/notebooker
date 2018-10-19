# This module is meant to be used primarily by Spark nodes, hence local imports *everywhere*


def ipython_to_html(ipynb_path, job_id, out_dir, ipynb_name):
    import nbformat
    from nbconvert import HTMLExporter
    from nbconvert.writers import FilesWriter
    from traitlets.config import Config
    c = Config()
    c.HTMLExporter.preprocessors = ['nbconvert.preprocessors.ExtractOutputPreprocessor']
    html_exporter_with_figs = HTMLExporter(config=c)

    writer = FilesWriter()
    with open(ipynb_path, 'r') as nb_file:
        nb = nbformat.reads(nb_file.read(), as_version=nbformat.v4.nbformat)
    resources_dir = '{}/resources'.format(job_id)
    html, resources = html_exporter_with_figs.from_notebook_node(nb, resources={'output_files_dir': resources_dir})

    writer.build_directory = out_dir
    r = writer.write(html, resources, notebook_name=ipynb_name)
    return r


def _output_ipynb_name(job_id, report_name):
    return '{}_{}.ipynb'.format(report_name, job_id)


def _output_dir(job_id, output_base_dir, report_name):
    import os
    return os.path.join(output_base_dir, report_name, job_id)


def _output_status(job_id, report_name, output_base_dir):
    ipynb_name = _output_ipynb_name(job_id, report_name)
    return {'ipynb_result': ipynb_name,
            'status': 'Checks done!',
            'report_name': report_name,
            'html_result_dir': _output_dir(job_id, output_base_dir, report_name),
            'html_result_filename': '{}.html'.format(ipynb_name)}


def _python_template(report_name):
    return 'notebook_templates/{}.py'.format(report_name)


def _ipynb_output_path(report_name):
    return 'notebook_templates/{}.ipynb'.format(report_name)


def run_checks(job_id, report_name, output_base_dir, input_json):
    import datetime
    import jupytext
    import os
    import papermill as pm
    import pkg_resources
    from ahl.logging import get_logger

    logger = get_logger(__name__)

    output_dir = _output_dir(job_id, output_base_dir, report_name)
    output_ipynb = _output_ipynb_name(job_id, report_name)

    if not os.path.isdir(output_dir):
        logger.info('Making dir @ {}'.format(output_dir))
        os.makedirs(output_dir)

    python_input_filename = _python_template(report_name)
    ipynb_output_filename = _ipynb_output_path(report_name)

    # "touch" the output file
    with open(ipynb_output_filename, 'a') as f:
        os.utime(ipynb_output_filename, None)

    python_template = pkg_resources.resource_filename(__name__, python_input_filename)
    jupytext.writef(jupytext.readf(python_template), ipynb_output_filename)
    output_ipynb_path = os.path.join(output_dir, output_ipynb)

    logger.info('Executing notebook at {} using parameters {} --> {}'.format(ipynb_output_filename, input_json, output_ipynb))
    pm.execute_notebook(ipynb_output_filename,
                        output_ipynb_path,
                        parameters=input_json,
                        log_output=True)

    logger.info('Saving output notebook @ {}/{}'.format(output_dir, output_ipynb))
    html_result = ipython_to_html(output_ipynb_path, job_id, output_dir, output_ipynb)
    html_result_filename = os.path.basename(html_result)

    result_metadata = {'ipynb_result': output_ipynb,
                       'status': 'Checks done!',
                       'report_name': report_name,
                       'html_result_dir': output_dir,
                       'html_result_filename': html_result_filename}
    if result_metadata != _output_status(job_id, report_name, output_base_dir):
        logger.warn('Output metadata did not match expected format! '
                    'It may not be able to recover this result if the webapp has to restart.')
    return result_metadata


if __name__ == '__main__':
    import os
    print run_checks('asdasda', 'watchdog_checks', os.path.dirname(os.path.realpath(__file__)), {})

