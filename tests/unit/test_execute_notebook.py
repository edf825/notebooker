import mock

from man.notebooker import execute_notebook
from ahl.testing.pytest.mongo_server import mongo_host
from click.testing import CliRunner

from man.notebooker.results import NotebookResultComplete, NotebookResultSerializer


def mock_nb_execute(input_path, output_path, **kw):
    with open(output_path, 'w') as f:
        f.write('{"cells": [], "metadata": {}}')


def test_main(mongo_host):
    with mock.patch('man.notebooker.execute_notebook.pm.execute_notebook') as exec_nb, \
         mock.patch('man.notebooker.execute_notebook.jupytext.readf') as read_nb, \
         mock.patch('man.notebooker.execute_notebook.PDFExporter') as pdf_exporter:
        pdf_contents = 'This is a PDF.'
        pdf_exporter().from_notebook_node.return_value = (pdf_contents, None)
        read_nb.return_value = {'cells': [], 'metadata': {}}
        exec_nb.side_effect = mock_nb_execute
        job_id = 'ttttteeeesssstttt'
        runner = CliRunner()
        cli_result = runner.invoke(execute_notebook.main, ['--report-name', 'test_report',
                                                           '--mongo-host', mongo_host,
                                                           '--job-id', job_id])
        assert cli_result.exit_code == 0
        serializer = NotebookResultSerializer(mongo_host=mongo_host)
        result = serializer.get_check_result(job_id)
        assert isinstance(result, NotebookResultComplete), 'Result is not instance of {}, ' \
                                                           'it is {}'.format(NotebookResultComplete, type(result))
        assert result.raw_ipynb_json and isinstance(result.raw_ipynb_json, basestring)
        assert result.pdf == pdf_contents
