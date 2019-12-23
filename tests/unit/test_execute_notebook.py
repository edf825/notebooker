from __future__ import unicode_literals
import mock
import pytest

from ahl.testing.pytest.mongo_server import mongo_host
from click.testing import CliRunner
from nbformat import NotebookNode, __version__ as nbv

from man.notebooker import execute_notebook
from man.notebooker.constants import NotebookResultComplete
from man.notebooker.execute_notebook import _get_overrides
from man.notebooker.serialization.serializers import MongooseNotebookResultSerializer


def mock_nb_execute(input_path, output_path, **kw):
    with open(output_path, 'w') as f:
        f.write('{"cells": [], "metadata": {}}')


def test_main(mongo_host):
    with mock.patch('man.notebooker.execute_notebook.pm.execute_notebook') as exec_nb, \
         mock.patch('man.notebooker.utils.conversion.jupytext.readf') as read_nb, \
         mock.patch('man.notebooker.utils.conversion.PDFExporter') as pdf_exporter:
        pdf_contents = b'This is a PDF.'
        pdf_exporter().from_notebook_node.return_value = (pdf_contents, None)
        versions = nbv.split(".")
        major, minor = int(versions[0]), int(versions[1])
        read_nb.return_value = NotebookNode({'cells': [], 'metadata': {}, "nbformat": major, "nbformat_minor": minor})
        exec_nb.side_effect = mock_nb_execute
        job_id = 'ttttteeeesssstttt'
        runner = CliRunner()
        cli_result = runner.invoke(execute_notebook.main, ['--report-name', 'test_report',
                                                           '--mongo-host', mongo_host,
                                                           '--job-id', job_id])
        assert cli_result.exit_code == 0
        serializer = MongooseNotebookResultSerializer(mongo_host=mongo_host)
        result = serializer.get_check_result(job_id)
        assert isinstance(result, NotebookResultComplete), 'Result is not instance of {}, ' \
                                                           'it is {}'.format(NotebookResultComplete, type(result))
        assert result.raw_ipynb_json
        assert result.pdf == pdf_contents


@pytest.mark.parametrize("json_overrides, iterate_override_values_of, expected_output",
                         [
                             ('{"test": [1, 2, 3]}', "", [{"test": [1, 2, 3]}]),
                             ('{"test": [1, 2, 3]}', "test", [{"test": 1}, {"test": 2}, {"test": 3}]),
                             ('{"test": [1, 2, 3], "a": 1}',
                              "test",
                              [{"test": 1, "a": 1}, {"test": 2, "a": 1}, {"test": 3, "a": 1}]),
                             ('[{"test": 1, "a": 1}, {"test": 2, "a": 1}, {"test": 3, "a": 1}]',
                              None,
                              [{"test": 1, "a": 1}, {"test": 2, "a": 1}, {"test": 3, "a": 1}]),
                             ('[{"test": 1, "a": 1}]',
                              None,
                              [{"test": 1, "a": 1}]),
                             ('[]',
                              None,
                              []),
                         ])
def test_get_overrides(json_overrides, iterate_override_values_of, expected_output):
    actual_output = _get_overrides(json_overrides, iterate_override_values_of)
    assert isinstance(actual_output, list)
    for override in actual_output:
        assert override in expected_output


@pytest.mark.parametrize("input_json, iterate_override_values_of, error_regex, error_message",
                         [
                             (
                                     '{"test": {"Equities": "hello", "FX": "world"}, "a": 1}',
                                     "test",
                                     None,
                                     "Can't iterate over a non-list or tuple of variables. "
                                     "The given value was a <class 'dict'> - {'Equities': 'hello', 'FX': 'world'}."
                             ), (
                                     '{"test": {"Equities": "hello", "FX": "world"}, "a": 1}',
                                     "notfound",
                                     "Can't iterate over override values unless it is given in the override.*",
                                     None,
                             ), (
                                     '{}',
                                     "test",
                                     "Can't iterate over override values unless it is given in the override.*",
                                     None,
                             ),
                         ])
def test_get_overrides_valueerror(input_json, iterate_override_values_of, error_regex, error_message):
    kwargs = {}  # Do this because in pytest==3.1.0, passing message=None fails.
    if error_message:
        kwargs["message"] = error_message
    if error_regex:
        kwargs["match"] = error_regex
    with pytest.raises(ValueError, **kwargs):
        _get_overrides(input_json, iterate_override_values_of)
