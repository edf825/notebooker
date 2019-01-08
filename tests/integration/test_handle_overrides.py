import datetime

import pytest
import re

from man.notebooker.handle_overrides import _handle_overrides


IMPORT_REGEX = re.compile('^(from [a-zA-Z0-9_.]+ )?import (?P<import_target>[a-zA-Z0-9_.]+)( as (?P<name>.+))?$')
VARIABLE_ASSIGNMENT_REGEX = re.compile('^(?P<variable_name>[a-zA-Z_]+) *= *(?P<value>.+)$')


@pytest.mark.parametrize(
    'test_name, input_str, expected_output_values, expected_issues',
    [
        (
            'Neither import nor variable declaration',
            'datetime.datetime(2018, 1, 1)',
            {},
            ["An error was encountered: name 'datetime' is not defined"]),
        (
            'Using un-imported module',
            'd = datetime.datetime(2018, 1, 1)',
            {},
            ["An error was encountered: name 'datetime' is not defined"]),
        (
            'Successfully importing and using a library',
            'import datetime\nd = datetime.datetime(2018, 1, 1)',
            {'d': datetime.datetime(2018, 1, 1)},
            []),
        (
            'Successfully importing and using a library',
            'from datetime import datetime as dt;d = dt(2018, 1, 1)\nq=\\\ndt(2011, 5, 1)',
            {'d': datetime.datetime(2018, 1, 1), 'q': datetime.datetime(2011, 5, 1)},
            []),
        (
            'Failing importing and using an un-imported library',
            'import datetimes\nd = datetime.datetime(2018, 1, 1)',
            {},
            ['An error was encountered: No module named datetimes']),
        (
            'Importing but just using an expression',
            'import datetime;datetime.datetime(2018, 1, 1)',
            {},
            ["Found an expression that did nothing! It has a value of type: <class '_ast.Call'>"]),
        (
            'Trying to declare an un-picklable variable',
            'a=1\nx = (_ for _ in range(10))',
            {},
            ["Could not pickle", "All input must be picklable (sorry!)"]),
    ])
def test_handle_overrides_normal(test_name, input_str, expected_output_values, expected_issues):
    override_dict, issues = _handle_overrides(input_str)
    for k, v in expected_output_values.items():
        assert expected_output_values[k] == override_dict.get(k)
    print issues
    for issue in expected_issues:
        assert any(issue in resulting_issue for resulting_issue in issues), '"{}" not found in {}'.format(issue, issues)

