import datetime

import freezegun
import mock
import hypothesis
import hypothesis.strategies as st
import pytest
import re

from man.notebooker.handle_overrides import handle_overrides, _handle_overrides_safe


IMPORT_REGEX = re.compile('^(from [a-zA-Z0-9_.]+ )?import (?P<import_target>[a-zA-Z0-9_.]+)( as (?P<name>.+))?$')
VARIABLE_ASSIGNMENT_REGEX = re.compile('^(?P<variable_name>[a-zA-Z_]+) *= *(?P<value>.+)$')


@hypothesis.given(st.text())
def test_handle_overrides_handles_anything_cleanly_no_process_junk(text):
    # Check that it doesn't just crash with random input
    with mock.patch('man.notebooker.handle_overrides.subprocess.check_output') as popen:
        popen.side_effect = lambda args: mock.MagicMock(res=_handle_overrides_safe(args[4], args[6]))
        handle_overrides(text)


@hypothesis.given(st.from_regex(VARIABLE_ASSIGNMENT_REGEX))
def test_handle_overrides_handles_anything_cleanly_no_process_variable(text):
    with mock.patch('man.notebooker.handle_overrides.subprocess.check_output') as popen:
        popen.side_effect = lambda args: mock.MagicMock(res=_handle_overrides_safe(args[4], args[6]))
        overrides, issues = handle_overrides(text)
    if any(t for t in text.split('\n') if t.strip()):
        assert len(issues) >= 1 or len(overrides) >= 1
    else:
        assert len(issues) == 0 and len(overrides) == 0


@hypothesis.given(st.from_regex(IMPORT_REGEX))
def test_handle_overrides_handles_anything_cleanly_no_process_import(text):
    with mock.patch('man.notebooker.handle_overrides.subprocess.check_output') as popen:
        popen.side_effect = lambda args: mock.MagicMock(res=_handle_overrides_safe(args[4], args[6]))
        overrides, issues = handle_overrides(text)
    if any(t for t in text.split('\n') if t.strip()):
        assert len(issues) >= 1 or len(overrides) >= 1
    else:
        assert len(issues) == 0 and len(overrides) == 0


@freezegun.freeze_time(datetime.datetime(2018, 1, 1))
@pytest.mark.parametrize('input_txt', ['import datetime;d=datetime.datetime.now()',
                                       'import datetime as dt;d=dt.datetime.now()',
                                       'from datetime import datetime;d=datetime.now()',
                                       'from datetime import datetime as dt;d=dt.now()'])
def test_handle_overrides_handles_imports(input_txt):
    with mock.patch('man.notebooker.handle_overrides.subprocess.check_output') as popen:
        popen.side_effect = lambda args: mock.MagicMock(res=_handle_overrides_safe(args[4], args[6]))
        overrides, issues = handle_overrides(input_txt)
    assert overrides == {'d': datetime.datetime(2018, 1, 1)}


@pytest.mark.parametrize('input_txt', ['import datetime;d=datetime.datetime(10, 1, 1)'])
def test_handle_overrides_handles_imports(input_txt):
    with mock.patch('man.notebooker.handle_overrides.subprocess.check_output') as popen:
        popen.side_effect = lambda args: mock.MagicMock(res=_handle_overrides_safe(args[4], args[6]))
        overrides, issues = handle_overrides(input_txt)
    assert overrides == {}
    assert issues == ['Could not JSON serialise a parameter ("d") - '
                      'this must be serialisable so that we can execute '
                      'the notebook with it! '
                      '(Error: datetime.datetime(10, 1, 1, 0, 0) is not JSON serializable, '
                      'Value: 0010-01-01 00:00:00)']

