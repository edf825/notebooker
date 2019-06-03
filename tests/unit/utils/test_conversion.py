import json
import shutil
import tempfile

import mock
import os
from click.testing import CliRunner

from man.notebooker.constants import TEMPLATE_BASE_DIR
from man.notebooker import convert_to_py
from man.notebooker.utils import conversion
from man.notebooker.utils.caching import set_cache, get_cache
from man.notebooker.utils.notebook_execution import _cleanup_dirs
from tests.utils import cache_blaster


@cache_blaster
def test_generate_ipynb_from_py():
    python_dir = tempfile.mkdtemp()
    try:
        set_cache('latest_sha', 'fake_sha_early')

        os.mkdir(python_dir + '/extra_path')
        with open(os.path.join(python_dir, 'extra_path', 'test_report.py'), 'w') as f:
            f.write('#hello world\n')

        with mock.patch('man.notebooker.utils.conversion._git_pull_templates') as pull:
            conversion.PYTHON_TEMPLATE_DIR = python_dir
            pull.return_value = 'fake_sha_early'
            conversion.generate_ipynb_from_py(TEMPLATE_BASE_DIR, 'extra_path/test_report')
            pull.return_value = 'fake_sha_later'
            conversion.generate_ipynb_from_py(TEMPLATE_BASE_DIR, 'extra_path/test_report')
            conversion.generate_ipynb_from_py(TEMPLATE_BASE_DIR, 'extra_path/test_report')

        assert get_cache('latest_sha') == 'fake_sha_later'
        expected_ipynb_path = os.path.join(
            TEMPLATE_BASE_DIR,
            'fake_sha_early',
            'extra_path',
            'test_report.ipynb'
        )
        assert os.path.exists(expected_ipynb_path), '.ipynb was not generated as expected!'
        expected_ipynb_path = os.path.join(
            TEMPLATE_BASE_DIR,
            'fake_sha_later',
            'extra_path',
            'test_report.ipynb'
        )
        assert os.path.exists(expected_ipynb_path), '.ipynb was not generated as expected!'

        with mock.patch('man.notebooker.utils.conversion.uuid.uuid4') as uuid4:
            with mock.patch('man.notebooker.utils.conversion.pkg_resources.resource_filename') as resource_filename:
                conversion.PYTHON_TEMPLATE_DIR = None
                uuid4.return_value = 'uuid'
                resource_filename.return_value = python_dir + '/extra_path/test_report.py'
                conversion.generate_ipynb_from_py(TEMPLATE_BASE_DIR, 'extra_path/test_report')

        expected_ipynb_path = os.path.join(
            TEMPLATE_BASE_DIR,
            'uuid',
            'extra_path',
            'test_report.ipynb')
        assert os.path.exists(expected_ipynb_path), '.ipynb was not generated as expected!'

        with mock.patch('man.notebooker.utils.conversion.uuid.uuid4') as uuid4:
            conversion.PYTHON_TEMPLATE_DIR = python_dir
            conversion.NOTEBOOKER_DISABLE_GIT = True
            uuid4.return_value = 'uuid_nogit'
            conversion.generate_ipynb_from_py(TEMPLATE_BASE_DIR, 'extra_path/test_report')

        expected_ipynb_path = os.path.join(
            TEMPLATE_BASE_DIR,
            'uuid_nogit',
            'extra_path',
            'test_report.ipynb')
        assert os.path.exists(expected_ipynb_path), '.ipynb was not generated as expected!'

    finally:
        _cleanup_dirs()
        shutil.rmtree(python_dir)


def test_generate_py_from_ipynb():
    ipynb_dir = tempfile.mkdtemp()
    py_dir = tempfile.mkdtemp()
    try:
        for fname in [os.path.join(ipynb_dir, x+'.ipynb') for x in list('abcd')]:
            with open(fname, 'w') as f:
                f.write(json.dumps({'cells': [{
                    "cell_type": "code",
                    "execution_count": 2,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "import datetime"
                    ]}], 'metadata': {}, 'nbformat': 4, 'nbformat_minor': 2}))

        runner = CliRunner()
        result = runner.invoke(convert_to_py.main, [os.path.join(ipynb_dir, 'a.ipynb'),
                                                    os.path.join(ipynb_dir, 'b.ipynb'),
                                                    os.path.join(ipynb_dir, 'c.ipynb'),
                                                    os.path.join(ipynb_dir, 'd.ipynb'), '--output-dir', py_dir])
        if result.exception:
            raise result.exception
        assert result.exit_code == 0

        for fname in [os.path.join(py_dir, x+'.py') for x in list('abcd')]:
            with open(fname, 'r') as f:
                result = f.read()
            assert "import datetime" in result
            assert result.startswith('# ---\n# jupyter:')
    finally:
        shutil.rmtree(ipynb_dir)
        shutil.rmtree(py_dir)


@mock.patch('man.notebooker.utils.conversion.set_cache')
@mock.patch('man.notebooker.utils.conversion.get_cache')
@mock.patch('man.notebooker.utils.conversion._git_pull_templates')
@mock.patch('man.notebooker.utils.conversion.uuid.uuid4')
def test__get_output_path_hex(uuid4, pull, get_cache, set_cache):
    # No-git path
    conversion.PYTHON_TEMPLATE_DIR = None
    uuid4.return_value = mock.sentinel.uuid4
    actual = conversion._get_output_path_hex()
    assert actual == str(mock.sentinel.uuid4)

    # Git path set new SHA
    conversion.PYTHON_TEMPLATE_DIR = mock.sentinel.pydir
    conversion.NOTEBOOKER_DISABLE_GIT = False
    pull.return_value = mock.sentinel.newsha
    get_cache.return_value = mock.sentinel.newsha2
    actual = conversion._get_output_path_hex()
    assert actual == mock.sentinel.newsha2
    set_cache.assert_called_once_with('latest_sha', mock.sentinel.newsha)

    # Git path old SHA
    get_cache.return_value = None
    actual = conversion._get_output_path_hex()
    assert actual == 'OLD'

    # Git path same SHA
    get_cache.return_value = pull.return_value = mock.sentinel.samesha
    set_cache.reset_mock()
    actual = conversion._get_output_path_hex()
    assert actual == mock.sentinel.samesha
    assert not set_cache.called
