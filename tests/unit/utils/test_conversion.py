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
from tests.utils import cache_blaster


@cache_blaster
def test_generate_ipynb_from_py():
    set_cache('latest_sha', 'fake_sha_early')

    python_dir = tempfile.mkdtemp()

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

    shutil.rmtree(TEMPLATE_BASE_DIR)
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


