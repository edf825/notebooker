# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import subprocess
import sys

from man.notebooker.utils import caching
from man.notebooker.web.routes.run_report import _monitor_stderr
from ..utils import cache_blaster


@cache_blaster
def test_monitor_stderr():
    dummy_process = """
from __future__ import print_function, unicode_literals
import time, sys
print('This is going to stdout', file=sys.stdout)
print('This is going to stderr', file=sys.stderr)
time.sleep(1)
print('This is going to stdout a bit later', file=sys.stdout)
print('This is going to stderr a bit later', file=sys.stderr)
"""
    expected_output = """This is going to stderr
This is going to stderr a bit later
"""
    p = subprocess.Popen([sys.executable, '-c', dummy_process], stderr=subprocess.PIPE)

    stderr_output = _monitor_stderr(p, 'abc123')
    assert stderr_output == expected_output
    assert caching.get_cache('run_output_abc123') == expected_output
