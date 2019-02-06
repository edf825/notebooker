#!/bin/env python
import os
try:
    from ahl.pkglib.setuptools import setup
except ImportError:
    print("AHL Package Utilities are not available. Please run \"easy_install ahl.pkgutils\"")
    import sys
    sys.exit(1)

if os.getenv('REGRESSION_TESTING'):
    template_test_deps = open(
        os.path.join(os.path.dirname(__file__), 'notebook_templates', 'notebook_requirements.txt')
    ).readlines()
else:
    template_test_deps = []

setup(
    namespace_packages=('man',),
    tests_require=[
        'openpyxl',
        'pytest',
        'pandas',
        'mock',
        'pytest-cov',
        'pytest-timeout',
        'pytest-xdist',
        'ahl.testing',
        'freezegun',
        'hypothesis>=3.83.2',
    ] + template_test_deps,
    setup_cfg='setup.cfg',
    zip_safe=False,  # so that we can get our templates from notebook_templates/
)
