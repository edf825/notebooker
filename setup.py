#!/bin/env python
import os
import six
try:
    from ahl.pkglib.setuptools import setup
except ImportError:
    print("AHL Package Utilities are not available. Please run \"easy_install ahl.pkgutils\"")
    import sys
    sys.exit(1)

if six.PY2:
    setup_cfg = 'setup-legacy.cfg'
else:
    setup_cfg = 'setup.cfg'

print('Using setup.cfg at {}'.format(setup_cfg))

setup(
    namespace_packages=('man',),
    setup_cfg=setup_cfg,
    zip_safe=False,  # so that we can get our templates from notebook_templates/
)
