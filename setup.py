#!/bin/env python
try:
    from ahl.pkglib.setuptools import setup
except ImportError:
    print("AHL Package Utilities are not available. Please run \"easy_install ahl.pkgutils\"")
    import sys
    sys.exit(1)

setup(namespace_packages=('man',),
      setup_cfg='setup.cfg',
      zip_safe=False,  # so that we can get our templates from man/notebooker/notebook_templates/
)
