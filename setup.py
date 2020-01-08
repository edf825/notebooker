#!/bin/env python
import os
try:
    from ahl.pkglib.setuptools import setup
except ImportError:
    print("AHL Package Utilities are not available. Please run \"easy_install ahl.pkglib\"")
    import sys
    sys.exit(1)

setup(
    namespace_packages=('man',),
    zip_safe=False,  # so that we can get our templates from notebook_templates/
)
