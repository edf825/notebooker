#!/bin/env python
try:
    from ahl.pkglib.setuptools import setup
except ImportError:
    print "AHL Package Utilities are not available. Please run \"easy_install ahl.pkgutils\""
    import sys
    sys.exit(1)

setup(namespace_packages=('idi',),
      setup_cfg='setup.cfg',
      zip_safe=False,  # This is required for pyramid/venusian, which can't handle ZipImporter.
)