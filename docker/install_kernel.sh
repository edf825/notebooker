#!/bin/bash
set -o xtrace -o errexit -o pipefail -o nounset

# Something goes very wrong with our dependencies. This seems to fix it.
# See https://github.com/ipython/ipython/issues/9656
rm -rf ${MEDUSA_ENV}/lib/python2.7/site-packages/backports/

${MEDUSA_ENV}/bin/python -m ipykernel install --user --name=man_notebooker_kernel
