#!/bin/bash
set -o xtrace -o errexit -o pipefail -o nounset

if ! req=$(ls /tmp/dist/*.egg 2>/dev/null); then
    req="man.notebooker==${VERSION}"
fi

${MEDUSA_ENV}/bin/easy_install ${req}
${MEDUSA_ENV}/bin/python -m ipykernel install --user --name=man_notebooker_kernel
