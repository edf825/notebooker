#!/bin/bash
set -o xtrace -o errexit -o pipefail -o nounset

if ! req=$(ls /tmp/dist/*.egg 2>/dev/null); then
    req="idi.datascience==${VERSION}"
fi

${MEDUSA_ENV}/bin/easy_install -i ${DEVPI_SERVER}/${DEVPI_INDEX}/+simple/ ${req}
