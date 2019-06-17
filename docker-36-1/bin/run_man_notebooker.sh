#!/bin/bash

set -x
set -o xtrace -o errexit -o pipefail -o nounset

if [[ ! -z "${NOTEBOOKER_DISABLE_GIT}" ]]; then 
  /usr/bin/checkout_repo.sh ${PY_TEMPLATE_DIR}
fi
/bin/sh -c "$@"
