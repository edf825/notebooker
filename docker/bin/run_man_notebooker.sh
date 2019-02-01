#!/bin/bash

set -x
set -o xtrace -o errexit -o pipefail -o nounset

. /usr/bin/docker_common.sh

run_env_check \
    --env USER_ID \
    --env GROUP_ID \
    --env OPERATING_USER \
    --env PY_TEMPLATE_DIR \
    --env GIT_REPO_TEMPLATE_DIR \
    --env NOTEBOOKER_TEMPLATE_GIT_URL \
    || exit $?

gosu ${USER_ID}:${GROUP_ID} /usr/bin/checkout_repo.sh ${PY_TEMPLATE_DIR}
exec gosu ${USER_ID}:${GROUP_ID} "$@"
