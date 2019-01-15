#!/bin/bash

set -x
set -o xtrace -o errexit -o pipefail -o nounset

. /usr/bin/docker_common.sh

run_env_check \
    --env USER_ID \
    --env GROUP_ID \
	--env OPERATING_USER \
	|| exit $?

exec gosu ${USER_ID}:${GROUP_ID} "$@"

