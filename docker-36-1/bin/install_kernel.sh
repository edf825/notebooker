#!/bin/bash
set -o xtrace -o errexit -o pipefail -o nounset

mkdir -p ${PY_TEMPLATE_DIR}
chmod -R 777 ${PY_TEMPLATE_DIR}

# Checkout in temp dir so that we can install requirements
/usr/bin/checkout_repo.sh /tmp/${PY_TEMPLATE_DIR}
${MEDUSA_ENV}/bin/easy_install $(cat /tmp/${PY_TEMPLATE_DIR}/${GIT_REPO_TEMPLATE_DIR}/${NOTEBOOK_REQUIREMENTS_FILE})
rm -rf /tmp/${PY_TEMPLATE_DIR}

# Set up our ipynb kernel where we just installed all the requirements.
${MEDUSA_ENV}/bin/python -m ipykernel install --name=man_notebooker_kernel
