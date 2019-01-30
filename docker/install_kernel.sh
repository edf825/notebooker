#!/bin/bash
set -o xtrace -o errexit -o pipefail -o nounset

# Checkout a temporary directory so we can install requirements
mkdir -p /tmp/${PY_TEMPLATE_DIR}
pushd /tmp/${PY_TEMPLATE_DIR}
git init
git remote add origin "${NOTEBOOKER_TEMPLATE_GIT_URL}"
git config core.sparseCheckout true
echo "/${GIT_REPO_TEMPLATE_DIR}" >> .git/info/sparse-checkout
git pull origin master
# We should now have templates at /tmp/${PY_TEMPLATE_DIR}/${GIT_REPO_TEMPLATE_DIR}
popd

mkdir -p ${PY_TEMPLATE_DIR}
chmod -R 777 ${PY_TEMPLATE_DIR}

${MEDUSA_ENV}/bin/easy_install $(cat /tmp/${PY_TEMPLATE_DIR}/${GIT_REPO_TEMPLATE_DIR}/${NOTEBOOK_REQUIREMENTS_FILE})
rm -rf /tmp/${PY_TEMPLATE_DIR}
${MEDUSA_ENV}/bin/python -m ipykernel install --name=man_notebooker_kernel
