#!/bin/bash
set -o xtrace -o errexit -o pipefail -o nounset

mkdir -p ${PY_TEMPLATE_DIR}
pushd ${PY_TEMPLATE_DIR}
git init
git remote add origin "${NOTEBOOKER_TEMPLATE_GIT_URL}"
git config core.sparseCheckout true
git --version
ls -la
echo "/${GIT_REPO_TEMPLATE_DIR}" >> .git/info/sparse-checkout
git pull origin master
# We should now have templates at ${PY_TEMPLATE_DIR}/${GIT_REPO_TEMPLATE_DIR}
popd
chmod -R 777 ${PY_TEMPLATE_DIR}

${MEDUSA_ENV}/bin/easy_install $(cat ${PY_TEMPLATE_DIR}/${GIT_REPO_TEMPLATE_DIR}/${NOTEBOOK_REQUIREMENTS_FILE})
${MEDUSA_ENV}/bin/python -m ipykernel install --name=man_notebooker_kernel
