#!/bin/bash
pushd ${PY_TEMPLATE_DIR}
git init
git remote add origin "${NOTEBOOKER_TEMPLATE_GIT_URL}"
git config core.sparseCheckout true
echo "/${GIT_REPO_TEMPLATE_DIR}" >> .git/info/sparse-checkout
git pull origin master
# We should now have templates at ${PY_TEMPLATE_DIR}/${GIT_REPO_TEMPLATE_DIR}
popd
exit 0
