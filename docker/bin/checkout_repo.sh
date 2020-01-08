#!/bin/bash
checkout_ref=${CHECKOUT_GIT_HASH:-origin/master}
echo "Checking out repo to $1 as of $checkout_ref"
mkdir -p $1
pushd $1
git init
git remote add origin "${NOTEBOOKER_TEMPLATE_GIT_URL}"
git config core.sparseCheckout true
echo "/${GIT_REPO_TEMPLATE_DIR}" >> .git/info/sparse-checkout
git fetch
git checkout $checkout_ref
# We should now have templates at ${PY_TEMPLATE_DIR}/${GIT_REPO_TEMPLATE_DIR}
popd
exit 0