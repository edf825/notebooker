#!/bin/bash
set -o xtrace -o errexit -o pipefail -o nounset

yum -y install texlive-xetex \
               texlive-ec \
               texlive-collection-fontsrecommended \
               texlive-collection-latexrecommended \
               texlive-collection-xetex \
               texlive-texconfig \
               texlive-xetex-def \
               texlive-adjustbox \
               texlive-upquote \
               texlive-ulem \
           && yum clean all

if ! req=$(ls /tmp/dist/*.egg 2>/dev/null); then
    req="man.notebooker==${VERSION}"
fi

${MEDUSA_ENV}/bin/easy_install ${req}
