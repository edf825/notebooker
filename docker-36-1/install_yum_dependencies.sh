#!/bin/bash

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
               pandoc \
               git \
           && yum clean all && rm -rf /var/cache/yum
