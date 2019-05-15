#!/bin/bash

# Set up the notebooker webapp for local usage
sudo yum install -y texlive-xetex \
               texlive-ec \
               texlive-collection-fontsrecommended \
               texlive-collection-latexrecommended \
               texlive-collection-xetex \
               texlive-texconfig \
               texlive-xetex-def \
               texlive-adjustbox \
               texlive-upquote \
               texlive-ulem \
               pandoc
python setup.py develop
pyinstall $(cat ./notebook_templates/notebook_requirements.txt)
python -m ipykernel install --user --name=man_notebooker_kernel
man_notebooker_webapp --port 11828 --debug --database-name mongoose_$USER
