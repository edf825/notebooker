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
               texlive-ulem
python setup.py develop
pyinstall $(cat ./notebook_templates/notebook_requirements.txt)
python -m ipykernel install --user --name=one_click_notebooks_kernel
man_notebooker_webapp --port 11828 --mongo-host mktdatad --debug &

cd notebook_templates
# Install the jupyter extension
jupyter bundlerextension enable --py notebooker_extension.bundler --user

# Start a jupyter notebook session
jupyter notebook --no-browser --ip=0.0.0.0
