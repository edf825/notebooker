# Man Notebooker

This is a tool which allows you to run parametrised notebooks either via
a webapp, or a CLI.

## Development
To run your own version of the webapp:

```
sudo yum install texlive-xetex \
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
python -m ipykernel install --user --name=one_click_notebooks_kernel
man_notebooker_webapp --port 11828 --mongo-host mktdatad --debug
```
