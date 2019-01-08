# Man Notebooker

This is a tool which allows you to run parametrised notebooks either via
a webapp, or a CLI (soon).

## Development
To run your own version:
```
python setup.py develop
python -m ipykernel install --user --name=one_click_notebooks_kernel
man_notebooker_webapp --port 11828
```
