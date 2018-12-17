# How to add a one-click-notebook

## You will need:

If you have already set up an environment, you can start at step (5).

1) (Optional) Create a new virtualenv

```bash
use_medusa_python 27-3
mkvirtualenv oneclicknotebooks
```

2) Install jupyter and jupytext

```bash
pyinstall jupyter jupytext
```

3) Create a jupyter config file and add a jupytext config line:

```bash
jupyter notebook --generate-config
echo 'c.NotebookApp.contents_manager_class = "jupytext.TextFileContentsManager"' >> ~/.jupyter/jupyter_notebook_config.py
echo 'c.ContentsManager.default_jupytext_formats = "ipynb,py"' >> ~/.jupyter/jupyter_notebook_config.py
```

4) If you haven't already, run the following in a virtualenv which has
all your desired libraries installed (e.g. ahl.lab). Note the kernel
*must* be called "one_click_notebooks_kernel".

```bash
pyinstall ipykernel
python -m ipykernel install --user --name=one_click_notebooks_kernel
```

5) Start jupyter:

```bash
jupyter notebook --ip 0.0.0.0 --no-browser
```

6) Follow the instructions given in a browser of your choice to start development:

```
Copy/paste this URL into your browser when you connect for the first time,
to login with a token:
    http://0.0.0.0:8889/?token=YOUR_UNIQUE_TOKEN
```

7) Create a new notebook in
idi/datascience/one_click_notebooks/notebook_templates with your
"one_click_notebooks_kernel" kernel

8) Enable viewing tags by selecting View > Cell Toolbar > Tags

9) Set one of the cells to have the tag "parameters". This will allow you
to override any of the values in this cell using one-click-notebooks.

10) Once you're happy, commit the .py file that has been generated alongside
your .ipynb file, and raise a pull request to get your new notebook on the website!


## Testing your notebook

Run your own one-click-notebook server by simply running:

```bash
python setup.py develop
python idi/datascience/one_click_notebooks/main.py
```

You should be able to run your new report if it is in the
correct directory.
