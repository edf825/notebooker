# How to add parameters to a notebook

## Parameters cell
When you have a notebook which you are happy with, you may want to add
a parameters cell. To do this:

1. Enable viewing tags by selecting View > Cell Toolbar > Tags

2. Set one of the cells to have the tag "parameters". This will allow you
to override any of the values in this cell using man.notebooker

# Deploying your notebook

## Easiest

1. Install the extension:
```
# Clone the notebooker repository
git clone ssh://git@ahlgit.maninvestments.com:7999/data/man.notebooker.git

# Change directory to the notebooker base dir
cd man.notebooker

# Install the jupyter extension
jupyter bundlerextension enable --py notebooker_extension.bundler --user

# Start a jupyter notebook session
jupyter notebook --no-browser --ip=0.0.0.0
```

2. Within the ipython notebook webapp, deploy the changed file using
the menu under _File > Deploy as > To BitBucket as .py_.

## Easy
If you have a notebook with a parameters tag which you simply want to convert,
you can do the following:

```
cd ~/work
git clone ssh://git@ahlgit.maninvestments.com:7999/data/man.notebooker.git
convert_ipynb_to_py nb1.ipynb nb2.ipynb --output-dir man.notebooker/notebook_templates
```

You will then have a repository at ~/work/man.notebooker which has the
changes you need.

## Tricky

If you have already set up an environment, you can start at step (5).

1. (Optional) Create a new virtualenv

```bash
use_medusa_python 27-3
mkvirtualenv oneclicknotebooks
```

2. Install jupyter and jupytext

```bash
pyinstall jupyter jupytext
```

3. Create a jupyter config file and add a jupytext config line:

```bash
jupyter notebook --generate-config
echo 'c.NotebookApp.contents_manager_class = "jupytext.TextFileContentsManager"' >> ~/.jupyter/jupyter_notebook_config.py
echo 'c.ContentsManager.default_jupytext_formats = "ipynb,py"' >> ~/.jupyter/jupyter_notebook_config.py
```

4. If you haven't already, run the following in a virtualenv which has
all your desired libraries installed (e.g. ahl.lab).

```bash
pyinstall ipykernel
python -m ipykernel install --user --name=one_click_notebooks_kernel
```

5. Start jupyter:

```bash
jupyter notebook --ip 0.0.0.0 --no-browser
```

6. Follow the instructions given in a browser of your choice to start development:

```
Copy/paste this URL into your browser when you connect for the first time,
to login with a token:
    http://0.0.0.0:8889/?token=YOUR_UNIQUE_TOKEN
```

7. Create a new notebook in notebook_templates with your
"one_click_notebooks_kernel" kernel


# Testing your notebook

Run your own man.notebooker server by following the readme in the
base repository.

# Adding a One-Click-Scheduled notebook

Add a one-click-scheduling job which calls the `man_execute_notebook`
entrypoint, like so:

```bash
man_execute_notebook --report-name plot_random_data --mongo-host mktdatad --overrides-as-json '{"n_points": 100}'
```

NB: Your command must include the report name, the rest is optional.

The results will be viewable on the web GUI, or you can optionally set
an email address to which the output can be sent.


## Important environment variables
If you want to test the webapp as if it is using docker, you will want
to set environment variables that point to the git repository. This
section explains what each do:

- PY_TEMPLATE_DIR: The directory which we are saving templates to
- NOTEBOOKER_TEMPLATE_GIT_URL: The git URL (with credentials) to pull
from the remote git repository which holds notebook templates
- GIT_REPO_TEMPLATE_DIR: The directory within the git repo which holds templates
