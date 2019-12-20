# Man Notebooker

## [Try it out here](http://notebooker.ing.k8s.dev.m/)

This is a tool which allows you to run parametrised notebooks either via
a webapp, or a CLI. To add your own notebook template, see the
tutorial [here](notebook_templates/README.md).

## Development
To run your own version of the webapp:

1. Create a BSONStore called NOTEBOOK_OUTPUT in your mongoose\_\<username\> database
  - Go to http://ci.res.ahl/cron/job/one-click-scheduling%20%28Research%29/job/data/job/ondemand/job/mongoose_init_library/
  - Parameters are:
       -  MONGOOSE_DB = research
       -  LIBRARY = jbannister.NOTEBOOK_OUTPUT
       -  LIBRARY_TYPE = BSONStore
2. Run [setup_notebooker_env.sh](setup_notebooker_env.sh).


## Deployment
To create a new instance of Notebooker:

1. Create a new repository under the [Notebooker Templates project](http://ahlgit.maninvestments.com/projects/NT) in Bitbucket.
   * Create a directory called `notebook_templates/` with your python templates.
   * In that directory, create a file called `notebook_requirements.txt` with all of your package requirements to be installed.
   * Commit the contents to `master` branch.
1. Create your pipeline builds:
   * Go to http://ci.res.ahl/build/job/Notebooker%20Builds/job/create-new-notebooker-templates-project/.
   * Add the git URL of your new repository, and choose whether you'd like 27-3 or 36-1.
   * Click *Build*.
   * The console output of the build will give you a Bitbucket URL to create a pull request with all the required docker and Jenkins setup.
1. A new pipeline project should also have been created for you under [Notebooker Builds](http://ci.res.ahl/build/job/Auto-Generated%20Jobs/job/Notebooker%20Builds/). If this hasn't appeared yet, run the seed job [here](http://ci.res.ahl/build/job/Auto-Generated%20Jobs/job/Configure%20All%20Jobs/). This build will:
   * Run sanity checks on your notebook templates
   * Check your notebook_requirements.txt has all of the correct requirements for the templates
   * Test that all of your templates can run without any parameters (HINT: make sure your templates run quickly without parameters, otherwise this test takes a while!)
   * Build a Docker image which can be used to execute your notebooks and run a Notebooker server.

To deploy as a Kubernetes instance, please refer to the [documentation](http://docs/core/services/kubernetes/#templating-kustomize). Also, see this [OCD PR](http://ahlgit.maninvestments.com/projects/DOCKER/repos/one-click-deploys/pull-requests/11621/diff).

### Securing notebooker instances

Notebooker instances can be secured using [Keycloak gatekeeper](http://docs/core/services/keycloak/#add-authentication-to-app). At a high level, this involves the following steps (with example hyperlinks):

* Include a keycloak [gatekeeper image](http://ahlgit.maninvestments.com/projects/DOCKER/repos/one-click-deploys/browse/k8s-kustomize/ahl-notebooker/notebooker_central_trading/environment_variables.yaml?at=4b5be740596#22) in the notebooker container, and [forward port 80 to 3000](http://ahlgit.maninvestments.com/projects/DOCKER/repos/one-click-deploys/browse/k8s-kustomize/ahl-notebooker/notebooker_central_trading/service_patch.yaml?at=4b5be740596).
* Through an admin/ATS, [register](http://docs/core/services/keycloak/#register-the-app) your notebooker instance as a client--this gives you a client secret. Have them also create and configure users and roles as required.
* [Seal](http://docs/core/services/kubernetes/resources/#sealedsecret) your client secret, and have the [sealed secrets](http://ahlgit.maninvestments.com/projects/DOCKER/repos/one-click-deploys/browse/k8s/ahl-notebooker/notebooker-central-trading-sealed.yaml?at=4b5be740596) passed to the [gatekeeper environment](http://ahlgit.maninvestments.com/projects/DOCKER/repos/one-click-deploys/browse/k8s-kustomize/ahl-notebooker/notebooker_central_trading/environment_variables.yaml?at=4b5be740596#23-33).
* Specify in gatekeeper [command line arguments](http://ahlgit.maninvestments.com/projects/DOCKER/repos/one-click-deploys/browse/k8s-kustomize/ahl-notebooker/notebooker_central_trading/environment_variables.yaml?at=4b5be740596#34) the client ID, [roles](http://ahlgit.maninvestments.com/projects/DOCKER/repos/one-click-deploys/browse/k8s-kustomize/ahl-notebooker/notebooker_central_trading/environment_variables.yaml?at=4b5be740596#48-51) that you require visiting users to have in order to access your notebooker URLs.

For testing purposes, it's recommended that you first point your gatekeeper to [dev keycloak instance](https://keycloak-lon.dev.m/auth/).

## Important environment variables

#### MONGO_HOST
The environment which mongo is reading from and writing to, e.g. "research"

#### DATABASE_NAME
The mongo database which we are saving to, e.g. "mongoose_notebooker"

#### RESULT_COLLECTION_NAME
The mongo collection which we are saving to, e.g. "DATASCIENCE_RESULTS".
This will be created for you if it doesn't exist already.

#### PY_TEMPLATE_DIR
The directory of the Notebook Templates git repository.

#### GIT_REPO_TEMPLATE_DIR
The subdirectory within your PY_TEMPLATE_DIR git repo which holds all of your templates.

#### NOTEBOOKER_DISABLE_GIT
A boolean flag to dictate whether we should pull from git master every time we try to
run a report or list the available templates.

#### OUTPUT_DIR
The temporary directory which will contain the output results of executing notebooks.
Defaults to a random directory in the current user's homedir.

#### TEMPLATE_DIR
The temporary directory which will contain the .ipynb templates which have been converted 
from the .py templates.
Defaults to a random directory in the current user's homedir.

#### NOTEBOOK_KERNEL_NAME
The name of the kernel which we are using to execute notebooks.
Defaults to `man_notebooker_kernel`.
