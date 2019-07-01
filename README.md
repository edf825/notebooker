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

  * Create a directory called notebook_templates/ with your python templates
  * In that directory, create a file called notebook_requirements.txt with all of your package requirements to be installed.
  
2. Create your pipeline builds:
  * Go to http://ci.res.ahl/build/job/Notebooker%20Builds/job/create-new-notebooker-templates-project/
  * Add the git URL of your new repository, and choose whether you'd like 27-3 or 36-1
  * Click "Build"
  * The console output of the build will give you a Bitbucket URL to create a pull request with all the required docker and Jenkins setup.
3. A new pipeline project should also have been created for you under [Notebooker Builds](http://ci.dev.ahl/dev/job/Auto-Generated%20Jobs/job/Notebooker%20Builds/)
  * If this hasn't appeared yet, run the seed job [here](http://ci.dev.ahl/dev/job/Auto-Generated%20Jobs/job/Configure%20All%20Jobs/)
4. This build will:
  * Run sanity checks on your notebook templates
  * Check your notebook_requirements.txt has all of the correct requirements for the templates
  * Test that all of your templates can run without any parameters (HINT: make sure your templates run quickly without parameters, otherwise this test takes a while!)
  * Build a Docker image which can be used to execute your notebooks and run a Notebooker server.
  

