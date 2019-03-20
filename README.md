# Man Notebooker

## [Try it out here](http://1.restech.res.ahl:11828)

This is a tool which allows you to run parametrised notebooks either via
a webapp, or a CLI. To add your own notebook template, see the
tutorial [here](notebook_templates/README.md).

## Development
To run your own version of the webapp:

1. Create a BSONStore called NOTEBOOK_OUTPUT in your mongoose_<username> database. e.g.:
  
  * Go to http://ci.res.ahl/cron/job/DATA/job/data-create-mongoose-library/build?delay=0sec
  * Parameters are:
    * MONGOOSE_DB = research
    * LIBRARY = jbannister.NOTEBOOK_OUTPUT
    * LIBRARY_TYPE = BSONStore


2. Run [setup_notebooker_env.sh](setup_notebooker_env.sh).
