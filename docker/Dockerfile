FROM shared-docker.repo.prod.m/man/base/medusa-python:36-1 

MAINTAINER AHL Data Science

ARG ENV_CHECK_VERSION=1.0.0
ARG GOSU_VERSION=1.9.0

ENV MEDUSA_ENV=/default-medusa-venv
ENV PATH="${MEDUSA_ENV}/bin:${PATH}"

ENV PYTHON_EGG_CACHE /tmp/egg-cache

ENV ENV_CHECK_VERSION=${ENV_CHECK_VERSION}
ENV GOSU_VERSION=${GOSU_VERSION}

ADD install_*.sh /tmp/
RUN /tmp/install_yum_dependencies.sh && rm -f /tmp/install_yum_dependencies.sh

ENV PY_TEMPLATE_DIR /etc/notebooker
ENV GIT_REPO_TEMPLATE_DIR notebook_templates
ENV NOTEBOOKER_TEMPLATE_GIT_URL http://ahlgit.maninvestments.com/scm/data/man.notebooker.git
ENV NOTEBOOK_REQUIREMENTS_FILE notebook_requirements.txt

ADD dist /tmp/dist
ADD bin/* /usr/bin/
# Don't install kernel here - child images will do this themselves.
ARG VERSION
RUN /tmp/install_man_notebooker.sh && rm -f /tmp/install_man_notebooker.sh /tmp/dist/*.egg

LABEL IMAGE_TAG ${VERSION}

ENTRYPOINT ["/usr/bin/run_man_notebooker.sh"]
