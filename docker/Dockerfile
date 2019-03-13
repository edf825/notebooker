FROM base.docker.prod.ahl/base/medusa-python:27-3.9

MAINTAINER AHL Data Science

ARG ENV_CHECK_VERSION=1.0.0
ARG GOSU_VERSION=1.9.0

ENV MEDUSA_ENV=/default-medusa-venv
ENV PATH="${MEDUSA_ENV}/bin:${PATH}"

ENV PYTHON_EGG_CACHE /tmp/egg-cache

ENV ENV_CHECK_VERSION=${ENV_CHECK_VERSION}
ENV GOSU_VERSION=${GOSU_VERSION}
RUN yum install -y ahl-gosu-${GOSU_VERSION} ahl-env-check-${ENV_CHECK_VERSION} git && yum clean all

ARG VERSION

ENV PY_TEMPLATE_DIR /etc/notebooker
ENV GIT_REPO_TEMPLATE_DIR notebook_templates
ENV NOTEBOOKER_TEMPLATE_GIT_URL http://ahldev:Dev1234@ahlgit.maninvestments.com/scm/data/man.notebooker.git
ENV NOTEBOOK_REQUIREMENTS_FILE notebook_requirements.txt

ADD dist /tmp/dist
ADD install_*.sh /tmp/
ADD bin/* /usr/bin/
RUN /tmp/install_latex.sh && rm -f /tmp/install_latex.sh
RUN /tmp/install_pandoc.sh && rm -f /tmp/install_pandoc.sh
RUN /tmp/install_kernel.sh && rm -f /tmp/install_kernel.sh
RUN /tmp/install_man_notebooker.sh && rm -f /tmp/install_man_notebooker.sh /tmp/dist/*.egg


LABEL IMAGE_TAG ${VERSION}

ENTRYPOINT ["/usr/bin/run_man_notebooker.sh"]
