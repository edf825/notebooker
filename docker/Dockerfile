FROM base.docker.prod.ahl/base/medusa-python:27-3.9

MAINTAINER AHL Data Science

ARG ENV_CHECK_VERSION=1.0.0
ARG GOSU_VERSION=1.9.0

ENV MEDUSA_ENV=/default-medusa-venv
ENV PATH="${MEDUSA_ENV}/bin:${PATH}"

ENV PYTHON_EGG_CACHE /tmp/egg-cache

ENV ENV_CHECK_VERSION=${ENV_CHECK_VERSION}
ENV GOSU_VERSION=${GOSU_VERSION}
RUN yum install -y ahl-gosu-${GOSU_VERSION} ahl-env-check-${ENV_CHECK_VERSION} && yum clean all

ARG VERSION

ADD dist /tmp/dist
ADD install_*.sh /tmp/
RUN /tmp/install_man_notebooker.sh && rm -f /tmp/install_man_notebooker.sh /tmp/dist/*.egg
RUN /tmp/install_kernel.sh && rm -f /tmp/install_kernel.sh

ADD bin/* /usr/bin/

LABEL IMAGE_TAG ${VERSION}

ENTRYPOINT ["/usr/bin/run_man_notebooker.sh"]
