FROM base.docker.prod.ahl/base/medusa-python:27-2

MAINTAINER AHL Data Science

ARG ENV_CHECK_VERSION=1.0.0
ARG GOSU_VERSION=1.9.0

ENV DEVPI_SERVER=http://ahldevpi.dev.man.com:3142 \
    DEVPI_INDEX=ahl/27-2 \
    MEDUSA_ENV=/opt/man/releases/python-medusa/27-2
ENV PATH="${MEDUSA_ENV}/bin:${PATH}"

ENV PYTHON_EGG_CACHE /tmp/egg-cache

ENV ENV_CHECK_VERSION=${ENV_CHECK_VERSION}
ENV GOSU_VERSION=${GOSU_VERSION}
RUN yum install -y ahl-gosu-${GOSU_VERSION} ahl-env-check-${ENV_CHECK_VERSION}

ARG VERSION

ADD dist /tmp/dist
ADD install_datascience.sh /tmp/
RUN /tmp/install_datascience.sh && rm -f /tmp/install_datascience.sh /tmp/dist/*.egg

ADD bin/* /usr/bin/

LABEL IMAGE_TAG ${VERSION}

ENTRYPOINT ["/usr/bin/run_datascience.sh"]
