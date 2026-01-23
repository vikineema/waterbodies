FROM mambaorg/micromamba:2.5.0 AS micromamba

ARG MAMBA_DOCKERFILE_ACTIVATE=1 
ENV PYTHONDONTWRITEBYTECODE=true 

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml requirements.txt /code/
RUN micromamba install -y -n base -f /code/environment.yml \
    && micromamba run -n base pip install --no-cache-dir -r /code/requirements.txt \
    && micromamba clean --all --force-pkgs-dirs --yes

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
USER $MAMBA_USER

COPY --chown=$MAMBA_USER:$MAMBA_USER . /code
RUN micromamba run -n base pip install --no-cache-dir /code

ARG BUILD_ENV=prod
RUN if [ "$BUILD_ENV" = "dev" ]; then \
    micromamba install --yes --name base  jupyterlab --verbose \
        && micromamba clean --all --force-pkgs-dirs --yes; \
    fi 

RUN micromamba clean --all --force-pkgs-dirs --yes \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete \
    && find /opt/conda/ -follow -type f -name '*.c' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyx' -delete \
    && find /opt/conda/ -follow -type f -name '*.md' -delete \
    && find /opt/conda/ -follow -type f -name '*.rst' -delete \
    && find /opt/conda/ -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true \
    && find /opt/conda/ -name '*.egg-info' -type d -exec rm -rf {} + 2>/dev/null || true \
    && find /opt/conda/ -name 'tests' -type d -exec rm -rf {} + 2>/dev/null || true \
    && find /opt/conda/ -name 'test' -type d -exec rm -rf {} + 2>/dev/null || true \
    && ( [ ! -d /opt/conda/lib/python*/site-packages/bokeh/server/static ] || find /opt/conda/lib/python*/site-packages/bokeh/server/static -follow -type f -name '*.js' ! -name '*.min.js' -delete ) \
    && micromamba run --name base pip cache purge \
    && rm -rf /tmp/* /code/.git /code/.github \
    && micromamba env export --name base --explicit

FROM ubuntu:jammy-20260109 AS awscli-builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip ca-certificates \
    && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip -q awscliv2.zip \
    && ./aws/install \
    && rm -rf /usr/local/aws-cli/v2/current/dist/awscli/examples \
    && rm -rf /usr/local/aws-cli/v2/current/dist/awscli/topics \
    && rm -rf awscliv2.zip ./aws

FROM ubuntu:jammy-20260109
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG NB_USER="jovyan"
ARG NB_UID="1000"
ARG NB_GID="100"
ENV PYTHON_ENV=/opt/conda
ENV DEBIAN_FRONTEND=noninteractive \ 
    SHELL=/bin/bash \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    LANGUAGE=C.UTF-8 \    
    USE_PYGEOS=0 \
    SPATIALITE_LIBRARY_PATH='mod_spatialite.so' \
    HOME="/home/${NB_USER}" \
    PATH="${PYTHON_ENV}/bin:${PATH}" \
    PROJ_DATA="${PYTHON_ENV}/share/proj" 

RUN if grep -q "${NB_UID}" /etc/passwd; then \
        userdel --remove $(id -un "${NB_UID}"); \
    fi \
    && useradd --no-log-init --create-home --shell /bin/bash --no-user-group --gid $NB_GID --uid $NB_UID  $NB_USER \
    && chown -R $NB_UID:$NB_GID $HOME

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --chown=$NB_UID:$NB_GID --from=micromamba /opt/conda $PYTHON_ENV
COPY --from=awscli-builder /usr/local/aws-cli /usr/local/aws-cli
ENV PATH="/usr/local/aws-cli/v2/current/bin:${PATH}"

USER $NB_USER
WORKDIR $HOME
RUN waterbodies --version