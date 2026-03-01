FROM mambaorg/micromamba:2.0.8 AS micromamba

ENV PYTHONDONTWRITEBYTECODE=true
ARG MAMBA_DOCKERFILE_ACTIVATE=1 

USER root
RUN mkdir -p /code
ADD . /code
WORKDIR /code

RUN micromamba install --yes --name base  --file environment.yml --verbose \
  && micromamba run --name base pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir /code

# Clean up
RUN micromamba clean --all --index-cache --packages --tarballs \
     --locks --trash --force-pkgs-dirs --yes \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type d -name '__pycache__' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete \
    && find /opt/conda/lib/python*/site-packages/bokeh/server/static -follow -type f -name '*.js' ! -name '*.min.js' -delete \
    && micromamba run --name base pip cache purge \
    && rm -rf /root/.cache \
    && micromamba env export --name base --explicit
     
RUN waterbodies --version

FROM ubuntu:jammy

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

## Non-root user creation
RUN if grep -q "${NB_UID}" /etc/passwd; then \
        userdel --remove $(id -un "${NB_UID}"); \
    fi
RUN useradd --no-log-init --create-home --shell /bin/bash --no-user-group --gid $NB_GID --uid $NB_UID  $NB_USER \
    && chown -R $NB_UID:$NB_GID $HOME

COPY --chown=$NB_UID:$NB_GID --from=micromamba /opt/conda $PYTHON_ENV

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
        && unzip awscliv2.zip \
        && ./aws/install \ 
        && rm awscliv2.zip \
        && rm -rf ./aws/

USER root
RUN apt clean autoclean -y \
  && apt autoremove -y \
  && rm -rf /var/lib/{apt,dpkg,cache}/

USER $NB_USER
WORKDIR $HOME
RUN waterbodies --version