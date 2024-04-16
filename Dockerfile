FROM osgeo/gdal:ubuntu-small-3.6.3

ENV SHELL=bash

ENV DEBIAN_FRONTEND=non-interactive

# Update sources list.
RUN apt clean && apt update \
  # Install basic tools for developer convenience.
  && apt install -y \
    curl \
    git \
    tmux \ 
    unzip \
    vim  \
    jq \
  # Install pip3.
  && apt install -y --fix-missing --no-install-recommends \
    python3-pip \
  && python -m pip install --upgrade pip \
  # For psycopg2
  && apt install -y libpq-dev \ 
  # For hdstats
    python3-dev \
    build-essential \
  # For spatialite 
    libsqlite3-mod-spatialite \
  # Clean up.
  && apt clean \
  && apt  autoclean \
  && apt autoremove \
  && rm -rf /var/lib/{apt,dpkg,cache,log}

# Install AWS CLI.
WORKDIR /tmp
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# Copy requirements.txt and install python packages from requirements.txt.
RUN mkdir -p /conf
COPY requirements.txt /conf/
RUN pip install -r /conf/requirements.txt

# Copy source code.
RUN mkdir -p /code
WORKDIR /code
ADD . /code
# Install source code.
RUN echo "Installing waterbodies through the Dockerfile."
RUN pip install .

RUN pip freeze && pip check

# Make sure it's working
RUN waterbodies --version