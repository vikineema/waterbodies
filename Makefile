#!make
SHELL := /usr/bin/env bash

include  .env
ENV_FILE =  $(abspath .env)
export ENV_FILE

CONDA_ENV_NAME := $(shell yq -r '.name' $(ENV_YAML))
.DEFAULT_GOAL := help

.PHONY: help setup up down clean test

PRODUCT_CATALOG = "https://raw.githubusercontent.com/digitalearthafrica/config/master/prod/products_prod.csv"

help: ## Print this help
	@grep -E '^##.*$$' $(MAKEFILE_LIST) | cut -c'4-'
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}'

activate-local-env: ## Activate the local Python virtual environment
	nix develop

setup-prod-env: ## Setup your production environment
	# Build the base production image
	docker compose pull
	docker compose build
	# Bring up your production Docker environment.
	docker compose up -d 
	# Setup the database
	docker compose exec -T waterbodies datacube -v system init
	# Add product definitions
	docker compose exec -T waterbodies datacube -v product add https://raw.githubusercontent.com/digitalearthafrica/config/master/products/wofs_ls.odc-product.yaml

build: ## Build the base test image
	docker compose -f compose_dev.yaml pull
	docker compose -f compose_dev.yaml build

up: ## Bring up your test Docker environment.
	docker compose -f compose_dev.yaml up -d 

init: ## Prepare the database, initialise the database schema.
	docker compose -f compose_dev.yaml exec -T waterbodies-test datacube -v system init

add-products: ## 3. Add the wofs_ls product definition for testing.
	docker compose -f compose_dev.yaml exec -T  waterbodies-test datacube -v product add https://raw.githubusercontent.com/digitalearthafrica/config/master/products/wofs_ls.odc-product.yaml

index: ## 4. Index the test data.
	cat index_tiles.sh | docker compose -f compose_dev.yaml exec -T index bash

setup-explorer: ## Setup the datacube explorer
	# Initialise and create product summaries
	docker compose -f compose_dev.yaml up -d explorer
	docker compose -f compose_dev.yaml exec -T explorer cubedash-gen --init --all
	# Services available on http://localhost:${EXPLORER_PORT}/products

install-waterbodies: ## 5. Install waterbodies
	docker compose exec -T waterbodies bash -c "pip install -e ."

sleep:
	sleep 1m

test-env: build up sleep init add-products index install-waterbodies

run-tests:
	docker compose -f compose_dev.yaml exec -T waterbodies-test bash -c "coverage run -m pytest ."
	docker compose -f compose_dev.yaml exec -T waterbodies-test bash -c "coverage report -m"
	docker compose -f compose_dev.yaml exec -T waterbodies-test bash -c "coverage xml"
	docker compose -f compose_dev.yaml exec -T waterbodies-test bash -c "coverage html"

down: ## Bring down the system
	docker compose down

shell: ## Start an interactive shell
	docker compose exec waterbodies bash

clean: ## Delete everything
	docker compose down --rmi all --volumes

logs: ## Show the logs from the stack
	docker compose logs --follow


pip_compile: ## Compile Python dependencies in a fresh environment
	# The commented lines below need to be run
	# individually in the terminal before running this target:
	# micromamba-shell
	# micromamba activate base
	# rm -rf $(HOME)/micromamba/envs/$(CONDA_ENV_NAME)/
	mkdir -p $(TMPDIR)
	# micromamba create -n $(CONDA_ENV_NAME) -f $(ENV_YAML) -y
	# DEV tools
	micromamba install -n $(CONDA_ENV_NAME) pip pip-tools -y
	micromamba run -n $(CONDA_ENV_NAME) pip-compile \
		--extra=lint \
		--extra=tests \
		--extra=viz \
		--output-file=requirements.txt \
		pyproject.toml \
		--verbose \
		--upgrade
