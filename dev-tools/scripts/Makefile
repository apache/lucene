# basic make safety: if error happens, delete output
.DELETE_ON_ERROR:

# explicitly declare shell used
SHELL := /bin/bash

# enforce that shell is picky
.SHELLFLAGS := -norc -euo pipefail -c

# don't litter up repo with bytecode
export PYTHONDONTWRITEBYTECODE=true

# don't self-check, don't ask questions
PIP_INSTALL_ARGS=--disable-pip-version-check --no-input --upgrade

# override to specific python executable if you need
PYTHON?=python3

# venv with dependencies in the standard location
VENV=${PWD}/.venv

# don't behave strangely if these files exist
.PHONY: lint format reformat autofix ruff ruff-fix pyright env clean smoke-test

# list of directories we check
SOURCES=$(wildcard *.py)

# check formatting, linting, and types
lint: format ruff pyright

# check all formatting
format: env
	# validate imports: if this fails, please run "make reformat" and commit changes.
	$(VENV)/bin/ruff check --select I $(SOURCES)
	# validate formatting: if this fails, please run "make reformat" and commit changes.
	$(VENV)/bin/ruff format --diff $(SOURCES)

# reformat code to conventions
reformat: env
	# organize imports
	$(VENV)/bin/ruff check --select I --fix $(SOURCES)
	# reformat sources
	$(VENV)/bin/ruff format $(SOURCES)

# fixes all code problems that are safe to automatically fix (including formatting)
autofix: ruff-fix reformat

# lints sources
ruff: env
	# validate sources with ruff linter: if this fails, try "make autofix".
	$(VENV)/bin/ruff check $(SOURCES)

# fixes (safe) issues that are autofixable such as deprecated/renamed APIs
ruff-fix: env
	# (safe) fixes sources with ruff linter
	$(VENV)/bin/ruff check --fix $(SOURCES)

# checks types
pyright: env
	# type-check sources with basedpyright
	$(VENV)/bin/basedpyright $(SOURCES)

# runs smoketester against a release
smoke-test: env
	# ensure that you provided URL so that we know what to test
	# run this command like: make smoke-test URL=https://dist.apache.org/...
	test -n "$(URL)"
	$(VENV)/bin/python smokeTestRelease.py $(URL)

# rebuild venv if dependencies change
env: $(VENV)/bin/activate
$(VENV)/bin/activate: requirements.txt Makefile
	# remove any existing venv
	rm -rf $(VENV)
	# check that python meets minimum version requirements
	$(PYTHON) -c "import sys; assert sys.version_info[0] == 3 and sys.version_info[1] >= 12, 'UPGRADE PYTHON'"
	# create virtual environment
	$(PYTHON) -m venv $(VENV)
	# install dependencies into venv
	$(VENV)/bin/pip install $(PIP_INSTALL_ARGS) -r requirements.txt
	# adjust timestamp for safety
	touch $(VENV)/bin/activate

# nuke venv
clean:
	rm -rf $(VENV)
