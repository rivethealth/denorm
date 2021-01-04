###
# Config
###

.DELETE_ON_ERROR:
.SECONDARY:
.SUFFIXES:

SHELL := /bin/bash

JOBS ?= $(shell nproc)
MAKEFLAGS += -j $(JOBS) -r

PATH += node_modules/.bin

LPAREN := (
RPAREN := )

###
# Clean
###

TARGET := awscli_saml.egg-info build target

.PHONY: clean
clean:
	rm -fr $(TARGET)

###
# Format
###
FORMAT_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.py')
PRETTIER_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) \$(LPAREN) -name '*.md' \$(RPAREN))

.PHONY: format
format: target/format.target

.PHONY: test-format
test-format: target/format-test.target

target/format.target: target/.node_modules.target $(FORMAT_SRC) $(PRETTIER_SRC) .prettierrc.yml
	black $(FORMAT_SRC)
	prettier -w $(PRETTIER_SRC)
	mkdir -p $(@D)
	touch $@ target/format-test.target

target/format-test.target: $(FORMAT_SRC)
	black --check $(FORMAT_SRC)
	mkdir -p $(@D)
	touch $@ target/format.target

###
# Pip
###
PY_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.py')

.PHONY: install
install:
	pip3 install -e .

.PHONY: package
package: target/package.target

upload: target/package-test.target
	python3 -m twine upload target/package/*

target/package.target: setup.py README.md $(PY_SRC)
	rm -fr $(@:.target=)
	mkdir -p $(@:.target=)
	./$< bdist_wheel -d $(@:.target=) sdist -d $(@:.target=)
	> $@

target/package-test.target: target/package.target
	python3 -m twine check target/package/*
	mkdir -p $(@D)
	> $@

###
# Docker
###

.PHONY: docker
docker:
	docker build -t rivethealth/aws-saml .

###
# Yarn
###

target/.node_modules.target: package.json yarn.lock
	mkdir -p $(@D)
	yarn install
	> $@
