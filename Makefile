###
# Config
###

.DELETE_ON_ERROR:
.SECONDARY:
.SUFFIXES:

SHELL := /bin/bash

JOBS ?= $(shell nproc)
MAKEFLAGS += -j $(JOBS) -r

export PATH := $(PATH):node_modules/.bin

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
# Schema
###

SCHEMA_SRC := $(shell find schema -name '*.yml')
SCHEMA_TGT := $(SCHEMA_SRC:schema/%.yml=denorm/formats/%.json)

.PHONY: schema
schema: $(SCHEMA_TGT)

denorm/formats/%.json: schema/%.yml
	mkdir -p $(@D)
	< $< yq > $@

###
# Markdown
###

SCHEMA_DOC := $(SCHEMA_TGT:denorm/formats/%.json=doc/%-schema.md)

.PHONY: doc
doc: doc/usage.md $(SCHEMA_DOC)

doc/%-schema.md: denorm/formats/%.json
	jsonschema2md $< $@

doc/usage.md: script/usage-doc.sh
	$< > $@

###
# Format
###
FORMAT_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.py')
PRETTIER_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) \$(LPAREN) -name '*.json' -o -name '*.md' -o -name '*.yml' \$(RPAREN))

.PHONY: format
format: target/format.target

.PHONY: test-format
test-format: target/format-test.target

target/format.target: target/node_modules.target $(FORMAT_SRC) $(PRETTIER_SRC) .prettierrc.yml
	doctoc --notitle --maxlevel 2 README.md
	doctoc --notitle --maxlevel 2 doc/join.md
	isort --profile black $(FORMAT_SRC)
	black -t py37 $(FORMAT_SRC)
	prettier -w $(PRETTIER_SRC)
	mkdir -p $(@D)
	touch $@ target/format-test.target

target/format-test.target: $(FORMAT_SRC)
	black --check -t py37 $(FORMAT_SRC)
	mkdir -p $(@D)
	touch $@ target/format.target

###
# Pip
###
PY_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.py')

.PHONY: install
install:
	pip3 install -e .[dev]

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
# Test
###

.PHONY: test
test: $(SCHEMA_TGT)
	pytest

###
# Yarn
###

target/node_modules.target: package.json yarn.lock
	mkdir -p $(@D)
	yarn install
	> $@
