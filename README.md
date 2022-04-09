# Denorm

[![PyPI](https://img.shields.io/pypi/v/denorm)](https://pypi.org/project/denorm/)

<p align="center">
  <img src="doc/logo.png">
</p>

Denormalized and aggregated tables for PostgreSQL.

Keywords: PostgreSQL, denormalization, aggregation, incremental view
maintenance, materialized view

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Overview](#overview)
- [Install](#install)
- [Usage](#usage)
- [Operations](#operations)
- [Performance](#performance)
- [Migration](#migration)
- [Limitations](#limitations)
- [Developing](#developing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

Dernom is similar to PostgreSQL's
[`REFRESH MATERIALIZED VIEW`](https://www.postgresql.org/docs/13/sql-refreshmaterializedview.html),
except that it updates materialized table incrementally.

How it works: Define the query, the tables, and their relationships in JSON.
Denorm generates the SQL DDL statements that create the necessary functions and
triggers. Apply the generated SQL statements to the database. Now, the
materialized target is automatically kept in sync with the source tables.

### Features

- Efficient incremental updates
- Arbitrarily complex SQL features and expressions
- Configurable consistency
- Deadlock-free

## Install

### Pip

```
pip3 install denorm
```

## Usage

For CLI usage, see [Usage](doc/usage.md).

## Operations

Denorm has two operations:

### Aggregate

Create a materalized aggregate of a single table.

See [Aggregate](doc/agg.md).

### Join

Create a materialized join of several tables.

See [Join](doc/join.md).

## Performance

Materialized views exchange slower write performance for higher read
performance.

While it's impossible to escape the fundamental trade-off, Denorm is as fast or
faster than hand-written triggers. It uses statement-level transitions tables to
make batch updates especially efficient.

In deferred mode, Denorm uses temp tables to defer updates until the end of the
transaction. Using temp tables and `ON DELETE COMMIT` reduces I/O overhead and
obviates the need for vacuuming. Since PostgreSQL does not support global
temporary tables, the tables are created as necessary for each session. Thus the
first saliant update in a session may have several millseconds of overhead as
the trigger creates the temporary tables. Pool connections to reduce overhead,
and vacuum reguarly to prevent system tables from bloating.

## Migration

Denorm does not generate migration scripts.

Consider a tool like [migra](https://databaseci.com/docs/migra) to help generate
migration scripts.

## Limitations

Denorm mangles names for generated objects, Long IDs and table names may run
into the PostgreSQL limit of 63 characters for identifiers.

## Developing

Install: `make install`

Generate JSONSchema: `make schema`

Test: `make test`

Generate documentation: `make doc`

Format: `make format`

### Publish

1. Update denorm/version.py.
2. Create commit `Version <version>`.
3. Tag `v<version>`.
4. Push master branch and tag.
5. Publish to PyPI: `make publish`.
