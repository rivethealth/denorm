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
- [Features](#features)
- [Install](#install)
- [Usage](#usage)
- [Operations](#operations)
- [Performance](#performance)
- [Migration](#migration)
- [Limitations](#limitations)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

Denorm uses schema defined in JSON. It generates SQL DDL statements for
functions and triggers. When these SQL stateements are applied to the database,
the triggers keep the target table up-to-date with the sources.

By using declarative JSON files about table relationships and calculations,
aggregated and denormalized tables can be easily maintained.

Denorm is similar to PostgreSQL's `CREATE MATERIALIZED VIEW`, except that denorm
updates the materialized view incrementally.

## Features

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

While it's impossible to completely escape that fundamental trade-off, Denorm is
implementated to be on par with hand-tuned methods, especially for batch
updates.

When applicable, Denorm uses tables with `ON COMMIT DELETE` to minimize I/O
overhead. However, since PostgreSQL does not support global temporary tables,
Denorm must use session temp tables. Thus the first update in a session may have
several millseconds of overhead in creating these tables. Be sure to pool
connections and vacuum reguarly to prevent system tables from bloating.

## Migration

Denorm does not generate migration scripts.

Consider a tool like [migra](https://databaseci.com/docs/migra) to help generate
migration scripts.

## Limitations

Denorm mangles names for generated objects, Long IDs and table names may run
into the PostgreSQL limit of 63 characters for identifiers.
