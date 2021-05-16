# Denorm

[![PyPI](https://img.shields.io/pypi/v/denorm)](https://pypi.org/project/denorm/)

Denormalized and aggregated tables for PostgreSQL.

Keywords: PostgreSQL, denormalization, aggregation, incremental view
maintenance, materialized view

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Install](#install)
- [Features](#features)
- [Usage](#usage)
- [Operations](#operations)
- [Performance](#performance)
- [Migration](#migration)
- [Limitations](#limitations)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Install

### Pip

```
pip3 install denorm
```

## Features

- Efficient incremental updates
- Arbitrarily complex SQL features and expressions
- Configurable consistency
- Deadlock-free

## Usage

For usage, see [Usage](doc/usage.md).

## Operations

Denorm has two modes of operation:

### Aggregate

Create a materalized aggregate of a single table.

For documentation, see [Aggregate](doc/agg.md).

Example query:

```sql
SELECT author_id, count(*) AS book_count
FROM book_author
GROUP BY 1
```

### Join

Create a materialized join of several tables.

For documentation, see [Join](doc/join.md).

Example query:

```sql
SELECT
  b.id,
  b.title,
  a.names
FROM
  book AS b
  CROSS JOIN LATERAL (
    SELECT coalesce(array_agg(a.name ORDER BY ba.ordinal), '{}') AS names
    FROM
      author AS a
      JOIN book_author AS ba ON a.id = ba.author_id
    WHERE b.id = ba.book_id
  ) AS a
```

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
