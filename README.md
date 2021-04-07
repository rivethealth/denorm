# Denorm

[![PyPI](https://img.shields.io/pypi/v/denorm)](https://pypi.org/project/denorm/)

Denormalized and aggregated tables for PostgreSQL.

Keywords: PostgreSQL, denormalization, aggregation, incremental view
maintenance, materialized view

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Install](#install)
- [Operations](#operations)
- [Usage](#usage)
- [Features](#features)
- [Performance](#performance)
- [Migration](#migration)
- [Limitations](#limitations)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Install

### Pip

```
pip3 install denorm
```

## Operations

Denorm has two modes of operation:

### Aggregate

Create a materalized aggregate. For example, create an incrementally updated
table of the following:

```sql
SELECT author_id, count(*) AS book_count
FROM book_author
GROUP BY 1
```

For full documentation, see [Aggregate](doc/agg.md).

### Join

Create a materialized join. For example, create an incrementally updated table
of the following:

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

For full documentation, see [Join](doc/join.md).

## Usage

See [Usage](doc/usage.md).

## Features

- Efficient incremental updates
- Arbitrarily complex SQL features and expressions
- Configurable consistency
- Deadlock-free

## Performance

Materialized views exchange slower write performance for higher read
performance.

While it's impossible to completely escape that fundamental trade-off, Denorm is
to be on par with hand-tuned methods.

Statement triggers reduce overhead for modifying many records.

When applicable, Denorm uses tables with `ON COMMIT DELETE` to minimize I/O
overhead. However, since PostgreSQL does not support global temporary tables,
Denorm must use session temp tables. Thus the first update in a session may have
several ms of overhead in setting up these tables. Be sure to pool connections
and vacuum reguarly to prevent system tables from bloating.

## Migration

Denorm does not generate migration scripts.

Consider a tool like [migra](https://databaseci.com/docs/migra) to help generate
migration scripts.

## Limitations

Note that PostgreSQL identifiers are limited to 63 characters. Denorm mangles
names for generated objects, so long IDs and table names may run into this
limit.
