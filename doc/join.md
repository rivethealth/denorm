# Join

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Overview](#overview)
- [Example](#example)
- [Options](#options)
- [Asynchronous joins](#asynchronous-joins)
- [Backfill](#backfill)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Overview

Join gathers keys from multiple tables, and then uses those keys to perform a
destination action.

```sh
< schema.json denorm create-join > output.sql
```

## Example

For a full working example, see [Join example](join-example.md).

## Options

See [Join config](join-schema.md) for documentation generated from JSONSchema.

### Root

#### Constistency

`consistency`

There are two modes. The default is immedidate.

##### Immediate

```json
"immediate"
```

The destination is updated at the end of the statement.

##### Deferred

```json
"deferred"
```

The destination is updated at the end of the transaction.

Deferring work involves overhead. It can be useful in a couple cases

- Deduplicating work. For example, if multiple "levels" of records (e.g. a
  record and children and grandchildren) are affected in the same transaction.
- Reducing lock duraton on destination records.

#### Context

Context propogates PostgreSQL settings through async joins.

Recall that custom settings must be two-part dot-delimited. See
[Customized Options](https://www.postgresql.org/docs/13/runtime-config-custom.html).

#### ID

`id`

The ID is used to name database objects. To prevent naming conflicts, the ID
should be unique within a schema.

#### Key

The destination primary key is specified by `key`, which is an array of the column
names and types.

```json
[{ "name": "id", "type": "bigint" }]
```

If `destinationTable.tableKey` is specified, `key` can be ommitted, and inferred from
that.

#### Lock

`lock`

Whether to use a lock table.

Using upserts in `REPEATABLE READ` transactions and multi-table joins can be
susceptible to ordering conditions.

To prevent these, denorm can use a value lock table on the target key.

#### Schema

`schema`

If specified, created generated objects in the this schema. If not specified,
objects are created and referenced without schema qualifiers.

#### Tables

`tables`

A map of IDs to source table definitions.

#### Destination query

`destinationQuery`

This query the gathered keys. Placeholders take the form of `${key}`. Literal
`$` characters are escaped as `$$`.

Placeholders:

- `key` - table (or parenthesized table expression) of keys
- `table` - table name, or empty if deferred

```json
"SELECT * FROM ${key}"
```

#### Destination table

`destinationTable`

See [Destination table](#destination-table-1).

### Destination table

The destination table that will receive changes.

#### Columns

`tableColumns`

The column names, in the same order as returned by the destination query.

#### Key

`tableKey`

The column names of the unique key of the table. Used for asynchronous joins.

#### Schema

`tableSchema`

The schema name of the table. If not specified, the table is referenced without
qualification.

#### Table

`table`

The table name.

### Table

The source table from which changes will be propogated.

Either the target key (direct relationship with the target) or the join
(transitive relationship with the target) must be specified.

#### Lock ID

The 16-bit
[advisory lock](https://www.postgresql.org/docs/12/explicit-locking.html#ADVISORY-LOCKS)
prefix to use for queueing, if the join is asynchronous. By default, the lock
space is generated from the ID and table ID.

The base lock ID is in the comment on the `lock` column.

#### Join table

`joinTargetTable`

The ID of the table to join to.

#### Join mode

`joinMode`

There are two modes for joining tables

##### Synchronous

```json
"sync"
```

Join to all dependency records in the current transaction.

##### Asynchronous

```json
"async"
```

If tables have an 1:N relationship with a very large N — say, tens of thousands
— it may not be feasible to process all records a single transaction. Denorm
allows the join to happen over multiple transactions.

Use `tableColumns` for the relevant data on the table, and `joinTargetKey` to indicate a
unique key on the foreign table. Also specify `tableKey` to indicate the unique key on
this table. These are used to track the iteration state.
(See comments in Performance section).

Building on the book example, suppose each book had a genre, and the genre's
name is to be included in the target table. A genre may have hundreds of
thousands of books, so we'll chunk updates to `genre` by iterating through
related `book` records by `id`.

<details>
<summary>book_full.yml</summary>

```yml
tables:
  author:
    tableName: author
    tableSchema: public
    joinOn: author.id = book_author.author_id
    joinTargetTable: book_author
  book:
    tableName: book
    tableColumns:
      - name: id
    tableSchema: public
    destinationKeyExpr: [book.id]
  book_author:
    tableName: book_author
    tableSchema: public
    destinationKeyExpr: [book_author.book_id]
  genre:
    tableName: genre
    tableSchema: public
    tableKey: [id]
    joinTargetTable: book
    joinTargetKey: [id]
    joinOn: book.genre_id = genre.id
    joinMode: async
```

</details>

See additional comments in [Asynchronous joins](#asynchronous-joins).

#### Join on

`joinOn`

The conditional expression for joining.

#### Join other

`joinOther`

Expressions to add to the joins, before joining to this table. This can add
extra context. Care should be taken to ensure that tables referenced here are
tracked and monitored for changes elsewhere.

#### Table

`tableName`

The table name. If not specified, the table is a "pseudo table." This is useful
for asynchronous backfills.

#### Destination key

`destinationKeyExpr`

SQL expressions for the destination table key.

#### Schema

`tableSchema`

The schema name. If unspecified, the table is referenced without schema
qualification.

## Asynchronous joins

For asynchronous joins, updates will not automatically affect the destination table.
Instead, the state of the join is tracked in a table (e.g.
`book_full__que__genre`) and must be processed by a worker.

```sql
-- Find an incomplete author change and refresh the destination for up to 1000 corresponding
-- book_author records.
-- Return whether additional processing remains to be done.
SELECT book_full__pcs__genre(1000);
```

This function should be called periodically. For lower latency updates, workers
can listen to the `public.book_full__que__genre` topic which notified whenever a
join requires processing.

### Errors

Errors in updating the destination no longer fail the original transaction. Ensure
that the query does not have errors, else they will halt asynchronous updates.

### Performance

The dependency table should have an btree index that covers the foreign key and
its own unique key, in that order.

In the earlier example, that index would be:

```sql
CREATE INDEX ON book (genre_id, id);
```

Take careful note of this requirement, as such indices do not usually include
the second part (a unique key on the table itself). However, this is essential
for good performance as it allows the join to continue where it left off,
without unnecessary scans.

## Backfill

Denorm can be leveraged to create an asynchronous fill of the entire table.

Add a tables entry (suggested name: all) with `joinTargetTable`, `joinMode: async`, and
`joinTargetKey`.

<details>
<summary>book_full.yml</summary>

```yml
tables:
  all:
    joinTargetTable: book
    joinMode: async
    joinTargetKey: [id]
  author:
    join: book_author
    joinOn: author.id = book_author.author_id
    name: author
  book:
    name: book
    targetKey: [book.id]
  book_author:
    name: book_author
    targetKey: [book_author.book_id]
    name: book_author
```

</details>

Then run

```sql
SELECT test__rfs__all();
```

and after successive `test__pcs__all()`, the table will be backfilled/refreshed.
