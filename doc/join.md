# Join

Join multiple tables.

## Overview

1. Create a target table that will hold the denormalized data.

2. Compose a query that will populate that table. Include a placeholder `$1` for
   the recordset of primary keys.

3. Author a JSON file describing the relationships between the underlying tables
   of the query.

4. Run the `denorm create-join` command to generate SQL DDL for triggers and
   functions.

5. Apply the generated SQL to the database.

Now the target table will be kept up-to-date whenever the relevant tables
change.

## Example

For a full working example, see [Join example](join-example.md).

## Schema

See [Join config](join-schema.md) for documentation of all options.

## Generated objects

The `id` is used to name database objects.

Database objects are created in `schema`, if given. Otherwise they will be
created in the default schema.

## Target

The target table must have a primary key. It may have columns not populated by
the query.

## Tables

Tables are the source tables from which changes will be propogated.

If the primary key of the target is known from the table,

- `targetKey` - List of expressions for primary key of target table

Otherwise, it must reference another table,

- `join` - Identify of the source to reference.
- `joinOn` - Expression for an inner join on the two sources.

## Lock

Using upserts in `REPEATABLE READ` transactions and multi-table joins can be
susceptible to ordering conditions.

To prevent these, denorm can use a value lock table on the target key.

## Constistency

There are two consistency modes.

### Immediate

The target is updated by the end of the statement.

### Deferred

The target is updated at the end of the transaction.

Deferring work involves overhead. It is useful for deduplicating work when
related data is modified multiple times. A common example is updating multiple
levels of records (insert a record and children and grandchildren all in the
same transaction).

## Join mode

There are two modes for joining tables

### Synchronous

Join to all dependency records in the current transaction.

### Asynchronous

If tables have an 1:N relationship with a very large N — say, tens of thousands
— it may not be feasible to process all records a single transaction. Denorm
allows the join to be broken up over multiple transactions.

Both the table and the dependency table must have a defined unique key.

Building on the book example, suppose each book had a genre, and the genre's
name is to be included in the target table. A genre may have hundreds of
thousands of books, so we'll chunk updates to `genre` by iterating through
related `book` records by `id`.

<details>
<summary>book_full.yml</summary>

```yml
tables:
  author:
    join: author.id = book_author.author_id
    joinDep: book_author
    name: book_author
    schema: public
  book:
    key: [id]
    name: book
    schema: public
    targetKey: [book.id]
  book_author:
    name: book_author
    schema: public
    targetKey: [book_author.book_id]
  genre:
    join: book.genre_id = genre.id
    joinDep: book
    joinMode: iterate
    name: genre
    schema: public
```

</details>

The state of the join is tracked in the table `book_full__que__genre`.

#### Worker

Updates will not automatically affect the target table. Instead, changes are
queued and must be processed seperately by a worker.

The queue uses
[advistory locks.](https://www.postgresql.org/docs/12/explicit-locking.html#ADVISORY-LOCKS)

```sql
-- Find an incomplete author change and refresh the target for up to 1000 corresponding
-- book_author records.
-- Return whether additional processing remains to be done.
SELECT book_full__pcs__genre(1000);
```

This function should be called periodically. Additionally workers can listen to
the `public.book_full__que__genre` listener.

</details>

### Errors

Errors in updating the target no longer fail the original transaction. Ensure
that the query does not have errors, else they will halt asynchronous updates.

### Performance

The dependency table should have an btree index that covers the foreign key and
its own unique key, in that order.

In the earlier example,

```sql
CREATE INDEX ON book (genre_id, id);
```

Take careful note of this requirement, as indices do not usually include the
unique key of the table.
