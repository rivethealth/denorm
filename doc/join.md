# Join

Join multiple tables.

## Steps

1. Create a target table that will hold the denormalized data.

2. Compose a query that will populate that table. Include a placeholder `$1` for
   the recordset of primary keys.

3. Author a JSON file describing the relationships between the underlying tables
   of the query.

4. Run the `denorm create-join` command to generate SQL DDL for triggers and
   functions.

5. Apply generated SQL to the database.

Now the target table will be kept up-to-date whenever the relevant tables
change.

## Example

For a full working example, see [Join example](join-example.md).

## Schema

See the JSONSchema ([JSON](../denorm/formats/join.json)
[YAML](../schema/join.yml)) for documentation of all options.

## Generated objects

ID is used to name database objects.

Database objects are created in the schema if given. Otherwise they will be
created in the default schema.

## Target

The target table must have a primary key. It may have columns not populated by
the query.

## Tables

Tables are the source tables from which changes will be propogated.

If the primary key of the target is known from the table,

- targetKey - List of expressions for primary key of target table

Otherwise, it must reference another table,

- dep - Identifier of the source to reference.
- depJoin - Expression for an inner join on the two sources.

## Constistency

There are two consistency modes.

### Immediate

The target is updated at the end of the statement.

### Deferred

The target is updated at the end of the transaction.

Deferring work involves overhead. It is useful for avoiding excess querying when
related data is modified multiple times. For example, when multiple levels of
records are updated (insert a record and its children and grandchildren all in
the same transaction).

## Dependency mode

### Sync

Join to all dependency records in the current transaction.

### Async

If tables have an 1:N relationship with a very large N, say tens of thousands,
it may not be feasible to process all records a single transaction. Denorm
allows the join to be broken up over multiple transactions.

Both the table and the dependency table must have a defined unique key. The
dependency table must have an btree index that covers the foreign key and its
own unique key.

Building on the book example, suppose each book had a genre, and the genre's
name is to be included in the target table. A genre may have hundreds of
thousands of books, so we'll chunk updates to `genre` by iterating through
`book` records by `id`.

<details>
<summary>book_full.yml</summary>

```yml
tables:
  author:
    dep: book_author
    depJoin: author.id = book_author.author_id
    name: book_author
    schema: public
  book:
    key: [id]
    name: book
    schema: public
    targetKey: [id]
  book_author:
    dep: book
    depJoin: book_author.book_id = book.id
    name: book_author
    schema: public
  genre:
    dep: book
    depJoin: book.genre_id = genre.id
    depMode: iterate
    name: genre
    schema: public
```

</details>

Note that for performance, `book` must have an index on `genre_id, id`.

#### Worker

Updates will not automatically affect the target table. Instead, changes are
queued and must be processed seperately by a worker.

The queue uses
[advistory locks.](https://www.postgresql.org/docs/12/explicit-locking.html#ADVISORY-LOCKS)

```sql
-- Find an incomplete author change and refresh the target for up to 1000 corresponding
-- book_author records.
-- Return whether additional processing remains to be done.
CALL book_full__pcs__genre(1000);
```

<details>
<summary>Functions may be called individually for additional control.</summary>

Run the begin function to find and lock a record that requires work.

```sql
-- Find and lock an incomplete author change.
SELECT book_full__beg__genre();
```

If this returns a non-null bigint, there is work available. Run a transaction
and pass that value to the update function.

```sql
BEGIN
-- Refresh the target for up to 1000 corresponding book_author records.
-- Return whether additional processing remains to be done.
SELECT book_full__rfs__genre($1, 1000);
COMMIT
```

Release the lock.

```sql
-- Unlock the author change.
SELECT book_full__end__genre($1)
```

</details>

This function should be called periodically. Additionally workers can listen to
the `public.book_full__chg__genre` listener.

### Errors

Errors in updating the target no longer fail the original transaction.

When processing begins, the change is moved to the back of the queue, regardless
of whether processing succeeds. Therefore, problematic records will not prevent
processing other change records, but you should fix the error soon, as that
change is stuck until then.

### Limitations

This option is only valid on single-table sources.
