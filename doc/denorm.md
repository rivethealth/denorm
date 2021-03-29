# Denormalization

## Generated objects

ID is used to name database objects.

Database objects are created in the schema if given. Otherwise they will be
created in the default schema.

## Target

The target table must have a primary key. It may have columns not populated by
the query.

## Sources

The purposes of `sources` is to calculate the primary keys of the target that
needed to be refreshed (inserted, updated, deleted).

Each sources `sources` item has:

- id - Identifier of this source
- tables - List of tables and columns. Columns are any columns used in
  `expression`, `identity`, or `join`.
- expression - From expression, using $1, $2, etc as the placeholders for
  tables. Alias the tables to provide a consistent name that can be referenced
  elsewhere.

If this source has the primary key,

- identity - Expression for primary key of target table

Otherwise, if additional tables need to be referenced to find the primary key,

- dep - Identifier of the source to reference.
- join - Expression for an inner join on the two sources.

Usually, each source has a single table, though denorm supports other cases, for
example a cartesian product of two tables.

## Refresh

- id
- expression
- identity
- iterator

## Constistency

_TODO: not yet implemented_

There are two supported consistency modes.

### transaction

This is the default. The target table is updated at the end of the transaction,
via a constraint trigger.

Deferring work until the end of a transaction is particularly useful for
avoiding excess work for "nested" records (insert a record and its children and
grandchildren all in the same transaction).

### iterate

Some tables may have a 1:N relationship with a very large N, say tens of
thousands.

In such cases, it may not be feasible to update the denormalized table in a
single transaction. Instead, denorm allows the update to be broken up over
multiple transactions.

For example, an author may have many books. (This particular example is
contrived, as in fact as an individual author will not have thousands of books,
so in this case it would really be preferrable to use the simpler transaction
consistency mode.)

Add `partition` and `iterator` expressions.

- partition - each iteration scope
- iterator - iteration key

<details>
<summary>book_full.yml</summary>

```yml
- id: author
  tables: [{ schema: public, table: author, columns: ["id"] }]
  expression: $1 AS author
  dep: book_author
  join: author.id = book_author.author_id
  partition: author.id
  iterator: book_author.id
```

</details>

Now, each occurrence of a changed `author.id` will iterate by `book_author.id`.

### Worker

Updates will not automatically affect the target table. Instead, changes are
queued and must be processed seperately by a worker.

The queue uses
[advistory locks.](https://www.postgresql.org/docs/12/explicit-locking.html#ADVISORY-LOCKS)
Choose a 52-bit space for locks with `--advisory-lock <num>` where `<num>` is an
integer from 0 to 65535. The lower end of the range is num\*2^52 By default, 0
is used. (If advisory locks overlap, there can be undue lock contention and
deadlocking.)

```sql
-- Find an incomplete author change and refresh the target for up to 1000 corresponding
-- book_author records.
-- Return whether additional processing remains to be done.
CALL book_full__process__author(1000);
```

<details>
<summary>Functions may be called individually for additional control.</summary>

```sql
-- Find and lock an incomplete author change.
SELECT book_full__lock__author();
```

If this returns a non-null bigint, there is work available. Run a transaction
with that value.

```sql
BEGIN
-- Refresh the target for up to 1000 corresponding book_author records.
-- Return whether additional processing remains to be done.
SELECT book_full__update__author($1, 1000);
COMMIT
```

Release the lock.

```sql
-- Unlock the author change.
SELECT book_full__unlock__author($1)
```

</details>

This function should be called periodically. Additionally workers can listen to
the `public.book_full__change__author` listener.

### Performance

In order to work efficiently, the foreign table must have an index on its
foreign and primary keys, in that order.

To iteratively process `author`, the following index is required on
`book_author`:

```sql
CREATE INDEX ON book_author (author_id, id);
```

### Errors

Errors in updating the target no longer fail the original transaction.

When processing begins, the change is moved to the back of the queue, regardless
of whether processing succeeds. Therefore, problematic records will not prevent
processing other change records, but you should fix the error soon, as that
change is stuck until then.

### Limitations

This option is only valid on single-table sources.

## Example

For a full working example, see [Denorm Example](denorm-example.md).
