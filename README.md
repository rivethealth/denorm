# Denorm

Denormalized and aggregated tables for PostgreSQL.

Keywords: PostgreSQL, denormalization, aggregation, incremental view
maintenance, materialized view

## Features

- Supports complex SQL features and expressions
- Configurable consistency

## Example

Consider a database of books.

<details>
<summary>DDL</summary>

```sql
CREATE TABLE author (
  id int PRIMARY KEY,
  name text NOT NULL
);

CREATE TABLE book (
  id int PRIMARY KEY,
  title text NOT NULL
);

CREATE TABLE book_author (
  id int PRIMARY KEY,
  book_id int NOT NULL REFERENCES book (id) ON DELETE CASCADE,
  author_id int NOT NULL REFERENCES author (id) ON DELETE CASCADE,
  ordinal int NOT NULL,
  UNIQUE (book_id, author_id),
  UNIQUE (book_id, ordinal)
);
```

</details>

### Denormalization

_Execute [examples/init/run](examples/init/run) to run this example._

Suppose you need a table of books with title and author names.

<details>
<summary>DDL</summary>

```sql
CREATE TABLE book_full (
  id int PRIMARY KEY,
  title text NOT NULL,
  author_names text[] NOT NULL
);
```

</details>

Author a query that collects the correct data for a recordset of primary keys
`$1` (in this case, book IDs).

<details>
<summary>DDL</summary>

```sql
  SELECT
    b.id,
    b.title,
    a.names
  FROM
    $1 AS k (id)
    JOIN book AS b ON k.id = b.id
    CROSS JOIN LATERAL (
      SELECT coalesce(array_agg(a.name ORDER BY ba.ordinal), '{}') AS names
      FROM
        author AS a
        JOIN book_author AS ba ON a.id = ba.author_id
      WHERE b.id = ba.book_id
    ) AS a
```

</details>

Create a YAML file with the query and the relationships between tables

<details>
<summary>book_full.yml</summary>

```yml
id: book_full
schema: public
target: { table: book_full }
sources:
  - id: book
    tables:
      - { schema: public, table: book, columns: ["id"] }
    expression: $1 AS book
    identity: book.id
  - id: book_author
    tables:
      - { schema: public, table: book_author, columns: ["book_id"] }
    expression: $1 AS book_author
    identity: book_author.book_id
  - id: author
    tables: [{ schema: public, table: author, columns: ["id"] }]
    expression: $1 AS author
    dep: book_author
    join: author.id = book_author.author_id
columns: [id, title, author_names]
query: >
  SELECT
    b.id,
    b.title,
    a.names
  FROM
    $1 AS k (id)
    JOIN book AS b ON k.id = b.id
    CROSS JOIN LATERAL (
      SELECT coalesce(array_agg(a.name ORDER BY ba.ordinal), '{}') AS names
      FROM
        author AS a
        JOIN book_author AS ba ON a.id = ba.author_id
      WHERE b.id = ba.book_id
    ) AS a
```

</details>

Demorm reduces required configuration by gathering information about the base
schema from a live database. While the database is running, generate the SQL DDL
statements.

```sh
PGPASSWORD=password denorm create_sql book_full.yml > book_full.sql
```

Then execute this SQL against the target database (could be the same one).

```sh
psql -1 -f book_full.sql
```

Now test

<details>
<summary>Data</summary>

```sql
INSERT INTO author (id, name)
VALUES
  (1, 'Neil Gaiman'),
  (2, 'Terry Pratchett');

INSERT INTO book (id, title)
VALUES
  (1, 'Good Omens'),
  (2, 'The Color of Magic');

INSERT INTO book_author (id, book_id, author_id, ordinal)
VALUES
  (1, 1, 1, 1),
  (2, 1, 2, 2),
  (3, 2, 2, 1);

```

</details>

View the results:

```sql
TABLE book_full;
```

```
 id |       title        |           author_names
----+--------------------+-----------------------------------
  1 | Good Omens         | {"Neil Gaiman","Terry Pratchett"}
  2 | The Color of Magic | {"Terry Pratchett"}
```

### Aggregation example

Suppose you need a rollup table for number of books written for each author.

This could be done using a "denormalized" view like above, but a more efficient
method is to use dedicated aggregation.

```sql
CREATE TABLE author_book_stat (
  author_id int PRIMARY KEY,
  book_count int NOT NULL
);
```

Author a query that proceduces the aggregation.

```sql
SELECT author_id, count(*)
FROM $1
GROUP BY 1
```

Create a YAML file

```yml
id: author_book_stat
schema: public
target:
  table: author_book_stat
  count: book_count
base:
  table: book_author
reduce: >
  count = $1.count + excluded.count
query: >
  SELECT author_id, count(*) FROM $1 GROUP BY 1
```

Run denorm and execute the DDL statemenets.

```sh
denorm create_agg author_book_stat.yml | psql
```

Now test

<details>
<summary>Data</summary>

```sql
INSERT INTO author (id, name)
VALUES
  (1, 'Neil Gaiman'),
  (2, 'Terry Pratchett');

INSERT INTO book (id, title)
VALUES
  (1, 'Good Omens'),
  (2, 'The Color of Magic');

INSERT INTO book_author (id, book_id, author_id, ordinal)
VALUES
  (1, 1, 1, 1),
  (2, 1, 2, 2),
  (3, 2, 2, 1);

```

</details>

View results:

```sql
TABLE author_book_stat
```

```

```

## Documentation

### Denormalization relationships

The purposes of `sources` is to calculate the primary keys of the target that
needed to be refreshed (inserted, updated, deleted).

Each sources `sources` item has:

- id - Identifier of this source
- tables - List of tables and columns. Columns are any columns used in
  `expression`, `identity`, or `join`.
- expression - From expression, using $1, $2, etc as the placeholders for
  tables. Use a table alias to ensure a consistent name elsewhere.

If this source has the primary key,

- identity - Expression for primary key of target table

Otherwise, if additional tables need to be referenced to find the primary key,

- dep - Identifier of the source to reference.
- join - Expression for an inner join on the two sources.

Usually, each source has a single table, though denorm supports other cases, for
example a full carteaian product of two tables.

### Constistency

There are two supported consistency modes.

#### transaction

This is the default. The target table is updated at the end of the transaction.

#### iterate

Some tables may have a 1:N relationship with a very large N, say tens of
thousands.

In such cases, it may not be feasible to update the denormalized table in a
single transaction. Instead, denorm allows the update to be broken up over
multiple transactions.

For example, an author may have many books. (This particular example is
contrived, as in fact as an individual author will not have thousands of books,
so it would be preferrable to use the simpler transaction consistency mode.)

Change the consistency to `iterate`.

<details>
<summary>book_full.yml</summary>

```yml
- id: author
  tables: [{ schema: public, table: author, columns: ["id"] }]
  expression: $1 AS author
  dep: book_author
  join: author.id = book_author.author_id
  consistency: iterate
```

</details>

Now, updates will not automatically affect the target table. Instead, changes
are queued and must be processed seperately.

```sql
-- Find an incomplete author change and refresh the target for up to 1000 corresponding
-- book_author records.
-- Return whether additional processing remains to be done.
CALL book_full__process__author(1000);
```

<details>
<summary>If you want more control over the processing, you can call functions individually,</summary>

```sql
SELECT book_full__lock__author();
```

If this returns a non-null bigint, there is work available. Run a transaction
with that value.

```sql
BEGIN
-- Return whether additional processing remains to be done.
SELECT book_full__update__author($1, 1000);
COMMIT
```

Release the lock.

```sql
SELECT book_full__unlock__author()
```

This technique uses advisory locks to avoid queue processing blocking queueing.

</details>

This function should be called periodically. Additionally workers can listen to
the `public.book_full__change__author` listener.

#### Performance

In order to work efficiently, the foreign table must have an index on its
foreign and primary keys, in that order.

To iteratively process `author`, the following index is required:

```sql
CREATE INDEX ON book_author (author_id, id);
```

#### Errors

Errors in updating the target no longer fail the original transaction.

When processing begins, the change is moved to the back of the queue, regardless
of whether processing succeeds. Therefore, problematic records will not prevent
processing other change records, but you should fix the error soon, as that
change is stuck until then.

#### Limitations

This option is only valid on single-table sources.

## Performance

Denormalization exchanges slower write performance for higher read performance.

While it's impossible to escape this reality, Denorm has been created to be as
fast as hand-tuned methods.

It uses statement triggers to reduce overhead. Deferred constraint triggers
batch work in a single transaction. This is particularly useful for "nested"
changes (insert a record and its children and grandchildren), where it avoids
the heavy lifting on every change.

PostgreSQL does not support global temporary tables, so Denorm uses session temp
tables with `ON COMMIT DROP`. The first update in a session has several ms of
overhead in setting up these tables. Be sure to pool connections and vacuum
reguarly to prevent system tables from bloating.

## Migration

Denorm does not generate migraton scripts.

Consider a tool like migra to help generate migration scripts.
