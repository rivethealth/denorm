# Agg

## Example

Suppose you have a database of books.

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
