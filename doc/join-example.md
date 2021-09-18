# Join example

Requires Docker and PostgreSQL client binaries.

## Run PostgreSQL

```sh
docker run \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_USER="$USER" \
  -p 5432:5432 \
  --rm \
  postgres
```

Open a new terminal session for the remaining commands, and run

```sh
export PGHOST=localhost
```

## Create database

```sh
psql -c '
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

CREATE TABLE book_full (
  id int PRIMARY KEY,
  title text NOT NULL,
  author_names text[] NOT NULL
);
'
```

## Apply denorm schema

```sh
echo "
id: book_full
tables:
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
targetTable:
  columns: [id, title, author_names]
  key: [id]
  name: book_full
targetQuery: >
  SELECT
    b.id,
    b.title,
    a.names
  FROM
    \${key} AS k (id)
    JOIN book AS b ON k.id = b.id
    CROSS JOIN LATERAL (
      SELECT coalesce(array_agg(a.name ORDER BY ba.ordinal), '{}') AS names
      FROM
        author AS a
        JOIN book_author AS ba ON a.id = ba.author_id
      WHERE b.id = ba.book_id
    ) AS a
" \
  | yq \
  | denorm create-join \
  | psql
```

## Add records

```sh
psql -c "
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
"
```

## Inspect results

```sh
psql -c "SELECT * FROM book_full ORDER BY id"
```

```txt
 id |       title        |           author_names
----+--------------------+-----------------------------------
  1 | Good Omens         | {"Neil Gaiman","Terry Pratchett"}
  2 | The Color of Magic | {"Terry Pratchett"}
(2 rows)
```
