# Aggregate example

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

Open a new terminal session for the remaining commands.

## Create database

```sh
PGHOST=localhost psql -c '
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

CREATE TABLE author_book_stat (
  author_id int PRIMARY KEY,
  _count bigint NOT NULL,
  book_count int NOT NULL
);
'
```

## Apply denorm schema

```sh
echo "
id: author_book_stat
source:
  name: book_author
target:
  name: author_book_stat
groups:
  author_id: author_id
aggregates:
  book_count:
    combine: existing.book_count + excluded.book_count
    value: sign * count(*)
" \
  | yq \
  | denorm create-agg \
  | PGHOST=localhost psql
```

## Add records

```sh
PGHOST=localhost psql -c "
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
PGHOST=localhost psql -c "SELECT * FROM author_book_stat ORDER BY author_id"
```

```txt
 author_id | _count | book_count
-----------+--------+------------
         1 |      1 |          1
         2 |      2 |          2
(2 rows)
```
