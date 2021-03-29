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

CREATE TABLE book_key (
  id int PRIMARY KEY
);
'
```

## Apply denorm schema

```sh
echo "
id: book_key
schema: public
target:
  key: [id]
  name: book_key
  schema: public
tables:
  - id: book
    key: [id]
    name: book
    schema: public
  - id: book_author
    dep: book
    join: book_author.book_id = book.id
    name: book_author
    schema: public
  - id: author
    dep: book_author
    join: author.id = book_author.author_id
    name: book_author
    schema: public
" \
  | yq \
  | denorm create-key \
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
PGHOST=localhost psql -c "SELECT * FROM book_key ORDER BY id"
```

```txt
 id
----
  1
  2
(2 rows)
```
