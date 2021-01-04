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
