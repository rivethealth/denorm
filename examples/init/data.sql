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
