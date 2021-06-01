import json

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
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
"""

_SCHEMA_JSON = {
    "id": "book_full",
    "schema": "public",
    "tables": {
        "author": {
            "join": "book_author",
            "joinOn": "author.id = book_author.author_id",
            "name": "book_author",
            "schema": "public",
        },
        "book": {"name": "book", "schema": "public", "targetKey": ["book.id"]},
        "book_author": {
            "name": "book_author",
            "schema": "public",
            "targetKey": ["book_author.book_id"],
        },
    },
    "targetTable": {
        "columns": ["id", "title", "author_names"],
        "name": "book_full",
        "schema": "public",
        "key": ["id"],
    },
    "targetQuery": """
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
    """,
}


def test_join_example(pg_database):
    with temp_file("denorm-") as schema_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        output = run_process(
            [
                "denorm",
                "create-join",
                "--schema",
                schema_file,
            ]
        )
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(output.decode("utf-8"))

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
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
                """
            )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM book_full ORDER BY id")
            result = cur.fetchall()
            assert result == [
                (1, "Good Omens", ["Neil Gaiman", "Terry Pratchett"]),
                (2, "The Color of Magic", ["Terry Pratchett"]),
            ]
