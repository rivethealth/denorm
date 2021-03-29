import json

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
    CREATE TABLE parent (
        id int PRIMARY KEY,
        name text NOT NULL
    );

    CREATE TABLE child (
        id int PRIMARY KEY,
        parent_id int REFERENCES parent (id)
    );

    CREATE TABLE child_key (
        id int PRIMARY KEY
    );
"""

_SCHEMA_JSON = {
    "id": "test",
    "tables": [
        {
            "id": "child",
            "key": ["id"],
            "name": "child",
            "schema": "public",
        },
        {
            "id": "parent",
            "dep": "child",
            "join": "parent.id = child.parent_id",
            "name": "parent",
            "schema": "public",
        },
    ],
    "target": {
        "key": ["id"],
        "name": "child_key",
        "schema": "public",
    },
}


def test_denorm(pg_database):
    with temp_file("denorm-") as schema_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        output = run_process(
            [
                "denorm",
                "create-key",
                "--schema",
                schema_file,
            ]
        )
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(output.decode("utf-8"))

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    INSERT INTO parent (id, name)
                    VALUES (1, 'A'), (2, 'B');

                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM child_key ORDER BY id")
            result = cur.fetchall()
            assert result == [(1,), (2,), (3,)]
