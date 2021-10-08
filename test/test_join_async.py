import copy
import json

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
    CREATE TABLE grandparent (
        id int PRIMARY KEY,
        name text NOT NULL
    );

    CREATE TABLE parent (
        id int PRIMARY KEY,
        grandparent_id int NOT NULL REFERENCES grandparent (id),
        name text NOT NULL
    );

    CREATE TABLE child (
        id int PRIMARY KEY,
        parent_id int NOT NULL REFERENCES parent (id)
    );

    CREATE TABLE child_full (
        id int PRIMARY KEY,
        grandparent_name text NOT NULL,
        parent_name text NOT NULL
    );
"""

_SCHEMA_JSON = {
    "context": ["context.example"],
    "id": "test",
    "tables": {
        "child": {
            "name": "child",
            "targetKey": ["child.id"],
        },
        "grandparent": {
            "join": "parent",
            "joinOn": "grandparent.id = parent.grandparent_id",
            "name": "grandparent",
        },
        "parent": {
            "key": [{"name": "id"}],
            "join": "child",
            "joinKey": ["id"],
            "joinMode": "async",
            "joinOn": "parent.id = child.parent_id",
            "name": "parent",
        },
    },
    "targetTable": {
        "key": ["id"],
        "columns": ["id", "grandparent_name", "parent_name"],
        "name": "child_full",
        "schema": "public",
    },
    "targetQuery": """
        SELECT c.id, g.name, p.name
        FROM ${key} AS d
            JOIN child c ON d.id = c.id
            JOIN parent p ON c.parent_id = p.id
            JOIN grandparent AS g ON p.grandparent_id = g.id
    """,
}


def test_join_async(pg_database):
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
            # print(output.decode("utf-8"))
            cur.execute(output.decode("utf-8"))

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    INSERT INTO grandparent (id, name)
                    VALUES (9, '_');

                    INSERT INTO parent (id, grandparent_id, name)
                    VALUES (1, 9, 'A'), (2, 9, 'B');

                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

        with connection("") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                while True:
                    cur.execute("SELECT test__pcs__parent(10)")
                    (result,) = cur.fetchone()
                    if not result:
                        break

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM child_full ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, "_", "A"), (2, "_", "A"), (3, "_", "B")]

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("UPDATE parent SET name = 'C' WHERE id = 2")

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM child_full ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, "_", "A"), (2, "_", "A"), (3, "_", "B")]

        # import time
        # time.sleep(1000000)

        with connection("") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                while True:
                    cur.execute("SELECT test__pcs__parent(10)")
                    (result,) = cur.fetchone()
                    if not result:
                        break

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("TABLE test__que__parent")
            result = cur.fetchall()
            assert result == []

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM child_full ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, "_", "A"), (2, "_", "A"), (3, "_", "C")]
