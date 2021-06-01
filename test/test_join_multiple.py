import copy
import json

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
    CREATE TABLE parent (
        id int PRIMARY KEY,
        name text NOT NULL
    );

    CREATE TABLE other (
        id int PRIMARY KEY,
        name text NOT NULL
    );

    CREATE TABLE other_override (
        id int PRIMARY KEY,
        parent_id int NOT NULL REFERENCES parent (id),
        other_id int NOT NULL REFERENCES other (id),
        name text NOT NULL
    );

    CREATE TABLE child (
        id int PRIMARY KEY,
        parent_id int NOT NULL REFERENCES parent (id)
    );

    CREATE TABLE grandchild (
        id int PRIMARY KEY,
        child_id int NOT NULL REFERENCES child (id),
        other_id int NOT NULL REFERENCES other (id)
    );

    CREATE TABLE child_full (
        id int PRIMARY KEY,
        parent_name text NOT NULL,
        other_names text[] NOT NULL
    );
"""

_SCHEMA_JSON = {
    "id": "test",
    "tables": {
        "child": {
            "name": "child",
            "targetKey": ["child.id"],
        },
        "grandchild": {
            "name": "grandchild",
            "targetKey": ["grandchild.child_id"],
        },
        "other": {
            "name": "other",
            "join": "grandchild",
            "joinOn": "other.id = grandchild.other_id",
        },
        "other_override": {
            "name": "other_override",
            "join": "grandchild",
            "joinOn": "(other_override.parent_id, other_override.other_id) = (child.parent_id, grandchild.other_id)",
            "joinOther": "JOIN child AS child ON child.id = grandchild.child_id",
        },
        "parent": {
            "join": "child",
            "joinOn": "parent.id = child.parent_id",
            "name": "parent",
        },
    },
    "targetTable": {
        "key": ["id"],
        "columns": ["id", "parent_name", "other_names"],
        "name": "child_full",
        "schema": "public",
    },
    "targetQuery": """
        SELECT c.id, p.name, gc.names
        FROM $1 AS d
            JOIN child c ON d.id = c.id
            JOIN parent p ON c.parent_id = p.id
            CROSS JOIN LATERAL (
                SELECT coalesce(array_agg(coalesce(oo.name, o.name)), '{}') AS names
                FROM grandchild AS gc
                  JOIN other AS o ON gc.other_id = o.id
                  LEFT JOIN other_override AS oo ON (c.parent_id, gc.other_id) = (oo.parent_id, oo.other_id)
                WHERE c.id = gc.child_id
            ) AS gc
    """,
}


def test_join(pg_database):
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
        # print(output.decode("utf-8"))
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(output.decode("utf-8"))

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    INSERT INTO parent (id, name)
                    VALUES (1, 'A'), (2, 'B');

                    INSERT INTO other (id, name)
                    VALUES (1, 'Other');

                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);

                    INSERT INTO grandchild (id, child_id, other_id)
                    VALUES (1, 2, 1);
                """
            )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM child_full ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, "A", []), (2, "A", ["Other"]), (3, "B", [])]

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    INSERT INTO other_override (id, parent_id, other_id, name)
                    VALUES (1, 1, 1, 'Override');
                """
            )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM child_full ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, "A", []), (2, "A", ["Override"]), (3, "B", [])]
