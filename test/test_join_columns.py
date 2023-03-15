import json

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
    CREATE TABLE parent (
        id int PRIMARY KEY,
        name text NOT NULL,
        other text NOT NULL
    );

    CREATE TABLE child (
        id int PRIMARY KEY,
        parent_id int REFERENCES parent (id)
    );

    CREATE TABLE child_full (
        id int PRIMARY KEY,
        parent_name text NOT NULL,
        parent_other text NOT NULL
    );
"""

_SCHEMA_JSON = {
    "id": "test",
    "tables": {
        "child": {
            "tableName": "child",
            "tableSchema": "public",
            "destinationKeyExpr": ["child.id"],
        },
        "parent": {
            "tableName": "parent",
            "tableSchema": "public",
            "tableColumns": [
                {"name": "id"},
                {"name": "name"},
                # purposefully exclude other and make sure its updates don't
                # modify the table
            ],
            "joinTargetTable": "child",
            "joinOn": "parent.id = child.parent_id",
        },
    },
    "destinationQuery": """
        SELECT c.id, p.name, p.other
        FROM ${key} AS d
            JOIN child c ON d.id = c.id
            JOIN parent p ON c.parent_id = p.id
    """,
    "destinationTable": {
        "tableName": "child_full",
        "tableSchema": "public",
        "tableKey": ["id"],
        "tableColumns": ["id", "parent_name", "parent_other"],
    },
}


def test_join_key(pg_database):
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
                    INSERT INTO parent (id, name, other)
                    VALUES (1, 'A', ''), (2, 'B', '');

                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM child_full ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, "A", ""), (2, "A", ""), (3, "B", "")]

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    UPDATE parent
                    SET other = 'other';
                """
            )

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM child_full ORDER BY id")
            result = cur.fetchall()
            assert result == [(1, "A", ""), (2, "A", ""), (3, "B", "")]
