import copy
import json

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
    CREATE TABLE table_parent (
        parent_id int PRIMARY KEY,
        name text NOT NULL
    );

    CREATE TABLE table_child (
        child_id int PRIMARY KEY,
        child_parent_id int REFERENCES table_parent (parent_id)
    );

    CREATE TABLE table_child_full (
        child_full_child_id int PRIMARY KEY,
        parent_name text NOT NULL
    );
"""

_SCHEMA_JSON = {
    "id": "test",
    "tables": {
        "all": {
            "joinTargetTable": "table_child",
            "joinTargetKey": ["child_id"],
            "joinMode": "async",
            "refreshFunction": True,
        },
        "table_child": {
            "tableSchema": "public",
            "tableName": "table_child",
            "tableKey": [{"name": "child_id", "type": "int"}],
            "refreshFunction": True,
            "destinationKeyExpr": ["table_child.child_id"],
        },
        "table_parent": {
            "tableSchema": "public",
            "tableName": "table_parent",
            "tableKey": [{"name": "parent_id", "type": "int"}],
            "joinTargetTable": "table_child",
            "joinTargetKey": ["child_id"],
            "joinMode": "async",
            "joinOn": "table_parent.parent_id = table_child.child_parent_id",
            "refreshFunction": True,
        },
    },
    "destinationTable": {
        "tableSchema": "public",
        "tableName": "table_child_full",
        "tableKey": ["child_full_child_id"],
        "tableColumns": ["child_full_child_id", "parent_name"],
    },
    "destinationQuery": """
        SELECT c.child_id, p.name
        FROM ${key} AS d
            JOIN table_child c ON d.child_full_child_id = c.child_id
            JOIN table_parent p ON c.child_parent_id = p.parent_id
    """,
}


def test_join_refresh_function_all(pg_database):
    with temp_file("denorm-") as schema_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    INSERT INTO table_parent (parent_id, name)
                    VALUES (1, 'A'), (2, 'B');

                    INSERT INTO table_child (child_id, child_parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

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
            cur.execute("SELECT test__rfs__all()")

        with connection("") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                while True:
                    cur.execute("SELECT test__pcs__all(10)")
                    (result,) = cur.fetchone()
                    if not result:
                        break

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM table_child_full ORDER BY child_full_child_id")
            result = cur.fetchall()
            assert result == [(1, "A"), (2, "A"), (3, "B")]


def test_join_refresh_function_parent(pg_database):
    with temp_file("denorm-") as schema_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    INSERT INTO table_parent (parent_id, name)
                    VALUES (1, 'A'), (2, 'B');

                    INSERT INTO table_child (child_id, child_parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

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
            cur.execute("SELECT test__rfs__table_parent(1)")

        with connection("") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                while True:
                    cur.execute("SELECT test__pcs__table_parent(10)")
                    (result,) = cur.fetchone()
                    if not result:
                        break

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM table_child_full ORDER BY child_full_child_id")
            result = cur.fetchall()
            assert result == [(1, "A"), (2, "A")]


def test_join_refresh_function_child(pg_database):
    with temp_file("denorm-") as schema_file:
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        with connection("") as conn, transaction(conn) as cur:
            cur.execute(
                """
                    INSERT INTO table_parent (parent_id, name)
                    VALUES (1, 'A'), (2, 'B');

                    INSERT INTO table_child (child_id, child_parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

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
            cur.execute("SELECT test__rfs__table_child(1) FROM table_child")
            cur.execute("SELECT test__rfs__table_child(2) FROM table_child")

        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM table_child_full ORDER BY child_full_child_id")
            result = cur.fetchall()
            assert result == [(1, "A"), (2, "A")]
