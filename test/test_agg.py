import copy
import json

from file import temp_file
from pg import connection, transaction
from process import run_process

_SCHEMA_SQL = """
    CREATE TABLE child (
        id int PRIMARY KEY,
        parent_id int
    );

    CREATE TABLE parent_child_stat (
        parent_id int PRIMARY KEY,
        _count bigint NOT NULL,
        child_count int NOT NULL
    );
"""

_SCHEMA_JSON = {
    "id": "test",
    "source": {"name": "child"},
    "target": {"name": "parent_child_stat"},
    "groups": {"parent_id": "parent_id"},
    "aggregates": {
        "child_count": {
            "value": "sign * count(*)",
        }
    },
}


def test_agg(pg_database):
    with temp_file("denorm-") as schema_file, connection("") as conn:
        with transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        output = run_process(
            [
                "denorm",
                "create-agg",
                "--schema",
                schema_file,
            ]
        )
        with transaction(conn) as cur:
            cur.execute(output.decode("utf-8"))

        with transaction(conn) as cur:
            cur.execute(
                """
                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )

        with transaction(conn) as cur:
            cur.execute("SELECT * FROM parent_child_stat ORDER BY parent_id")
            result = cur.fetchall()
            assert result == [(1, 2, 2), (2, 1, 1)]


def test_agg_defer(pg_database):
    with temp_file("denorm-") as schema_file, connection("") as conn:
        with transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        with open(schema_file, "w") as f:
            schema_json = copy.deepcopy(_SCHEMA_JSON)
            schema_json["consistency"] = "deferred"
            json.dump(schema_json, f)

        output = run_process(
            [
                "denorm",
                "create-agg",
                "--schema",
                schema_file,
            ]
        )
        with transaction(conn) as cur:
            cur.execute(output.decode("utf-8"))

        with transaction(conn) as cur:
            cur.execute(
                """
                    INSERT INTO child (id, parent_id)
                    VALUES (1, 1), (2, 1), (3, 2);
                """
            )
            cur.execute("TABLE parent_child_stat")
            result = cur.fetchall()
            assert result == []

        with transaction(conn) as cur:
            cur.execute("SELECT * FROM parent_child_stat ORDER BY parent_id")
            result = cur.fetchall()
            assert result == [(1, 2, 2), (2, 1, 1)]
