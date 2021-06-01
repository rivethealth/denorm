import typing

from pg_sql import SqlId, SqlString, sql_list

from .join_common import JoinTarget, Key, Structure
from .sql_query import upsert_query


def create_lock_table(structure: Structure, key: Key, target: JoinTarget):
    lock_table = structure.lock_table()

    yield f"""
CREATE UNLOGGED TABLE {lock_table}
AS {key.definition}
WITH NO DATA
    """.strip()

    yield f"""
ALTER TABLE {lock_table}
  ADD PRIMARY KEY ({sql_list(SqlId(name) for name in key.names)})
    """.strip()

    yield f"""
COMMENT ON TABLE {lock_table} IS {SqlString(f"Value lock on {target.name} primary key")}
    """.strip()


def lock_sql(structure: Structure, key: typing.List[str], sql: str):
    lock_table = structure.lock_table()
    lock_query = upsert_query(
        columns=key,
        key=key,
        query=key_query,
        target=lock_table,
    )

    return f"""
{lock_query};

{sql}

DELETE FROM {lock_table}
    """.strip()
