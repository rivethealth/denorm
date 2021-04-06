import typing

from .format import format
from .formats.join import (
    JoinConsistency,
    JoinDepMode,
    JoinHook,
    JoinSync,
    JoinTable,
    JoinTarget,
)
from .graph import recurse
from .join_common import Structure, foreign_column, local_column
from .sql import (
    SqlId,
    SqlNumber,
    SqlObject,
    SqlQuery,
    SqlString,
    SqlTableExpression,
    sql_list,
    table_fields,
    update_excluded,
)
from .sql_query import sync_query, upsert_query
from .string import indent


def create_lock_table(structure: Structure, target: JoinTarget):
    lock_table = structure.lock_table()
    key = [SqlId(column) for column in target.key]

    yield f"""
CREATE UNLOGGED TABLE {lock_table}
AS SELECT {sql_list(key)}
FROM {target.sql}
WITH NO DATA
    """.strip()

    yield f"""
ALTER TABLE {lock_table}
  ADD PRIMARY KEY ({sql_list(key)})
    """.strip()

    yield f"""
COMMENT ON TABLE {lock_table} IS {SqlString(f"Value lock on {target.name} primary key")}
    """.strip()


class ProcessQuery:
    def __init__(
        self,
        consistency: JoinConsistency,
        query: str,
        setup: typing.Optional[JoinHook],
        structure: Structure,
        sync: JoinSync,
        table_id: str,
        tables: typing.Dict[str, JoinTable],
        target: JoinTarget,
    ):
        self._consistency = consistency
        self._query = query
        self._setup = setup
        self._structure = structure
        self._sync = sync
        self._tables = tables
        self._target = target

        dep_ids = recurse(
            table_id,
            lambda id: tables[id].dep
            if tables[id].dep_mode == JoinDepMode.SYNC
            else None,
        )
        self._deps = [(id, tables[id]) for id in dep_ids]

    def prepare(self):
        return _prepare(
            consistency=self._consistency,
            deps=self._deps,
            setup=self._setup,
            structure=self._structure,
        )

    def gather(self, root: str):
        _, last_table = self._deps[0]
        if last_table.dep is not None:
            foreign = self._tables[last_table.dep]
        else:
            foreign = None

        return _gather(
            consistency=self._consistency,
            deps=self._deps,
            foreign=foreign,
            root=root,
            structure=self._structure,
            sync=self._sync,
            target=self._target,
            query=self._query,
        )

    def finalize(self):
        return _finalize(
            consistency=self._consistency,
            deps=self._deps,
            query=self._query,
            structure=self._structure,
            sync=self._sync,
            target=self._target,
        )


def _prepare(
    consistency: JoinConsistency,
    structure: Structure,
    setup: typing.Optional[JoinHook],
    deps: typing.List[typing.Tuple[str, JoinTable]],
):
    if setup is None or deps[0][1].dep_mode == JoinDepMode.ASYNC:
        setup_sql = ""
    else:
        setup_sql = f"""
PERFORM {setup.sql}();
    """.strip()

    if consistency == JoinConsistency.DEFERRED:
        return f"""
{setup_sql}

PERFORM {structure.setup_function()}();
        """.strip()

    return f"""
{setup_sql}
    """.strip()


def _gather(
    consistency: JoinConsistency,
    deps: typing.List[typing.Tuple[str, JoinTable]],
    foreign: typing.Optional[JoinTable],
    query: typing.Optional[str],
    root: str,
    structure: Structure,
    sync: JoinSync,
    target: JoinTarget,
) -> SqlQuery:
    key_query = ""
    for i, (dep_id, dep) in enumerate(reversed(deps)):
        table_sql = root if i == len(deps) - 1 else str(dep.sql)

        if dep.target_key is not None:
            target_columns = [SqlId(column) for column in dep.target_key]
            key_query += (
                f"SELECT DISTINCT {table_fields(SqlId(dep_id), target_columns)}"
            )
            key_query += f"\nFROM"
            key_query += f"\n  {table_sql} AS {SqlId(dep_id)}"
        elif dep.dep_mode == JoinDepMode.ASYNC:
            dep_columns = [SqlId(column) for column in dep.key]
            key_query += f"SELECT DISTINCT {table_fields(SqlId(dep_id), dep_columns)}"
            key_query += f"\nFROM"
            key_query += f"\n  {table_sql} AS {SqlId(dep_id)}"
        else:
            key_query += f"\n  JOIN {table_sql} AS {SqlId(dep_id)} ON {dep.dep_join}"

    last_id, last_table = deps[-1]
    if last_table.dep_mode == JoinDepMode.ASYNC:
        queue_table = structure.queue_table(last_id)

        local_columns = [local_column(column) for column in last_table.key]

        queue_query = f"""
INSERT INTO {queue_table} ({sql_list(local_columns)})
{key_query}
ORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(last_table.key))}
ON CONFLICT ({sql_list(local_columns)}) DO UPDATE
  SET {update_excluded(foreign_column(column) for column in foreign.key)},
    seq = excluded.seq,
    tries = excluded.tries
        """.strip()

        return SqlQuery(queue_query)
    elif consistency == JoinConsistency.DEFERRED:
        return upsert_query(
            columns=target.key,
            key=target.key,
            query=key_query,
            target=structure.key_table(),
        )
    elif sync == JoinSync.UPSERT:
        if query is not None:
            query = format(query, f"(\n{indent(key_query, 1)})\n")
        else:
            query = key_query

        return upsert_query(
            columns=target.columns or target.key,
            key=target.key,
            query=query,
            target=target.sql,
        )
    else:
        key_table = SqlId("_key")

        if query is not None:
            query = format(query, str(key_table))
        else:
            query = f"TABLE {key_table}"

        result = sync_query(
            columns=target.columns or target.key,
            key=target.key,
            key_table=key_table,
            query=query,
            target=target.sql,
        )
        result.prepend(SqlTableExpression(key_table, key_query))
        return result


def _finalize(
    consistency: JoinConsistency,
    deps: typing.List[typing.Tuple[str, JoinTable]],
    query: typing.Optional[str],
    structure: Structure,
    sync: JoinSync,
    target: JoinTarget,
) -> str:
    last_id, last_table = deps[-1]
    if last_table.dep_mode == JoinDepMode.ASYNC:
        queue_table = structure.queue_table(last_id)
        return f"""
NOTIFY {SqlId(str(queue_table))};
        """.strip()

    if consistency == JoinConsistency.DEFERRED:
        refresh_table = structure.refresh_table()
        return f"""
INSERT INTO {refresh_table}
SELECT
WHERE NOT EXISTS (TABLE {refresh_table});
"""

    if query is None:
        return ""

    lock_table = structure.lock_table()
    query = format(query, str(lock_table))

    if sync == JoinSync.FULL:
        update_query = sync_query(
            columns=target.columns or target.key,
            key=target.key,
            key_table=lock_table,
            query=query,
            target=target.sql,
        )
    else:
        update_query = upsert_query(
            columns=target.columns or target.key,
            key=target.key,
            query=query,
            target=target.sql,
        )

    return f"""
-- synchronize
{str(update_query)};

-- clear locks
DELETE FROM {lock_table};
    """.strip()
