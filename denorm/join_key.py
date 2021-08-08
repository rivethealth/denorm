import typing

from pg_sql import SqlId, SqlNumber, SqlObject, SqlString, sql_list

from .format import format
from .formats.join import (
    JoinConsistency,
    JoinHook,
    JoinJoinMode,
    JoinRefresh,
    JoinTable,
    JoinTargetTable,
)
from .graph import closure
from .join_common import JoinTarget, Structure, foreign_column, local_column
from .join_lock import lock_sql
from .sql import SqlQuery, SqlTableExpr, table_fields, update_excluded
from .sql_query import sync_query, upsert_query
from .string import indent


class KeyConsumer(typing.Protocol):
    def sql(
        key_query: str,
        exprs: typing.List[SqlTableExpr] = [],
        last_expr: typing.Optional[SqlTableExpr] = None,
    ) -> str:
        pass


class TargetRefresh(KeyConsumer):
    def __init__(
        self,
        setup: typing.Optional[JoinHook],
        lock: bool,
        key: typing.List[str],
        structure: Structure,
        target: JoinTarget,
    ):
        self._key = key
        self._lock = lock
        self._setup = setup
        self._structure = structure
        self._target = target

    def sql(
        self,
        key_query: str,
        exprs: typing.List[SqlTableExpr] = [],
        last_expr: typing.Optional[str] = None,
    ):
        if self._setup is None:
            # or deps[0][1].join_mode == JoinJoinMode.ASYNC
            setup_sql = ""
        else:
            setup_sql = f"PERFORM {self._setup.sql}();"

        if self._lock:
            lock_table = self._structure.lock_table()

            inner = f"""
ANALYZE {lock_table};

{target_query};
            """.strip()

            target_query = self._target.sql(lock_table)
            for expr in reversed(exprs):
                target_query.prepend(expr)
            if last_expr is not None:
                target_query.append(SqlId("other_"), last_expr)

            return f"""
{setup_sql}

{lock_sql(structure, self._key.names, inner)}
            """.strip()
        else:
            key_table = SqlId("_key")
            target_query = self._target.sql(key_table)
            target_query.prepend(SqlTableExpr(key_table, key_query))
            for expr in reversed(exprs):
                target_query.prepend(expr)
            if last_expr is not None:
                target_query.append(SqlId("other_"), last_expr)
            return f"""
{setup_sql}

{target_query};
            """.strip()


class KeyResolver:
    def __init__(
        self,
        action: KeyConsumer,
        key: typing.List[str],
        structure: Structure,
        table_id: str,
        tables: typing.Dict[str, JoinTable],
    ):
        self._action = action
        self._key = key
        self._structure = structure
        self._tables = tables
        self._table_id = table_id

        dep_ids = closure(
            [table_id],
            lambda id: [tables[id].join] or []
            if tables[id].join_mode == JoinJoinMode.SYNC and tables[id].join
            else [],
        )
        self._deps = [(id, tables[id]) for id in dep_ids]

    def sql(
        self,
        root: SqlObject,
        exprs: typing.List[SqlTableExpr] = [],
        last_expr: typing.Optional[str] = None,
    ) -> str:
        key_query = ""
        for i, (dep_id, dep) in enumerate(self._deps):
            table_sql = root if i == len(self._deps) - 1 else str(dep.sql)

            if dep.target_key is not None:
                key_query += f"SELECT DISTINCT {sql_list(f'{k} AS {SqlId(t)}' for t, k in zip(self._key, dep.target_key))}"
                key_query += f"\nFROM"
                key_query += f"\n  {table_sql} AS {SqlId(dep_id)}"
            elif dep.join_mode == JoinJoinMode.ASYNC:
                dep_columns = [SqlId(column) for column in dep.key]
                key_query += (
                    f"SELECT DISTINCT {table_fields(SqlId(dep_id), dep_columns)}"
                )
                key_query += f"\nFROM"
                key_query += f"\n  {table_sql} AS {SqlId(dep_id)}"
            else:
                if dep.join_other is not None:
                    key_query += f"\n  {dep.join_other}"
                key_query += f"\n  JOIN {table_sql} AS {SqlId(dep_id)} ON {dep.join_on}"

        last_id, last_table = self._deps[-1]
        if last_table.join_mode == JoinJoinMode.ASYNC:
            from .join_async import enqueue_sql

            return enqueue_sql(
                id=last_id,
                table=last_table,
                structure=self._structure,
                key_query=key_query,
                exprs=exprs,
                last_expr=last_expr,
            )

        return self._action.sql(key_query, exprs=exprs, last_expr=last_expr)
