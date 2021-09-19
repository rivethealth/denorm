import typing

from pg_sql import SqlObject, sql_list

from .format import format
from .formats.join import JoinRefresh, JoinTargetTable
from .join_common import JoinTarget, Key
from .sql import SqlQuery
from .sql_query import sync_query, upsert_query


class JoinTableTarget(JoinTarget):
    """
    Insert into to a table
    """

    def __init__(self, table: JoinTargetTable, query: str):
        self._table = table
        self._query = query

    def key(self) -> typing.Optional[Key]:
        if self._table.key:
            definition = f"""
SELECT {sql_list(self._table.key)}
FROM {self._table.sql}
            """.strip()
            names = self._table.key
            return Key(definition=definition, names=names)

    def sql(self, key_table: SqlObject, table_id: typing.Optional[str]) -> SqlQuery:
        formatted = format(
            self._query, {"key": str(key_table), "table": table_id or ""}
        )

        return sync_query(
            columns=self._table.columns or self._table.key,
            key=self._table.key,
            key_table=key_table,
            query=formatted,
            target=self._table.sql,
        )
