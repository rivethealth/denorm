import typing

from pg_sql import SqlObject, sql_list

from .format import format
from .formats.join import JoinRefresh, JoinTargetTable
from .join_common import JoinTarget, Key
from .sql import SqlQuery
from .sql_query import insert_query, sync_query, upsert_query


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

    def sql(self, key_table: SqlObject) -> SqlQuery:
        formatted = format(self._query, str(key_table))

        if self._table.refresh == JoinRefresh.FULL:
            query = sync_query(
                columns=self._table.columns or self._table.key,
                key=self._table.key,
                key_table=key_table,
                query=formatted,
                target=self._table.sql,
            )
        elif self._table.refresh == JoinRefresh.INSERT:
            query = insert_query(
                columns=self._table.columns or self._table.key,
                query=formatted,
                target=self._table.sql,
            )
        elif self._table.refresh == JoinRefresh.UPSERT:
            query = upsert_query(
                columns=self._table.columns or self._table.key,
                key=self._table.key,
                query=formatted,
                target=self._table.sql,
            )

        return query
