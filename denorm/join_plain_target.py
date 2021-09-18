import typing

from pg_sql import SqlObject, sql_list

from .format import format
from .join_common import JoinTarget, Key
from .sql import SqlQuery


class JoinPlainTarget(JoinTarget):
    """
    Insert into to a table
    """

    def __init__(self, query: str):
        self._query = query

    def key(self) -> typing.Optional[Key]:
        pass

    def sql(self, key_table: SqlObject, table_id: typing.Optional[str]) -> SqlQuery:
        formatted = format(
            self._query, {"key": str(key_table), "table": table_id or ""}
        )
        return SqlQuery(formatted)
