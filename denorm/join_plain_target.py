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

    def sql(self, key_table: SqlObject) -> SqlQuery:
        formatted = format(self._query, str(key_table))
        return SqlQuery(formatted)
