import collections
import dataclasses
import re
import typing

from pg_sql import SqlId, SqlObject, sql_list

from .string import indent


@dataclasses.dataclass
class SqlTableExpr:
    name: SqlId
    query: str

    def __str__(self) -> str:
        return f"""
{self.name} AS (
{indent(self.query, 1)}
)""".strip()


@dataclasses.dataclass
class SqlQuery:
    query: str
    expressions: typing.List[SqlTableExpr] = dataclasses.field(default_factory=list)

    def append(self, id: SqlId, query: str):
        self.expressions.append(SqlTableExpr(id, self.query))
        self.query = query

    def prepend(self, expression: SqlTableExpr):
        self.expressions.insert(0, expression)

    def __str__(self) -> str:
        result = ""
        if self.expressions:
            expressions = ",\n".join(str(expr) for expr in self.expressions)
            result += f"""
WITH
{indent(expressions, 1)}
        """.strip()
        result += "\n"
        return f"{result}{self.query}"


def update_excluded(columns: typing.Iterable[SqlId]):
    return sql_list(f"{column} = excluded.{column}" for column in columns)


def table_fields(id: SqlId, columns: typing.List[SqlId]):
    return sql_list(str(SqlObject(id, column)) for column in columns)
