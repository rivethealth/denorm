import collections
import dataclasses
import re
import typing

# https://www.postgresql.org/docs/current/sql-keywords-appendix.html
RESERVED_WORDS = [
    "ALL"
    "ANALYSE"
    "ANALYZE"
    "AND"
    "ANY"
    "ARRAY"
    "AS"
    "ASC"
    "ASYMMETRIC"
    "AUTHORIZATION"
    "BINARY"
    "BOTH"
    "CASE"
    "CAST"
    "CHECK"
    "COLLATE"
    "COLLATION"
    "COLUMN"
    "CONCURRENTLY"
    "CONSTRAINT"
    "CREATE"
    "CROSS"
    "CURRENT_CATALOG"
    "CURRENT_DATE"
    "CURRENT_ROLE"
    "CURRENT_SCHEMA"
    "CURRENT_TIME"
    "CURRENT_TIMESTAMP"
    "CURRENT_USER"
    "DEFAULT"
    "DEFERRABLE"
    "DESC"
    "DISTINCT"
    "DO"
    "ELSE"
    "END"
    "EXCEPT"
    "FALSE"
    "FETCH"
    "FOR"
    "FOREIGN"
    "FREEZE"
    "FROM"
    "FULL"
    "GRANT"
    "GROUP"
    "HAVING"
    "ILIKE"
    "IN"
    "INITIALLY"
    "INNER"
    "INTERSECT"
    "INTO"
    "IS"
    "ISNULL"
    "JOIN"
    "LATERAL"
    "LEADING"
    "LEFT"
    "LIKE"
    "LIMIT"
    "LOCALTIME"
    "LOCALTIMESTAMP"
    "NATURAL"
    "NOT"
    "NOTNULL"
    "NULL"
    "OFFSET"
    "ON"
    "ONLY"
    "OR"
    "ORDER"
    "OUTER"
    "OVERLAPS"
    "PLACING"
    "PRIMARY"
    "REFERENCES"
    "RETURNING"
    "RIGHT"
    "SELECT"
    "SESSION_USER"
    "SIMILAR"
    "SOME"
    "SYMMETRIC"
    "TABLE"
    "TABLESAMPLE"
    "THEN"
    "TO"
    "TRAILING"
    "TRUE"
    "UNION"
    "UNIQUE"
    "USER"
    "USING"
    "VARIADIC"
    "VERBOSE"
    "WHEN"
    "WHERE"
    "WINDOW"
    "WITH"
]

_RESERVED_WORDS_SET = set(RESERVED_WORDS)

_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9$_]*$")


@dataclasses.dataclass
class SqlId:
    name: str

    def __str__(self) -> str:
        if self.name.upper() not in _RESERVED_WORDS_SET and _IDENTIFIER_PATTERN.match(
            self.name
        ):
            return self.name
        inner = self.name.replace("\\", "\\\\").replace('"', '"')
        return f'"{inner}"'


@dataclasses.dataclass
class SqlNumber:
    value: int

    def __str__(self) -> str:
        return str(self.value)


@dataclasses.dataclass
class SqlString:
    value: str

    def __str__(self) -> str:
        inner = self.value.replace("'", "''")
        return f"'{inner}'"


@dataclasses.dataclass
class SqlObject:
    names: typing.List[str]

    def __init__(self, *args: typing.List[str]):
        self.names = args

    def __str__(self) -> str:
        return ".".join(str(name) for name in self.names)


def update_excluded(columns: typing.Iterable[SqlId]):
    return sql_list(f"{column} = excluded.{column}" for column in columns)


def composite_fields(id: SqlId, columns: typing.List[SqlId]):
    return sql_list(str(SqlObject(id, column)) for column in columns)


def table_fields(id: SqlId, columns: typing.List[SqlId]):
    return sql_list(str(SqlObject(id, column)) for column in columns)


def sql_list(items: typing.Iterable):
    return ", ".join(str(item) for item in items)
