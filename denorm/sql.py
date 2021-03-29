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

_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9$_]+$")


@dataclasses.dataclass
class SqlIdentifier:
    name: str

    def __str__(self) -> str:
        if self.name.upper() not in _RESERVED_WORDS_SET and _IDENTIFIER_PATTERN.match(
            self.name
        ):
            return self.name
        inner = self.name.replace("\\", "\\\\").replace('"', '"')
        return f'"{inner}"'


@dataclasses.dataclass
class SqlLiteral:
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
        return ".".join(str(SqlIdentifier(name)) for name in self.names)


@dataclasses.dataclass
class SqlList:
    parts: typing.List

    def __str__(self) -> str:
        return ", ".join(str(part) for part in self.parts)


def conflict_update(columns: typing.List[str]) -> str:
    return ", ".join(
        f"{SqlIdentifier(column)} = excluded.{SqlIdentifier(column)}"
        for column in columns
    )
