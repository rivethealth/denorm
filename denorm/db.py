import collections
import dataclasses
import typing

Sql = str


def conflict_update(column_names: typing.List[str]) -> Sql:
    return ", ".join(
        f"{sql_ident(column)} = excluded.{sql_ident(column)}" for column in column_names
    )


def sql_str(value: str) -> Sql:
    return "'{}'".format(value.replace("'", "''"))


def sql_ident(value: str) -> Sql:
    return f'"{value}"'


@dataclasses.dataclass(frozen=True)
class ColumnDef:
    name: str
    type: str


@dataclasses.dataclass(frozen=True)
class ObjectRef:
    schema: typing.Optional[str]
    name: str

    @property
    def sql(self) -> Sql:
        if self.schema is None:
            return sql_ident(self.name)

        return f"{sql_ident(self.schema)}.{sql_ident(self.name)}"


def primary_key(cur, table: ObjectRef) -> typing.List[ColumnDef]:
    cur.execute(
        """
        SELECT
            a.attname,
            format_type(a.atttypid, a.atttypmod)
        FROM
            pg_class AS c
            JOIN pg_index AS i ON c.oid = i.indrelid AND i.indisprimary
            JOIN pg_attribute AS a ON c.oid = a.attrelid AND a.attnum = ANY(i.indkey)
        WHERE c.oid = %s::regclass
        ORDER BY a.attnum
        """,
        (table.sql,),
    )
    return [ColumnDef(*row) for row in cur.fetchall()]


def columns(cur, table: ObjectRef) -> typing.List[ColumnDef]:
    cur.execute(
        """
        SELECT
            a.attname,
            format_type(a.atttypid, a.atttypmod)
        FROM
            pg_class AS c
            JOIN pg_attribute AS a ON c.oid = a.attrelid
        WHERE c.oid = %s::regclass AND 0 < a.attnum
        ORDER BY a.attnum
        """,
        (table.sql,),
    )
    return [ColumnDef(*row) for row in cur.fetchall()]
