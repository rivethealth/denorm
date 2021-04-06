import typing

from .sql import SqlId, SqlNumber, SqlObject, sql_list, table_fields, update_excluded
from .string import indent


def sync_query(
    columns: typing.List[SqlId],
    key: typing.List[SqlId],
    key_table: SqlObject,
    query: str,
    target: SqlObject,
    expressions: typing.Optional[str] = None,
):
    data_columns = [SqlId(column) for column in columns if column not in key]

    query += (
        f"\nORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(key))}"
        if target.names[0] != "pg_temp"
        else ""
    )

    if data_columns:
        return f"""
WITH
{indent(expressions, 1) + ', ' if expressions is not None else '' }
  _upsert AS (
    INSERT INTO {target} ({sql_list(columns)})
{indent(query, 2)}
    ON CONFLICT ({sql_list(key)}) DO UPDATE
      SET {update_excluded(data_columns)}
    RETURNING {sql_list(key)}
  )
DELETE FROM {target} AS t
USING {key_table} AS l
WHERE
  ({table_fields(SqlId('t'), key)}) = ({table_fields(SqlId('l'), key)})
  AND NOT EXISTS (
  SELECT
  FROM _upsert AS u
  WHERE ({table_fields(SqlId('t'), key)}) = ({table_fields(SqlId('u'), key)})
)
            """.strip()
    else:
        return f"""
WITH
  _upsert AS (
    INSERT INTO {target} ({sql_list(columns)})
{indent(query, 2)}
    ON CONFLICT ({sql_list(key)}) DO UPDATE
      SET {update_excluded(key)}
    RETURNING {sql_list(key)}
  )
DELETE FROM {target} AS t
USING _upsert AS u
  LEFT JOIN {key_table} AS k ON ({table_fields(SqlId('u'), key)}) = ({table_fields(SqlId('k'), key)})
WHERE
  ({table_fields(SqlId('t'), key)}) = ({table_fields(SqlId('u'), key)})
  AND k.* IS NOT DISTINCT FROM NULL
        """.strip()


def upsert_query(
    columns: typing.List[SqlId], key: typing.List[SqlId], query: str, target: SqlObject
):
    data_columns = [SqlId(column) for column in columns if column not in key]

    query += (
        f"\nORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(key))}"
        if target.names[0] != "pg_temp"
        else ""
    )

    if data_columns:
        return f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
  SET {update_excluded(data_columns)}
        """.strip()
    elif target.names[0] == "pg_temp":
        return f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO NOTHING
        """.strip()
    else:
        return f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
  SET {update_excluded(key)}
  WHERE false
        """.strip()
