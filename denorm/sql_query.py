import typing

from .sql import (
    SqlId,
    SqlNumber,
    SqlObject,
    SqlQuery,
    SqlTableExpression,
    sql_list,
    table_fields,
    update_excluded,
)
from .string import indent


def sync_query(
    columns: typing.List[SqlId],
    key: typing.List[SqlId],
    key_table: SqlObject,
    query: str,
    target: SqlObject,
) -> SqlQuery:
    data_columns = [SqlId(column) for column in columns if column not in key]

    if target.names[0] != "pg_temp":
        query += f"\nORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(key))}"

    if data_columns:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
    SET {update_excluded(data_columns)}
RETURNING {sql_list(key)}
        """.strip()
    else:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
    SET {update_excluded(key)}
    WHERE false
RETURNING {sql_list(key)}
        """.strip()

    upsert_expression = SqlTableExpression(SqlId("_upsert"), upsert_query)

    delete_query = f"""
DELETE FROM {target} AS t
USING _upsert AS u
  LEFT JOIN {key_table} AS k ON ({table_fields(SqlId('u'), key)}) = ({table_fields(SqlId('k'), key)})
WHERE
  ({table_fields(SqlId('t'), key)}) = ({table_fields(SqlId('u'), key)})
  AND k.* IS NOT DISTINCT FROM NULL
    """.strip()

    return SqlQuery(delete_query, expressions=[upsert_expression])


def upsert_query(
    columns: typing.List[SqlId],
    key: typing.List[SqlId],
    query: str,
    target: SqlObject,
    expressions: typing.Optional[str] = None,
) -> SqlQuery:
    data_columns = [SqlId(column) for column in columns if column not in key]

    if target.names[0] != "pg_temp":
        query += f"\nORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(key))}"

    if data_columns:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
    SET {update_excluded(data_columns)}
        """.strip()
    else:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
    SET {update_excluded(key)}
    WHERE false
        """.strip()

    return SqlQuery(upsert_query)
