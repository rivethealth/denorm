import typing

from pg_sql import SqlId, SqlNumber, SqlObject, sql_list

from .sql import SqlQuery, SqlTableExpr, table_fields, update_excluded
from .string import indent


def insert_query(
    columns: typing.List[SqlId],
    query: str,
    target: SqlObject,
    expressions: typing.Optional[str] = None,
) -> SqlQuery:
    """
    Insert data
    """
    insert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
    """.strip()

    return SqlQuery(insert_query)


def sync_query(
    columns: typing.List[SqlId],
    key: typing.List[SqlId],
    key_table: SqlObject,
    query: str,
    target: SqlObject,
) -> SqlQuery:
    """
    Insert, update, and delete
    """
    data_columns = [SqlId(column) for column in columns if column not in key]
    is_temp = target.names[0].name == "pg_temp"

    if not is_temp:
        query += f"\nORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(key))}"

    if data_columns:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
    SET {update_excluded(data_columns)}
RETURNING {sql_list(key)}
        """.strip()
    elif not is_temp:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
    SET {update_excluded(key)}
    WHERE false
RETURNING {sql_list(key)}
        """.strip()
    else:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO NOTHING
"""
    upsert_expression = SqlTableExpr(SqlId("_upsert"), upsert_query)

    delete_query = f"""
DELETE FROM {target} AS t
USING {key_table} AS k
  LEFT JOIN _upsert AS u ON ({table_fields(SqlId('k'), key)}) = ({table_fields(SqlId('u'), key)})
WHERE
  ({table_fields(SqlId('t'), key)}) = ({table_fields(SqlId('k'), key)})
  AND u.* IS NOT DISTINCT FROM NULL
    """.strip()

    return SqlQuery(delete_query, expressions=[upsert_expression])


def upsert_query(
    columns: typing.List[SqlId],
    key: typing.List[SqlId],
    query: str,
    target: SqlObject,
) -> SqlQuery:
    """
    Upsert data
    """
    data_columns = [SqlId(column) for column in columns if column not in key]
    is_temp = target.names[0].name == "pg_temp"

    if not is_temp:
        query += f"\nORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(key))}"

    if data_columns:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
    SET {update_excluded(data_columns)}
        """.strip()
    elif not is_temp:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO UPDATE
    SET {update_excluded(key)}
    WHERE false
        """.strip()
    else:
        upsert_query = f"""
INSERT INTO {target} ({sql_list(columns)})
{query}
ON CONFLICT ({sql_list(key)}) DO NOTHING
"""

    return SqlQuery(upsert_query)
