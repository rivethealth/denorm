from pg_sql import SqlId, SqlObject, SqlString

from .formats.join import JoinTable
from .join_async import enqueue_sql
from .join_common import Structure
from .join_key import KeyResolver
from .sql import SqlTableExpr, sql_list
from .string import indent


def param_name(name) -> SqlId:
    return SqlId(f"_{name}")


def create_refresh_function(
    structure: Structure, resolver: KeyResolver, table_id: str, table: JoinTable
):
    if table.key:
        columns = sql_list(
            f"{param_name(column.name)} AS {column.sql}" for column in table.key
        )
        key_query = f"SELECT {columns}"
    else:
        key_query = "SELECT false AS _"

    key = SqlId("key")
    query = resolver.sql(SqlObject(key), [SqlTableExpr(name=key, query=key_query)])

    function = structure.refresh_table_function(table_id)

    params = sql_list(
        f"{param_name(column.name)} {column.type}" for column in (table.key or [])
    )
    yield f"""
CREATE FUNCTION {function}({params}) RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
{indent(query, 2)}
  END;
$$
    """.strip()

    comment = SqlString(f"Recalculate, based on {table.join}")
    yield f"""
COMMENT ON FUNCTION {function} IS {comment}
    """.strip()
