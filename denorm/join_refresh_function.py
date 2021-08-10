from pg_sql import SqlId, SqlString

from .formats.join import JoinTable
from .join_async import enqueue_sql
from .join_common import Structure
from .sql import sql_list
from .string import indent


def param_name(name) -> SqlId:
    return SqlId(f"_{name}")


def create_refresh_function(structure: Structure, table_id: str, table: JoinTable):
    if table.key:
        key_query = f"SELECT {sql_list(param_name(name) for name in table.key)}"
    else:
        key_query = "SELECT false"

    enqueue = enqueue_sql(table_id, table, structure, key_query, [], None)

    function = structure.refresh_table_function(table_id)

    params = sql_list(
        f"{param_name(name)} {type}"
        for name, type in zip(table.key or [], table.key_type or [])
    )
    yield f"""
CREATE FUNCTION {function}({params}) RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
{indent(enqueue, 2)}
  END;
$$
    """.strip()

    comment = SqlString(f"Recalculate, based on {table.join}")
    yield f"""
COMMENT ON FUNCTION {function} IS {comment}
    """.strip()
