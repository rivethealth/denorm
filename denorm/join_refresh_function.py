from pg_sql import SqlString

from .formats.join import JoinTable
from .join_async import enqueue_sql
from .join_common import Structure
from .string import indent


def create_refresh_function(structure: Structure, table_id: str, table: JoinTable):
    enqueue = enqueue_sql(table_id, table, structure, "SELECT false AS _", [], None)

    function = structure.refresh_table_function(table_id)

    yield f"""
CREATE FUNCTION {function}() RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
{indent(enqueue, 2)}
  END;
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {function} IS {SqlString(f"Recalculate all, based on {table.join}")}
"""
