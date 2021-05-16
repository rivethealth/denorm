import typing

from pg_sql import SqlId, SqlString

from .agg_common import AggStructure
from .formats.agg import AggTable
from .sql import table_fields


def create_cleanup(
    id: str, groups: typing.Dict[str, str], structure: AggStructure, target: AggTable
):
    cleanup_function = structure.cleanup_function()
    cleanup_trigger = structure.cleanup_trigger()
    group_columns = [SqlId(group) for group in groups]

    yield f"""
CREATE FUNCTION {cleanup_function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
    DELETE FROM {target.sql} AS t
    USING _new AS n
    WHERE
      ({table_fields(SqlId("t"), group_columns)}) = ({table_fields(SqlId("n"), group_columns)})
      AND n._count = 0;

    RETURN NULL;
  END;
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {cleanup_function} IS {SqlString(f'Cleanup records for {id}')}
    """.strip()

    yield f"""
CREATE TRIGGER {cleanup_trigger} AFTER UPDATE ON {target.sql}
REFERENCING NEW TABLE AS _new
FOR EACH STATEMENT EXECUTE PROCEDURE {cleanup_function}()
    """.strip()

    yield f"""
COMMENT ON TRIGGER {cleanup_trigger} ON {target.sql} IS {SqlString(f'Cleanup records for {id}')}
    """.strip()
