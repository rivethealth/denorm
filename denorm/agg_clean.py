import typing

from pg_sql import SqlId, SqlNumber, SqlString, sql_list

from .agg_common import AggStructure
from .formats.agg import AggTable
from .sql import table_fields


def create_cleanup(
    id: str,
    groups: typing.Dict[str, str],
    shard: bool,
    structure: AggStructure,
    target: AggTable,
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


def create_compress(
    id: str,
    groups: typing.Dict[str, str],
    aggregates: typing.Dict[str, str],
    structure: AggStructure,
    target: AggTable,
):
    compress_function = structure.compress_function()
    group_columns = [SqlId(group) for group in groups]

    yield f"""
CREATE FUNCTION {compress_function} () RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
    WITH
      data AS (
        DELETE FROM {target.sql}
        RETURNING *
      )
    INSERT INTO {target.sql} ({sql_list(list(groups) + list(aggregates))})
    SELECT
        {sql_list(list(groups) + list(aggregates.values()))}
    FROM data
    GROUP BY {sql_list([SqlNumber(i + 1) for i in range(len(groups))])}
    HAVING
      ({sql_list(agg.value for agg in aggregates.values())})
      IS DISTINCT FROM ({sql_list(agg.identity for agg in aggregates.values())})
  END;
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {compress_function} IS {SqlString(f'Compress aggregate for {id}')}
    """.strip()
