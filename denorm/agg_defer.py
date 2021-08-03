import typing

from pg_sql import SqlId, SqlNumber, SqlString, sql_list

from .agg_common import AggStructure
from .formats.agg import AggAggregate, AggConfig, AggTable


def create_refresh_function(
    id: str,
    structure: AggStructure,
    aggregates: typing.Dict[str, AggAggregate],
    groups: typing.Dict[str, str],
    target: AggTable,
):
    refresh_function = structure.refresh_function()
    refresh_table = structure.refresh_table()
    tmp_table = structure.tmp_table()
    group_columns = [SqlId(col) for col in groups]
    aggregate_columns = [SqlId(col) for col in aggregates]

    yield f"""
CREATE FUNCTION {refresh_function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
    DELETE FROM {refresh_table};

    WITH
      _delete AS (
        DELETE FROM {tmp_table}
        RETURNING *
      )
    INSERT INTO {target.sql} AS existing (
      {sql_list(group_columns)},
      {sql_list(aggregate_columns)}
    )
    SELECT
      {sql_list(group_columns)},
      {sql_list(aggregate_columns)}
    FROM {tmp_table}
    ORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(groups))}
    ON CONFLICT ({sql_list(group_columns)}) DO UPDATE
      SET {sql_list(f'{SqlId(col)} = {agg.combine_expression(col)}' for col, agg in aggregates.items())};

    RETURN NULL;
  END;
$$
    """.strip()


def create_setup_function(
    id: str,
    structure: AggStructure,
    aggregates: typing.Dict[str, AggAggregate],
    groups: typing.Dict[str, str],
    target: AggTable,
):
    refresh_constraint = structure.refresh_constraint()
    refresh_function = structure.refresh_function()
    refresh_table = structure.refresh_table()
    setup_function = structure.setup_function()
    tmp_table = structure.tmp_table()

    group_columns = [SqlId(col) for col in groups]
    aggregate_columns = [SqlId(col) for col in aggregates]

    yield f"""
CREATE FUNCTION {setup_function} () RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
    IF to_regclass({SqlString(str(refresh_table))}) IS NOT NULL THEN
      RETURN;
    END IF;

    CREATE TEMP TABLE {tmp_table}
    ON COMMIT DELETE ROWS
    AS SELECT
      {sql_list(group_columns)},
      {sql_list(aggregate_columns)}
    FROM {target.sql}
    WITH NO DATA;

    ALTER TABLE {tmp_table}
      ADD PRIMARY KEY ({sql_list(group_columns)});

    CREATE TEMP TABLE {refresh_table} (
    ) ON COMMIT DELETE ROWS;

    CREATE CONSTRAINT TRIGGER {refresh_constraint} AFTER INSERT ON {refresh_table}
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE PROCEDURE {refresh_function}();
  END;
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {setup_function} IS {SqlString(f"Set up temp tables for {id}")}
    """.strip()
