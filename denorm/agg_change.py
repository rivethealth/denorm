import typing

from pg_sql import SqlId, SqlNumber, SqlString, sql_list

from .agg_common import AggStructure
from .formats.agg import AggAggregate, AggConsistency, AggTable
from .string import indent


def create_change(
    aggregates: typing.Dict[str, AggAggregate],
    filter: typing.Optional[str],
    source: AggTable,
    id: str,
    consistency: AggConsistency,
    groups: typing.Dict[str, str],
    structure: AggStructure,
    target: AggTable,
):
    change_function = structure.change_function()
    group_columns = [SqlId(col) for col in groups]
    aggregate_columns = [SqlId(col) for col in aggregates]

    where = f"WHERE {filter}" if filter is not None else ""
    if consistency == AggConsistency.DEFERRED:
        setup_function = structure.setup_function()
        refresh_table = structure.refresh_table()
        target_table = structure.tmp_table()
        order = ""
        setup = f"""
PERFORM {setup_function}();
        """.strip()
        finalize = f"""
INSERT INTO {refresh_table}
SELECT
WHERE NOT EXISTS (TABLE {refresh_table});
        """.strip()
    elif consistency == AggConsistency.IMMEDIATE:
        target_table = target.sql
        order = f"ORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(groups))}"
        setup = ""
        finalize = ""

    yield f"""
CREATE FUNCTION {change_function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  DECLARE
    sign smallint := TG_ARGV[0]::smallint;
  BEGIN
{indent(setup, 2)}

    INSERT INTO {target_table} AS existing (
      {sql_list(group_columns)},
      {sql_list(aggregate_columns)}
    )
    SELECT
      {sql_list(value for value in groups.values())},
      {sql_list(agg.value for agg in aggregates.values())}
    FROM _change AS {SqlId(id)}
    {where}
    GROUP BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(groups))}
    {order}
    ON CONFLICT ({sql_list(group_columns)}) DO UPDATE
      SET {sql_list(f'{SqlId(col)} = {agg.combine_expression(col)}' for col, agg in aggregates.items())};

{indent(finalize, 2)}

    RETURN NULL;
  END;
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {change_function} IS {SqlString(f'Handle changes for {id}')}
    """.strip()

    delete_trigger = structure.delete_trigger()
    yield f"""
CREATE TRIGGER {delete_trigger} AFTER DELETE ON {source.sql}
REFERENCING OLD TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}('-1');
    """.strip()

    insert_trigger = structure.insert_trigger()
    yield f"""
CREATE TRIGGER {insert_trigger} AFTER INSERT ON {source.sql}
REFERENCING NEW TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}('1');
    """.strip()

    update_1_trigger = structure.update_1_trigger()
    yield f"""
CREATE TRIGGER {update_1_trigger} AFTER UPDATE ON {source.sql}
REFERENCING NEW TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}('1');
    """.strip()

    update_2_trigger = structure.update_2_trigger()
    yield f"""
CREATE TRIGGER {update_2_trigger} AFTER UPDATE ON {source.sql}
REFERENCING OLD TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}('-1');
    """.strip()
