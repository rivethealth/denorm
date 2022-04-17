import typing

from pg_sql import SqlId, SqlNumber, SqlString, sql_list

from .agg_common import AggStructure
from .formats.agg import AggAggregate, AggConsistency, AggTable
from .sql import table_fields
from .string import indent


def _create_change_function(
    aggregates: typing.Dict[str, AggAggregate],
    consistency: AggConsistency,
    filter: typing.Optional[str],
    groups: typing.Dict[str, str],
    id: str,
    shard: typing.Union[bool, typing.Dict[str, str]],
    source: AggTable,
    structure: AggStructure,
    target: AggTable,
    update: bool,
):
    change_function = (
        structure.change2_function() if update else structure.change1_function()
    )
    group_columns = [SqlId(col) for col in groups]
    aggregate_columns = [SqlId(col) for col in aggregates]

    setup = f"""
IF NOT EXISTS (TABLE {"_change1" if update else "_change"}) THEN
  RETURN NULL;
END IF;
    """.strip()

    where = f"WHERE {filter}" if filter is not None else ""
    if consistency == AggConsistency.DEFERRED:
        setup_function = structure.setup_function()
        refresh_table = structure.refresh_table()
        target_table = structure.tmp_table()
        order = ""
        setup = f"""
{setup}

PERFORM {setup_function}();
        """.strip()
        finalize = f"""
IF found THEN
  INSERT INTO {refresh_table}
  SELECT
  WHERE NOT EXISTS (TABLE {refresh_table});
END IF;
        """.strip()
    elif consistency == AggConsistency.IMMEDIATE:
        target_table = target.sql
        order = f"ORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(groups))}"
        finalize = ""

    if update:
        data = f"""
(
    SELECT -1 AS sign, *
    FROM _change1
    UNION ALL
    SELECT 1, *
    FROM _change2
)
        """.strip()
        vars = ""
    else:
        data = "_change"
        vars = """
sign smallint := TG_ARGV[0]::smallint;
        """.strip()

    query = f"""
SELECT
    {sql_list(value for value in groups.values())},
    {sql_list(agg.value for agg in aggregates.values())}
FROM {data} AS {SqlId(id)}
{where}
GROUP BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(groups))}
HAVING ({sql_list(agg.value for agg in aggregates.values())}) IS DISTINCT FROM ({sql_list(agg.identity for agg in aggregates.values())})
    """.strip()

    if shard:
        # 1. aggregate changes
        # 2. lock records where possible
        # 3. update records
        # 4. insert for records that have not been updated
        # Note: #4 does require checking #3 to prevent consideration of locked
        # dead records.
        body = f"""
WITH
  locked AS (
    SELECT *
    FROM
      (
{indent(query, 4)}
      ) AS d ({sql_list(group_columns + aggregate_columns)})
      LEFT JOIN LATERAL (
        SELECT ctid
        FROM {target_table} AS t
        WHERE ({table_fields(SqlId("d"), group_columns)}) = ({sql_list(group_columns)})
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      ) AS t ON TRUE
  ),
  update AS (
      UPDATE {target_table} AS existing
      SET {sql_list(f'{SqlId(col)} = {agg.combine_expression(col)}' for col, agg in aggregates.items())}
      FROM locked AS excluded
      WHERE existing.ctid = excluded.ctid
      RETURNING excluded.ctid
  )
INSERT INTO {target_table} ({sql_list(group_columns)}, {sql_list(aggregate_columns)})
SELECT {sql_list(group_columns)}, {sql_list(aggregate_columns)}
FROM locked AS l
  LEFT JOIN update AS u ON l.ctid = u.ctid
WHERE u.ctid IS NULL;
        """.strip()
    else:
        body = f"""
INSERT INTO {target_table} AS existing (
    {sql_list(group_columns)},
    {sql_list(aggregate_columns)}
)
{query}
{order}
ON CONFLICT ({sql_list(group_columns)}) DO UPDATE
    SET {sql_list(f'{SqlId(col)} = {agg.combine_expression(col)}' for col, agg in aggregates.items())};
        """.strip()

    yield f"""
CREATE FUNCTION {change_function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  DECLARE
{indent(vars, 2)}
  BEGIN
{indent(setup, 2)}

{indent(body, 2)}

{indent(finalize, 2)}
    RETURN NULL;
  END;
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {change_function} IS {SqlString(f'Handle changes for {id}')}
    """.strip()

    if update:
        update_trigger = structure.update_trigger()
        yield f"""
CREATE TRIGGER {update_trigger} AFTER UPDATE ON {source.sql}
REFERENCING OLD TABLE AS _change1 NEW TABLE AS _change2
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}()
        """.strip()
    else:
        delete_trigger = structure.delete_trigger()
        yield f"""
CREATE TRIGGER {delete_trigger} AFTER DELETE ON {source.sql}
REFERENCING OLD TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}('-1')
        """.strip()

        insert_trigger = structure.insert_trigger()
        yield f"""
CREATE TRIGGER {insert_trigger} AFTER INSERT ON {source.sql}
REFERENCING NEW TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}('1')
        """.strip()


def create_change(
    aggregates: typing.Dict[str, AggAggregate],
    consistency: AggConsistency,
    filter: typing.Optional[str],
    groups: typing.Dict[str, str],
    id: str,
    shard: bool,
    source: AggTable,
    structure: AggStructure,
    target: AggTable,
):
    for update in [False, True]:
        yield from _create_change_function(
            aggregates=aggregates,
            consistency=consistency,
            filter=filter,
            groups=groups,
            id=id,
            shard=shard,
            source=source,
            structure=structure,
            target=target,
            update=update,
        )
