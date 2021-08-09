import hashlib
import typing

from pg_sql import SqlId, SqlNumber, SqlObject, SqlString, sql_list

from .formats.join import JoinTable
from .join_common import Structure, foreign_column, local_column
from .join_key import KeyResolver
from .sql import SqlQuery, SqlTableExpr, table_fields, update_excluded
from .string import indent


def create_queue(
    id: str,
    table_id: str,
    structure: Structure,
    resolver: KeyResolver,
    tables: typing.Dict[str, JoinTable],
):
    table = tables[table_id]
    dep = table.join
    foreign_table = tables[dep]

    if table.lock_id is not None:
        lock_id = table.lock_id
    else:
        digest = hashlib.md5(f"{id}__{table_id}".encode("utf-8")).digest()
        lock_id = int.from_bytes(digest[0:2], "big", signed=True)
    lock_base = lock_id * (2 ** 48)

    queue_table = structure.queue_table(table_id)

    local_columns = [local_column(column) for column in (table.key or ["_"])]
    foreign_columns = [foreign_column(column) for column in table.join_key]

    yield f"""
CREATE TABLE {queue_table}
AS SELECT
  {sql_list(f"{SqlObject(SqlId('l'), SqlId(column))} AS {local_column(column)}" for column in (table.key or ["_"]))},
  {sql_list(f"{SqlObject(SqlId('f'), SqlId(column))} AS {foreign_column(column)}" for column in table.join_key)},
  NULL::bigint AS seq,
  NULL::bigint AS lock
FROM
  {table.sql} AS l
  CROSS JOIN {foreign_table.sql} AS f
WITH NO DATA
    """.strip()

    yield f"""
ALTER TABLE {queue_table}
  ADD PRIMARY KEY ({sql_list(local_columns)}),
  ALTER lock ADD GENERATED BY DEFAULT AS IDENTITY,
  ALTER lock SET NOT NULL,
  ALTER seq ADD GENERATED BY DEFAULT AS IDENTITY,
  ALTER seq SET NOT NULL
    """.strip()

    yield f"""
COMMENT ON TABLE {queue_table} IS {SqlString(f"Asynchronous processing of changes to {table.sql}")}
    """.strip()

    for column in table.key or ["_"]:
        yield f"""
COMMENT ON COLUMN {queue_table}.{local_column(column)} IS {SqlString(f"{table.sql} key: {SqlId(column)}")}
"""

    for column in table.join_key:
        yield f"""
COMMENT ON COLUMN {queue_table}.{foreign_column(column)} IS {SqlString(f"{foreign_table.sql} iterator: {SqlId(column)}")}
"""

    yield f"""
COMMENT ON COLUMN {queue_table}.seq IS 'Order to process'
    """.strip()

    yield f"""
COMMENT ON COLUMN {queue_table}.lock IS 'Lock ID'
    """.strip()

    yield f"""
CREATE INDEX ON {queue_table} (seq)
    """.strip()

    foreign_key_table = SqlObject(SqlId("_foreign_key"))

    item = SqlId("_item")
    new_item = SqlId("_new_item")

    get_item = f"""
SELECT
  {table_fields(item, local_columns)},
  {table_fields(SqlId("k"), [SqlId(column) for column in table.join_key])},
  _item.seq,
  _item.lock
INTO _new_item
FROM {SqlObject(foreign_key_table)} AS k
ORDER BY {table_fields(SqlId("k"), table.join_key)} DESC
    """.strip()

    if table.join_on is not None:
        join = f"""
JOIN (VALUES ({table_fields(item, local_columns)})) AS {SqlId(table_id)} ({sql_list(SqlId(col) for col in table.key)})
ON {table.join_on}
        """.strip()
    else:
        join = ""

    key1_query = f"""
SELECT {SqlId(dep)}.*
FROM {foreign_table.sql} AS {SqlId(dep)}
{join}
ORDER BY {sql_list(SqlObject(SqlId(dep), SqlId(name)) for name in table.join_key)}
LIMIT max_records
    """.strip()
    gather1 = resolver.sql(
        foreign_key_table,
        exprs=[SqlTableExpr(foreign_key_table, key1_query)],
        last_expr=get_item,
    )

    key2_query = f"""
SELECT {SqlId(dep)}.*
FROM {foreign_table.sql} AS {SqlId(dep)}
{join}
WHERE ({table_fields(item, foreign_columns)}) < ({table_fields(SqlId(dep), (SqlId(column) for column in table.join_key))})
ORDER BY {sql_list(SqlObject(SqlId(dep), SqlId(name)) for name in table.join_key)}
LIMIT max_records
    """.strip()
    gather2 = resolver.sql(
        foreign_key_table,
        exprs=[SqlTableExpr(foreign_key_table, key2_query)],
        last_expr=get_item,
    )

    process_function = structure.queue_process_function(table_id)
    yield f"""
CREATE FUNCTION {process_function} (max_records bigint) RETURNS bool
LANGUAGE plpgsql AS $$
  DECLARE
    _item {queue_table};
    _new_item {queue_table};
  BEGIN
    -- find item
    SELECT (q.*) INTO _item
    FROM {queue_table} AS q
    WHERE pg_try_advisory_xact_lock({lock_base} + q.lock)
    ORDER BY q.seq
    LIMIT 1;

    IF _item IS NULL THEN
      -- if no item found, exit
      RETURN false;
    END IF;

    IF ({table_fields(item, (foreign_column(column) for column in table.join_key))}) IS NULL THEN
      -- if there is no iterator, start at the beginning
{indent(gather1, 3)}
    ELSE
      -- if there is an iterator, start at the iterator
{indent(gather2, 3)}
    END IF;

    IF _new_item IS NULL THEN
      -- if the iterator was at the end, remove the queue item
      DELETE FROM {queue_table} AS q
      WHERE
        ({table_fields(SqlId("q"), local_columns)}, q.seq)
          = ({table_fields(item, local_columns)}, _item.seq);
    ELSE
      -- update the queue item with the new iterator
      UPDATE {queue_table} AS q
      SET {sql_list(f'{column} = (_new_item).{column}' for column in foreign_columns)}, seq = nextval(pg_get_serial_sequence({SqlString(str(queue_table))}, 'seq'))
      WHERE
          ({table_fields(SqlId("q"), local_columns)}, q.seq)
          = ({table_fields(item, local_columns)}, _item.seq);
    END IF;

    -- notify listeners that the queue has been updated
    NOTIFY {SqlId(str(queue_table))};

    RETURN true;
  END;
$$
""".strip()

    yield f"""
COMMENT ON FUNCTION {process_function} IS {SqlString(f"Refresh for {queue_table}")}
    """.strip()


def enqueue_sql(
    id: str,
    table: JoinTable,
    structure: Structure,
    key_query: str,
    exprs: typing.List[SqlTableExpr],
    last_expr: typing.Optional[str],
):
    queue_table = structure.queue_table(id)

    local_columns = [local_column(column) for column in (table.key or ["_"])]

    if table.key:
        order = (
            f"ORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(table.key))}"
        )
    else:
        order = ""

    insert = f"""
INSERT INTO {queue_table} ({sql_list(local_columns)})
{key_query}
{order}
ON CONFLICT ({sql_list(local_columns)}) DO UPDATE
  SET {update_excluded(foreign_column(column) for column in table.join_key)},
    seq = excluded.seq
    """.strip()
    query = SqlQuery(insert, expressions=exprs)
    if last_expr is not None:
        query.append(SqlId("_other"), last_expr)

    return f"""
{query};

NOTIFY {SqlId(str(queue_table))};
    """.strip()
