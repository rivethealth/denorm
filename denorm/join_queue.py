import hashlib
import typing

from .formats.join import JoinTable
from .join_common import Structure, foreign_column, local_column
from .join_query import ProcessQuery
from .sql import (
    SqlId,
    SqlNumber,
    SqlObject,
    SqlString,
    SqlTableExpression,
    table_fields,
    sql_list,
    table_fields,
)
from .string import indent


def create_queue(
    table_id: str,
    structure: Structure,
    process_query: ProcessQuery,
    tables: typing.Dict[str, JoinTable],
):
    table = tables[table_id]
    foreign_table = tables[table.dep]

    if table.lock_id is not None:
        lock_id = table.lock_id
    else:
        digest = hashlib.md5(table_id.encode("utf-8")).digest()
        lock_id = int.from_bytes(digest[0:2], "big") // 2
    lock_base = lock_id * (2 ** 48)

    queue_table = structure.queue_table(table_id)

    local_columns = [local_column(column) for column in table.key]
    foreign_columns = [foreign_column(column) for column in foreign_table.key]

    yield f"""
CREATE TABLE {queue_table}
AS SELECT
  {sql_list(f"{SqlObject(SqlId('l'), SqlId(column))} AS {local_column(column)}" for column in table.key)},
  {sql_list(f"{SqlObject(SqlId('f'), SqlId(column))} AS {foreign_column(column)}" for column in foreign_table.key)},
  NULL::bigint AS seq,
  NULL::bigint AS lock,
  NULL::int AS tries
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
  ALTER seq SET NOT NULL,
  ALTER tries SET DEFAULT 0,
  ALTER tries SET NOT NULL
    """.strip()

    yield f"""
COMMENT ON TABLE {queue_table} IS {SqlString(f"Asynchronous processing of changes to {table.sql}")}
    """.strip()

    yield f"""
COMMENT ON COLUMN {queue_table}.seq IS 'Order to process'
    """.strip()

    yield f"""
COMMENT ON COLUMN {queue_table}.lock IS 'Lock ID'
    """.strip()

    yield f"""
COMMENT ON COLUMN {queue_table}.tries IS 'Number of tries'
    """.strip()

    yield f"""
CREATE INDEX ON {queue_table} (seq)
    """.strip()

    begin_function = structure.queue_begin_function(table_id)
    yield f"""
CREATE FUNCTION {begin_function} (_max_tries int DEFAULT NULL) RETURNS {queue_table}
LANGUAGE plpgsql AS $$
  DECLARE
    _item {queue_table};
  BEGIN
    SELECT (q.*) INTO _item
    FROM {queue_table} AS q
    WHERE pg_try_advisory_lock({lock_base} + q.lock)
    ORDER BY q.seq
    LIMIT 1;

    IF _item IS NULL THEN
      RETURN NULL;
    END IF;

    UPDATE {queue_table} AS q
      SET tries = q.tries + 1
    WHERE ({table_fields(SqlId("q"), local_columns)}) = ({table_fields(SqlId("_item"), local_columns)});

    RETURN _item;
  END;
$$
""".strip()

    yield f"""
COMMENT ON FUNCTION {begin_function} IS 'Begin refresh for {queue_table}'
    """.strip()

    foreign_key_table = SqlId("_foreign_key")

    update_queue = f"""
SELECT (k.*) INTO result
FROM {SqlObject(foreign_key_table)} AS k
ORDER BY k.* DESC
    """.strip()

    key1_query = f"""
SELECT {table_fields(SqlId(table.dep), (SqlId(column) for column in foreign_table.key))}
FROM {foreign_table.sql} AS {SqlId(table.dep)}
JOIN (VALUES ({table_fields(SqlId("item"), local_columns)})) AS {SqlId(table_id)} ({sql_list(SqlId(col) for col in table.key)})
    ON {table.dep_join}
ORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(foreign_table.key))}
LIMIT max_records
    """.strip()
    gather1 = process_query.gather(foreign_key_table)
    gather1.prepend(SqlTableExpression(foreign_key_table, key1_query))
    gather1.append(SqlId("_other"), update_queue)

    key2_query = f"""
SELECT {table_fields(SqlId(table.dep), (SqlId(column) for column in foreign_table.key))}
FROM {foreign_table.sql} AS {SqlId(table.dep)}
JOIN (VALUES ({table_fields(SqlId("item"), local_columns)})) AS {SqlId(table_id)} ({sql_list(SqlId(col) for col in table.key)})
  ON {table.dep_join}
ORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(foreign_table.key))}
LIMIT max_records
    """.strip()
    gather2 = process_query.gather(foreign_key_table)
    gather2.prepend(SqlTableExpression(foreign_key_table, key2_query))
    gather2.append(SqlId("_other"), update_queue)

    refresh_function = structure.queue_refresh_function(table_id)
    yield f"""
CREATE FUNCTION {refresh_function} (item {queue_table}, max_records bigint) RETURNS {queue_table}
LANGUAGE plpgsql AS $$
  DECLARE
    result {queue_table};
  BEGIN
    IF ({table_fields(SqlId("item"), (foreign_column(column) for column in foreign_table.key))}) IS NULL THEN
{indent(str(gather1), 3)};
    ELSE
{indent(str(gather2), 3)};
    END IF;

{indent(process_query.finalize(), 2)}


    RETURN (
      {table_fields(SqlId('item'), local_columns)},
      {table_fields(SqlId('result'), foreign_columns)},
      item.seq,
      item.lock,
      item.tries
    );
  END;
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {refresh_function} IS {SqlString(f"Refresh for {queue_table}")}
    """.strip()

    end_function = structure.queue_end_function(table_id)
    yield f"""
CREATE FUNCTION {end_function} (item {queue_table}) RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
    IF {table_fields(SqlId("item"), foreign_columns)} IS NULL THEN
      DELETE FROM {queue_table} AS q
      WHERE
        ({table_fields(SqlId("q"), local_columns)}, item.seq)
          = ({table_fields(SqlId("item"), local_columns)}, item.seq);
    ELSE
      UPDATE {queue_table} AS q
      SET {sql_list(f'{column} = (item).{column}' for column in foreign_columns)}
      WHERE
        ({table_fields(SqlId("q"), local_columns)}, q.seq)
          = ({table_fields(SqlId("item"), local_columns)}, item.seq);
    END IF;

    PERFORM pg_advisory_unlock({lock_base} + (item).lock);

    NOTIFY {SqlId(str(queue_table))};
  END;
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {refresh_function} IS {SqlString(f"End refresh for {queue_table}")}
    """.strip()

    process_function = structure.queue_process_function(table_id)
    yield f"""
CREATE PROCEDURE {process_function} (max_records bigint, max_tries int DEFAULT NULL, INOUT result bool DEFAULT false)
LANGUAGE plpgsql AS $$
  DECLARE
    item {queue_table};
  BEGIN
    item := {begin_function}(max_tries);

    IF item IS NULL THEN
     RETURN;
    END IF;

    COMMIT;

    item := {refresh_function}(item, max_records);

    COMMIT;

    PERFORM {end_function}(item);

    COMMIT;

    result := true;
  END;
$$
    """.strip()

    yield f"""
COMMENT ON PROCEDURE {process_function} IS {SqlString(f"Refresh for {queue_table}")}
    """.strip()
