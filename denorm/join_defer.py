import typing

from .format import format
from .formats.join import JoinSync, JoinTarget
from .join_common import Structure
from .sql import SqlId, SqlNumber, SqlObject, SqlString, sql_list, update_excluded
from .sql_query import sync_query, upsert_query
from .string import indent


def create_refresh_function(
    id: str,
    query: typing.Optional[str],
    structure: Structure,
    sync: JoinSync,
    target: JoinTarget,
):
    key = [SqlId(column) for column in target.key]
    key_table = structure.key_table()
    lock_table = structure.lock_table()
    refresh_function = structure.refresh_function()
    refresh_table = structure.refresh_table()

    query = (
        format(query, str(lock_table)) if query is not None else f"TABLE {lock_table}"
    )

    if sync == JoinSync.FULL:
        update_query = sync_query(
            columns=target.columns or target.key,
            key=target.key,
            key_table=lock_table,
            query=query,
            target=target.sql,
        )
    elif sync == JoinSync.UPSERT:
        update_query = upsert_query(
            columns=target.columns or target.key,
            key=target.key,
            query=query,
            target=target.sql,
        )

    yield f"""
CREATE FUNCTION {refresh_function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
    DELETE FROM {refresh_table};

    -- lock keys
    WITH
        _change AS (
            DELETE FROM {key_table}
            RETURNING *
        )
    INSERT INTO {lock_table} ({sql_list(key)})
    SELECT *
    FROM _change
    ORDER BY {sql_list(SqlNumber(i + 1) for i, _ in enumerate(key))}
    ON CONFLICT ({sql_list(key)}) DO UPDATE
        SET {update_excluded(key)}
        WHERE false;

    -- update
{indent(str(update_query), 2)};

    -- clear locks
    DELETE FROM {lock_table};

    RETURN NULL;
  END;
$$
""".strip()

    yield f"""
COMMENT ON FUNCTION {refresh_function} IS {SqlString(f'Refresh {id}')}
    """.strip()


def create_setup_function(
    structure: Structure,
    id: str,
    target: JoinTarget,
):
    key_table = structure.key_table()
    refresh_constraint = structure.refresh_constraint()
    refresh_function = structure.refresh_function()
    refresh_table = structure.refresh_table()
    setup_function = structure.setup_function()

    key = [SqlId(column) for column in target.key]

    yield f"""
CREATE FUNCTION {setup_function} () RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
    IF to_regclass({SqlString(str(refresh_table))}) IS NOT NULL THEN
      RETURN;
    END IF;

    CREATE TEMP TABLE {key_table}
    AS SELECT {sql_list(key)}
    FROM {target.sql}
    WITH NO DATA;

    ALTER TABLE {key_table}
      ADD PRIMARY KEY ({sql_list(key)});

    CREATE TEMP TABLE {refresh_table} (
    ) ON COMMIT DELETE ROWS;

    CREATE CONSTRAINT TRIGGER {refresh_constraint} AFTER INSERT ON {refresh_table}
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE PROCEDURE {refresh_function}();
  END
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {setup_function} IS {SqlString(f"Set up temp tables for {id}")}
    """.strip()
