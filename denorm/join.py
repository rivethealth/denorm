"""
Procedures:
* ID__refresh - Perform refresh
  - When consistency is deferred
* ID__setup - Create the temporary tables
  - When consistency is deferred
* ID__chg__SOURCE - Process changes

Tables:
* BASE (existing) - Table to watch
  - Existing
  - Triggers
    * ID__del__SOURCE - Record deletes
    * ID__ins__SOURCE - Record inserts
    * ID__upd1__SOURCE - Record updates
    * ID__upd2__SOURCE - Record updates
* TARGET (existing) - Table to populate
* ID__iterate__SOURCE - Queue changes for iteration
  - When iteration is used
* ID__lock - Value lock

Temp tables:
* ID__key - Keys to update
  - When consistency is deferred
* ID__refresh - Fire constraint trigger at end of transaction
  - When consistency is deferred
  - Triggers:
      * join (deferred) - Perform refresh
"""


import dataclasses
import json
import typing

import dataclasses_json
import jsonschema
import yaml

from .format import format
from .formats.join import (
    JOIN_DATA_JSON_FORMAT,
    JoinConfig,
    JoinConsistency,
    JoinHook,
    JoinSync,
    JoinTable,
    JoinTarget,
)
from .graph import recurse
from .resource import ResourceFactory
from .sql import SqlIdentifier, SqlList, SqlLiteral, SqlObject, conflict_update
from .string import indent


@dataclasses.dataclass
class JoinIo:
    config: ResourceFactory[typing.TextIO]
    output: ResourceFactory[typing.TextIO]


def create_join(io: JoinIo):
    schema = JOIN_DATA_JSON_FORMAT.load(io.config)

    with io.output() as f:
        for statement in _statements(schema):
            print(f"{statement};\n", file=f)


def _statements(config: JoinConfig):
    lock_table = config.sql_object(f"{config.id}__lock")

    yield from _create_lock_table(table=lock_table, target=config.target)

    if config.consistency == JoinConsistency.DEFERRED:
        key_table = SqlObject(f"{config.id}__key")
        setup_function = config.sql_object(f"{config.id}__setup")
        refresh_table = SqlObject("pg_temp", f"{config.id}__refresh")

        refresh_function = config.sql_object(f"{config.id}__refresh")

        yield from _create_refresh_function(
            key_table=key_table,
            lock_table=lock_table,
            refresh_table=refresh_table,
            query=config.query,
            sync=config.sync,
            target=config.target,
            function=refresh_function,
        )

        yield from _create_setup_function(
            function=setup_function,
            id=config.id,
            key_table=key_table,
            refresh_function=refresh_function,
            refresh_table=refresh_table,
            target=config.target,
        )
    else:
        key_table = None
        setup_function = None
        refresh_table = None

    for id, table in config.tables.items():
        change_function = config.sql_object(f"{config.id}__chg__{id}")

        dep_ids = recurse(id, lambda id: config.tables[id].dep)
        deps = [(id, config.tables[id]) for id in dep_ids]

        yield from _create_change_function(
            consistency=config.consistency,
            setup=config.setup,
            deps=deps,
            function=change_function,
            id=config.id,
            key_table=key_table,
            lock_table=lock_table,
            query=config.query,
            refresh_table=refresh_table,
            setup_function=setup_function,
            sync=config.sync,
            table=table,
            target=config.target,
        )

        yield from _create_change_triggers(
            table_id=id, table=table, change_function=change_function, id=config.id
        )


def _create_change_function(
    function: SqlObject,
    consistency: JoinConsistency,
    deps: typing.Tuple[str, typing.List[JoinTable]],
    id: str,
    key_table: typing.Optional[SqlObject],
    lock_table: typing.Optional[SqlObject],
    query: typing.Optional[str],
    refresh_table: typing.Optional[SqlObject],
    setup: typing.Optional[JoinHook],
    setup_function: typing.Optional[SqlObject],
    sync: JoinSync,
    table: JoinTable,
    target: JoinTarget,
):
    key_sql = SqlList([SqlIdentifier(column) for column in target.key])

    key_query = ""
    for i, (dep_id, dep) in enumerate(reversed(deps)):
        table_sql = SqlObject("_change") if i == len(deps) - 1 else dep.sql

        if dep.target_key is not None:
            dep_columns_sql = SqlList(
                [SqlObject(dep_id, column) for column in dep.target_key]
            )
            key_query += f"SELECT DISTINCT {dep_columns_sql}"
            key_query += f"\nFROM"
            key_query += f"\n  {table_sql} AS {SqlIdentifier(dep_id)}"
        else:
            key_query += (
                f"\n  JOIN {table_sql} AS {SqlIdentifier(dep_id)} ON {dep.dep_join}"
            )

    if setup is not None:
        before = f"PERFORM {setup.sql}();"
    else:
        before = ""

    if consistency == JoinConsistency.DEFERRED:
        yield f"""
CREATE FUNCTION {function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
{indent(before, 2)}
    PERFORM {setup_function}();

    INSERT INTO {key_table} ({key_sql})
{indent(key_query, 2)}
    ON CONFLICT ({key_sql}) DO NOTHING;

    INSERT INTO {refresh_table}
    SELECT
    WHERE NOT EXISTS (TABLE {refresh_table});

    RETURN NULL;
  END;
$$
""".strip()
    elif query is not None:
        update_sql = _update_sql(
            sync=sync, target=target, lock_table=lock_table, query=query
        )

        yield f"""
CREATE FUNCTION {function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
{indent(before, 2)}

    -- lock keys
    INSERT INTO {lock_table} ({key_sql})
{indent(key_query, 2)}
    ORDER BY {key_sql}
    ON CONFLICT ({key_sql}) DO UPDATE
        SET {conflict_update(target.key)}
        WHERE false;

    -- update
{indent(update_sql, 1)}

    -- clear locks
    DELETE FROM {lock_table};

    RETURN NULL;
  END;
$$
""".strip()
    else:
        yield f"""
CREATE FUNCTION {function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
{indent(before, 2)}
    INSERT INTO {target.sql} ({key_sql})
{indent(key_query, 2)}
    ORDER BY {key_sql}
    ON CONFLICT ({key_sql}) DO UPDATE
        SET {conflict_update(target.key)}
        WHERE false;

    RETURN NULL;
  END;
$$
""".strip()


def _create_change_triggers(
    table_id: str, table: JoinTable, change_function: SqlObject, id: str
):
    delete_trigger = SqlIdentifier(f"{id}__del__{table_id}")
    insert_trigger = SqlIdentifier(f"{id}__ins__{table_id}")
    update_old_trigger = SqlIdentifier(f"{id}__upd1__{table_id}")
    update_new_trigger = SqlIdentifier(f"{id}__upd2__{table_id}")

    yield f"""
CREATE TRIGGER {delete_trigger} AFTER DELETE ON {table.sql}
REFERENCING OLD TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}()
""".strip()

    yield f"""
CREATE TRIGGER {insert_trigger} AFTER INSERT ON {table.sql}
REFERENCING NEW TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}()
""".strip()

    yield f"""
CREATE TRIGGER {update_old_trigger} AFTER UPDATE ON {table.sql}
REFERENCING OLD TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}()
""".strip()

    yield f"""
CREATE TRIGGER {update_new_trigger} AFTER UPDATE ON {table.sql}
REFERENCING NEW TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function}()
""".strip()


def _create_lock_table(table: SqlObject, target: JoinTarget):
    key_sql = SqlList([SqlIdentifier(column) for column in target.key])

    yield f"""
CREATE UNLOGGED TABLE {table}
AS SELECT {key_sql}
FROM {target.sql}
WITH NO DATA
        """.strip()

    yield f"""
ALTER TABLE {table}
    ADD PRIMARY KEY ({key_sql})
        """.strip()

    yield f"""
COMMENT ON TABLE {table} IS {SqlLiteral(f"Value lock on {target.name} primary key")}
        """.strip()


def _create_refresh_function(
    function: SqlObject,
    key_table: SqlObject,
    lock_table: SqlObject,
    query: str,
    refresh_table: SqlObject,
    sync: JoinSync,
    target: JoinTarget,
):
    key_sql = SqlList([SqlIdentifier(column) for column in target.key])

    update_sql = _update_sql(
        sync=sync, target=target, lock_table=lock_table, query=query
    )

    yield f"""
CREATE FUNCTION {function} () RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- lock keys
    WITH
        _change AS (
            DELETE FROM {key_table}
            RETURNING *
        )
    INSERT INTO {lock_table} ({key_sql})
    SELECT *
    FROM _change
    ORDER BY {key_sql}
    ON CONFLICT ({key_sql}) DO UPDATE
        SET {conflict_update(target.key)}
        WHERE false;

    -- update
{indent(update_sql, 1)}

    -- clear locks
    DELETE FROM {lock_table};

    RETURN NULL;
END;
$$
""".strip()

    yield f"""
COMMENT ON FUNCTION {function} IS {SqlLiteral(f'Refresh {target.name}')}
    """.strip()


def _create_setup_function(
    function: SqlObject,
    id: str,
    key_table: SqlObject,
    refresh_table: SqlObject,
    refresh_function: SqlObject,
    target: JoinTarget,
):
    key_sql = SqlList([SqlIdentifier(column) for column in target.key])

    yield f"""
CREATE FUNCTION {function} () RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
    IF to_regclass({SqlLiteral(str(refresh_table))}) IS NOT NULL THEN
      RETURN;
    END IF;

    CREATE TEMP TABLE {key_table}
    AS SELECT {key_sql}
    FROM {target.sql}
    WITH NO DATA;

    ALTER TABLE {key_table}
    ADD PRIMARY KEY ({key_sql});

    CREATE TEMP TABLE {refresh_table} (
    ) ON COMMIT DELETE ROWS;

    CREATE CONSTRAINT TRIGGER {id} AFTER INSERT ON {refresh_table}
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE PROCEDURE {refresh_function}();
  END
$$
""".strip()

    yield f"""
COMMENT ON FUNCTION {function} IS {SqlLiteral(f"Set up temp tables for {id}")}
""".strip()


def _update_sql(target: JoinTarget, sync: JoinSync, lock_table: SqlObject, query: str):
    key_sql = SqlList([SqlIdentifier(column) for column in target.key])

    if sync == JoinSync.FULL:
        data_conflict_sql = conflict_update(
            [column for column in target.columns if column not in target.key]
        )

        target_columns_sql = SqlList(
            [
                SqlIdentifier(column)
                for column in (target.columns if target.columns else target.key)
            ]
        )

        l_keys = SqlList([SqlObject("l", column) for column in target.key])
        t_keys = SqlList([SqlObject("t", column) for column in target.key])

        return f"""
WITH
    _upsert AS (
        INSERT INTO {target.sql} ({target_columns_sql})
{indent(format(query, str(lock_table)), 3)}
        ON CONFLICT ({key_sql}) DO UPDATE
            SET {data_conflict_sql}
        RETURNING {key_sql}
    )
DELETE FROM {target.sql} AS t
USING {lock_table} AS l
WHERE
    ({t_keys}) = ({l_keys})
    AND NOT EXISTS (
        SELECT
        FROM _upsert
        WHERE ({key_sql}) = ({t_keys})
    );
""".strip()
    elif JoinSync.UPSERT:
        return f"""
INSERT INTO {target.sql} ({key_sql})
{indent(key_query, 2)}
    ORDER BY {key_sql}
    ON CONFLICT ({key_sql}) DO UPDATE
        SET {conflict_update(target.key)}
        WHERE false;
""".strip()
