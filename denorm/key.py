"""
Procedures:
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
"""


import dataclasses
import json
import typing

import dataclasses_json
import jsonschema
import yaml

from .format import format
from .formats.key import KEY_DATA_JSON_FORMAT, Key, KeyHooks, KeyTable, KeyTarget
from .graph import recurse
from .resource import ResourceFactory
from .sql import SqlIdentifier, SqlLiteral, SqlObject, conflict_update
from .string import indent


@dataclasses.dataclass
class KeyIo:
    config: ResourceFactory[typing.TextIO]
    output: ResourceFactory[typing.TextIO]


def create_key(io: KeyIo):
    schema = KEY_DATA_JSON_FORMAT.load(io.config)

    with io.output() as f:
        for statement in _statements(schema):
            print(f"{statement};\n", file=f)


def _statements(config: Key):
    table_by_id = {table.id: table for table in config.tables}
    for table in config.tables:
        change_function = config.sql_object(f"{config.id}__chg__{table.id}")

        dep_ids = recurse(table.id, lambda id: table_by_id[id].dep)
        deps = [table_by_id[id] for id in dep_ids]

        yield from _create_change_function(
            hooks=config.hooks,
            deps=deps,
            function=change_function,
            id=config.id,
            table=table,
            target=config.target,
        )

        yield from _create_change_triggers(
            table=table, change_function=change_function, id=config.id
        )


def _create_change_function(
    function: SqlObject,
    deps: typing.List[KeyTable],
    hooks: KeyHooks,
    id: str,
    table: KeyTable,
    target: KeyTarget,
):
    key_sql = ", ".join(str(SqlIdentifier(column)) for column in target.key)

    key_query = ""
    for dep in reversed(deps):
        table_sql = SqlObject("_change") if table.id == dep.id else dep.sql

        if dep.key is not None:
            dep_columns_sql = ", ".join(
                str(SqlObject(dep.id, column)) for column in dep.key
            )
            key_query += f"SELECT DISTINCT {dep_columns_sql}"
            key_query += f"\nFROM"
            key_query += f"\n  {table_sql} AS {SqlIdentifier(dep.id)}"
        else:
            key_query += (
                f"\n  JOIN {table_sql} AS {SqlIdentifier(dep.id)} ON {dep.join}"
            )

    if hooks.before is not None:
        before = f"PERFORM {hooks.before.sql}();"
    else:
        before = ""

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


def _create_change_triggers(table: KeyTable, change_function: SqlObject, id: str):
    delete_trigger = SqlIdentifier(f"{id}__del__{table.id}")
    insert_trigger = SqlIdentifier(f"{id}__ins__{table.id}")
    update_old_trigger = SqlIdentifier(f"{id}__upd1__{table.id}")
    update_new_trigger = SqlIdentifier(f"{id}__upd2__{table.id}")

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


def _create_lock_table(table: SqlObject, target: KeyTarget):
    key_sql = ", ".join(str(SqlIdentifier(column)) for column in target.key)

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
