"""
Procedures:
* ID__refresh - Perform refresh
  - When consistency is deferred
* ID__setup - Create the temporary tables
  - When consistency is deferred
* ID__chg1__SOURCE - Process changes
* ID__chg2__SOURCE - Process changes

Tables:
* BASE (existing) - Table to watch
  - Existing
  - Triggers
    * ID__del__SOURCE - Record deletes
    * ID__ins__SOURCE - Record inserts
    * ID__upd__SOURCE - Record updates
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
import typing

from pg_sql import SqlId, sql_list

from .format import format
from .formats.join import (
    JOIN_DATA_JSON_FORMAT,
    JoinConfig,
    JoinConsistency,
    JoinJoinMode,
)
from .join_async import create_queue
from .join_change import create_change
from .join_common import JoinTarget, Key, Structure
from .join_defer import DeferredKeys, create_refresh_function, create_setup_function
from .join_key import KeyResolver, TargetRefresh
from .join_lock import create_lock_table
from .join_plain_target import JoinPlainTarget
from .join_refresh_function import (
    create_refresh_function as create_table_refresh_function,
)
from .join_table_target import JoinTableTarget
from .resource import ResourceFactory
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


def _target(config: JoinConfig) -> JoinTarget:
    if config.target_table:
        return JoinTableTarget(config.target_table, config.target_query)
    else:
        return JoinPlainTarget(config.target_query)


def _statements(config: JoinConfig):
    structure = Structure(config.schema, config.id)

    target = _target(config)
    key = target.key()
    if key is None:
        definition = f"SELECT {sql_list(f'NULL::{column.type} AS {column.sql}' for column in config.key)}"
        names = [column.name for column in config.key]
        key = Key(definition=definition, names=names)

    if config.lock:
        yield from create_lock_table(
            structure=structure, key=key, target=config.target_table
        )

    refresh_action = TargetRefresh(
        key=key.names,
        setup=config.setup,
        structure=structure,
        lock=config.lock,
        target=target,
    )

    if config.consistency == JoinConsistency.DEFERRED:
        yield from create_refresh_function(
            id=config.id,
            structure=structure,
            refresh=refresh_action,
        )

        yield from create_setup_function(
            structure=structure,
            id=config.id,
            target=config.target_table,
            key=key,
        )

    for table_id, table in config.tables.items():
        if table.join_mode != JoinJoinMode.ASYNC:
            continue

        resolver = KeyResolver(
            action=refresh_action,
            key=key.names,
            structure=structure,
            table_id=table.join,
            tables=config.tables,
        )

        yield from create_queue(
            id=config.id,
            resolver=resolver,
            structure=structure,
            table_id=table_id,
            tables=config.tables,
        )

        if table.refresh_function:
            yield from create_table_refresh_function(
                structure=structure,
                table=table,
                table_id=table_id,
            )

    for table_id, table in config.tables.items():
        if table.name is None:
            continue

        if config.consistency == JoinConsistency.DEFERRED:
            action = DeferredKeys(key=key.names, structure=structure)
        elif config.consistency == JoinConsistency.IMMEDIATE:
            action = refresh_action

        resolver = KeyResolver(
            action=action,
            key=key.names,
            structure=structure,
            table_id=table_id,
            tables=config.tables,
        )

        yield from create_change(
            id=config.id,
            resolver=resolver,
            structure=structure,
            table=table.sql,
            table_id=table_id,
        )
