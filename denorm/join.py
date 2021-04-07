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

from .format import format
from .formats.join import (
    JOIN_DATA_JSON_FORMAT,
    JoinConfig,
    JoinConsistency,
    JoinDepMode,
)
from .join_async import create_queue
from .join_change import create_change
from .join_common import Structure
from .join_defer import create_refresh_function, create_setup_function
from .join_query import ProcessQuery, create_lock_table
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


def _statements(config: JoinConfig):
    structure = Structure(config.schema, config.id)

    yield from create_lock_table(structure=structure, target=config.target)

    if config.consistency == JoinConsistency.DEFERRED:
        yield from create_refresh_function(
            id=config.id,
            structure=structure,
            query=config.query,
            sync=config.sync,
            target=config.target,
        )

        yield from create_setup_function(
            structure=structure,
            id=config.id,
            target=config.target,
        )

    for table_id, table in config.tables.items():
        if table.dep_mode != JoinDepMode.ASYNC:
            continue

        process_query = ProcessQuery(
            sync=config.sync,
            tables=config.tables,
            structure=structure,
            consistency=JoinConsistency.IMMEDIATE,
            target=config.target,
            setup=config.setup,
            table_id=table.dep,
            query=config.query,
        )

        yield from create_queue(
            id=config.id,
            process_query=process_query,
            structure=structure,
            table_id=table_id,
            tables=config.tables,
        )

    for table_id, table in config.tables.items():
        process_query = ProcessQuery(
            consistency=config.consistency,
            query=config.query,
            setup=config.setup,
            structure=structure,
            sync=config.sync,
            table_id=table_id,
            tables=config.tables,
            target=config.target,
        )

        yield from create_change(
            id=config.id,
            process_query=process_query,
            structure=structure,
            table=table.sql,
            table_id=table_id,
        )
