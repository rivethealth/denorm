import dataclasses
import typing

from .agg_change import create_change
from .agg_clean import create_cleanup
from .agg_common import AggStructure
from .agg_defer import create_refresh_function, create_setup_function
from .formats.agg import AGG_DATA_JSON_FORMAT, AggAggregate, AggConfig, AggConsistency
from .resource import ResourceFactory


@dataclasses.dataclass
class AggIo:
    config: ResourceFactory[typing.TextIO]
    output: ResourceFactory[typing.TextIO]


def create_agg(io: AggIo):
    schema = AGG_DATA_JSON_FORMAT.load(io.config)

    with io.output() as f:
        for statement in _statements(schema):
            print(f"{statement};\n", file=f)


def _statements(config: AggConfig):
    structure = AggStructure(config.schema, config.id)

    config.aggregates["_count"] = AggAggregate(value="sign * count(*)")

    if config.consistency == AggConsistency.DEFERRED:
        yield from create_refresh_function(
            aggregates=config.aggregates,
            groups=config.groups,
            id=config.id,
            structure=structure,
            target=config.target,
        )

        yield from create_setup_function(
            aggregates=config.aggregates,
            groups=config.groups,
            id=config.id,
            structure=structure,
            target=config.target,
        )

    yield from create_change(
        aggregates=config.aggregates,
        consistency=config.consistency,
        filter=config.filter,
        groups=config.groups,
        id=config.id,
        source=config.source,
        structure=structure,
        target=config.target,
    )

    yield from create_cleanup(
        id=config.id, groups=config.groups, structure=structure, target=config.target
    )
