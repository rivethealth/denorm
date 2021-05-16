import dataclasses
import enum
import typing

import dataclasses_json
from pg_sql import SqlId, SqlObject

from ..json import DataJsonFormat, ValidatingDataJsonFormat, package_json_format


class AggConsistency(enum.Enum):
    DEFERRED = "deferred"
    IMMEDIATE = "immediate"


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class AggAggregate:
    value: str
    combine: typing.Optional[str] = None

    def combine_expression(self, name):
        return (
            self.combine
            if self.combine is not None
            else f"existing.{SqlId(name)} + excluded.{SqlId(name)}"
        )


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class AggTable:
    name: str
    schema: typing.Optional[str] = None

    @property
    def sql(self) -> SqlObject:
        return (
            SqlObject(SqlId(self.schema), SqlId(self.name))
            if self.schema is not None
            else SqlObject(SqlId(self.name))
        )


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class AggConfig:
    groups: typing.Dict[str, str]
    id: str
    aggregates: typing.Dict[str, AggAggregate]
    source: AggTable
    target: AggTable
    consistency: AggConsistency = AggConsistency.IMMEDIATE
    filter: typing.Optional[str] = None
    schema: typing.Optional[str] = None


AGG_JSON_FORMAT = package_json_format("denorm.formats", "agg.json")

AGG_DATA_JSON_FORMAT = DataJsonFormat(AGG_JSON_FORMAT, AggConfig.schema())
