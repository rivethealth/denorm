import dataclasses
import enum
import typing

import dataclasses_json

from ..json import DataJsonFormat, ValidatingDataJsonFormat, package_json_format
from ..sql import SqlObject


class DenormConsistency(enum.Enum):
    DEFERRED = "deferred"
    IMMEDIATE = "immediate"


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class DenormTarget:
    key: typing.List[str]
    name: str
    schema: typing.Optional[str] = None
    columns: typing.Optional[typing.List[str]] = None

    @property
    def sql(self) -> SqlObject:
        return (
            SqlObject(self.schema, self.name)
            if self.schema is not None
            else SqlObject(self.name)
        )


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class DenormTable:
    id: str
    name: str
    dep: typing.Optional[str] = None
    key: typing.Optional[typing.List[str]] = None
    join: typing.Optional[str] = None
    schema: typing.Optional[str] = None

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(id(self))

    @property
    def sql(self) -> SqlObject:
        return (
            SqlObject(self.schema, self.name)
            if self.schema is not None
            else SqlObject(self.name)
        )


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class DenormHook:
    name: str
    schema: typing.Optional[str] = None

    @property
    def sql(self) -> SqlObject:
        return (
            SqlObject(self.schema, name) if self.schema is not None else SqlObject(name)
        )


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class DenormHooks:
    before: typing.Optional[DenormHook] = None


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class Denorm:
    id: str
    query: str
    tables: typing.List[DenormTable]
    target: DenormTarget
    consistency: DenormConsistency = DenormConsistency.IMMEDIATE
    hooks: DenormHooks = DenormHooks(before=None)
    schema: typing.Optional[str] = None

    def sql_object(self, name):
        return (
            SqlObject(self.schema, name) if self.schema is not None else SqlObject(name)
        )


class DenormInvalid(Exception):
    def __init__(self, message):
        super().__init__(message)


def validate_denorm(denorm: Denorm):
    if denorm.consistency == DenormConsistency.DEFERRED and denorm.query is None:
        raise DenormInvalid("Deferrable mode is only used with query")

    if denorm.target.columns is None and denorm.query is not None:
        raise DenormInvalid("Query requires target column list")


DENORM_JSON_FORMAT = package_json_format("denorm.formats", "denorm.json")

DENORM_DATA_JSON_FORMAT = ValidatingDataJsonFormat(
    DataJsonFormat(DENORM_JSON_FORMAT, Denorm.schema()),
    validate_denorm,
)
