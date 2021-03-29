import dataclasses
import enum
import typing

import dataclasses_json

from ..json import DataJsonFormat, ValidatingDataJsonFormat, package_json_format
from ..sql import SqlObject


class JoinConsistency(enum.Enum):
    DEFERRED = "deferred"
    IMMEDIATE = "immediate"


class JoinSync(enum.Enum):
    FULL = "full"
    UPSERT = "upsert"


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class JoinTarget:
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
class JoinTable:
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
class JoinHook:
    name: str
    schema: typing.Optional[str] = None

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
class JoinHooks:
    before: typing.Optional[JoinHook] = None


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class JoinConfig:
    id: str
    query: str
    tables: typing.List[JoinTable]
    target: JoinTarget
    consistency: JoinConsistency = JoinConsistency.IMMEDIATE
    hooks: JoinHooks = JoinHooks(before=None)
    schema: typing.Optional[str] = None
    sync: JoinSync = JoinSync.FULL

    def sql_object(self, name):
        return (
            SqlObject(self.schema, name) if self.schema is not None else SqlObject(name)
        )


class JoinInvalid(Exception):
    def __init__(self, message):
        super().__init__(message)


def validate_join(join: JoinConfig):
    if join.consistency == JoinConsistency.DEFERRED and join.query is None:
        raise JoinInvalid("Deferrable mode is only used with query")

    if join.target.columns is None and join.query is not None:
        raise JoinInvalid("Query requires target column list")


JOIN_JSON_FORMAT = package_json_format("denorm.formats", "join.json")

JOIN_DATA_JSON_FORMAT = ValidatingDataJsonFormat(
    DataJsonFormat(JOIN_JSON_FORMAT, JoinConfig.schema()),
    validate_join,
)
