import dataclasses
import enum
import typing

import dataclasses_json

from ..json import DataJsonFormat, ValidatingDataJsonFormat, package_json_format
from ..sql import SqlObject


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class KeyTarget:
    key: typing.List[str]
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
@dataclasses.dataclass
class KeyTable:
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
class KeyHook:
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
class KeyHooks:
    before: typing.Optional[KeyHook] = None


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class Key:
    id: str
    tables: typing.List[KeyTable]
    target: KeyTarget
    hooks: KeyHooks = KeyHooks(before=None)
    schema: typing.Optional[str] = None

    def sql_object(self, name):
        return (
            SqlObject(self.schema, name) if self.schema is not None else SqlObject(name)
        )


class KeyInvalid(Exception):
    def __init__(self, message):
        super().__init__(message)


KEY_JSON_FORMAT = package_json_format("denorm.formats", "key.json")

KEY_DATA_JSON_FORMAT = DataJsonFormat(KEY_JSON_FORMAT, Key.schema())
