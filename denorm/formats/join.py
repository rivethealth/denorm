import dataclasses
import enum
import typing

import dataclasses_json
from pg_sql import SqlId, SqlObject

from ..json import DataJsonFormat, ValidatingDataJsonFormat, package_json_format


class JoinConsistency(enum.Enum):
    DEFERRED = "deferred"
    IMMEDIATE = "immediate"


class JoinJoinMode(enum.Enum):
    ASYNC = "async"
    SYNC = "sync"


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
            SqlObject(SqlId(self.schema), SqlId(self.name))
            if self.schema is not None
            else SqlObject(SqlId(self.name))
        )


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass
class JoinTable:
    name: str
    join: typing.Optional[str] = None
    join_on: typing.Optional[str] = None
    join_mode: JoinJoinMode = JoinJoinMode.SYNC
    join_other: typing.Optional[str] = None
    key: typing.Optional[typing.List[str]] = None
    lock_id: typing.Optional[int] = None
    schema: typing.Optional[str] = None
    target_key: typing.Optional[typing.List[str]] = None

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(id(self))

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
@dataclasses.dataclass(frozen=True)
class JoinHook:
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
class JoinConfig:
    id: str
    tables: typing.Dict[str, JoinTable]
    target: JoinTarget
    consistency: JoinConsistency = JoinConsistency.IMMEDIATE
    query: typing.Optional[str] = None
    schema: typing.Optional[str] = None
    setup: typing.Optional[JoinHook] = None
    sync: JoinSync = JoinSync.FULL


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
