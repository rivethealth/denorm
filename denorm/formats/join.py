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


class JoinRefresh(enum.Enum):
    INSERT = "insert"
    FULL = "full"
    UPSERT = "upsert"


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
)
@dataclasses.dataclass
class JoinColumn:
    name: str
    value: typing.Optional[str] = None

    @property
    def sql(self) -> SqlId:
        return SqlId(self.name)


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
)
@dataclasses.dataclass
class JoinKeyColumn:
    name: str
    type: typing.Optional[str] = None
    value: typing.Optional[str] = None

    @property
    def sql(self) -> SqlId:
        return SqlId(self.name)


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
)
@dataclasses.dataclass
class JoinTargetTable:
    name: str
    columns: typing.Optional[typing.List[str]] = None
    key: typing.Optional[typing.List[str]] = None
    schema: typing.Optional[str] = None
    refresh: JoinRefresh = JoinRefresh.FULL

    @property
    def sql(self) -> SqlObject:
        return (
            SqlObject(SqlId(self.schema), SqlId(self.name))
            if self.schema is not None
            else SqlObject(SqlId(self.name))
        )


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
)
@dataclasses.dataclass
class JoinTable:
    columns: typing.Optional[typing.List[JoinColumn]] = None
    name: typing.Optional[str] = None
    join: typing.Optional[str] = None
    join_key: typing.Optional[typing.List[str]] = None
    join_on: typing.Optional[str] = None
    join_mode: JoinJoinMode = JoinJoinMode.SYNC
    join_other: typing.Optional[str] = None
    key: typing.Optional[typing.List[JoinKeyColumn]] = None
    key_type: typing.Optional[typing.List[str]] = None
    refresh_function: bool = False
    lock_id: typing.Optional[int] = None
    schema: typing.Optional[str] = None
    target_key: typing.Optional[typing.List[str]] = None

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(id(self))

    @property
    def sql(self) -> SqlObject:
        if self.name is None:
            return "(SELECT false AS _)"

        return (
            SqlObject(SqlId(self.schema), SqlId(self.name))
            if self.schema is not None
            else SqlObject(SqlId(self.name))
        )


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
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
)
@dataclasses.dataclass
class JoinConfig:
    id: str
    tables: typing.Dict[str, JoinTable]
    consistency: JoinConsistency = JoinConsistency.IMMEDIATE
    context: typing.List[str] = dataclasses.field(default_factory=list)
    key: typing.Optional[typing.List[JoinKeyColumn]] = None
    lock: bool = False
    schema: typing.Optional[str] = None
    setup: typing.Optional[JoinHook] = None
    target_query: typing.Optional[str] = "TABLE ${key}"
    target_table: typing.Optional[JoinTargetTable] = None


class JoinInvalid(Exception):
    def __init__(self, message):
        super().__init__(message)


def validate_join(join: JoinConfig):
    for table_id, table in join.tables.items():
        if table.refresh_function and any(
            column.type is None for column in table.key or []
        ):
            raise JoinInvalid(
                f"Table {table_id} refreshFunction required types for columns"
            )


JOIN_JSON_FORMAT = package_json_format("denorm.formats", "join.json")

JOIN_DATA_JSON_FORMAT = ValidatingDataJsonFormat(
    DataJsonFormat(JOIN_JSON_FORMAT, JoinConfig.schema()),
    validate_join,
)
