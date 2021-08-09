import dataclasses
import typing

from pg_sql import SqlId, SqlObject

from .sql import SqlQuery


@dataclasses.dataclass
class Key:
    definition: str
    names: typing.List[str]


class Structure:
    def __init__(self, schema: str, id: str):
        self._schema = schema
        self._id = id

    def _name(self, name: str):
        return SqlId(f"{self._id}__{name}")

    def _sql_object(self, name: SqlId):
        return (
            SqlObject(SqlId(self._schema), name)
            if self._schema is not None
            else SqlObject(name)
        )

    def change_1_function(self, table_id: str):
        return self._sql_object(self._name(f"chg1__{table_id}"))

    def change_2_function(self, table_id: str):
        return self._sql_object(self._name(f"chg2__{table_id}"))

    def delete_trigger(self, table_id: str) -> SqlId:
        return self._name(f"del__{table_id}")

    def insert_trigger(self, table_id: str) -> SqlId:
        return self._name(f"ins__{table_id}")

    def update_trigger(self, table_id: str) -> SqlId:
        return self._name(f"upd__{table_id}")

    def lock_table(self) -> SqlObject:
        return self._sql_object(self._name("lock"))

    def queue_table(self, table_id: str) -> SqlObject:
        return self._sql_object(self._name(f"que__{table_id}"))

    def queue_process_function(self, table_id: str) -> SqlObject:
        return self._sql_object(self._name(f"pcs__{table_id}"))

    def key_table(self) -> SqlObject:
        return SqlObject(SqlId("pg_temp"), self._name("key"))

    def refresh_constraint(self) -> SqlId:
        return SqlId(self._id)

    def refresh_function(self) -> SqlObject:
        return self._sql_object(self._name("refresh"))

    def refresh_table(self) -> SqlObject:
        return SqlObject(SqlId("pg_temp"), self._name("refresh"))

    def refresh_table_function(self, table_id: str) -> SqlId:
        return self._sql_object(self._name(f"rfs__{table_id}"))

    def setup_function(self) -> SqlObject:
        return self._sql_object(self._name("setup"))


def local_column(column: str) -> str:
    return SqlId(f"local_{column}")


def foreign_column(column: str) -> str:
    return SqlId(f"foreign_{column}")


class JoinTarget(typing.Protocol):
    def key(self) -> typing.Optional[Key]:
        pass

    def sql(self, key_table: SqlObject) -> SqlQuery:
        pass
