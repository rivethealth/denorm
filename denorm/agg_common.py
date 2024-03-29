import typing

from pg_sql import SqlId, SqlObject


class AggStructure:
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

    def change1_function(self) -> SqlObject:
        return self._sql_object(self._name("change1"))

    def change2_function(self) -> SqlObject:
        return self._sql_object(self._name("change2"))

    def insert_trigger(self) -> SqlObject:
        return self._sql_object(self._name("ins"))

    def update_trigger(self) -> SqlObject:
        return self._sql_object(self._name("upd"))

    def delete_trigger(self) -> SqlObject:
        return self._sql_object(self._name("del"))

    def cleanup_function(self) -> SqlObject:
        return self._sql_object(self._name("cleanup"))

    def cleanup_trigger(self) -> SqlId:
        return self._name("cleanup")

    def compress_function(self) -> SqlObject:
        return self._sql_object(self._name("compress"))

    def refresh_constraint(self) -> SqlId:
        return SqlId(self._id)

    def refresh_function(self) -> SqlObject:
        return self._sql_object(self._name("refresh"))

    def refresh_table(self) -> SqlObject:
        return SqlObject(SqlId("pg_temp"), self._name("refresh"))

    def setup_function(self) -> SqlObject:
        return self._sql_object(self._name("setup"))

    def tmp_table(self) -> SqlObject:
        return SqlObject(SqlId("pg_temp"), self._name("tmp"))
