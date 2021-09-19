import enum
import typing

from pg_sql import SqlId, SqlObject, SqlString, sql_list

from .formats.join import JoinTable
from .join_common import Structure
from .join_key import KeyResolver
from .string import indent


class _ChangeType(enum.Enum):
    CHANGE_1 = enum.auto()
    CHANGE_2 = enum.auto()


def create_change(
    id: str,
    table: JoinTable,
    table_id: str,
    resolver: KeyResolver,
    structure: Structure,
):
    change_1_function = structure.change_1_function(table_id)
    change_2_function = structure.change_2_function(table_id)

    for change_type in (_ChangeType.CHANGE_1, _ChangeType.CHANGE_2):
        if change_type == _ChangeType.CHANGE_1:
            change_function = change_1_function
        elif change_type == _ChangeType.CHANGE_2:
            change_function = change_2_function

        yield from _create_change_function(
            change_type=change_type,
            function=change_function,
            id=id,
            resolver=resolver,
            table=table,
            table_id=table_id,
        )

    delete_trigger = structure.delete_trigger(table_id)
    yield f"""
CREATE TRIGGER {delete_trigger} AFTER DELETE ON {table.sql}
REFERENCING OLD TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_1_function}()
    """.strip()

    insert_trigger = structure.insert_trigger(table_id)
    yield f"""
CREATE TRIGGER {insert_trigger} AFTER INSERT ON {table.sql}
REFERENCING NEW TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_1_function}()
    """.strip()

    update_trigger = structure.update_trigger(table_id)
    yield f"""
CREATE TRIGGER {update_trigger} AFTER UPDATE ON {table.sql}
REFERENCING OLD TABLE AS _old NEW TABLE AS _new
FOR EACH STATEMENT EXECUTE PROCEDURE {change_2_function}()
    """.strip()


def _create_change_function(
    change_type: _ChangeType,
    function: SqlObject,
    id: str,
    resolver: KeyResolver,
    table_id: str,
    table: JoinTable,
):
    def query(name: SqlObject):
        if table.columns is None:
            return f"TABLE {name}"
        values = sql_list(
            column.sql if column.value is None else f"{column.value} AS {column.sql}"
            for column in table.columns
        )
        return f"SELECT {values} FROM {name}"

    if change_type == _ChangeType.CHANGE_1:
        change = SqlObject("_change")
        root = f"({query(change)})"
    elif change_type == _ChangeType.CHANGE_2:
        old = SqlObject("_old")
        new = SqlObject("_new")
        root = f"""
(
    ({query(old)} EXCEPT ALL {query(new)})
    UNION ALL
    ({query(new)} EXCEPT ALL {query(old)})
)
    """.strip()

    yield f"""
CREATE FUNCTION {function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
{indent(resolver.sql(root), 2)}

    RETURN NULL;
  END;
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {function} IS {SqlString(f'Handle changes to {table_id} for {id}')}
    """.strip()
