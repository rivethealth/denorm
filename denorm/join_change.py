import enum
import typing

from pg_sql import SqlObject, SqlString

from .join_common import Structure
from .join_key import KeyResolver
from .string import indent


class _ChangeType(enum.Enum):
    CHANGE_1 = enum.auto()
    CHANGE_2 = enum.auto()


def create_change(
    id: str,
    table: SqlObject,
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
            table_id=table_id,
        )

    delete_trigger = structure.delete_trigger(table_id)
    yield f"""
CREATE TRIGGER {delete_trigger} AFTER DELETE ON {table}
REFERENCING OLD TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_1_function}()
    """.strip()

    insert_trigger = structure.insert_trigger(table_id)
    yield f"""
CREATE TRIGGER {insert_trigger} AFTER INSERT ON {table}
REFERENCING NEW TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_1_function}()
    """.strip()

    update_trigger = structure.update_trigger(table_id)
    yield f"""
CREATE TRIGGER {update_trigger} AFTER UPDATE ON {table}
REFERENCING OLD TABLE AS _old NEW TABLE AS _new
FOR EACH STATEMENT EXECUTE PROCEDURE {change_2_function}()
    """.strip()


def _create_change_function(
    change_type: _ChangeType,
    function: SqlObject,
    id: str,
    resolver: KeyResolver,
    table_id: str,
):
    root = (
        str(SqlObject("_change"))
        if change_type == _ChangeType.CHANGE_1
        else "(TABLE _old UNION ALL TABLE _new)"
    )

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
