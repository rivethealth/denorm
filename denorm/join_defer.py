import typing

from pg_sql import SqlId, SqlNumber, SqlObject, SqlString, sql_list

from .format import format
from .join_common import JoinTarget, Key, Structure
from .join_key import KeyConsumer, TargetRefresh
from .sql import SqlTableExpr
from .sql_query import sync_query, upsert_query
from .string import indent


def create_refresh_function(
    id: str,
    structure: Structure,
    refresh: TargetRefresh,
):
    refresh_function = structure.refresh_function()
    refresh_table = structure.refresh_table()

    key_table = structure.key_table()
    refresh_sql = refresh.sql(f"TABLE {key_table}")

    yield f"""
CREATE FUNCTION {refresh_function} () RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
    -- analyze
    ANALYZE {refresh_table};

    -- refresh
{indent(str(refresh_sql), 2)}

    -- clear refresh
    DELETE FROM {refresh_table};

    RETURN NULL;
  END;
$$
""".strip()

    yield f"""
COMMENT ON FUNCTION {refresh_function} IS {SqlString(f'Refresh {id}')}
    """.strip()


def create_setup_function(
    structure: Structure,
    id: str,
    key: Key,
    target: JoinTarget,
):
    key_table = structure.key_table()
    refresh_constraint = structure.refresh_constraint()
    refresh_function = structure.refresh_function()
    refresh_table = structure.refresh_table()
    setup_function = structure.setup_function()

    yield f"""
CREATE FUNCTION {setup_function} () RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
    IF to_regclass({SqlString(str(refresh_table))}) IS NOT NULL THEN
      RETURN;
    END IF;

    CREATE TEMP TABLE {key_table}
    ON COMMIT DELETE ROWS
    AS {key.definition}
    WITH NO DATA;

    ALTER TABLE {key_table}
      ADD PRIMARY KEY ({sql_list([SqlId(name) for name in key.names])});

    CREATE TEMP TABLE {refresh_table} (
    ) ON COMMIT DELETE ROWS;

    CREATE CONSTRAINT TRIGGER {refresh_constraint} AFTER INSERT ON {refresh_table}
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE PROCEDURE {refresh_function}();
  END
$$
    """.strip()

    yield f"""
COMMENT ON FUNCTION {setup_function} IS {SqlString(f"Set up temp tables for {id}")}
    """.strip()


class DeferredKeys(KeyConsumer):
    def __init__(self, key: typing.List[str], structure: Structure):
        self._key = key
        self._structure = structure

    def sql(
        self,
        key_query: str,
        exprs: typing.List[SqlTableExpr] = [],
        last_expr: typing.Optional[str] = None,
    ):
        setup_function = self._structure.setup_function()

        refresh_table = self._structure.refresh_table()

        query = upsert_query(
            columns=self._key,
            key=self._key,
            query=key_query,
            target=self._structure.key_table(),
        )
        for expr in reversed(exprs):
            query.prepend(expr)
        if last_expr is not None:
            query.append(SqlId("_other"), last_expr)

        return f"""
PERFORM {setup_function}();

{query};

INSERT INTO {refresh_table}
SELECT
WHERE NOT EXISTS (TABLE {refresh_table});
        """.strip()
