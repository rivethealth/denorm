import dataclasses
import dataclasses_json
import itertools
import json
import jsonschema
import psycopg2
import typing
import yaml
from .db import columns, conflict_update, sql_ident, sql_str, primary_key, ObjectRef
from .format import format
from .graph import recurse
from .string import indent

try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class SourceTableConfig:
    table: str
    columns: typing.Tuple[str]
    schema: typing.Optional[str] = None


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class TargetTableConfig:
    table: str
    schema: typing.Optional[str] = None


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class SourceConfig:
    id: str
    tables: typing.Tuple[SourceTableConfig]
    expression: str
    dep: typing.Optional[str] = None
    identity: typing.Optional[str] = None
    join: typing.Optional[str] = None

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return hash(id(self))


@dataclasses_json.dataclass_json
@dataclasses.dataclass(frozen=True)
class DenormConfig:
    columns: typing.Tuple[str]
    id: str
    query: str
    sources: typing.Tuple[SourceConfig]
    target: TargetTableConfig
    schema: typing.Optional[str] = None


"""
Procedures:
* target__refresh - Perform refresh
* target__setup - Create the temporary tables
* target__chg__base (multiple) - Record changes

Temp tables:
* target__refresh - Fire constraint trigger at end of transaction
  - Triggers:
      * denorm - Invoke target__refresh() to perform refresh
* target__chg__base (multiple) - Changes to base

Tables:
* base (existing, multiple) - Tables to watch
  - Triggers
    * target - Record changes
* target (existing) - Table to populate
* target__lock - Value lock on primary key
* target__iterate__base (multiple) - Queue changes for iteration
"""


def cli(args):
    with pkg_resources.open_text("denorm", "schema.json") as f:
        json_schema = json.load(f)

    with open(args.input, "r") as f:
        json_input = yaml.safe_load(f)

    jsonschema.validate(json_input, json_schema)

    schema = DenormConfig.from_dict(json_input, infer_missing=None)

    with psycopg2.connect("") as conn:
        with conn.cursor() as cur:
            for statement in _result(schema, cur):
                print(f"{statement};\n")


def _result(config: DenormConfig, cur):
    target_key = primary_key(cur, ObjectRef(config.target.schema, config.target.table))
    target_key_names = set(column.name for column in target_key)

    target_columns_sql = ", ".join(sql_ident(column) for column in config.columns)
    target_keys_sql = ", ".join(sql_ident(column.name) for column in target_key)
    target_keys_defs_sql = ",\n".join(
        f"{column.name} {column.type}" for column in target_key
    )
    target_l_keys_sql = ", ".join(
        f"l.{sql_ident(column.name)}" for column in target_key
    )
    target_t_keys_sql = ", ".join(
        f"t.{sql_ident(column.name)}" for column in target_key
    )
    target_data_conflict_sql = conflict_update(
        [column for column in config.columns if column not in target_key_names]
    )

    lock_table_name = f"{config.id}__lock"
    lock_table = ObjectRef(config.schema, lock_table_name)

    target_table_name = config.target.table

    source_tables = set(table for source in config.sources for table in source.tables)
    source_by_id = {source.id: source for source in config.sources}

    yield f"""
CREATE UNLOGGED TABLE {sql_ident(lock_table_name)} (
{indent(target_keys_defs_sql, 1)},
  PRIMARY KEY ({target_keys_sql})
)
""".strip()

    yield f"""
COMMENT ON TABLE {sql_ident(lock_table_name)} IS {sql_str(f"Value lock on {target_table_name} primary key")}
""".strip()

    setup_function_name = f"{config.id}__setup"
    setup_function = ObjectRef(config.schema, setup_function_name)

    refresh_table_name = f"{config.id}__refresh"

    refresh_function_name = f"{config.id}__refresh"
    refresh_function = ObjectRef(config.schema, refresh_function_name)

    ids_sqls = []
    for source in config.sources:

        sources = recurse(
            source,
            lambda source: source_by_id[source.dep]
            if source.dep is not None
            else source.dep,
        )
        sources.reverse()

        join = ""

        for dep in sources[:-1]:
            expression = format(
                dep.expression,
                *(ObjectRef(table.schema, table.table).sql for table in dep.tables),
            )
            join += (
                expression if dep.join is None else f"\nJOIN {expression} ON {dep.join}"
            )

        def changed_sql(table):
            change_table_name = f"{config.id}__chg__{table.schema or ''}_{table.table}"
            return f"pg_temp.{sql_ident(change_table_name)}"

        table_choices = (
            [
                ObjectRef(table.schema, table.table).sql,
                changed_sql(table),
            ]
            for table in source.tables
        )
        for i, tables in enumerate(itertools.product(*table_choices)):
            if not i:
                continue

            join2 = join

            expression = format(
                source.expression,
                *tables,
            )
            join2 += (
                expression
                if source.join is None
                else f"\nJOIN {expression} ON {source.join}"
            )

            ids_sqls.append(
                f"""
SELECT {sources[0].identity}
FROM
{indent(join2, 1)}
""".strip()
            )

    ids_sql = "\nUNION\n".join(ids_sqls)

    change_sql = ""
    delete_change_sql = ""

    for table in source_tables:
        change_table = f"{config.id}__chg__{table.schema or ''}_{table.table}"

        change_function_name = f"{config.id}__chg__{table.schema or ''}_{table.table}"
        change_function = ObjectRef(config.schema, change_function_name)

        delete_trigger = f"{config.id}__delete"
        insert_trigger = f"{config.id}__insert"
        update_old_trigger = f"{config.id}__update1"
        update_new_trigger = f"{config.id}__update2"

        table_columns = [
            column
            for column in columns(cur, ObjectRef(table.schema, table.table))
            if column.name in table.columns
        ]
        column_defs_sql = ",\n".join(
            f"{column.name} {column.type}" for column in table_columns
        )
        column_names_sql = ",".join(sql_ident(column.name) for column in table_columns)

        change_sql += f"""
CREATE TEMP TABLE {sql_ident(change_table)} (
{indent(column_defs_sql, 1)}
) ON COMMIT DELETE ROWS
""".strip()
        change_sql += ";\n"

        yield f"""
CREATE FUNCTION {change_function.sql}() RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
    PERFORM {setup_function.sql}();

    INSERT INTO pg_temp.{sql_ident(change_table)}({column_names_sql})
    SELECT {column_names_sql}
    FROM _change;

    INSERT INTO pg_temp.{sql_ident(refresh_table_name)}
    SELECT
    WHERE NOT EXISTS (TABLE pg_temp.{sql_ident(refresh_table_name)});

    RETURN NULL;
  END;
$$
""".strip()

        yield f"""
CREATE TRIGGER {sql_ident(delete_trigger)} AFTER DELETE ON {ObjectRef(table.schema, table.table).sql}
REFERENCING OLD TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function.sql}()
""".strip()

        yield f"""
CREATE TRIGGER {sql_ident(insert_trigger)} AFTER INSERT ON {ObjectRef(table.schema, table.table).sql}
REFERENCING NEW TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function.sql}()
""".strip()

        yield f"""
CREATE TRIGGER {sql_ident(update_old_trigger)} AFTER UPDATE ON {ObjectRef(table.schema, table.table).sql}
REFERENCING OLD TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function.sql}()
""".strip()

        yield f"""
CREATE TRIGGER {sql_ident(update_new_trigger)} AFTER UPDATE ON {ObjectRef(table.schema, table.table).sql}
REFERENCING NEW TABLE AS _change
FOR EACH STATEMENT EXECUTE PROCEDURE {change_function.sql}()
""".strip()

        delete_change_sql += f"""
DELETE FROM {change_table}
""".strip()
        delete_change_sql += ";\n"

    change_sql = change_sql.strip()
    delete_change_sql = delete_change_sql.strip()

    yield f"""
CREATE FUNCTION {ObjectRef(config.schema, setup_function_name).sql}() RETURNS void
LANGUAGE plpgsql AS $$
  BEGIN
    IF to_regclass({sql_str(f"pg_temp.{refresh_table_name}")}) IS NOT NULL THEN
      RETURN;
    END IF;

    CREATE TEMP TABLE {sql_ident(refresh_table_name)} (
    ) ON COMMIT DELETE ROWS;

    CREATE CONSTRAINT TRIGGER denorm AFTER INSERT ON pg_temp.{sql_ident(refresh_table_name)}
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE PROCEDURE {refresh_function.sql}();

{indent(change_sql, 2)}
  END
$$
""".strip()

    yield f"""
COMMENT ON FUNCTION {setup_function.sql} IS {sql_str(f"Set up temp tables for {target_table_name}")}
""".strip()

    lock_conflict_update = conflict_update([column.name for column in target_key])

    yield f"""
CREATE FUNCTION {refresh_function.sql}() RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
    -- lock keys
    INSERT INTO {lock_table.sql} ({target_keys_sql})
{indent(ids_sql, 2)}
    ON CONFLICT ({target_keys_sql}) DO UPDATE
      SET {lock_conflict_update}
    WHERE false;

    -- update
    WITH
      _upsert AS (
        INSERT INTO {target_table_name} ({target_columns_sql})
{indent(format(config.query, lock_table.sql), 4)}
        ON CONFLICT ({target_keys_sql}) DO UPDATE
          SET {target_data_conflict_sql}
        RETURNING {target_keys_sql}
      )
    DELETE FROM {target_table_name} AS t
    USING {lock_table.sql} AS l
    WHERE
      ({target_t_keys_sql}) = ({target_l_keys_sql})
      AND NOT EXISTS (
        SELECT
        FROM _upsert
        WHERE ({target_keys_sql}) = ({target_t_keys_sql})
      );

    -- clear changes
{indent(delete_change_sql, 2)}

    -- clear locks
    DELETE FROM {lock_table.sql};

    -- clear refresh status
    DELETE FROM pg_temp.{refresh_table_name};

    RETURN NULL;
  END;
$$
""".strip()

    yield f"""
COMMENT ON FUNCTION {refresh_function.sql} IS {sql_str(f'Refresh {config.target.table}')}
""".strip()
