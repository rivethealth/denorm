# Join config

_Configuration for joining tables._

## Properties

- **`consistency`** _(string)_: Consistency level. Immediate applies at the end
  of the command. Deferred applies at the end of a transaction. Must be one of:
  `['deferred', 'immediate']`. Default: `immedidate`.
- **`context`** _(array)_: PostgreSQL settings to propogate through async joins.
  Default: `[]`.
  - **Items** _(string)_
- **`id`** _(string)_: ID for name mangling created objects.
- **`lock`** _(boolean)_: Whether to lock before refreshing target. Default:
  `False`.
- **`key`** _(['array', 'null'])_: Key. If null, uses values from targetTable.
  Default: `None`.
- **`setup`**: Setup function. Refer to _#/definitions/hook_. Default: `None`.
- **`schema`** _(string)_: Schema for created objects. If not set, the default
  schema is used. Default: `None`.
- **`tables`** _(object)_: Map from ID to table. Can contain additional
  properties.
- **`targetQuery`** _(['string', 'null'])_: Query to populate the target table.
  Default: `TABLE ${key}`.
- **`targetTable`**: Refer to _#/definitions/targetTable_.

## Definitions

- **`column`**: Column.
  - **`name`** _(string)_: Name of column.
  - **`value`** _(['null', 'string'])_: SQL expression. If null, the column
    value is used. Default: `None`.
- **`keyColumn`**: Column.
  - **`name`** _(string)_: Name of column.
  - **`type`** _(['null', 'string'])_: Type of column. Default: `None`.
  - **`value`** _(['null', 'string'])_: SQL expression. If null, the column
    value is used. Default: `None`.
- **`table`** _(object)_: Source table. Cannot contain additional properties.
  - **`columns`** _(['null', 'array'])_: Columns to select from table. Default:
    `None`.
    - **Items**: Refer to _#/definitions/column_.
  - **`lockId`** _(['integer', 'null'])_: Advisory lock space to use for
    queueing. If null, it is generated from the ID and table ID. Minimum:
    `-32678`. Maximum: `32677`. Default: `None`.
  - **`join`** _(['string', 'null'])_: IDs of dependency table. Default: `None`.
  - **`joinKey`** _(['array', 'null'])_: Unique key for foreign table. Default:
    `None`.
    - **Items** _(string)_
  - **`joinMode`** _(['string', 'null'])_: Mode of dependency join. Must be one
    of: `['async', 'sync']`. Default: `sync`.
  - **`joinOther`** _(['string', 'null'])_: Expressions to add to join. Default:
    `None`.
  - **`joinOn`** _(['string', 'null'])_: SQL expression to join to dependency.
    Default: `None`.
  - **`key`** _(['array', 'null'])_: Unique key. Default: `None`.
    - **Items**: Refer to _#/definitions/keyColumn_.
  - **`name`** _(string)_: Name of table.
  - **`refreshFunction`** _(boolean)_: Whether to generate a refresh function.
    Default: `False`.
  - **`schema`** _(string)_: Name of schema. Default: `None`.
  - **`targetKey`** _(['array', 'null'])_: SQL expressions for target key
    values. Default: `None`.
    - **Items** _(string)_
- **`targetTable`**: Target table.
  - **`columns`** _(['array', 'null'])_: Columns. Default: `None`.
    - **Items** _(string)_
  - **`key`** _(['array', 'null'])_: Key columns. Default: `None`.
    - **Items** _(string)_
  - **`schema`** _(string)_: Schema of table. If null, the table is not
    schema-qualified. Default: `None`.
  - **`name`** _(string)_: Name of table.
