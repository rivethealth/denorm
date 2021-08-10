# Join config

_Configuration for joining tables._

## Properties

- **`consistency`** _(string)_: Consistency level. Must be one of:
  `['deferred', 'immediate']`. Default: `immedidate`.
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
  Default: `TABLE $1`.
- **`targetTable`**: Refer to _#/definitions/targetTable_.

## Definitions

- **`hook`** _(object)_: Hook function.
  - **`schema`** _(string)_: Schema of function.
  - **`name`** _(string)_: Name of function.
- **`column`**: Column.
  - **`name`** _(string)_: Name of column.
  - **`type`** _(string)_: Type of column.
- **`table`** _(object)_: Table.
  - **`lockId`** _(['integer', 'null'])_: Advisory lock space to use for
    queueing. If null, it is generated from the ID and table ID. Minimum:
    `-32678`. Maximum: `32677`. Default: `None`.
  - **`join`** _(['string', 'null'])_: IDs of dependency table. Default: `None`.
  - **`joinKey`** _(['array', 'null'])_: Unique key for foreign table. Default:
    `None`.
    - **Items** _(string)_
  - **`joinOn`** _(['string', 'null'])_: SQL expression to join to dependency.
    Default: `None`.
  - **`joinMode`** _(['string', 'null'])_: Mode of dependency join. Must be one
    of: `['async', 'sync']`. Default: `sync`.
  - **`key`** _(['array', 'null'])_: Unique key. Default: `None`.
    - **Items** _(string)_
  - **`keyType`** _(['array', 'null'])_: Key column types. Default: `None`.
    - **Items** _(string)_
  - **`name`** _(string)_: Name of table.
  - **`refreshFunction`** _(['boolean'])_: Default: `False`.
  - **`targetKey`** _(['array', 'null'])_: SQL expressions for target key
    values. Default: `None`.
    - **Items** _(string)_
- **`targetTable`**: Target table.
  - **`columns`** _(['array', 'null'])_: Columns. Default: `None`.
    - **Items** _(string)_
  - **`key`** _(['array', 'null'])_: Key columns. Default: `None`.
    - **Items** _(string)_
  - **`refresh`** _(string)_: Insert inserts. Full inserts, updates, and
    deletes. Insert only inserts. Upsert inserts and updates. Must be one of:
    `['full', 'insert', 'upsert']`. Default: `full`.
  - **`schema`** _(string)_: Schema of table. If null, the table is not
    schema-qualified. Default: `None`.
  - **`name`** _(string)_: Name of table.
