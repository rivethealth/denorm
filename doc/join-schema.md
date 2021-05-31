# Join config

_Configuration for joining tables._

## Properties

- **`consistency`** _(string)_: Consistency level. Must be one of:
  `['deferred', 'immediate']`. Default: `immedidate`.
- **`id`** _(string)_: ID for name mangling created objects.
- **`query`** _(string)_: Query to populate the target table.
- **`setup`**: Setup function. Refer to _#/definitions/hook_. Default: `None`.
- **`schema`** _(string)_: Schema for created objects. If not set, the default
  schema is used. Default: `None`.
- **`sync`** _(string)_: Synchronization mode. Insert inserts. Upsert inserts
  and updates. Full inserts, updates, and deletes. Must be one of:
  `['full', 'insert', 'upsert']`. Default: `full`.
- **`tables`** _(object)_: Map from ID to table. Can contain additional
  properties.
- **`target`**: Refer to _#/definitions/target_.

## Definitions

- **`hook`** _(object)_: Hook function.
  - **`schema`** _(string)_: Schema of function.
  - **`name`** _(string)_: Name of function.
- **`table`** _(object)_: Table.
  - **`lockId`** _(['integer', 'null'])_: Advisory lock space to use for
    queueing. If null, it is generated from the ID and table ID. Minimum:
    `-32678`. Maximum: `32677`. Default: `None`.
  - **`join`** _(['string', 'null'])_: IDs of dependency table. Default: `None`.
  - **`joinOn`** _(['string', 'null'])_: SQL expression to join to dependency.
    Default: `None`.
  - **`joinMode`** _(['string', 'null'])_: Mode of dependency join. Must be one
    of: `['async', 'sync']`. Default: `sync`.
  - **`key`** _(array)_: Unique key. Default: `None`.
    - **Items** _(string)_
  - **`name`** _(string)_: Name of table.
  - **`targetKey`** _(['array', 'null'])_: SQL expressions for target key
    values. Default: `None`.
    - **Items** _(string)_
- **`target`**
  - **`columns`** _(['array', 'null'])_: Columns. Default: `None`.
    - **Items** _(string)_
  - **`key`** _(array)_: Key columns.
    - **Items** _(string)_
  - **`schema`** _(string)_: Schema of table. If null, the table is not
    schema-qualified. Default: `None`.
  - **`name`** _(string)_: Name of table.
