# Join config

*Configuration for joining tables.*

## Properties

- **`consistency`** *(string)*: Consistency level. Must be one of: `['deferred', 'immediate']`. Default: `immedidate`.
- **`id`** *(string)*: ID for name mangling created objects.
- **`lock`** *(boolean)*: Whether to lock before refreshing target. Default: `False`.
- **`key`** *(['array', 'null'])*: Key. If null, uses values from targetTable. Default: `None`.
- **`setup`**: Setup function. Refer to *#/definitions/hook*. Default: `None`.
- **`schema`** *(string)*: Schema for created objects. If not set, the default schema is used. Default: `None`.
- **`tables`** *(object)*: Map from ID to table. Can contain additional properties.
- **`targetQuery`** *(['string', 'null'])*: Query to populate the target table. Default: `TABLE $1`.
- **`targetTable`**: Refer to *#/definitions/targetTable*.
## Definitions

- **`hook`** *(object)*: Hook function.
  - **`schema`** *(string)*: Schema of function.
  - **`name`** *(string)*: Name of function.
- **`column`**: Column.
  - **`name`** *(string)*: Name of column.
  - **`type`** *(string)*: Type of column.
- **`table`** *(object)*: Table.
  - **`lockId`** *(['integer', 'null'])*: Advisory lock space to use for queueing. If null, it is generated from the ID and table ID. Minimum: `-32678`. Maximum: `32677`. Default: `None`.
  - **`join`** *(['string', 'null'])*: IDs of dependency table. Default: `None`.
  - **`joinKey`** *(['array', 'null'])*: Unique key for foreign table. Default: `None`.
    - **Items** *(string)*
  - **`joinOther`** *(['string', 'null'])*: Expressions to add to join. Default: `None`.
  - **`joinOn`** *(['string', 'null'])*: SQL expression to join to dependency. Default: `None`.
  - **`joinMode`** *(['string', 'null'])*: Mode of dependency join. Must be one of: `['async', 'sync']`. Default: `sync`.
  - **`key`** *(['array', 'null'])*: Unique key. Default: `None`.
    - **Items** *(string)*
  - **`keyType`** *(['array', 'null'])*: Key column types. Default: `None`.
    - **Items** *(string)*
  - **`name`** *(string)*: Name of table.
  - **`refreshFunction`** *(['boolean'])*: Default: `False`.
  - **`targetKey`** *(['array', 'null'])*: SQL expressions for target key values. Default: `None`.
    - **Items** *(string)*
- **`targetTable`**: Target table.
  - **`columns`** *(['array', 'null'])*: Columns. Default: `None`.
    - **Items** *(string)*
  - **`key`** *(['array', 'null'])*: Key columns. Default: `None`.
    - **Items** *(string)*
  - **`refresh`** *(string)*: Insert inserts. Full inserts, updates, and deletes. Insert only inserts. Upsert inserts and updates. Must be one of: `['full', 'insert', 'upsert']`. Default: `full`.
  - **`schema`** *(string)*: Schema of table. If null, the table is not schema-qualified. Default: `None`.
  - **`name`** *(string)*: Name of table.
