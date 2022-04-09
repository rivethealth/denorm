# Aggregate config

*Configuration for aggregating a table.*

## Properties

- **`aggregates`**: Aggregates. Can contain additional properties.
- **`consistency`** *(string)*: Consistency. Must be one of: `['deferred', 'immediate']`. Default: `immediate`.
- **`filter`** *(['string', 'null'])*: Row filter. Default: `None`.
- **`groups`**: Can contain additional properties.
- **`id`** *(string)*: ID used to name-mangle.
- **`schema`** *(['string', 'null'])*: Schema for created objects. Default: `None`.
- **`shard`** *(boolean)*: Whether the table is sharded. Default: `False`.
- **`source`**: Refer to *#/definitions/table*.
- **`target`**: Refer to *#/definitions/table*.
## Definitions

- **`aggregate`** *(object)*: Cannot contain additional properties.
  - **`combine`** *(['string', 'null'])*: Combining expression. If null, defaults to existing.$name + excluding.$name. Default: `None`.
  - **`value`** *(string)*
- **`group`** *(string)*: Group expression.
- **`table`**: Table.
  - **`name`** *(string)*: Name.
  - **`schema`** *(['string', 'null'])*: Schema. Default: `None`.
