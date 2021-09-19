# Aggregate config

_Configuration for aggregating a table._

## Properties

- **`aggregates`**: Aggregates. Can contain additional properties.
- **`consistency`** _(string)_: Consistency. Must be one of:
  `['deferred', 'immediate']`. Default: `immediate`.
- **`filter`** _(['string', 'null'])_: Row filter. Default: `None`.
- **`groups`**: Can contain additional properties.
- **`id`** _(string)_: ID used to name-mangle.
- **`schema`** _(['string', 'null'])_: Schema for created objects. Default:
  `None`.
- **`source`**: Refer to _#/definitions/table_.
- **`target`**: Refer to _#/definitions/table_.

## Definitions

- **`aggregate`** _(object)_: Cannot contain additional properties.
  - **`combine`** _(['string', 'null'])_: Combining expression. If null,
    defaults to existing.$name + excluding.$name. Default: `None`.
  - **`value`** _(string)_
- **`group`** _(string)_: Group expression.
- **`table`**: Table.
  - **`name`** _(string)_: Name.
  - **`schema`** _(['string', 'null'])_: Schema. Default: `None`.
