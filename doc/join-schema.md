# Join config

_Configuration for joining tables._

## Properties

- <a id="properties/consistency"></a>**`consistency`** _(string)_: Consistency
  level. Immediate applies at the end of the command. Deferred applies at the
  end of a transaction. Must be one of: "deferred" or "immediate". Default:
  `"immediate"`.
- <a id="properties/context"></a>**`context`** _(array)_: PostgreSQL settings to
  propogate through async joins. Default: `[]`.
  - <a id="properties/context/items"></a>**Items** _(string)_
- <a id="properties/id"></a>**`id`** _(string, required)_: ID for name mangling
  created objects.
- <a id="properties/lock"></a>**`lock`** _(boolean)_: Whether to lock before
  refreshing target. Default: `false`.
- <a id="properties/key"></a>**`key`** _(array or null)_: Key. If null, uses
  values from destinationTable. Default: `null`.
- <a id="properties/setup"></a>**`setup`**: Setup function. Refer to
  _[#/definitions/hook](#definitions/hook)_. Default: `null`.
- <a id="properties/schema"></a>**`schema`** _(string)_: Schema for created
  objects. If not set, the default schema is used. Default: `null`.
- <a id="properties/tables"></a>**`tables`** _(object, required)_: Map from ID
  to table. Can contain additional properties.
  - <a id="properties/tables/additionalProperties"></a>**Additional
    properties**: Refer to _[#/definitions/table](#definitions/table)_.
- <a id="properties/destinationQuery"></a>**`destinationQuery`** _(string or
  null)_: Query to populate the destination table columns. If no table is
  specified, destination query could still be a function call or something else.
  Default: `"TABLE ${key}"`.
- <a id="properties/destinationTable"></a>**`destinationTable`**: Refer to
  _[#/definitions/destinationTable](#definitions/destinationTable)_.

## Definitions

- <a id="definitions/column"></a>**`column`**: Column.
  - <a id="definitions/column/properties/name"></a>**`name`** _(string,
    required)_: Name of column.
  - <a id="definitions/column/properties/value"></a>**`value`** _(null or
    string)_: SQL expression. If null, the column value is used. Default:
    `null`.
- <a id="definitions/keyColumn"></a>**`keyColumn`**: Column.
  - <a id="definitions/keyColumn/properties/name"></a>**`name`** _(string,
    required)_: Name of column.
  - <a id="definitions/keyColumn/properties/type"></a>**`type`** _(null or
    string)_: Type of column. Default: `null`.
  - <a id="definitions/keyColumn/properties/value"></a>**`value`** _(null or
    string)_: SQL expression. If null, the column value is used. Default:
    `null`.
- <a id="definitions/table"></a>**`table`** _(object)_: Source table. Cannot
  contain additional properties.
  - **All of**
    - <a id="definitions/table/allOf/0"></a>
      - **One of**
    - <a id="definitions/table/allOf/1"></a>
      - **One of**
  - <a id="definitions/table/properties/lockId"></a>**`lockId`** _(integer or
    null)_: Advisory lock space to use for queueing. If null, it is generated
    from the ID and table ID. Minimum: `-32678`. Maximum: `32677`. Default:
    `null`.
  - <a id="definitions/table/properties/tableSchema"></a>**`tableSchema`**
    _(string)_: Name of schema. Default: `"None"`.
  - <a id="definitions/table/properties/tableName"></a>**`tableName`**
    _(string)_: Name of table.
  - <a id="definitions/table/properties/tableKey"></a>**`tableKey`** _(array or
    null)_: Unique key for the table. Default: `null`.
    - <a id="definitions/table/properties/tableKey/items"></a>**Items**: Refer
      to _[#/definitions/keyColumn](#definitions/keyColumn)_.
  - <a id="definitions/table/properties/tableColumns"></a>**`tableColumns`**
    _(null or array)_: Columns in the table that will be watched for changes. If
    null then all columns will be watched. Default: `null`.
    - <a id="definitions/table/properties/tableColumns/items"></a>**Items**:
      Refer to _[#/definitions/column](#definitions/column)_.
  - <a id="definitions/table/properties/joinTargetTable"></a>**`joinTargetTable`**
    _(string or null)_: IDs of dependency table. Default: `null`.
  - <a id="definitions/table/properties/joinTargetKey"></a>**`joinTargetKey`**
    _(array or null)_: Unique key for foreign table. Default: `null`.
    - <a id="definitions/table/properties/joinTargetKey/items"></a>**Items**
      _(string)_
  - <a id="definitions/table/properties/joinOn"></a>**`joinOn`** _(string or
    null)_: SQL expression to join to dependency. Default: `null`.
  - <a id="definitions/table/properties/joinMode"></a>**`joinMode`** _(string or
    null)_: Mode of dependency join. Large many to one should use 'async'. Must
    be one of: "async" or "sync". Default: `"sync"`.
  - <a id="definitions/table/properties/joinOther"></a>**`joinOther`** _(string
    or null)_: Expressions to add to join. Default: `null`.
  - <a id="definitions/table/properties/refreshFunction"></a>**`refreshFunction`**
    _(boolean)_: Whether to generate a refresh function. Default: `false`.
  - <a id="definitions/table/properties/destinationKeyExpr"></a>**`destinationKeyExpr`**
    _(array or null)_: SQL expressions for this table's columns that make up the
    destination table's key columns ([this table name].[column name]). Default:
    `null`.
    - <a id="definitions/table/properties/destinationKeyExpr/items"></a>**Items**
      _(string)_
- <a id="definitions/destinationTable"></a>**`destinationTable`**: Destination
  table where denormalized data will be stored.
  - <a id="definitions/destinationTable/properties/tableSchema"></a>**`tableSchema`**
    _(string)_: Schema of table. If null, the table is not schema-qualified.
    Default: `null`.
  - <a id="definitions/destinationTable/properties/tableName"></a>**`tableName`**
    _(string, required)_: Name of table.
  - <a id="definitions/destinationTable/properties/tableKey"></a>**`tableKey`**
    _(array or null)_: Key columns. Default: `null`.
    - <a id="definitions/destinationTable/properties/tableKey/items"></a>**Items**
      _(string)_
  - <a id="definitions/destinationTable/properties/tableColumns"></a>**`tableColumns`**
    _(array or null)_: Columns. Must match columns from destinationQuery select
    (?). Default: `null`.
    - <a id="definitions/destinationTable/properties/tableColumns/items"></a>**Items**
      _(string)_
