# Join

Aggregate a single table.

## Steps

1. Create a target table that will hold the aggregated data.

2. Create expressions for the groups and aggregtes.

3. Author a JSON file describing the groups and aggregates.

4. Run the `denorm create-agg` command to generate SQL DDL for triggers and
   functions.

5. Apply generated SQL to the database.

Now the target table will be kept up-to-date whenever the relevant tables
change.

## Example

For a full working example, see [Aggregate example](agg-example.md).

## Schema

See [Aggregate config](agg-schema.md) for documentation of all options.

## Target

The target table must have a column `_count bigint`. This tracks the number of
source records, so that aggregated records can be removed.

## Groups

The grouping key of the aggregate.

## Aggregates

Each aggregate defines it's value, using the variable `sign` to determine the
sign of operation, e.g. `sign * count(*)`.

It also defines a scalar combining expression, e.g.
`existing.example = excluded.example`.

## Filter

A filter expression may be specified.

## Generated objects

ID is used to name database objects.

Database objects are created in the schema if given. Otherwise they will be
created in the default schema.

## Constistency

There are two consistency modes.

### Immediate

The target is updated at the end of the statement.

### Deferred

The target is updated at the end of the transaction.

Deferring work involves overhead. It is useful for avoiding lock contention on
the target table.
