"""Test that async joins correctly substitute ${table} variable with async table name."""

import json

from file import temp_file
from pg import connection, transaction
from process import run_process

# Schema with multiple async tables and a target table that records which table triggered the update
_SCHEMA_SQL = """
    CREATE TABLE product (
        id int PRIMARY KEY,
        name text NOT NULL
    );

    CREATE TABLE inventory (
        id int PRIMARY KEY,
        product_id int NOT NULL REFERENCES product (id),
        quantity int NOT NULL
    );

    CREATE TABLE pricing (
        id int PRIMARY KEY,
        product_id int NOT NULL REFERENCES product (id),
        price decimal NOT NULL
    );

    CREATE TABLE product_index (
        product_id int PRIMARY KEY,
        source_table text NOT NULL
    );
"""

# Join config with two async tables - ${table} variable should reflect which async table triggered
_SCHEMA_JSON = {
    "id": "product_index",
    "tables": {
        "product": {
            "tableName": "product",
            "destinationKeyExpr": ["product.id"],
        },
        "inventory": {
            "tableKey": [{"name": "product_id"}],
            "joinTargetTable": "product",
            "joinTargetKey": ["id"],
            "joinMode": "async",
            "joinOn": "inventory.product_id = product.id",
            "tableName": "inventory",
        },
        "pricing": {
            "tableKey": [{"name": "product_id"}],
            "joinTargetTable": "product",
            "joinTargetKey": ["id"],
            "joinMode": "async",
            "joinOn": "pricing.product_id = product.id",
            "tableName": "pricing",
        },
    },
    "destinationTable": {
        "tableKey": ["product_id"],
        "tableColumns": ["product_id", "source_table"],
        "tableName": "product_index",
        "tableSchema": "public",
    },
    # This targetQuery uses ${table} to record which table triggered the update
    "destinationQuery": """
        SELECT
            d.product_id,
            '${table}' AS source_table
        FROM ${key} AS d
            JOIN product p ON d.product_id = p.id
    """,
}


def test_join_async_table_variable_different_sources(pg_database):
    """Test that different async tables correctly identify themselves via ${table}."""
    with temp_file("denorm-") as schema_file:
        # Create schema
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        # Generate denorm SQL
        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        output = run_process(
            [
                "denorm",
                "create-join",
                "--schema",
                schema_file,
            ]
        )

        # Apply denorm SQL
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(output.decode("utf-8"))

        # Insert test data - two products, one updated via inventory, one via pricing
        with connection("") as conn, transaction(conn) as cur:
            cur.execute("""
                INSERT INTO product (id, name)
                VALUES (1, 'Widget'), (2, 'Gadget');

                INSERT INTO inventory (id, product_id, quantity)
                VALUES (1, 1, 100);

                INSERT INTO pricing (id, product_id, price)
                VALUES (1, 2, 19.99);
                """)

        # Process inventory queue - should tag product 1 with source_table='inventory'
        with connection("") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                while True:
                    cur.execute("SELECT product_index__pcs__inventory(10)")
                    (result,) = cur.fetchone()
                    if not result:
                        break

        # Process pricing queue - should tag product 2 with source_table='pricing'
        with connection("") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                while True:
                    cur.execute("SELECT product_index__pcs__pricing(10)")
                    (result,) = cur.fetchone()
                    if not result:
                        break

        # Verify results - each product should be tagged with the correct source table
        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM product_index ORDER BY product_id")
            result = cur.fetchall()

            # Product 1 was indexed via inventory async table
            assert result[0] == (
                1,
                "inventory",
            ), f"Expected (1, 'inventory') but got {result[0]}"

            # Product 2 was indexed via pricing async table
            assert result[1] == (
                2,
                "pricing",
            ), f"Expected (2, 'pricing') but got {result[1]}"


def test_join_async_table_variable_updates(pg_database):
    """Test that when a product is updated via different async tables, the source_table changes."""
    with temp_file("denorm-") as schema_file:
        # Create schema
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        # Generate denorm SQL
        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        output = run_process(
            [
                "denorm",
                "create-join",
                "--schema",
                schema_file,
            ]
        )

        # Apply denorm SQL
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(output.decode("utf-8"))

        # Insert product with inventory
        with connection("") as conn, transaction(conn) as cur:
            cur.execute("""
                INSERT INTO product (id, name)
                VALUES (1, 'Widget');

                INSERT INTO inventory (id, product_id, quantity)
                VALUES (1, 1, 100);
                """)

        # Process inventory queue
        with connection("") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                while True:
                    cur.execute("SELECT product_index__pcs__inventory(10)")
                    (result,) = cur.fetchone()
                    if not result:
                        break

        # Verify product was indexed from inventory
        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM product_index WHERE product_id = 1")
            result = cur.fetchone()
            assert result == (
                1,
                "inventory",
            ), f"Expected (1, 'inventory') but got {result}"

        # Now add pricing for the same product
        with connection("") as conn, transaction(conn) as cur:
            cur.execute("""
                INSERT INTO pricing (id, product_id, price)
                VALUES (1, 1, 9.99);
                """)

        # Process pricing queue - should update source_table to 'pricing'
        with connection("") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                while True:
                    cur.execute("SELECT product_index__pcs__pricing(10)")
                    (result,) = cur.fetchone()
                    if not result:
                        break

        # Verify product was updated to show pricing as the source
        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM product_index WHERE product_id = 1")
            result = cur.fetchone()
            assert result == (1, "pricing"), f"Expected (1, 'pricing') but got {result}"


def test_join_async_table_variable_base_table(pg_database):
    """Test that direct changes to the base (non-async) table use the base table name."""
    with temp_file("denorm-") as schema_file:
        # Create schema
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(_SCHEMA_SQL)

        # Generate denorm SQL
        with open(schema_file, "w") as f:
            json.dump(_SCHEMA_JSON, f)

        output = run_process(
            [
                "denorm",
                "create-join",
                "--schema",
                schema_file,
            ]
        )

        # Apply denorm SQL
        with connection("") as conn, transaction(conn) as cur:
            cur.execute(output.decode("utf-8"))

        # Insert product directly (triggers the base table change handler)
        with connection("") as conn, transaction(conn) as cur:
            cur.execute("""
                INSERT INTO product (id, name)
                VALUES (1, 'Widget');
                """)

        # Verify product was indexed from the base 'product' table
        with connection("") as conn, transaction(conn) as cur:
            cur.execute("SELECT * FROM product_index WHERE product_id = 1")
            result = cur.fetchone()
            assert result == (1, "product"), f"Expected (1, 'product') but got {result}"
