-- Test DDL event triggers

-- Verify event triggers were created
SELECT evtname FROM pg_event_trigger WHERE evtname LIKE 'ducklake%' ORDER BY evtname;

CREATE TABLE products (
    product_id int NOT NULL,
    product_name text NOT NULL,
    price numeric(10,2)
) USING ducklake;

-- Verify table was created in PostgreSQL
SELECT relname, am.amname
FROM pg_class c
JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'products';

INSERT INTO products VALUES (1, 'Widget', 19.99);
INSERT INTO products VALUES (2, 'Gadget', 29.99);

SELECT * FROM products ORDER BY product_id;

CREATE TABLE orders (
    order_id int NOT NULL,
    order_date timestamp,
    customer_name varchar(100),
    total_amount numeric(12,2)
) USING ducklake;

-- Verify second table
SELECT relname FROM pg_class WHERE relname = 'orders' AND relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake');

DROP TABLE products;

-- Verify it's gone from PostgreSQL
SELECT relname FROM pg_class WHERE relname = 'products';

-- Clean up
DROP TABLE orders;
