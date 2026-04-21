-- pg_vortex: hybrid queries combining a heap table with a Vortex file.
--
-- Fixture: data/users.vortex
--   (1, 'alice',  'US'), (2, 'bob', 'US'), (3, 'carol', 'CA'),
--   (4, 'dmitri', 'DE'), (5, 'elena', 'DE')
--
-- Heap: orders(user_id bigint, amount int)
--   (1, 100), (1, 50), (2, 30), (4, 200), (6, 999)   -- 6 has no vortex match

\set pwd `pwd`

-- Heap-side table.
CREATE TABLE orders(user_id bigint, amount int);
INSERT INTO orders VALUES (1, 100), (1, 50), (2, 30), (4, 200), (6, 999);

-- 1. Inner join heap x vortex + per-country aggregation.
SELECT u.country, COUNT(*) AS orders, SUM(o.amount) AS revenue
FROM read_vortex(:'pwd' || '/data/users.vortex') AS u(id bigint, name text, country text)
JOIN orders o ON o.user_id = u.id
GROUP BY u.country
ORDER BY u.country;

-- 2. LEFT JOIN: vortex users with no orders in the heap.
SELECT u.name
FROM read_vortex(:'pwd' || '/data/users.vortex') AS u(id bigint, name text, country text)
LEFT JOIN orders o ON o.user_id = u.id
WHERE o.user_id IS NULL
ORDER BY u.name;

-- 3. Anti-join the other direction: heap orders whose user is missing from
-- the vortex file.
SELECT o.user_id, o.amount
FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM read_vortex(:'pwd' || '/data/users.vortex')
                AS u(id bigint, name text, country text)
    WHERE u.id = o.user_id
)
ORDER BY o.user_id;

-- 4. CTE composition: vortex drives a filtered row set used as an IN list.
WITH de_users AS (
    SELECT id FROM read_vortex(:'pwd' || '/data/users.vortex')
                 AS u(id bigint, name text, country text)
    WHERE country = 'DE'
)
SELECT user_id, SUM(amount) AS total
FROM orders
WHERE user_id IN (SELECT id FROM de_users)
GROUP BY user_id
ORDER BY user_id;

-- 5. UNION of vortex ids and heap user_ids.
SELECT id FROM read_vortex(:'pwd' || '/data/users.vortex')
              AS u(id bigint, name text, country text)
UNION
SELECT user_id FROM orders
ORDER BY id;

-- 6. Subquery in SELECT list: cross-source column pair.
SELECT u.name,
       (SELECT SUM(amount) FROM orders o WHERE o.user_id = u.id) AS spent
FROM read_vortex(:'pwd' || '/data/users.vortex')
         AS u(id bigint, name text, country text)
ORDER BY u.name;

DROP TABLE orders;
