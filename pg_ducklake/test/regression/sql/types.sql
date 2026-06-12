-- Inlined-data type categories: native types keep their PG type in the
-- inlined table; not-native types use a different PG type; VARIANT (see
-- variant.sql) and GEOMETRY never inline.  Full mapping: docs/data_types.md.

CALL ducklake.set_option('data_inlining_row_limit', 100);

------------------------------------------------------------
-- Test 1: Native types are inlined as-is
------------------------------------------------------------

CREATE TABLE types_native (
    b BOOLEAN,
    i2 SMALLINT,
    i4 INT,
    i8 BIGINT,
    f4 REAL,
    f8 DOUBLE PRECISION,
    t TIME,
    ttz TIMETZ,
    iv INTERVAL,
    u UUID
) USING ducklake;

-- DuckLake serializes INTERVAL as '%d months %d days %lld microseconds';
-- PG14's interval parser uses a 32-bit intermediate, so the microsecond
-- field must stay below INT32_MAX (~35 minutes).
INSERT INTO types_native VALUES (
    true, 1, 2, 3, 1.5, 2.5,
    '12:30:00', '12:30:00+05:30', '1 day 30 minutes',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
);

-- Query via ducklake -- should return normal values
SELECT * FROM types_native;

SELECT table_name AS inlined_table_name
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'types_native')
\gset

-- Verify inlined table column types match the source PG types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'ducklake' AND table_name = :'inlined_table_name'
AND column_name NOT IN ('row_id', 'begin_snapshot', 'end_snapshot')
ORDER BY ordinal_position;

-- Query inlined table directly -- values should be identical
SELECT b, i2, i4, i8, f4, f8, t, ttz, iv, u FROM ducklake.:inlined_table_name ORDER BY row_id;

DROP TABLE types_native;

------------------------------------------------------------
-- Test 2: Not-native types (TEXT, DATE, TIMESTAMP[TZ]) use a different PG
-- type in the inlined table.  BYTEA is skipped: the SPI converter maps
-- BYTEAOID to VARCHAR, so BLOB read-back fails (pre-existing bug).
------------------------------------------------------------

CREATE TABLE types_not_native (
    v TEXT,             -- DuckDB VARCHAR      -> inlined as BYTEA
    d DATE,             -- DuckDB DATE         -> inlined as character varying
    ts TIMESTAMP,       -- DuckDB TIMESTAMP    -> inlined as character varying
    tstz TIMESTAMPTZ    -- DuckDB TIMESTAMP_TZ -> inlined as character varying
) USING ducklake;

INSERT INTO types_not_native VALUES (
    'hello',
    '2024-06-15', '2024-06-15 12:30:00', '2024-06-15 12:30:00+05:30'
);

-- Query via ducklake -- should return normal human-readable values
SELECT * FROM types_not_native;

SELECT table_name AS inlined_table_name
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'types_not_native')
\gset

-- Verify inlined table column types differ from the source PG types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'ducklake' AND table_name = :'inlined_table_name'
AND column_name NOT IN ('row_id', 'begin_snapshot', 'end_snapshot')
ORDER BY ordinal_position;

-- Query inlined table directly -- values are stored in the inlined PG type
-- v: bytea (need convert_from to see text)
-- d, ts, tstz: character varying (DuckDB text representation, not PG format)
SELECT convert_from(v, 'UTF8') AS v, d, ts, tstz
FROM ducklake.:inlined_table_name ORDER BY row_id;

DROP TABLE types_not_native;

CALL ducklake.set_option('data_inlining_row_limit', 0);
