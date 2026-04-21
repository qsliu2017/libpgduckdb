-- pg_vortex: minimal libpgduckdb_core.a consumer.
--
-- Registers exactly one entry point: read_vortex(path). Caller supplies the
-- column list via the standard "AS (col type, ...)" clause, e.g.
--
--     SELECT * FROM read_vortex('/tmp/x.vortex') AS t(id int, name text);
--
-- The underlying read_vortex() function lives in DuckDB's community Vortex
-- extension; pg_vortex does not install or load it automatically. Before
-- calling read_vortex() from SQL, run once per server process:
--
--     DO $$ BEGIN PERFORM 1; END $$;  -- open the backend
--     -- then inside DuckDB: INSTALL vortex FROM community; LOAD vortex;
--
-- (For the minimal example we leave extension management to the user.)

CREATE FUNCTION @extschema@.read_vortex(path text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_vortex_read_vortex'
LANGUAGE C STRICT;
