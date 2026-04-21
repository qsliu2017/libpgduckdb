#pragma once

// libpgduckdb's single extension point. Lib code calls these function pointers
// unconditionally; each one has a safe default so a consumer that registers
// nothing still links and runs (it just operates without the policies an
// extension would layer on -- no GUCs, no pseudo-types, no per-connection
// secrets/extensions reload).
//
// Consumers (see examples/pg_duckdb/src/pgduckdb_lib_hooks.cpp) reassign the
// pointers in their _PG_init *before* the first call into lib.

#include "duckdb.hpp"

#include "pgduckdb/pg/declarations.hpp" // Oid typedef without pulling in postgres.h

namespace pgduckdb::hooks {

/*
 * Called once, before DuckDBManager constructs the duckdb::DuckDB instance.
 * Receives the DBConfig so the consumer can set user_agent, memory / thread
 * limits, extension / temporary directories, extension-loading policy, etc.
 * Default: no-op (DBConfig stays at DuckDB's defaults plus whatever lib sets
 * itself -- null-ordering, custom_user_agent="libpgduckdb").
 */
extern void (*pre_database_init)(duckdb::DBConfig &config);

/*
 * Called once after DuckDBManager creates the DuckDB instance and the cached
 * connection, and after lib has attached the 'pgduckdb' storage extension.
 * Consumer uses this to register optimizer extensions, INSTALL/LOAD DuckDB
 * extensions, set TimeZone / default_collation, attach ':memory:' as pg_temp,
 * etc. Default: no-op.
 */
extern void (*post_database_init)(duckdb::DuckDB &db, duckdb::Connection &conn);

/*
 * Called at the start of DuckDBManager::CreateConnection and GetConnection,
 * for every new / reused connection. Consumer uses this to drop+reload
 * secrets, reload extensions when its extension-table sequence has advanced,
 * set disabled_filesystems, etc. Default: no-op.
 */
extern void (*refresh_connection_state)(duckdb::ClientContext &context);

/*
 * Called at the start of CreateConnection and GetConnection to verify the
 * current Postgres user is allowed to run DuckDB queries. Default: no-op
 * (every user is allowed). Ext typically checks a duckdb.postgres_role GUC
 * and elog(ERROR)s if the user is not in that role.
 */
extern void (*require_execution_allowed)();

/*
 * Pseudo-type OID lookups. Lib's type-conversion code dispatches on these
 * when converting DuckDB STRUCT / UNION / MAP / unresolved types back into
 * Postgres values; a consumer that exposes such pseudo-types as SQL types
 * returns their OIDs here. Default: InvalidOid (pseudo-type support off).
 */
extern Oid (*duckdb_row_oid)();
extern Oid (*duckdb_struct_oid)();
extern Oid (*duckdb_union_oid)();
extern Oid (*duckdb_map_oid)();
extern Oid (*duckdb_unresolved_type_oid)();
extern Oid (*duckdb_struct_array_oid)();
extern Oid (*duckdb_union_array_oid)();
extern Oid (*duckdb_map_array_oid)();

/*
 * Scan-path tunables. Returning 0 from a "threads" / "workers" hook means
 * "let DuckDB / Postgres decide"; non-zero caps the value. Defaults: 0.
 */
extern int (*threads_for_postgres_scan)();
extern int (*max_workers_per_postgres_scan)();

/*
 * Logging + conversion policy. Defaults: false (do nothing / reject).
 */
extern bool (*log_pg_explain)();
extern bool (*convert_unsupported_numeric_to_double)();

} // namespace pgduckdb::hooks
