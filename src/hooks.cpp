// Defaults for the libpgduckdb extension points. See hooks.hpp.
//
// Each pointer is initialized to a no-op function; extensions reassign them
// from their _PG_init. We cannot use `nullptr` + a runtime branch in every
// call site because lib code calls them on hot paths -- a stable, valid
// function is cheaper than an `if`.

#include "pgduckdb/hooks.hpp"

extern "C" {
#include "postgres.h" // InvalidOid
}

namespace pgduckdb::hooks {

namespace {

void
DefaultPreDatabaseInit(duckdb::DBConfig &) {
}

void
DefaultPostDatabaseInit(duckdb::DuckDB &, duckdb::Connection &) {
}

void
DefaultRefreshConnectionState(duckdb::ClientContext &) {
}

void
DefaultRequireExecutionAllowed() {
}

Oid
DefaultInvalidOid() {
	return InvalidOid;
}

int
DefaultZeroInt() {
	return 0;
}

bool
DefaultFalseBool() {
	return false;
}

} // anonymous namespace

void (*pre_database_init)(duckdb::DBConfig &) = DefaultPreDatabaseInit;
void (*post_database_init)(duckdb::DuckDB &, duckdb::Connection &) = DefaultPostDatabaseInit;
void (*refresh_connection_state)(duckdb::ClientContext &) = DefaultRefreshConnectionState;
void (*require_execution_allowed)() = DefaultRequireExecutionAllowed;

Oid (*duckdb_row_oid)() = DefaultInvalidOid;
Oid (*duckdb_struct_oid)() = DefaultInvalidOid;
Oid (*duckdb_union_oid)() = DefaultInvalidOid;
Oid (*duckdb_map_oid)() = DefaultInvalidOid;
Oid (*duckdb_unresolved_type_oid)() = DefaultInvalidOid;
Oid (*duckdb_struct_array_oid)() = DefaultInvalidOid;
Oid (*duckdb_union_array_oid)() = DefaultInvalidOid;
Oid (*duckdb_map_array_oid)() = DefaultInvalidOid;

int (*threads_for_postgres_scan)() = DefaultZeroInt;
int (*max_workers_per_postgres_scan)() = DefaultZeroInt;

bool (*log_pg_explain)() = DefaultFalseBool;
bool (*convert_unsupported_numeric_to_double)() = DefaultFalseBool;

} // namespace pgduckdb::hooks
