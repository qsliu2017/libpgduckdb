#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_guc.hpp"

#include "pgddb/pgddb_duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
}

#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgddb/pgddb_node.hpp"
#include "pgduckdb/pgduckdb_ruleutils.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"

/*
 * pg_duckdb's CustomScan identity. The address of duckdb_scan_scan_methods
 * is what IsDuckdbPlan compares against; the name "DuckDBScan" is what
 * RegisterCustomScanMethods keys on in PG's process-global hash.
 */
CustomScanMethods duckdb_scan_scan_methods;
static CustomExecMethods duckdb_scan_exec_methods;

extern "C" {

#ifdef PG_MODULE_MAGIC_EXT
#ifndef PG_DUCKDB_VERSION
// Should always be defined via build system, but keep a fallback here for
// static analysis tools etc.
#define PG_DUCKDB_VERSION "unknown"
#endif
PG_MODULE_MAGIC_EXT(.name = "pg_duckdb", .version = PG_DUCKDB_VERSION);
#else
PG_MODULE_MAGIC;
#endif

void
_PG_init(void) {
	if (!process_shared_preload_libraries_in_progress) {
		ereport(ERROR, (errmsg("pg_duckdb needs to be loaded via shared_preload_libraries"),
		                errhint("Add pg_duckdb to shared_preload_libraries.")));
	}

	pgduckdb::InitGUC();
	pgduckdb::InitGUCHooks();
	pgduckdb::InitRuleutilsHooks();
	pgduckdb::InitTypeHooks();
	DuckdbInitHooks();
	DuckdbInitNode("DuckDBScan", &duckdb_scan_scan_methods, &duckdb_scan_exec_methods);
	pgduckdb::InitBackgroundWorkersShmem();
	pgduckdb::RegisterDuckdbXactCallback();
}
} // extern "C"
