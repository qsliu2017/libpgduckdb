/*
 * pgducklake_ddl.cpp -- Extension-scope DDL bootstrap entry points.
 *
 * @scope extension: ducklake_initialize, ducklake_only_procedure,
 *                   ducklake_only_function
 *
 * ducklake_initialize() bootstraps the DuckDB catalog during CREATE EXTENSION.
 * ducklake_only_procedure / ducklake_only_function are error stubs used as
 * the C body for SQL objects that are meant to run inside DuckDB only.
 * pg_ducklake's own planner path rewrites these calls before execution;
 * reaching the stub means the call slipped through (e.g. the planner hook
 * was disabled, or the function was invoked from a context the rewrite
 * doesn't cover), so we surface a clear error rather than a confusing
 * wrong-answer.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "pgducklake/utility/cpp_wrapper.hpp"
#include "pgduckdb/pgduckdb_contracts.hpp"

extern "C" {
#include "postgres.h"

#include "commands/extension.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
}

extern "C" {

DECLARE_PG_FUNCTION(ducklake_initialize) {
  elog(LOG, "ducklake_initialize() called");

  if (!creating_extension) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ducklake_initialize() can only be called during "
                                                                     "CREATE EXTENSION")));
  }

  if (pgducklake::PgDuckLakeMetadataManager::IsInitialized()) {
    ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_SCHEMA), errmsg("DuckLake reserved schema \"ducklake\" is already in use")));
  }

  // Force DuckDB initialization (no-op if already alive).
  // On first CREATE: this triggers DuckDBManager::Initialize() which
  //   calls ducklake_load_extension() -> ducklake_attach_catalog().
  // On DROP+CREATE: DuckDB is already alive, the catalog was detached
  //   by the utility hook during DROP, so we re-attach it here.
  bool duckdb_already_initialized = (ducklake_get_duckdb_database() != nullptr);

  const char *init_errmsg = nullptr;
  int ret = pgducklake::ExecuteDuckDBQuery("SELECT 1", &init_errmsg);
  if (ret != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to initialize DuckDB: %s", init_errmsg ? init_errmsg : "unknown error")));
  }

  if (duckdb_already_initialized) {
    ducklake_attach_catalog();
  }

  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_only_procedure) {
  char *proc_name = DatumGetCString(DirectFunctionCall1(regprocout, ObjectIdGetDatum(fcinfo->flinfo->fn_oid)));
  elog(ERROR, "Procedure '%s' only works with DuckDB execution", proc_name);
}

// pg_ducklake-local mirror of pg_duckdb's duckdb_only_function. Every SQL
// function in sql/pg_ducklake--*.sql that used to reference
// $libdir/pg_duckdb's duckdb_only_function now binds to this symbol
// instead, so pg_ducklake can be loaded without pg_duckdb.so present.
DECLARE_PG_FUNCTION(ducklake_only_function) {
  char *function_name = DatumGetCString(DirectFunctionCall1(regprocout, ObjectIdGetDatum(fcinfo->flinfo->fn_oid)));
  elog(ERROR, "Function '%s' only works with DuckDB execution", function_name);
}

// ---------- ducklake.row pseudo-type I/O stubs --------------------------
//
// ducklake.row is a pseudo composite type: its shape is determined by the
// DuckDB query the SRF was invoked with, not by PG's type system. These
// I/O functions exist so CREATE TYPE can succeed; invoking them directly
// is always an error, matching pg_duckdb's duckdb.row behavior.
DECLARE_PG_FUNCTION(ducklake_row_in) {
  elog(ERROR, "Creating the ducklake.row type is not supported");
}

DECLARE_PG_FUNCTION(ducklake_row_out) {
  elog(ERROR, "Converting a ducklake.row to a string is not supported");
}

DECLARE_PG_FUNCTION(ducklake_row_subscript) {
  // Returns an internal SubscriptRoutines pointer in pg_duckdb; we do not
  // implement subscripting since ducklake.row is only ever produced by
  // ducklake.query() output and flattened via SELECT * -- subscripting
  // cannot reach it. Reporting nullptr is safe (Postgres never calls the
  // subscript routines without first asking subscript_parse, which here
  // never runs).
  PG_RETURN_POINTER(nullptr);
}

// ---------- DuckDB lifecycle helpers exposed as SQL ---------------------

DECLARE_PG_FUNCTION(ducklake_recycle_ddb) {
  pgduckdb::DuckdbRecycleDuckDB();
  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_raw_query) {
  const char *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
  const char *err = nullptr;
  if (pgducklake::ExecuteDuckDBQuery(query, &err) != 0) {
    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("DuckDB error: %s", err ? err : "unknown")));
  }
  PG_RETURN_BOOL(true);
}

} // extern "C"
