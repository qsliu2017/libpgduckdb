#pragma once

extern "C" {
#include "postgres.h"

#include "access/tableam.h"
#include "nodes/parsenodes.h"
}

namespace duckdb {
class DuckDB;
}

// Upstream function - keep extern "C" linkage
extern "C" bool RegisterDuckdbTableAm(const char *name, const TableAmRoutine *am);

// DDL deparse entry points live in pg_duckdb's own pgduckdb_deparse.hpp
// (examples/pg_duckdb/include/pgduckdb/pgduckdb_deparse.hpp in this repo).
// We re-declare them here so pg_ducklake TUs do not need to vendor that
// header; the symbols are resolved at backend load time against the
// already-loaded pg_duckdb.so.
extern "C" char *pgduckdb_get_tabledef(Oid relation_oid);
extern "C" char *pgduckdb_get_alter_tabledef(Oid relation_oid, AlterTableStmt *alter_stmt);
extern "C" char *pgduckdb_get_rename_relationdef(Oid relation_oid, RenameStmt *rename_stmt);

// Our C++ additions — all in namespace pgduckdb
namespace pgduckdb {

typedef void (*DuckDBLoadExtension)(duckdb::DuckDB &db);
typedef bool (*DuckdbExternalTableCheck)(Oid relid);
typedef char *(*DuckdbRelationNameCallback)(Oid relid);

bool RegisterDuckdbLoadExtension(DuckDBLoadExtension extension);
bool RegisterDuckdbExternalTableCheck(DuckdbExternalTableCheck callback);
void RegisterDuckdbRelationNameCallback(DuckdbRelationNameCallback callback);
bool DuckdbIsAlterTableInProgress(void);
bool DuckdbIsInitialized(void);
void DuckdbRecycleDuckDB(void);
void DuckdbUnsafeSetNextExpectedCommandId(uint32_t command_id);
// Allow a PostgreSQL internal subtransaction while a DuckDB transaction is active.
// pg_ducklake uses this to wrap metadata commit writes so DuckLake's retry loop
// can catch and recover from constraint conflicts without crashing the backend.
void DuckdbAllowSubtransaction(bool allow);
void DuckdbLockGlobalProcess(void);
void DuckdbUnlockGlobalProcess(void);
bool DuckdbSetForceExecution(bool value);
bool DuckdbEnsureCacheValid(void);
void RegisterDuckdbOnlyExtension(const char *extension_name);
void RegisterDuckdbOnlyFunction(const char *function_name);
void RegisterPassthroughType(const char *pg_schema, const char *pg_type_name, const char *duckdb_type_name);
const char *GetPassthroughTypeName(Oid pg_type_oid);

} // namespace pgduckdb
