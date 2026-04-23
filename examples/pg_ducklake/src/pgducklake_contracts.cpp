/*
 * pgducklake_contracts.cpp -- in-process implementation of the pg_duckdb
 * contract surface declared in include/pgduckdb/pgduckdb_contracts.hpp.
 *
 * Upstream pg_ducklake called into a peer pg_duckdb.so for these functions;
 * under libpgduckdb we link the plumbing directly and own the DuckDB
 * instance ourselves, so the contracts collapse into local state plus a
 * lazy DuckDB bootstrap. pg_duckdb.so is no longer a runtime dependency.
 */

// DuckDB headers must parse before postgres.h -- PG's elog.h #defines FATAL,
// which collides with duckdb::ExceptionType::FATAL.
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"

#include "pgduckdb/pgduckdb_contracts.hpp"
#include "pgduckdb/pgduckdb_process_lock.hpp"

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

extern "C" {
#include "postgres.h"

#include "utils/elog.h"
}

// ducklake_load_extension lives in pgducklake_duckdb.cpp. Declaration kept
// local so we can invoke it from the lazy-init path without pulling the
// whole header graph (pgducklake_duckdb.hpp transitively wants ducklake
// extension headers we don't need here).
extern void ducklake_load_extension(duckdb::DuckDB &db);

namespace {

// --- Backend-local DuckDB instance ---------------------------------------
//
// A single duckdb::DuckDB per backend. Constructed lazily on the first call
// that needs it (see pgducklake::DuckdbIsInitialized / EnsureInitialized
// below) and destroyed by DuckdbRecycleDuckDB. The pointer is guarded by
// GlobalProcessLock because DuckDB worker threads may also touch it.
std::unique_ptr<duckdb::DuckDB> g_duckdb_instance;

// Callback registered by pg_ducklake's _PG_init -- invoked every time we
// (re)construct the backend's DuckDB instance so extensions/macros/catalogs
// get reattached.
pgduckdb::DuckDBLoadExtension g_load_extension_cb = nullptr;

// --- Local flags replacing pg_duckdb's global state ----------------------
//
// These were all backed by statics inside pg_duckdb under the original
// architecture; mirror them here now that pg_ducklake owns the lifecycle.
bool g_alter_table_in_progress = false;
bool g_allow_subtransaction = false;
bool g_force_execution = false;

// Passthrough type map -- pg_ducklake registers ducklake.variant -> VARIANT
// at _PG_init; upstream pg_duckdb used this for planner-side type routing.
// pg_ducklake itself doesn't plan-route today, so we keep the map purely as
// a lookup available to any future code path that needs it.
struct PassthroughKey {
	std::string schema;
	std::string name;
};
std::vector<std::tuple<PassthroughKey, std::string /*duckdb_type*/>> g_passthrough_types;

} // namespace

// ---------- pg_duckdb table AM registration ----------------------------

// Upstream used this to teach pg_duckdb's planner about the ducklake AM
// so it could route DML to DuckDB. In the libpgduckdb world pg_ducklake's
// CREATE ACCESS METHOD ducklake TYPE TABLE HANDLER ... (in
// sql/pg_ducklake--0.1.0.sql) is the authoritative registration, and we
// do not do planner routing at this layer -- so the contract degrades to
// a no-op that always reports success.
extern "C" bool
RegisterDuckdbTableAm(const char * /*name*/, const TableAmRoutine * /*am*/) {
	return true;
}

// ---------- DDL deparse stubs ------------------------------------------
//
// Upstream pg_duckdb provided pgduckdb_get_tabledef /
// pgduckdb_get_alter_tabledef / pgduckdb_get_rename_relationdef on top of
// its deparse machinery. The libpgduckdb port currently has the lower
// layer (pgduckdb_get_querydef) but not these three wrappers; porting them
// requires lifting pg_duckdb's pgduckdb_deparse.cpp wholesale and is
// deferred. pg_ducklake's DDL paths will report a clear error until then
// -- dlopen still succeeds.
extern "C" char *
pgduckdb_get_tabledef(Oid /*relation_oid*/) {
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	                errmsg("pg_ducklake DDL deparse not yet implemented on libpgduckdb")));
	return nullptr;
}

extern "C" char *
pgduckdb_get_alter_tabledef(Oid /*relation_oid*/, AlterTableStmt * /*alter_stmt*/) {
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	                errmsg("pg_ducklake DDL deparse not yet implemented on libpgduckdb")));
	return nullptr;
}

extern "C" char *
pgduckdb_get_rename_relationdef(Oid /*relation_oid*/, RenameStmt * /*rename_stmt*/) {
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	                errmsg("pg_ducklake DDL deparse not yet implemented on libpgduckdb")));
	return nullptr;
}

// ---------- pgduckdb:: namespace implementations -----------------------

namespace pgduckdb {

bool
RegisterDuckdbLoadExtension(DuckDBLoadExtension extension) {
	g_load_extension_cb = extension;
	return true;
}

bool
RegisterDuckdbExternalTableCheck(DuckdbExternalTableCheck /*callback*/) {
	// Was a pg_duckdb planner hook for "is this relation a ducklake table";
	// pg_ducklake's FDW path doesn't need it now that we own execution.
	return true;
}

void
RegisterDuckdbRelationNameCallback(DuckdbRelationNameCallback /*callback*/) {
	// Same motivation as above -- only pg_duckdb's deparse path consulted this.
}

bool
DuckdbIsAlterTableInProgress(void) {
	return g_alter_table_in_progress;
}

bool
DuckdbIsInitialized(void) {
	return g_duckdb_instance != nullptr;
}

void
DuckdbRecycleDuckDB(void) {
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	g_duckdb_instance.reset();
}

void
DuckdbUnsafeSetNextExpectedCommandId(uint32_t /*command_id*/) {
	// xact-id tracking lived in pg_duckdb's DuckDBXactCallback; pg_ducklake
	// writes never flow through that path in the libpgduckdb build, so this
	// degrades to a no-op until we wire our own xact callbacks.
}

void
DuckdbAllowSubtransaction(bool allow) {
	g_allow_subtransaction = allow;
}

void
DuckdbLockGlobalProcess(void) {
	GlobalProcessLock::GetLock().lock();
}

void
DuckdbUnlockGlobalProcess(void) {
	GlobalProcessLock::GetLock().unlock();
}

bool
DuckdbSetForceExecution(bool value) {
	bool prev = g_force_execution;
	g_force_execution = value;
	return prev;
}

bool
DuckdbEnsureCacheValid(void) {
	// pg_duckdb maintained a syscache-aware metadata cache; we do not.
	// Reporting "valid" keeps pg_ducklake's pre-flight checks happy.
	return true;
}

void
RegisterDuckdbOnlyExtension(const char * /*extension_name*/) {
	// pg_duckdb used this to skip planner re-routing for its own functions.
	// Irrelevant in-process.
}

void
RegisterDuckdbOnlyFunction(const char * /*function_name*/) {
	// Same story -- planner allowlist, not consulted here.
}

void
RegisterPassthroughType(const char *pg_schema, const char *pg_type_name, const char *duckdb_type_name) {
	g_passthrough_types.emplace_back(PassthroughKey{pg_schema, pg_type_name}, duckdb_type_name);
}

const char *
GetPassthroughTypeName(Oid /*pg_type_oid*/) {
	// The original implementation resolved pg_type_oid via PG syscache. We
	// never plumbed the reverse lookup here; returning nullptr signals "no
	// passthrough mapping," which is the safe default for every caller.
	return nullptr;
}

} // namespace pgduckdb

// ---------- In-tree DuckDB lifecycle -----------------------------------
//
// Exposed to the rest of pg_ducklake via pgducklake_duckdb.hpp. The lazy
// init path is the one spot that actually constructs a duckdb::DuckDB and
// replays the pg_ducklake extension-load callback against it.

namespace pgducklake {

duckdb::DuckDB &
EnsureDuckDBInitialized() {
	std::lock_guard<std::recursive_mutex> lock(pgduckdb::GlobalProcessLock::GetLock());
	if (!g_duckdb_instance) {
		g_duckdb_instance = std::make_unique<duckdb::DuckDB>(nullptr);
		if (g_load_extension_cb) {
			g_load_extension_cb(*g_duckdb_instance);
		}
	}
	return *g_duckdb_instance;
}

void
SetAlterTableInProgress(bool value) {
	g_alter_table_in_progress = value;
}

} // namespace pgducklake
