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
#include "pgduckdb/pgduckdb_duckdb_manager.hpp"
#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgducklake/pgducklake_defs.hpp"

extern "C" {
#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
}

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
// A single duckdb::DuckDB per backend, owned by a DuckLakeManager subclass
// of libpgduckdb's lazy-singleton DuckDBManager base. The base handles
// lazy ctor + Reset; we override only OnInitialized to replay the
// pg_ducklake extension-load callback. Access is guarded by
// GlobalProcessLock because DuckDB worker threads may also touch it.

// Callback registered by pg_ducklake's _PG_init -- invoked every time we
// (re)construct the backend's DuckDB instance so extensions/macros/catalogs
// get reattached.
pgduckdb::DuckDBLoadExtension g_load_extension_cb = nullptr;

class DuckLakeManager : public pgduckdb::DuckDBManager {
protected:
	void
	OnInitialized(duckdb::DuckDB &db) override {
		if (g_load_extension_cb) {
			g_load_extension_cb(db);
		}
	}
};

DuckLakeManager g_manager;

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

// ---------- DDL deparse routine --------------------------------------
//
// libpgduckdb's pgduckdb_get_tabledef / pgduckdb_get_alter_tabledef /
// pgduckdb_get_rename_relationdef do all the PG-DDL -> DuckDB-DDL work;
// we just supply the consumer-specific callbacks the generic path can't
// know on its own (AM name, column-type validation, AM ownership check).

namespace pgducklake {

// Pull the AM name for a relation's rd_tableam; returns nullptr for rels
// without a table AM (views, etc.). Used to gate the ducklake-specific
// deparse mapping so heap relations referenced from INSERT/CTAS queries
// keep their default "public"."t" shape.
static const char *
RelationAmName(Oid relation_oid) {
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relation_oid));
	if (!HeapTupleIsValid(tp)) {
		return nullptr;
	}
	Form_pg_class relform = (Form_pg_class)GETSTRUCT(tp);
	Oid amoid = relform->relam;
	ReleaseSysCache(tp);
	if (!OidIsValid(amoid)) {
		return nullptr;
	}
	// PG has no get_am_name; read amname directly from pg_am.
	HeapTuple amtup = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(amtup)) {
		return nullptr;
	}
	Form_pg_am amform = (Form_pg_am)GETSTRUCT(amtup);
	char *result = pstrdup(NameStr(amform->amname));
	ReleaseSysCache(amtup);
	return result;
}

static bool
IsDuckLakeRelation(Oid relation_oid) {
	const char *am = RelationAmName(relation_oid);
	return am && strcmp(am, "ducklake") == 0;
}

static const char *
DuckLakeTableAmName(Relation relation) {
	// Only the ducklake AM emits the pgducklake-prefixed DuckDB name.
	// Non-ducklake relations (heap tables referenced from ducklake DDL,
	// e.g. default-expression lookups) fall through to the lib default.
	if (relation && relation->rd_tableam) {
		const char *am = RelationAmName(RelationGetRelid(relation));
		if (am && strcmp(am, "ducklake") == 0) {
			return "ducklake";
		}
	}
	return nullptr;
}

// Render `"pgducklake"."schema"."table"` for ducklake relations; fall
// through to the lib default for everything else (so default-expr walks
// over heap relations during ducklake DDL still produce plain PG names).
static char *
DuckLakeRelationName(Oid relation_oid) {
	if (!IsDuckLakeRelation(relation_oid)) {
		return nullptr;
	}
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relation_oid));
	if (!HeapTupleIsValid(tp)) {
		elog(ERROR, "cache lookup failed for relation %u", relation_oid);
	}
	Form_pg_class relform = (Form_pg_class)GETSTRUCT(tp);
	const char *relname = NameStr(relform->relname);
	const char *schema = get_namespace_name(relform->relnamespace);
	ReleaseSysCache(tp);

	const char *db_and_schema = pgduckdb_db_and_schema_string(schema ? schema : "public", "ducklake");
	return psprintf("%s.%s", db_and_schema, quote_identifier(relname));
}

static void
DuckLakeValidateColumnType(Form_pg_attribute /*column*/) {
	// No extra column-type validation today. The lib walker already
	// rejects PG types without a format_type_with_typemod rendering;
	// anything DuckDB can't store surfaces when ExecuteDuckDBQuery runs
	// the emitted CREATE TABLE.
}

// Map (PG schema, AM name) -> (DuckDB catalog, DuckDB schema). pg_ducklake
// attaches its DuckLake metadata as catalog PGDUCKLAKE_DUCKDB_CATALOG
// ("pgducklake") in ducklake_attach_catalog(), so every ducklake-AM table
// lives under that catalog; the PG schema name carries over verbatim
// (DuckDB creates schemas on demand via "CREATE SCHEMA IF NOT EXISTS").
//
// For non-ducklake AMs (e.g. during deparse of a ducklake view over a
// heap relation) we fall through to nullptr so the lib default handles
// them. NULL return is the "use default" signal documented on the
// DeparseRoutine contract.
static List *
DuckLakeDbAndSchema(const char *pg_schema, const char *table_am_name) {
	if (table_am_name && strcmp(table_am_name, "ducklake") == 0) {
		return list_make2(pstrdup(PGDUCKLAKE_DUCKDB_CATALOG), pstrdup(pg_schema));
	}
	return nullptr;
}

// Exposed for the pg_ducklake table-AM registration / DDL trigger path.
// Declared in pgducklake_table.hpp; define once here so the linker can
// find the routine wherever it's referenced.
const DeparseRoutine ducklake_deparse_routine = {
    /* default_database_name      */ nullptr,
    /* relation_name              */ DuckLakeRelationName,
    /* function_name              */ nullptr,
    /* db_and_schema              */ DuckLakeDbAndSchema,
    /* is_duckdb_row_type         */ nullptr,
    /* is_unresolved_type         */ nullptr,
    /* is_fake_type               */ nullptr,
    /* var_is_duckdb_row          */ nullptr,
    /* func_returns_duckdb_row    */ nullptr,
    /* duckdb_subscript_var       */ nullptr,
    /* strip_first_subscript      */ nullptr,
    /* write_row_refname          */ nullptr,
    /* subscript_has_custom_alias */ nullptr,
    /* reconstruct_star_step      */ nullptr,
    /* replace_subquery_with_view */ nullptr,
    /* is_not_default_expr        */ nullptr,
    /* show_type                  */ nullptr,
    /* add_tablesample_percent    */ nullptr,
    /* table_am_name              */ DuckLakeTableAmName,
    /* validate_column_type       */ DuckLakeValidateColumnType,
    /* assert_owned_relation      */ nullptr,
};

} // namespace pgducklake

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
	return g_manager.IsInitialized();
}

void
DuckdbRecycleDuckDB(void) {
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	g_manager.Reset();
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
	return g_manager.GetDatabase();
}

void
SetAlterTableInProgress(bool value) {
	g_alter_table_in_progress = value;
}

} // namespace pgducklake
