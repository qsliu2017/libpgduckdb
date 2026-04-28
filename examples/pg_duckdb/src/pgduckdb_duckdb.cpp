// pg_duckdb's PgDuckDBManager. Subclass of pgduckdb::DuckDBManager (the slim
// lazy-singleton lib base). The base owns the duckdb::DuckDB pointer + the
// lazy GetDatabase() / Reset() shape; this file owns the GUC-driven DBConfig
// (Configure), the post-construction plumbing (OnInitialized: storage
// extension register, ATTACH pgduckdb / pg_temp, TimeZone / collation,
// INSTALL/LOAD), and the cached-connection / refresh-state machinery
// (CreateConnection / GetConnection / RefreshConnectionState / secrets_valid /
// extensions_table_seq).

#include <filesystem>

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/pg/guc.hpp"
#include "pgduckdb/pg/permissions.hpp"
#include "pgduckdb/pg/string_utils.hpp"
#include "pgduckdb/pg/transactions.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_extensions.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_secrets_helper.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_unsupported_type_optimizer.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgduckdb/utility/signal_guard.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

extern "C" {
#include "postgres.h"

#include "access/attnum.h"
#include "access/tupdesc.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "executor/tuptable.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
}

namespace pgduckdb {

// ------------------------------------------------------------------
// Internal helpers -- config / extension / secret / sequence plumbing
// used by Initialize() and RefreshConnectionState().
// ------------------------------------------------------------------

namespace {

template <typename T>
std::string
ToDebugString(T value) {
	return std::to_string(value);
}

template <>
std::string
ToDebugString(char *value) {
	return std::string(value);
}

// DuckDB v1.5 removed direct field access on DBConfigOptions for several
// settings (allow_unsigned_extensions, enable_external_access, etc.).
// Route everything through SetOptionByName so the set of tunables follows
// upstream automatically.
#define SET_DUCKDB_OPTION_FROM_GUC(ddb_option_name)                                                                    \
	config.SetOptionByName(#ddb_option_name, duckdb::Value(duckdb_##ddb_option_name));                                 \
	elog(DEBUG2, "[PGDuckDB] Set DuckDB option: '" #ddb_option_name "'=%s",                                            \
	     ToDebugString(duckdb_##ddb_option_name).c_str());

int64
GetSeqLastValue(const char *seq_name) {
	Oid duckdb_namespace = get_namespace_oid("duckdb", false);
	Oid table_seq_oid = get_relname_relid(seq_name, duckdb_namespace);
	return PostgresFunctionGuard(DirectFunctionCall1Coll, pg_sequence_last_value, InvalidOid, table_seq_oid);
}

void
LoadSecrets(duckdb::ClientContext &context) {
	auto queries = InvokeCPPFunc(pg::ListDuckDBCreateSecretQueries);
	foreach_ptr(char, query, queries) {
		DuckDBQueryOrThrow(context, query);
	}
}

void
DropSecrets(duckdb::ClientContext &context) {
	auto secrets =
	    pgduckdb::DuckDBQueryOrThrow(context, "SELECT name FROM duckdb_secrets() WHERE name LIKE 'pgduckdb_secret_%';");
	while (auto chunk = secrets->Fetch()) {
		for (size_t i = 0, s = chunk->size(); i < s; ++i) {
			auto drop_secret_cmd = duckdb::StringUtil::Format("DROP SECRET %s;", chunk->GetValue(0, i).ToString());
			pgduckdb::DuckDBQueryOrThrow(context, drop_secret_cmd);
		}
	}
}

void
InstallExtensions(duckdb::ClientContext &context) {
	auto duckdb_extensions = ReadDuckdbExtensions();
	for (auto &extension : duckdb_extensions) {
		DuckDBQueryOrThrow(context, ddb::InstallExtensionQuery(extension.name, extension.repository));
	}
}

void
LoadExtensions(duckdb::ClientContext &context) {
	auto duckdb_extensions = ReadDuckdbExtensions();
	for (auto &extension : duckdb_extensions) {
		if (extension.autoload) {
			DuckDBQueryOrThrow(context, ddb::LoadExtensionQuery(extension.name));
		}
	}
}

std::string
DisabledFileSystems() {
	if (pgduckdb::pg::AllowRawFileAccess()) {
		return duckdb_disabled_filesystems;
	}
	if (IsEmptyString(duckdb_disabled_filesystems)) {
		return "LocalFileSystem";
	}
	std::vector<std::string> fs_list = duckdb::StringUtil::Split(duckdb_disabled_filesystems, ',');
	for (auto &fs : fs_list) {
		std::string trimmed_fs = fs;
		duckdb::StringUtil::Trim(trimmed_fs);
		if (duckdb::StringUtil::CIEquals(trimmed_fs, "LocalFileSystem")) {
			return duckdb_disabled_filesystems;
		}
	}
	return "LocalFileSystem," + std::string(duckdb_disabled_filesystems);
}

// ----- TypeResolver callbacks -----

// Ext-first Postgres -> DuckDB type mapping. Return INVALID LogicalType when
// the Oid is not one of ours (lib falls through to its built-in switch).
duckdb::LogicalType
PgDuckDBPostgresToDuckDBType(Form_pg_attribute attribute) {
	Oid oid = attribute->atttypid;
	// Match both the base pseudo-type and its array variant.
	if (oid == DuckdbStructOid() || oid == DuckdbStructArrayOid()) {
		return duckdb::LogicalTypeId::STRUCT;
	}
	if (oid == DuckdbUnionOid() || oid == DuckdbUnionArrayOid()) {
		return duckdb::LogicalTypeId::UNION;
	}
	if (oid == DuckdbMapOid() || oid == DuckdbMapArrayOid()) {
		return duckdb::LogicalTypeId::MAP;
	}
	return duckdb::LogicalType::INVALID;
}

Oid
PgDuckDBDuckDBToPostgresOid(const duckdb::LogicalType &type) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::STRUCT:
		return DuckdbStructOid();
	case duckdb::LogicalTypeId::UNION:
		return DuckdbUnionOid();
	case duckdb::LogicalTypeId::MAP:
		return DuckdbMapOid();
	default:
		return InvalidOid;
	}
}

Oid
PgDuckDBDuckDBToPostgresArrayOid(const duckdb::LogicalType &type) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::STRUCT:
		return DuckdbStructArrayOid();
	case duckdb::LogicalTypeId::UNION:
		return DuckdbUnionArrayOid();
	case duckdb::LogicalTypeId::MAP:
		return DuckdbMapArrayOid();
	default:
		return InvalidOid;
	}
}

bool
PgDuckDBConvertDuckValueToPostgres(Oid oid, const duckdb::Value &value, TupleTableSlot *slot, int col) {
	// Scalar pseudo-types collapse to text via DuckDB's ToString().
	if (oid == DuckdbStructOid() || oid == DuckdbUnionOid() || oid == DuckdbMapOid()) {
		slot->tts_values[col] = ConvertDuckValueToStringDatum(value);
		return true;
	}
	// Array pseudo-types delegate to the lib-provided multi-dim array walk.
	if (oid == DuckdbStructArrayOid()) {
		ConvertDuckToPostgresRuntimeArray(slot, value, col, DuckdbStructOid(), ConvertDuckValueToStringDatum);
		return true;
	}
	if (oid == DuckdbUnionArrayOid()) {
		ConvertDuckToPostgresRuntimeArray(slot, value, col, DuckdbUnionOid(), ConvertDuckValueToStringDatum);
		return true;
	}
	if (oid == DuckdbMapArrayOid()) {
		ConvertDuckToPostgresRuntimeArray(slot, value, col, DuckdbMapOid(), ConvertDuckValueToStringDatum);
		return true;
	}
	return false;
}

bool
PgDuckDBConvertUnsupportedNumericToDouble() {
	return duckdb_convert_unsupported_numeric_to_double;
}

// Instance handed to the PostgresStorageExtension and the lib type-conversion
// entry points. `const` so its address is handed to lib without lifetime
// concerns; referenced storage is the static global below.
const TypeResolver g_pg_duckdb_type_resolver = {
    /* .postgres_to_duckdb                    = */ PgDuckDBPostgresToDuckDBType,
    /* .duckdb_to_postgres_oid                = */ PgDuckDBDuckDBToPostgresOid,
    /* .duckdb_to_postgres_array_oid          = */ PgDuckDBDuckDBToPostgresArrayOid,
    /* .convert_duck_value_to_postgres        = */ PgDuckDBConvertDuckValueToPostgres,
    /* .convert_unsupported_numeric_to_double = */ PgDuckDBConvertUnsupportedNumericToDouble,
};

PostgresStorageOptions
MakePgDuckDBStorageOptions() {
	PostgresStorageOptions opts;
	opts.threads_for_postgres_scan = duckdb_threads_for_postgres_scan;
	opts.max_workers_per_postgres_scan = duckdb_max_workers_per_postgres_scan;
	opts.log_pg_explain = duckdb_log_pg_explain;
	return opts;
}

} // anonymous namespace

const TypeResolver *
GetTypeResolver() {
	return &g_pg_duckdb_type_resolver;
}

// ------------------------------------------------------------------
// ddb::DidWrites -- consulted by the xact / executor hooks to decide
// whether a DuckDB write happened in the current statement.
// ------------------------------------------------------------------

namespace ddb {
bool
DidWrites() {
	if (!PgDuckDBManager::IsInitialized()) {
		return false;
	}

	auto connection = PgDuckDBManager::GetConnectionUnsafe();
	auto &context = *connection->context;
	return DidWrites(context);
}

bool
DidWrites(duckdb::ClientContext &context) {
	if (!context.transaction.HasActiveTransaction()) {
		return false;
	}
	return context.ActiveTransaction().ModifiedDatabase() != nullptr;
}
} // namespace ddb

// ------------------------------------------------------------------
// PgDuckDBManager
// ------------------------------------------------------------------

PgDuckDBManager PgDuckDBManager::manager_instance;

// pg_duckdb owns the full Initialize: builds a GUC-driven DBConfig and
// passes it to DuckDB("", &config). The base path uses DuckDB(nullptr) and
// is not parameterizable -- see lib header. ThreadSignalBlockGuard scopes
// the entire construction window so DuckDB worker threads don't inherit a
// signal mask that lets them steal signals from the Postgres main thread.
void
PgDuckDBManager::Initialize() {
	elog(DEBUG2, "(PGDuckDB/PgDuckDBManager) Creating DuckDB instance");

	pgduckdb::ThreadSignalBlockGuard guard;

	duckdb::DBConfig config;
	config.SetOptionByName("default_null_order", "postgres");

	// --- Pre-database-init (was hooks::pre_database_init). ---

	// Make sure directories provided in config exist.
	std::filesystem::create_directories(duckdb_temp_directory);
	std::filesystem::create_directories(duckdb_extension_directory);

	std::string user_agent = "pg_duckdb";
	if (!IsEmptyString(duckdb_custom_user_agent)) {
		user_agent += ", ";
		user_agent += duckdb_custom_user_agent;
	}
	const char *application_name = pg::GetConfigOption("application_name", true);
	if (!IsEmptyString(application_name)) {
		user_agent += ", ";
		user_agent += application_name;
	}
	config.SetOptionByName("custom_user_agent", user_agent);

	SET_DUCKDB_OPTION_FROM_GUC(allow_unsigned_extensions);
	SET_DUCKDB_OPTION_FROM_GUC(enable_external_access);
	SET_DUCKDB_OPTION_FROM_GUC(allow_community_extensions);
	SET_DUCKDB_OPTION_FROM_GUC(autoinstall_known_extensions);
	SET_DUCKDB_OPTION_FROM_GUC(autoload_known_extensions);
	SET_DUCKDB_OPTION_FROM_GUC(temp_directory);
	SET_DUCKDB_OPTION_FROM_GUC(extension_directory);

	if (duckdb_maximum_memory > 0) {
		// Convert the memory limit from MB (as set by Postgres GUC_UNIT_MB, which is actually MiB; see
		// memory_unit_conversion_table in guc.c) to a string with the "MiB" suffix, as required by DuckDB's memory
		// parser.
		std::string memory_limit = std::to_string(duckdb_maximum_memory) + "MiB";
		config.options.maximum_memory = duckdb::DBConfig::ParseMemoryLimit(memory_limit);
		elog(DEBUG2, "[PGDuckDB] Set DuckDB option: 'maximum_memory'=%dMB", duckdb_maximum_memory);
	}
	if (duckdb_max_temp_directory_size != NULL && strlen(duckdb_max_temp_directory_size) != 0) {
		config.SetOptionByName("max_temp_directory_size", duckdb_max_temp_directory_size);
		elog(DEBUG2, "[PGDuckDB] Set DuckDB option: 'max_temp_directory_size'=%s", duckdb_max_temp_directory_size);
	}
	if (duckdb_threads > -1) {
		SET_DUCKDB_OPTION_FROM_GUC(threads);
	}

	database = new duckdb::DuckDB("", &config);
	OnInitialized(*database);
}

void
PgDuckDBManager::OnInitialized(duckdb::DuckDB &db) {
	auto &dbconfig = duckdb::DBConfig::GetConfig(*db.instance);
	// Pass the provider (not a frozen snapshot) so scans pick up SET-at-runtime
	// changes to duckdb.log_pg_explain / threads_for_postgres_scan / etc.
	// v1.5: storage/optimizer extensions are registered via the new static
	// Register() methods; direct map/vector access was removed.
	duckdb::StorageExtension::Register(
	    dbconfig, "pgduckdb",
	    duckdb::make_shared_ptr<PostgresStorageExtension>(&MakePgDuckDBStorageOptions, &g_pg_duckdb_type_resolver));

	auto &extension_manager = db.instance->GetExtensionManager();
	auto extension_active_load = extension_manager.BeginLoad("pgduckdb");
	D_ASSERT(extension_active_load);
	duckdb::ExtensionInstallInfo extension_install_info;
	extension_active_load->FinishLoad(extension_install_info);

	connection = duckdb::make_uniq<duckdb::Connection>(db);

	auto &context = *connection->context;

	auto &db_manager = duckdb::DatabaseManager::Get(context);
	default_dbname = db_manager.GetDefaultDatabase(context);
	pgduckdb::DuckDBQueryOrThrow(context, "ATTACH DATABASE 'pgduckdb' (TYPE pgduckdb)");

	// --- Post-database-init (was hooks::post_database_init). ---

	// Register the unsupported type optimizer to run after all other optimizations.
	duckdb::OptimizerExtension::Register(dbconfig, UnsupportedTypeOptimizer::GetOptimizerExtension());

	std::string pg_time_zone(pg::GetConfigOption("TimeZone"));
	pgduckdb::DuckDBQueryOrThrow(context, "SET TimeZone =" + duckdb::KeywordHelper::WriteQuoted(pg_time_zone));
	pgduckdb::DuckDBQueryOrThrow(context, "SET default_collation =" +
	                                          duckdb::KeywordHelper::WriteQuoted(duckdb_default_collation));
	pgduckdb::DuckDBQueryOrThrow(context, "ATTACH DATABASE ':memory:' AS pg_temp;");

	if (duckdb_autoinstall_known_extensions) {
		InstallExtensions(context);
	}
	LoadExtensions(context);
}

void
PgDuckDBManager::OnReset() {
	connection = nullptr;
}

void
PgDuckDBManager::Reset() {
	manager_instance.DuckDBManager::Reset();
}

void
PgDuckDBManager::RefreshConnectionState(duckdb::ClientContext &context) {
	std::string disabled_filesystems = DisabledFileSystems();
	if (disabled_filesystems != "") {
		/*
		 * DuckDB does not allow us to disable this setting on the database
		 * after the first non-superuser connection; any further connections
		 * inherit this restriction. Shouldn't be a problem in practice.
		 */
		pgduckdb::DuckDBQueryOrThrow(context, "SET disabled_filesystems=" +
		                                          duckdb::KeywordHelper::WriteQuoted(disabled_filesystems));
	}

	if (strlen(duckdb_azure_transport_option_type) > 0) {
		pgduckdb::DuckDBQueryOrThrow(context, "SET azure_transport_option_type=" +
		                                          duckdb::KeywordHelper::WriteQuoted(duckdb_azure_transport_option_type));
	}

	const auto extensions_table_last_seq = GetSeqLastValue("extensions_table_seq");
	if (extensions_table_seq < extensions_table_last_seq) {
		LoadExtensions(context);
		extensions_table_seq = extensions_table_last_seq;
	}

	if (!secrets_valid) {
		DropSecrets(context);
		LoadSecrets(context);
		secrets_valid = true;
	}
}

/*
 * Creates a new connection to the global DuckDB instance. This should only be
 * used in some rare cases, where a temporary new connection is needed instead
 * of the global cached connection that is returned by GetConnection.
 */
duckdb::unique_ptr<duckdb::Connection>
PgDuckDBManager::CreateConnection() {
	pgduckdb::RequireDuckdbExecution();

	auto &instance = Get();
	auto connection = duckdb::make_uniq<duckdb::Connection>(instance.GetDatabase());
	auto &context = *connection->context;

	instance.RefreshConnectionState(context);

	return connection;
}

/* Returns the cached connection to the global DuckDB instance. */
duckdb::Connection *
PgDuckDBManager::GetConnection(bool force_transaction) {
	pgduckdb::RequireDuckdbExecution();

	auto &instance = Get();
	auto &context = *instance.connection->context;

	if (!context.transaction.HasActiveTransaction()) {
		if (IsSubTransaction()) {
			throw duckdb::NotImplementedException("SAVEPOINT and subtransactions are not supported in DuckDB");
		}

		if (force_transaction || pg::IsInTransactionBlock(false)) {
			/*
			 * We only want to open a new DuckDB transaction if we're already
			 * in a Postgres transaction block. Always opening a transaction
			 * incurs a significant performance penalty for single statement
			 * queries on cloud backends. So we only want to do this when
			 * actually necessary.
			 */
			instance.connection->BeginTransaction();
		}
	}

	instance.RefreshConnectionState(context);

	return instance.connection.get();
}

/*
 * Returns the cached connection to the global DuckDB instance, but does not do
 * any checks required to correctly initialize the DuckDB transaction nor
 * refreshes the secrets/extensions/etc. Only use this in rare cases where you
 * know for sure that the connection is already initialized correctly for the
 * current query, and you just want a pointer to it.
 */
duckdb::Connection *
PgDuckDBManager::GetConnectionUnsafe() {
	auto &instance = Get();
	return instance.connection.get();
}

/*
 * Called by ext code (GUC assign hooks, DDL event triggers) when the
 * duckdb.create_*_secret tables change. Next connection refresh will re-load.
 */
void
InvalidateDuckDBSecretsIfInitialized() {
	if (PgDuckDBManager::IsInitialized()) {
		PgDuckDBManager::Get().secrets_valid = false;
	}
}

// ------------------------------------------------------------------
// DuckDBQueryOrThrow(const std::string&) -- the "no connection argument"
// overload used by pg_duckdb call sites that want the current cached
// connection. Formerly in lib; moves here because it depends on
// PgDuckDBManager. The other two overloads (taking a context / connection
// ref) stay in lib.
// ------------------------------------------------------------------

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(const std::string &query) {
	auto connection = pgduckdb::PgDuckDBManager::GetConnection();
	return DuckDBQueryOrThrow(*connection, query);
}

} // namespace pgduckdb
