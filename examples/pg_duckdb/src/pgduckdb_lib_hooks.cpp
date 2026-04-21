// pg_duckdb's registration of libpgduckdb's extension points. The bodies here
// used to live inline inside DuckDBManager::Initialize / RefreshConnectionState
// in lib; moving them behind function-pointer hooks keeps lib free of
// pg_duckdb-specific GUC / catalog / secret knowledge.

#include <filesystem>

#include "duckdb.hpp"

#include "pgduckdb/hooks.hpp"
#include "pgduckdb/pg/guc.hpp"
#include "pgduckdb/pg/permissions.hpp"
#include "pgduckdb/pg/string_utils.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_extensions.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_secrets_helper.hpp"
#include "pgduckdb/pgduckdb_unsupported_type_optimizer.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

extern "C" {
#include "postgres.h"

#include "catalog/namespace.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
}

namespace pgduckdb {

// State that used to hang off DuckDBManager. Per-backend, invalidated on
// recycle via InvalidateDuckDBSecretsIfInitialized (below).
namespace {

bool g_secrets_valid = false;
int64_t g_extensions_table_seq = 0;

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

#define SET_DUCKDB_OPTION_FROM_GUC(ddb_option_name)                                                                    \
	config.options.ddb_option_name = duckdb_##ddb_option_name;                                                         \
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

void
PreDatabaseInit(duckdb::DBConfig &config) {
	// Make sure directories provided in config exist.
	std::filesystem::create_directories(duckdb_temporary_directory);
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
	SET_DUCKDB_OPTION_FROM_GUC(temporary_directory);
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
	if (duckdb_maximum_threads > -1) {
		SET_DUCKDB_OPTION_FROM_GUC(maximum_threads);
	}
}

void
PostDatabaseInit(duckdb::DuckDB &db, duckdb::Connection &conn) {
	auto &dbconfig = duckdb::DBConfig::GetConfig(*db.instance);
	// Register the unsupported type optimizer to run after all other optimizations.
	dbconfig.optimizer_extensions.push_back(UnsupportedTypeOptimizer::GetOptimizerExtension());

	auto &context = *conn.context;
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
RefreshConnectionState(duckdb::ClientContext &context) {
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
	if (g_extensions_table_seq < extensions_table_last_seq) {
		LoadExtensions(context);
		g_extensions_table_seq = extensions_table_last_seq;
	}

	if (!g_secrets_valid) {
		DropSecrets(context);
		LoadSecrets(context);
		g_secrets_valid = true;
	}
}

void
RequireExecutionAllowed() {
	pgduckdb::RequireDuckdbExecution();
}

Oid
HookRowOid() {
	return DuckdbRowOid();
}
Oid
HookStructOid() {
	return DuckdbStructOid();
}
Oid
HookUnionOid() {
	return DuckdbUnionOid();
}
Oid
HookMapOid() {
	return DuckdbMapOid();
}
Oid
HookUnresolvedTypeOid() {
	return DuckdbUnresolvedTypeOid();
}
Oid
HookStructArrayOid() {
	return DuckdbStructArrayOid();
}
Oid
HookUnionArrayOid() {
	return DuckdbUnionArrayOid();
}
Oid
HookMapArrayOid() {
	return DuckdbMapArrayOid();
}

int
HookThreadsForPostgresScan() {
	return duckdb_threads_for_postgres_scan;
}
int
HookMaxWorkersPerPostgresScan() {
	return duckdb_max_workers_per_postgres_scan;
}
bool
HookLogPgExplain() {
	return duckdb_log_pg_explain;
}
bool
HookConvertUnsupportedNumericToDouble() {
	return duckdb_convert_unsupported_numeric_to_double;
}

} // namespace

/*
 * Wires every libpgduckdb hook to its pg_duckdb body. Call from _PG_init
 * before any code reaches DuckDBManager::Get.
 */
void
RegisterLibHooks() {
	hooks::pre_database_init = PreDatabaseInit;
	hooks::post_database_init = PostDatabaseInit;
	hooks::refresh_connection_state = RefreshConnectionState;
	hooks::require_execution_allowed = RequireExecutionAllowed;

	hooks::duckdb_row_oid = HookRowOid;
	hooks::duckdb_struct_oid = HookStructOid;
	hooks::duckdb_union_oid = HookUnionOid;
	hooks::duckdb_map_oid = HookMapOid;
	hooks::duckdb_unresolved_type_oid = HookUnresolvedTypeOid;
	hooks::duckdb_struct_array_oid = HookStructArrayOid;
	hooks::duckdb_union_array_oid = HookUnionArrayOid;
	hooks::duckdb_map_array_oid = HookMapArrayOid;

	hooks::threads_for_postgres_scan = HookThreadsForPostgresScan;
	hooks::max_workers_per_postgres_scan = HookMaxWorkersPerPostgresScan;
	hooks::log_pg_explain = HookLogPgExplain;
	hooks::convert_unsupported_numeric_to_double = HookConvertUnsupportedNumericToDouble;
}

/*
 * Called by ext code (GUC assign hooks, DDL event triggers) when the
 * duckdb.create_*_secret tables change. Next connection refresh will re-load.
 */
void
InvalidateDuckDBSecretsIfInitialized() {
	if (DuckDBManager::IsInitialized()) {
		g_secrets_valid = false;
	}
}

} // namespace pgduckdb
