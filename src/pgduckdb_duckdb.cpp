#include "pgduckdb/pgduckdb_duckdb.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/hooks.hpp"
#include "pgduckdb/pg/transactions.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"

#include "pgduckdb/utility/signal_guard.hpp"

extern "C" {
#include "postgres.h"
}

namespace pgduckdb {

namespace ddb {
bool
DidWrites() {
	if (!DuckDBManager::IsInitialized()) {
		return false;
	}

	auto connection = DuckDBManager::GetConnectionUnsafe();
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

DuckDBManager DuckDBManager::manager_instance;

void
DuckDBManager::Initialize() {
	elog(DEBUG2, "(PGDuckDB/DuckDBManager) Creating DuckDB instance");

	// Block signals before initializing DuckDB to ensure signal is handled by the Postgres main thread only
	pgduckdb::ThreadSignalBlockGuard guard;

	duckdb::DBConfig config;
	config.SetOptionByName("custom_user_agent", "libpgduckdb");
	config.SetOptionByName("default_null_order", "postgres");

	// Consumer (e.g. pg_duckdb) fills in memory / thread / extension / user-
	// agent options. Lib only provides the Postgres-compat defaults above.
	hooks::pre_database_init(config);

	database = new duckdb::DuckDB(/*connection_string=*/"", &config);

	auto &dbconfig = duckdb::DBConfig::GetConfig(*database->instance);
	dbconfig.storage_extensions["pgduckdb"] = duckdb::make_uniq<PostgresStorageExtension>();

	auto &extension_manager = database->instance->GetExtensionManager();
	auto extension_active_load = extension_manager.BeginLoad("pgduckdb");
	D_ASSERT(extension_active_load);
	duckdb::ExtensionInstallInfo extension_install_info;
	extension_active_load->FinishLoad(extension_install_info);

	connection = duckdb::make_uniq<duckdb::Connection>(*database);

	auto &context = *connection->context;

	auto &db_manager = duckdb::DatabaseManager::Get(context);
	default_dbname = db_manager.GetDefaultDatabase(context);
	pgduckdb::DuckDBQueryOrThrow(context, "ATTACH DATABASE 'pgduckdb' (TYPE pgduckdb)");

	// Consumer runs INSTALL/LOAD, registers optimizer extensions, attaches
	// pg_temp, sets TimeZone / collation, etc.
	hooks::post_database_init(*database, *connection);
}

void
DuckDBManager::Reset() {
	manager_instance.connection = nullptr;
	delete manager_instance.database;
	manager_instance.database = nullptr;
}

/*
 * Creates a new connection to the global DuckDB instance. This should only be
 * used in some rare cases, where a temporary new connection is needed instead
 * of the global cached connection that is returned by GetConnection.
 */
duckdb::unique_ptr<duckdb::Connection>
DuckDBManager::CreateConnection() {
	hooks::require_execution_allowed();

	auto &instance = Get();
	auto connection = duckdb::make_uniq<duckdb::Connection>(*instance.database);
	auto &context = *connection->context;

	hooks::refresh_connection_state(context);

	return connection;
}

/* Returns the cached connection to the global DuckDB instance. */
duckdb::Connection *
DuckDBManager::GetConnection(bool force_transaction) {
	hooks::require_execution_allowed();

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

	hooks::refresh_connection_state(context);

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
DuckDBManager::GetConnectionUnsafe() {
	auto &instance = Get();
	return instance.connection.get();
}

} // namespace pgduckdb
