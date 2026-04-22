#pragma once

// pg_duckdb's DuckDBManager. Owns the single duckdb::DuckDB instance + the
// cached connection, plus all pg_duckdb-specific lifecycle glue (GUC-driven
// DBConfig, secret / extension / optimizer installation, per-connection
// refresh of secrets / extensions / disabled filesystems).
//
// Previously split into: lib's pgduckdb_duckdb.{cpp,hpp} (the class body) +
// ext's pgduckdb_lib_hooks.cpp (the lifecycle callbacks). The inversion
// folds both back together here, so libpgduckdb_core.a carries no
// DuckDBManager symbols and no hooks function pointers.

#include "duckdb.hpp"

namespace pgduckdb {

namespace ddb {
bool DidWrites();
bool DidWrites(duckdb::ClientContext &context);
} // namespace ddb

class DuckDBManager {
public:
	static inline bool
	IsInitialized() {
		return manager_instance.database != nullptr;
	}

	static inline DuckDBManager &
	Get() {
		if (!manager_instance.database) {
			manager_instance.Initialize();
		}
		return manager_instance;
	}

	static duckdb::unique_ptr<duckdb::Connection> CreateConnection();
	static duckdb::Connection *GetConnection(bool force_transaction = false);
	static duckdb::Connection *GetConnectionUnsafe();

	inline const std::string &
	GetDefaultDBName() const {
		return default_dbname;
	}

	inline duckdb::DuckDB &
	GetDatabase() {
		return *database;
	}

	static void Reset();

private:
	DuckDBManager() : database(nullptr), connection(nullptr), default_dbname("<!UNSET!>") {
	}

	DuckDBManager(const DuckDBManager &) = delete;
	DuckDBManager &operator=(const DuckDBManager &) = delete;

	static DuckDBManager manager_instance;

	void Initialize();

	// Refreshes per-connection state (secrets, extensions, disabled
	// filesystems, azure transport option). Called at the top of
	// CreateConnection / GetConnection.
	void RefreshConnectionState(duckdb::ClientContext &context);

	/*
	 * FIXME: Use a unique_ptr instead of a raw pointer. For now this is not
	 * possible because some DuckDB extensions (historically MotherDuck) could
	 * cause an ABORT when the DuckDB database's destructor runs at process
	 * exit, crashing Postgres. Not running the destructor also doesn't really
	 * have any downsides since the process is going to die anyway. It's even
	 * slightly more efficient not to run the destructor at all. But we should
	 * still fix this, because running the destructor is a good way to find
	 * bugs (such as the one originally reported in duckdb/pg_duckdb#279).
	 */
	duckdb::DuckDB *database;
	duckdb::unique_ptr<duckdb::Connection> connection;
	std::string default_dbname;

	// Per-backend state that tracks whether the cached connection's
	// duckdb_secrets / extensions are up-to-date. RefreshConnectionState
	// drops+reloads secrets when `secrets_valid` flips to false, and
	// re-runs LOAD extensions when the `duckdb.extensions_table_seq`
	// sequence advances past `extensions_table_seq`.
	bool secrets_valid = false;
	int64_t extensions_table_seq = 0;

	// Lets the InvalidateDuckDBSecretsIfInitialized free function flip
	// secrets_valid back to false.
	friend void InvalidateDuckDBSecretsIfInitialized();
};

/*
 * Called by ext code (GUC assign hooks, DDL event triggers) when the
 * duckdb.create_*_secret tables change. Forces the next connection
 * refresh to drop+reload secrets.
 */
void InvalidateDuckDBSecretsIfInitialized();

struct TypeResolver;

/*
 * The pg_duckdb-specific resolver for DuckDB <-> Postgres type mapping,
 * covering the `duckdb.struct`/`duckdb.union`/`duckdb.map` pseudo-types
 * (plus their array variants). Ext code that calls lib type-conversion
 * entry points (GetPostgresDuckDBType / GetPostgresArrayDuckDBType /
 * ConvertDuckToPostgresValue / etc.) must pass this resolver so lib can
 * round-trip composite types.
 */
const TypeResolver *GetTypeResolver();

} // namespace pgduckdb
