#pragma once

// pg_duckdb's PgDuckDBManager. Subclass of libpgduckdb's slim
// pgduckdb::DuckDBManager (lazy-singleton + 3 virtual hooks). This file owns
// pg_duckdb's specifics: cached connection, GUC-driven DBConfig, secret /
// extension / optimizer installation, per-connection refresh of secrets /
// extensions / disabled filesystems.
//
// Lifecycle history: previously split into lib's pgduckdb_duckdb.{cpp,hpp} +
// ext's pgduckdb_lib_hooks.cpp; the inversion folded both into ext. The
// current shape factors out only the lazy-singleton nucleus into lib so
// pg_ducklake can subclass too, while pg_duckdb keeps its full lifecycle here.

#include "pgduckdb/pgduckdb_duckdb_manager.hpp"

#include "duckdb.hpp"

namespace pgduckdb {

namespace ddb {
bool DidWrites();
bool DidWrites(duckdb::ClientContext &context);
} // namespace ddb

class PgDuckDBManager : public DuckDBManager {
public:
	static inline bool
	IsInitialized() {
		return manager_instance.DuckDBManager::IsInitialized();
	}

	static inline PgDuckDBManager &
	Get() {
		(void)manager_instance.GetDatabase(); // lazy-init through the base
		return manager_instance;
	}

	static duckdb::unique_ptr<duckdb::Connection> CreateConnection();
	static duckdb::Connection *GetConnection(bool force_transaction = false);
	static duckdb::Connection *GetConnectionUnsafe();

	inline const std::string &
	GetDefaultDBName() const {
		return default_dbname;
	}

	static void Reset();

protected:
	// Override Initialize() so we can build a GUC-driven DBConfig and pass it
	// to duckdb::DuckDB(""). The base's Initialize uses DuckDB(nullptr) which
	// doesn't take a config; passing one stack-locally would dangle (see
	// pgduckdb_duckdb_manager.hpp).
	void Initialize() override;
	void OnInitialized(duckdb::DuckDB &db) override;
	void OnReset() override;

private:
	PgDuckDBManager() = default;

	PgDuckDBManager(const PgDuckDBManager &) = delete;
	PgDuckDBManager &operator=(const PgDuckDBManager &) = delete;

	static PgDuckDBManager manager_instance;

	// Refreshes per-connection state (secrets, extensions, disabled
	// filesystems, azure transport option). Called at the top of
	// CreateConnection / GetConnection.
	void RefreshConnectionState(duckdb::ClientContext &context);

	duckdb::unique_ptr<duckdb::Connection> connection;
	std::string default_dbname = "<!UNSET!>";

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
