// Slim base class definitions; the heavy lifting lives in the subclass
// hooks (PgDuckDBManager in pg_duckdb, DuckLakeManager in pg_ducklake).

#include "pgduckdb/pgduckdb_duckdb_manager.hpp"

#include "duckdb/main/database.hpp"

namespace pgduckdb {

duckdb::DuckDB &
DuckDBManager::GetDatabase() {
	if (!database) {
		Initialize();
	}
	return *database;
}

void
DuckDBManager::Initialize() {
	// Construct with nullptr-config (matches pg_ducklake's pre-refactor path).
	// Passing a stack-local `&DBConfig{}` causes DuckLake's catalog init to
	// hang -- DBConfig's encoding/arrow/type subsystems capture `*this` by
	// reference and dangle when the stack object goes out of scope.
	database = new duckdb::DuckDB(nullptr);
	OnInitialized(*database);
}

void
DuckDBManager::Reset() {
	OnReset();
	delete database;
	database = nullptr;
}

void
DuckDBManager::OnInitialized(duckdb::DuckDB &) {
}

void
DuckDBManager::OnReset() {
}

} // namespace pgduckdb
