#pragma once

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
};

} // namespace pgduckdb
