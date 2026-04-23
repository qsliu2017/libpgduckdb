/*
 * pgducklake_duckdb_query.cpp -- Run DuckDB queries against pg_ducklake's
 * own backend-local DuckDB instance.
 *
 * @scope backend: cached connection, last_error thread_local
 *
 * Upstream pg_ducklake delegated to pg_duckdb's duckdb.raw_query() UDF via
 * SPI. Under libpgduckdb we own the instance directly (see
 * pgducklake_contracts.cpp::EnsureDuckDBInitialized) and run the query
 * against a duckdb::Connection allocated here -- no SPI, no pg_duckdb.so
 * dependency.
 */

// DuckDB headers must parse before postgres.h -- PG's elog.h #defines FATAL,
// which collides with duckdb::ExceptionType::FATAL.
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_guc.hpp"

#include <memory>
#include <string>

extern "C" {
#include "postgres.h"

#include "utils/elog.h"
}

namespace pgducklake {

// EnsureDuckDBInitialized is defined in pgducklake_contracts.cpp.
duckdb::DuckDB &EnsureDuckDBInitialized();

int
ExecuteDuckDBQuery(const char *query, const char **errmsg_out) {
	static thread_local std::string last_error;

	try {
		auto &db = EnsureDuckDBInitialized();
		duckdb::Connection conn(db);
		auto result = conn.Query(query);
		if (result->HasError()) {
			last_error = result->GetError();
			if (errmsg_out) {
				*errmsg_out = last_error.c_str();
			}
			return 1;
		}
		return 0;
	} catch (const duckdb::Exception &ex) {
		last_error = ex.what();
		if (errmsg_out) {
			*errmsg_out = last_error.c_str();
		}
		return 1;
	} catch (const std::exception &ex) {
		last_error = ex.what();
		if (errmsg_out) {
			*errmsg_out = last_error.c_str();
		}
		return 1;
	}
}

void
SyncDefaultTablePathToDuckDB() {
	if (default_table_path && default_table_path[0] != '\0') {
		std::string set_query =
		    "SET ducklake_default_table_path = " + duckdb::KeywordHelper::WriteQuoted(std::string(default_table_path));
		const char *errmsg = nullptr;
		if (ExecuteDuckDBQuery(set_query.c_str(), &errmsg) != 0) {
			elog(WARNING, "failed to sync ducklake.default_table_path to DuckDB: %s", errmsg ? errmsg : "unknown");
		}
	}
}

} // namespace pgducklake
