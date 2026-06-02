/*
 * pgducklake_duckdb_query.cpp -- Execute DuckDB queries against libpgddb.
 *
 * @scope backend: last_error thread_local
 *
 * Drives DuckDB queries directly through libpgddb's DuckDBManager
 * connection. The upstream design routed every query through
 * pg_duckdb's duckdb.raw_query() UDF (PG SPI -> pg_duckdb's planner ->
 * DuckDB) so pg_ducklake could stay PG-only; the libpgddb consumer
 * model gives us the connection in process, so we use it directly.
 *
 * Used by DDL triggers, VACUUM, freeze, FDW attach, and the utility
 * hook.
 */

#include <duckdb/common/string_util.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/parser/keyword_helper.hpp>

#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_guc.hpp"

#include <string>

extern "C" {
#include "postgres.h"

#include "utils/elog.h"
}

namespace pgducklake {

void SyncDefaultTablePathToDuckDB() {
  if (default_table_path && default_table_path[0] != '\0') {
    std::string set_query =
        "SET ducklake_default_table_path = " + duckdb::KeywordHelper::WriteQuoted(std::string(default_table_path));
    try {
      DuckDBQueryOrThrow(set_query);
    } catch (const std::exception &e) {
      elog(WARNING, "failed to sync ducklake.default_table_path to DuckDB: %s", DuckDBErrorMessage(e).c_str());
    }
  }
}

} // namespace pgducklake
