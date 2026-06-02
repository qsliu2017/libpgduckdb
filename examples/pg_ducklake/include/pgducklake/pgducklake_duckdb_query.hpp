#pragma once

/*
 * pgducklake_duckdb_query.hpp — DuckDB GUC sync helper.
 *
 * DuckDB query execution lives in pgducklake_duckdb.hpp
 * (DuckDBQueryOrThrow).
 */

namespace pgducklake {

/*
 * Sync the ducklake.default_table_path PG GUC to DuckDB's
 * ducklake_default_table_path extension option.  No-op when empty.
 */
void SyncDefaultTablePathToDuckDB();

} // namespace pgducklake
