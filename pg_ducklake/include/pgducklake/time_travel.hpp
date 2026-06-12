#pragma once

/* time_travel.hpp -- DuckDB table function time_travel(table_name,
 * version/timestamp) for querying DuckLake tables at historical snapshots. */

#include "duckdb/function/function_set.hpp"

namespace pgducklake {

duckdb::TableFunctionSet GetTimeTravelFunctions();

} // namespace pgducklake
