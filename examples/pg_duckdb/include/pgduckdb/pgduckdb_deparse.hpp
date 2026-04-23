#pragma once

/*
 * pg_duckdb's DeparseRoutine: overrides the libpgduckdb default deparse
 * callbacks with pg_duckdb-specific behaviour -- pseudo-type handling
 * (duckdb.row, duckdb.unresolved_type, duckdb.json), "SELECT *"
 * reconstruction, USING duckdb table-am schema rewriting, duckdb.view
 * subquery replacement, and DDL-emit callbacks (table AM name,
 * column-type validation, AM ownership assertion).
 *
 * Callers invoke pgduckdb_get_querydef / pgduckdb_get_tabledef /
 * pgduckdb_get_alter_tabledef / pgduckdb_get_rename_relationdef with
 * &pgduckdb::pg_duckdb_deparse_routine. CREATE VIEW deparse still lives
 * here because it wraps an already-deparsed query string.
 */

#include "pgduckdb/pgduckdb_ruleutils.h"

namespace pgduckdb {
extern const DeparseRoutine pg_duckdb_deparse_routine;
} // namespace pgduckdb

char *pgduckdb_get_viewdef(const ViewStmt *stmt, const char *postgres_schema_name, const char *view_name,
                           const char *duckdb_query_string);
