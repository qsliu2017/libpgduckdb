#pragma once

/*
 * pg_duckdb's DeparseRoutine: overrides the libpgduckdb default deparse
 * callbacks with pg_duckdb-specific behaviour -- pseudo-type handling
 * (duckdb.row, duckdb.unresolved_type, duckdb.json), "SELECT *"
 * reconstruction, USING duckdb table-am schema rewriting, duckdb.view
 * subquery replacement, etc.
 *
 * Callers that previously invoked pgduckdb_get_querydef(query) should now
 * call pgduckdb_get_querydef(query, &pgduckdb::pg_duckdb_deparse_routine).
 *
 * The DDL deparse entries (CREATE/ALTER/RENAME TABLE, CREATE VIEW) used
 * to live in lib's pgduckdb_ruleutils.cpp -- they're ext-only and now
 * live here too.
 */

#include "pgduckdb/pgduckdb_ruleutils.h"

namespace pgduckdb {
extern const DeparseRoutine pg_duckdb_deparse_routine;
} // namespace pgduckdb

char *pgduckdb_get_tabledef(Oid relation_id);
char *pgduckdb_get_alter_tabledef(Oid relation_oid, AlterTableStmt *alter_stmt);
char *pgduckdb_get_rename_relationdef(Oid relation_oid, RenameStmt *rename_stmt);
char *pgduckdb_get_viewdef(const ViewStmt *stmt, const char *postgres_schema_name, const char *view_name,
                           const char *duckdb_query_string);
