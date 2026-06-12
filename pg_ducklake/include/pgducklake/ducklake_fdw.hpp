#pragma once

/*
 * ducklake_fdw.hpp -- FDW for DuckLake tables.  PostgreSQL-backed catalogs
 * support full DML; frozen HTTP-hosted snapshots are read-only.  Queries are
 * routed through DuckDB.
 */

struct Query;

namespace pgducklake {
void RegisterForeignTablesInQuery(Query *query);
bool QueryReferencesDucklakeForeignTable(Query *query);
} // namespace pgducklake
