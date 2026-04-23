/*
 * pgducklake_hooks.hpp
 *
 * PostgreSQL hook registration and shared hook state for pg_ducklake.
 */

#pragma once

extern "C" {
struct Query;
struct PlannedStmt;
}

namespace pgducklake {
void InitHooks();

// Query-plan pipeline for ducklake SELECTs -- see pgducklake_query_plan.cpp.
// InitQueryPlan registers the DuckLakeScan CustomScan methods; must run
// once per backend from _PG_init.
void InitQueryPlan();

// If `parse` references any ducklake table, returns a PlannedStmt wrapping
// a DuckLakeScan that runs the whole query inside our DuckDB instance.
// Returns nullptr for queries that don't touch ducklake -- caller should
// fall through to the previous planner.
PlannedStmt *DuckLakePlanQuery(Query *parse, int cursor_options);
} // namespace pgducklake
