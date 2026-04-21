/*-------------------------------------------------------------------------
 *
 * pgduckdb_ruleutils.h
 *		Public interface for libpgduckdb's Postgres-Query-tree ->
 *		DuckDB-SQL deparse machinery.
 *
 * The deparse machinery is a fork of Postgres' src/backend/utils/adt/ruleutils.c
 * (vendored in src/vendor/pg_ruleutils_<N>.c, one per supported PG major).
 * pg_duckdb-specific behaviour is injected via a struct-of-function-pointers
 * extension point, `DeparseRoutine`, modeled after PG-native FdwRoutine /
 * TableAmRoutine. A consumer fills in any fields it wants to override and
 * passes the struct to `pgduckdb_get_querydef`; fields left NULL fall back
 * to lib defaults (plain Postgres -> DuckDB).
 *
 * This header is #include-able from both C (the vendored pg_ruleutils_*.c
 * translation units) and C++ (the dispatchers and ext callbacks).
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "lib/stringinfo.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Context object threaded through get_target_list() while the walker is
 * reconstructing a "SELECT *" that Postgres has already expanded into a list
 * of Vars. Populated and consulted by the `reconstruct_star_step` hook.
 */
typedef struct StarReconstructionContext {
	List *target_list;
	int varno_star;
	int varattno_star;
	bool added_current_star;
} StarReconstructionContext;

/*
 * Extension points for pgduckdb_get_querydef. Mirrors the PG-native
 * FdwRoutine pattern: a consumer fills a DeparseRoutine and passes a
 * pointer per call. All fields are optional; any NULL falls back to the
 * lib default. A NULL `routine` passed to an entry point is equivalent
 * to &PGDUCKDB_DEFAULT_DEPARSE_ROUTINE (plain Postgres -> DuckDB).
 *
 * Callbacks are expected to be pure with respect to deparse state (the
 * walker owns that). They may read syscache.
 */
typedef struct DeparseRoutine {
	/* Qualified name for relid. NULL -> lib default ("schema"."table"). */
	char *(*relation_name)(Oid relid);

	/* Qualified name for funcid; *use_variadic_p set if VARIADIC.
	 * NULL -> lib default (return NULL; walker uses format_procedure_qualified). */
	char *(*function_name)(Oid funcid, bool *use_variadic_p);

	/* Map PG (schema, table_am_name) -> DuckDB (db, schema) as a 2-cstring
	 * List. NULL -> lib default: "public" -> {DuckDBManager default db,
	 * "main"}; other schemas -> {default db, pg_schema}. The ext
	 * overrides this to add pg_temp handling. */
	List *(*db_and_schema)(const char *pg_schema, const char *table_am_name);

	/* Pseudo-type predicates. NULL -> always false. */
	bool (*is_duckdb_row_type)(Oid);
	bool (*is_unresolved_type)(Oid);
	bool (*is_fake_type)(Oid);
	bool (*var_is_duckdb_row)(Var *);
	bool (*func_returns_duckdb_row)(RangeTblFunction *);
	Var *(*duckdb_subscript_var)(Expr *);
	SubscriptingRef *(*strip_first_subscript)(SubscriptingRef *, StringInfo);
	char *(*write_row_refname)(StringInfo, char *refname, bool is_top_level);
	bool (*subscript_has_custom_alias)(Plan *, List *rtable, Var *, char *colname);

	/* SELECT * expansion reconstruction. NULL -> false. */
	bool (*reconstruct_star_step)(StarReconstructionContext *, ListCell *);

	/* Replace subquery RTE with a view reference. NULL -> false. */
	bool (*replace_subquery_with_view)(Query *, StringInfo);

	/* Misc emit tweaks. */
	bool (*is_not_default_expr)(Node *, void *ctx);			   /* NULL -> always true */
	int (*show_type)(Const *, int original_showtype);		   /* NULL -> return original */
	void (*add_tablesample_percent)(const char *tsm_name, StringInfo, int num_args); /* NULL -> PG default */
} DeparseRoutine;

extern const DeparseRoutine PGDUCKDB_DEFAULT_DEPARSE_ROUTINE;

/*
 * The one lib-level public entry. Deparse a parsed Postgres Query tree
 * back to a SQL string DuckDB can parse. `routine` may be NULL (->
 * defaults).
 */
extern char *pgduckdb_get_querydef(Query *query, const DeparseRoutine *routine);

/*
 * General deparse state for the vendored ruleutils walker: true at the
 * outermost query, false inside subqueries. Set/restored by
 * pgduckdb_get_querydef.
 */
extern bool outermost_query;

/*
 * Scope a DeparseRoutine for the duration of `body()`. Used by ext when
 * it invokes the vendored deparse machinery via paths other than
 * pgduckdb_get_querydef -- e.g. DDL emit routines that compose pieces of
 * deparse output themselves.
 *
 * Behaves like pgduckdb_get_querydef wrt save/restore. `routine` may be
 * NULL.
 */
extern char *pgduckdb_deparse_with_routine(const DeparseRoutine *routine, char *(*body)(void *arg), void *arg);

/*
 * Legacy callback symbols used by the vendored pg_ruleutils_*.c walker.
 * These are dispatchers: consult the currently-scoped DeparseRoutine,
 * fall back to lib defaults. Do NOT call directly from new code; use
 * pgduckdb_get_querydef with a DeparseRoutine instead. They remain
 * exported only because the vendored walker calls them.
 */
extern char *pgduckdb_relation_name(Oid relid);
extern char *pgduckdb_function_name(Oid funcid, bool *use_variadic_p);
extern bool pgduckdb_is_not_default_expr(Node *node, void *context);
extern List *pgduckdb_db_and_schema(const char *pg_schema, const char *table_am_name);
extern const char *pgduckdb_db_and_schema_string(const char *pg_schema, const char *table_am_name);
extern bool pgduckdb_is_duckdb_row(Oid);
extern bool pgduckdb_is_unresolved_type(Oid);
extern bool pgduckdb_is_fake_type(Oid);
extern bool pgduckdb_var_is_duckdb_row(Var *);
extern bool pgduckdb_func_returns_duckdb_row(RangeTblFunction *);
extern Var *pgduckdb_duckdb_subscript_var(Expr *);
extern bool pgduckdb_reconstruct_star_step(StarReconstructionContext *, ListCell *);
extern bool pgduckdb_replace_subquery_with_view(Query *, StringInfo);
extern int pgduckdb_show_type(Const *, int original_showtype);
extern bool pgduckdb_subscript_has_custom_alias(Plan *, List *rtable, Var *, char *colname);
extern SubscriptingRef *pgduckdb_strip_first_subscript(SubscriptingRef *, StringInfo);
extern char *pgduckdb_write_row_refname(StringInfo, char *refname, bool is_top_level);
extern void pgduckdb_add_tablesample_percent(const char *tsm_name, StringInfo, int num_args);

/*
 * Shared helpers used by both the vendored walker and ext callbacks.
 */
extern bool is_system_sampling(const char *tsm_name, int num_args);
extern bool is_bernoulli_sampling(const char *tsm_name, int num_args);

#ifdef __cplusplus
} /* extern "C" */
#endif
