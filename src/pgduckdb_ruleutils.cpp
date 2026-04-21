/*-------------------------------------------------------------------------
 *
 * pgduckdb_ruleutils.cpp
 *		Lib-side deparse glue: defaults + dispatchers for DeparseRoutine,
 *		plus the single public entry (pgduckdb_get_querydef) and the
 *		routine-scoping helper (pgduckdb_deparse_with_routine).
 *
 * The heavy lifting is done by the vendored walker in
 * src/vendor/pg_ruleutils_<N>.c. That walker calls a handful of
 * pgduckdb_*() hook functions which, in this file, dispatch through a
 * thread-local-ish `current_routine` pointer to either a caller-supplied
 * DeparseRoutine or the lib defaults. Consumers that want pg_duckdb's
 * pseudo-type / "SELECT *" / duckdb.row behaviour pass their own
 * DeparseRoutine (see examples/pg_duckdb/src/pgduckdb_deparse.cpp).
 *
 * Also hosts the LIKE-escape helpers (pg_duckdb_get_oper_expr_*) since
 * they're consumer-agnostic DuckDB dialect adjustments.
 *
 *-------------------------------------------------------------------------
 */
#include "duckdb.hpp"
#include "pgduckdb/pg/string_utils.hpp"

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
#include "pgduckdb/pgduckdb_ruleutils.h"
#include "pgduckdb/vendor/pg_list.hpp"
}

extern "C" {

/*
 * Tracks whether the walker is currently emitting the outermost query.
 * Consulted by get_target_list() to decide whether "SELECT *" should be
 * reconstructed with "r.*" for duckdb.row targets. Saved/restored around
 * every pgduckdb_get_querydef / pgduckdb_deparse_with_routine call.
 */
bool outermost_query = true;

/*
 * The DeparseRoutine in effect for the current deparse. Defaults to the
 * lib-only no-op routine; pgduckdb_get_querydef / pgduckdb_deparse_with_routine
 * swap it in and restore on exit.
 */
static const DeparseRoutine *current_routine = nullptr;

/* ------------------------------------------------------------------
 * Lib defaults -- plain Postgres -> DuckDB deparse with no knowledge of
 * pg_duckdb's pseudo-types, duckdb.row, or "USING duckdb" table AM. Each
 * default is the safe fallback when a DeparseRoutine leaves a field NULL.
 * ------------------------------------------------------------------ */

static char *
DefaultRelationName(Oid relation_oid) {
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relation_oid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relation_oid);
	Form_pg_class relation = (Form_pg_class)GETSTRUCT(tp);
	const char *relname = NameStr(relation->relname);
	const char *schemaname = get_namespace_name(relation->relnamespace);
	char *result = psprintf("%s.%s", quote_identifier(schemaname ? schemaname : "public"), quote_identifier(relname));
	ReleaseSysCache(tp);
	return result;
}

static char *
DefaultFunctionName(Oid /* funcid */, bool * /* use_variadic_p */) {
	/* Returning NULL tells the walker to fall through to format_procedure_qualified. */
	return nullptr;
}

/*
 * Default db resolver -- called when a DeparseRoutine leaves
 * default_database_name NULL. The lib-only name is "memory", matching
 * DuckDB's in-memory instance. The ext overrides this to report its
 * attached 'pgduckdb' database name via DuckDBManager.
 */
static const char *
DefaultDefaultDatabaseName(void) {
	return "memory";
}

static inline const DeparseRoutine *ActiveRoutine(void);

static List *
DefaultDbAndSchema(const char *pg_schema, const char * /* table_am_name */) {
	const DeparseRoutine *r = ActiveRoutine();
	const char *db_name = (r && r->default_database_name) ? r->default_database_name() : DefaultDefaultDatabaseName();
	if (pg_schema && strcmp(pg_schema, "public") == 0) {
		return list_make2((void *)db_name, (void *)"main");
	}
	return list_make2((void *)db_name, (void *)(pg_schema ? pg_schema : "main"));
}

static bool
DefaultFalseTypeOid(Oid /* type_oid */) {
	return false;
}

static bool
DefaultFalseVar(Var * /* var */) {
	return false;
}

static bool
DefaultFalseRtFunc(RangeTblFunction * /* rtfunc */) {
	return false;
}

static Var *
DefaultNullExpr(Expr * /* expr */) {
	return nullptr;
}

static SubscriptingRef *
DefaultStripFirstSubscript(SubscriptingRef *sbsref, StringInfo /* buf */) {
	return sbsref;
}

static char *
DefaultWriteRowRefname(StringInfo buf, char *refname, bool /* is_top_level */) {
	appendStringInfoString(buf, quote_identifier(refname));
	return refname;
}

static bool
DefaultSubscriptHasCustomAlias(Plan * /* plan */, List * /* rtable */, Var * /* subscript_var */, char * /* colname */) {
	return false;
}

static bool
DefaultReconstructStarStep(StarReconstructionContext * /* ctx */, ListCell * /* tle_cell */) {
	return false;
}

static bool
DefaultReplaceSubqueryWithView(Query * /* query */, StringInfo /* buf */) {
	return false;
}

static bool
DefaultIsNotDefaultExpr(Node * /* node */, void * /* context */) {
	return true;
}

static int
DefaultShowType(Const * /* constval */, int original_showtype) {
	return original_showtype;
}

static void
DefaultAddTablesamplePercent(const char * /* tsm_name */, StringInfo /* buf */, int /* num_args */) {
	/* PG native tablesample has no trailing percent. */
}

const DeparseRoutine PGDUCKDB_DEFAULT_DEPARSE_ROUTINE = {
    /* .default_database_name      = */ DefaultDefaultDatabaseName,
    /* .relation_name              = */ DefaultRelationName,
    /* .function_name              = */ DefaultFunctionName,
    /* .db_and_schema              = */ DefaultDbAndSchema,
    /* .is_duckdb_row_type         = */ DefaultFalseTypeOid,
    /* .is_unresolved_type         = */ DefaultFalseTypeOid,
    /* .is_fake_type               = */ DefaultFalseTypeOid,
    /* .var_is_duckdb_row          = */ DefaultFalseVar,
    /* .func_returns_duckdb_row    = */ DefaultFalseRtFunc,
    /* .duckdb_subscript_var       = */ DefaultNullExpr,
    /* .strip_first_subscript      = */ DefaultStripFirstSubscript,
    /* .write_row_refname          = */ DefaultWriteRowRefname,
    /* .subscript_has_custom_alias = */ DefaultSubscriptHasCustomAlias,
    /* .reconstruct_star_step      = */ DefaultReconstructStarStep,
    /* .replace_subquery_with_view = */ DefaultReplaceSubqueryWithView,
    /* .is_not_default_expr        = */ DefaultIsNotDefaultExpr,
    /* .show_type                  = */ DefaultShowType,
    /* .add_tablesample_percent    = */ DefaultAddTablesamplePercent,
};

/* ------------------------------------------------------------------
 * Dispatchers. The vendored pg_ruleutils_<N>.c walker calls these; each
 * forwards to the current DeparseRoutine field if non-NULL, else to the
 * default implementation.
 * ------------------------------------------------------------------ */

static inline const DeparseRoutine *
ActiveRoutine() {
	return current_routine ? current_routine : &PGDUCKDB_DEFAULT_DEPARSE_ROUTINE;
}

char *
pgduckdb_relation_name(Oid relid) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->relation_name) {
		char *result = r->relation_name(relid);
		if (result)
			return result;
	}
	return DefaultRelationName(relid);
}

char *
pgduckdb_function_name(Oid funcid, bool *use_variadic_p) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->function_name)
		return r->function_name(funcid, use_variadic_p);
	return DefaultFunctionName(funcid, use_variadic_p);
}

List *
pgduckdb_db_and_schema(const char *pg_schema, const char *table_am_name) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->db_and_schema) {
		List *result = r->db_and_schema(pg_schema, table_am_name);
		if (result)
			return result;
	}
	return DefaultDbAndSchema(pg_schema, table_am_name);
}

const char *
pgduckdb_db_and_schema_string(const char *pg_schema, const char *table_am_name) {
	List *db_and_schema = pgduckdb_db_and_schema(pg_schema, table_am_name);
	const char *db_name = (const char *)linitial(db_and_schema);
	const char *schema_name = (const char *)lsecond(db_and_schema);
	return psprintf("%s.%s", quote_identifier(db_name), quote_identifier(schema_name));
}

bool
pgduckdb_is_duckdb_row(Oid type_oid) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->is_duckdb_row_type)
		return r->is_duckdb_row_type(type_oid);
	return false;
}

bool
pgduckdb_is_unresolved_type(Oid type_oid) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->is_unresolved_type)
		return r->is_unresolved_type(type_oid);
	return false;
}

bool
pgduckdb_is_fake_type(Oid type_oid) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->is_fake_type)
		return r->is_fake_type(type_oid);
	return false;
}

bool
pgduckdb_var_is_duckdb_row(Var *var) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->var_is_duckdb_row)
		return r->var_is_duckdb_row(var);
	return false;
}

bool
pgduckdb_func_returns_duckdb_row(RangeTblFunction *rtfunc) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->func_returns_duckdb_row)
		return r->func_returns_duckdb_row(rtfunc);
	return false;
}

Var *
pgduckdb_duckdb_subscript_var(Expr *expr) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->duckdb_subscript_var)
		return r->duckdb_subscript_var(expr);
	return nullptr;
}

SubscriptingRef *
pgduckdb_strip_first_subscript(SubscriptingRef *sbsref, StringInfo buf) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->strip_first_subscript)
		return r->strip_first_subscript(sbsref, buf);
	return DefaultStripFirstSubscript(sbsref, buf);
}

char *
pgduckdb_write_row_refname(StringInfo buf, char *refname, bool is_top_level) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->write_row_refname)
		return r->write_row_refname(buf, refname, is_top_level);
	return DefaultWriteRowRefname(buf, refname, is_top_level);
}

bool
pgduckdb_subscript_has_custom_alias(Plan *plan, List *rtable, Var *subscript_var, char *colname) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->subscript_has_custom_alias)
		return r->subscript_has_custom_alias(plan, rtable, subscript_var, colname);
	return false;
}

bool
pgduckdb_reconstruct_star_step(StarReconstructionContext *ctx, ListCell *tle_cell) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->reconstruct_star_step)
		return r->reconstruct_star_step(ctx, tle_cell);
	return false;
}

bool
pgduckdb_replace_subquery_with_view(Query *query, StringInfo buf) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->replace_subquery_with_view)
		return r->replace_subquery_with_view(query, buf);
	return false;
}

bool
pgduckdb_is_not_default_expr(Node *node, void *context) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->is_not_default_expr)
		return r->is_not_default_expr(node, context);
	return true;
}

int
pgduckdb_show_type(Const *constval, int original_showtype) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->show_type)
		return r->show_type(constval, original_showtype);
	return original_showtype;
}

void
pgduckdb_add_tablesample_percent(const char *tsm_name, StringInfo buf, int num_args) {
	const DeparseRoutine *r = ActiveRoutine();
	if (r->add_tablesample_percent) {
		r->add_tablesample_percent(tsm_name, buf, num_args);
		return;
	}
	DefaultAddTablesamplePercent(tsm_name, buf, num_args);
}

/* ------------------------------------------------------------------
 * Helpers exported to both the vendored walker and ext callbacks.
 * ------------------------------------------------------------------ */

bool
is_system_sampling(const char *tsm_name, int num_args) {
	return (pg_strcasecmp(tsm_name, "system") == 0) && (num_args == 1);
}

bool
is_bernoulli_sampling(const char *tsm_name, int num_args) {
	return (pg_strcasecmp(tsm_name, "bernoulli") == 0) && (num_args == 1);
}

/* ------------------------------------------------------------------
 * Public entry points.
 * ------------------------------------------------------------------ */

/*
 * pgduckdb_get_querydef returns the definition of a given query in DuckDB
 * syntax. Dates are forced to ISO/YMD for the duration of the deparse
 * because that's the only format DuckDB's parser accepts.
 *
 * RAII scope guard ensures current_routine and outermost_query are
 * restored even if the vendored walker throws a C++ exception.
 */
namespace {
struct RoutineScope {
	const DeparseRoutine *saved_routine;
	bool saved_outermost;
	int saved_guc_nestlevel;

	RoutineScope(const DeparseRoutine *next)
	    : saved_routine(current_routine), saved_outermost(outermost_query), saved_guc_nestlevel(NewGUCNestLevel()) {
		current_routine = next ? next : &PGDUCKDB_DEFAULT_DEPARSE_ROUTINE;
		outermost_query = true;
		SetConfigOption("DateStyle", "ISO, YMD", PGC_USERSET, PGC_S_SESSION);
	}

	~RoutineScope() {
		AtEOXact_GUC(false, saved_guc_nestlevel);
		current_routine = saved_routine;
		outermost_query = saved_outermost;
	}

	RoutineScope(const RoutineScope &) = delete;
	RoutineScope &operator=(const RoutineScope &) = delete;
};
} // namespace

char *
pgduckdb_get_querydef(Query *query, const DeparseRoutine *routine) {
	RoutineScope scope(routine);
	return pgduckdb_pg_get_querydef_internal(query, false);
}

/*
 * Scope a DeparseRoutine for the duration of a user-supplied callback.
 * Used by ext DDL deparse routines that compose multiple pieces of
 * vendored-walker output themselves (e.g. CREATE TABLE with default
 * expressions deparsed via pgduckdb_deparse_expression).
 */
char *
pgduckdb_deparse_with_routine(const DeparseRoutine *routine, char *(*body)(void *arg), void *arg) {
	RoutineScope scope(routine);
	return body(arg);
}

/* ------------------------------------------------------------------
 * DuckDB LIKE/ILIKE escape shim. DuckDB does not use an escape character
 * in LIKE expressions by default; the walker calls these three hooks to
 * wrap emitted pattern-match operators with an explicit ESCAPE clause
 * and to negate !~~ / !~~* forms.
 *
 * These are consumer-agnostic DuckDB-dialect fixups so they stay in lib.
 * ------------------------------------------------------------------ */

struct PGDuckDBGetOperExprContext {
	const char *pg_op_name;
	const char *duckdb_op_name;
	const char *escape_pattern;
	bool is_likeish_op;
	bool is_negated;
};

void *
pg_duckdb_get_oper_expr_make_ctx(const char *op_name, Node **, Node **arg2) {
	auto ctx = (PGDuckDBGetOperExprContext *)palloc0(sizeof(PGDuckDBGetOperExprContext));
	ctx->pg_op_name = op_name;
	ctx->duckdb_op_name = nullptr;
	ctx->escape_pattern = "'\\'";
	ctx->is_likeish_op = false;
	ctx->is_negated = false;

	if (AreStringEqual(op_name, "~~")) {
		ctx->duckdb_op_name = "LIKE";
		ctx->is_likeish_op = true;
	} else if (AreStringEqual(op_name, "~~*")) {
		ctx->duckdb_op_name = "ILIKE";
		ctx->is_likeish_op = true;
	} else if (AreStringEqual(op_name, "!~~")) {
		ctx->duckdb_op_name = "LIKE";
		ctx->is_likeish_op = true;
		ctx->is_negated = true;
	} else if (AreStringEqual(op_name, "!~~*")) {
		ctx->duckdb_op_name = "ILIKE";
		ctx->is_likeish_op = true;
		ctx->is_negated = true;
	}

	if (ctx->is_likeish_op && IsA(*arg2, FuncExpr)) {
		auto arg2_func = (FuncExpr *)*arg2;
		auto func_name = get_func_name(arg2_func->funcid);
		if (!AreStringEqual(func_name, "like_escape") && !AreStringEqual(func_name, "ilike_escape")) {
			elog(ERROR, "Unexpected function in LIKE expression: '%s'", func_name);
		}

		*arg2 = (Node *)linitial(arg2_func->args);
		ctx->escape_pattern = pgduckdb_deparse_expression((Node *)lsecond(arg2_func->args), nullptr, false, false);
	}

	return ctx;
}

void
pg_duckdb_get_oper_expr_prefix(StringInfo buf, void *vctx) {
	auto ctx = static_cast<PGDuckDBGetOperExprContext *>(vctx);
	if (ctx->is_likeish_op && ctx->is_negated) {
		appendStringInfo(buf, "NOT (");
	}
}

void
pg_duckdb_get_oper_expr_middle(StringInfo buf, void *vctx) {
	auto ctx = static_cast<PGDuckDBGetOperExprContext *>(vctx);
	auto op = ctx->duckdb_op_name ? ctx->duckdb_op_name : ctx->pg_op_name;
	appendStringInfo(buf, " %s ", op);
}

void
pg_duckdb_get_oper_expr_suffix(StringInfo buf, void *vctx) {
	auto ctx = static_cast<PGDuckDBGetOperExprContext *>(vctx);
	if (ctx->is_likeish_op) {
		appendStringInfo(buf, " ESCAPE %s", ctx->escape_pattern);
		if (ctx->is_negated) {
			appendStringInfo(buf, ")");
		}
	}
}

} /* extern "C" */

namespace pgduckdb {

ScopedDeparseRoutine::ScopedDeparseRoutine(const DeparseRoutine *routine)
    : saved_(current_routine) {
	current_routine = routine ? routine : &PGDUCKDB_DEFAULT_DEPARSE_ROUTINE;
}

ScopedDeparseRoutine::~ScopedDeparseRoutine() {
	current_routine = saved_;
}

} // namespace pgduckdb
