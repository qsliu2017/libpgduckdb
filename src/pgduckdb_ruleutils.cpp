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
#include "pgduckdb/pg/locale.hpp"
#include "pgduckdb/pg/relations.hpp"
#include "pgduckdb/pg/string_utils.hpp"

extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/heap.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "lib/stringinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
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
    /* DDL-emit callbacks -- all nullptr; the helpers in this TU fall back
     * to sensible defaults when the routine fields are NULL. */
    /* .table_am_name              = */ nullptr,
    /* .validate_column_type       = */ nullptr,
    /* .assert_owned_relation      = */ nullptr,
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

/* ------------------------------------------------------------------
 * DDL emitters: pgduckdb_get_tabledef / pgduckdb_get_alter_tabledef /
 * pgduckdb_get_rename_relationdef. Generic-ish PG-DDL -> DuckDB-DDL
 * deparse; consumer-specific pieces (table AM name, per-column type
 * validation, AM ownership assertion) are threaded through
 * DeparseRoutine callbacks.
 * ------------------------------------------------------------------ */

extern "C" {

namespace {

const char *
RoutineTableAmName(const DeparseRoutine *r, Relation relation) {
	if (r && r->table_am_name) {
		const char *name = r->table_am_name(relation);
		if (name)
			return name;
	}
	return "main";
}

void
RoutineValidateColumnType(const DeparseRoutine *r, Form_pg_attribute attr) {
	if (r && r->validate_column_type) {
		r->validate_column_type(attr);
	}
}

void
RoutineAssertOwnedRelation(const DeparseRoutine *r, Relation relation) {
	if (r && r->assert_owned_relation) {
		r->assert_owned_relation(relation);
	}
}

/*
 * Cook a raw CHECK-constraint Node tree (vendored from PG's heap.c
 * StoreRelCheck / cookConstraint path) down to a storable expr the
 * ruleutils walker can deparse.
 */
Node *
CookCheckConstraint(ParseState *pstate, Node *raw_constraint, const char *relname) {
	Node *expr = transformExpr(pstate, raw_constraint, EXPR_KIND_CHECK_CONSTRAINT);
	expr = coerce_to_boolean(pstate, expr, "CHECK");
	assign_expr_collations(pstate, expr);
	if (list_length(pstate->p_rtable) != 1) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		                errmsg("only table \"%s\" can be referenced in check constraint", relname)));
	}
	return expr;
}

struct TableDefCtx {
	Oid relation_oid;
	const DeparseRoutine *routine;
	char *result;
};

char *
RunGetTableDef(void *arg) {
	auto *ctx = static_cast<TableDefCtx *>(arg);
	Relation relation = relation_open(ctx->relation_oid, AccessShareLock);
	const char *relation_name = pgduckdb_relation_name(ctx->relation_oid);
	const char *postgres_schema_name = get_namespace_name_or_temp(relation->rd_rel->relnamespace);
	const char *table_am_name = RoutineTableAmName(ctx->routine, relation);
	const char *db_and_schema = pgduckdb_db_and_schema_string(postgres_schema_name, table_am_name);

	StringInfoData buffer;
	initStringInfo(&buffer);

	if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("Using DuckDB as a table access method on a partitioned table is not supported")));
	} else if (relation->rd_rel->relkind != RELKIND_RELATION) {
		elog(ERROR, "Only regular tables are supported in DuckDB");
	}

	if (relation->rd_rel->relispartition) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("DuckDB tables cannot be used as a partition")));
	}

	appendStringInfo(&buffer, "CREATE SCHEMA IF NOT EXISTS %s; ", db_and_schema);

	appendStringInfoString(&buffer, "CREATE ");
	if (relation->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
		/* allowed */
	} else if (relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT) {
		elog(ERROR, "Only TEMP and non-UNLOGGED tables are supported in DuckDB");
	}

	appendStringInfo(&buffer, "TABLE %s (", relation_name);

	if (list_length(RelationGetFKeyList(relation)) > 0) {
		elog(ERROR, "DuckDB tables do not support foreign keys");
	}

	List *relation_context = pgduckdb_deparse_context_for(relation_name, ctx->relation_oid);

	TupleDesc tuple_descriptor = RelationGetDescr(relation);
	TupleConstr *tuple_constraints = tuple_descriptor->constr;
	AttrDefault *default_value_list = tuple_constraints ? tuple_constraints->defval : nullptr;

	bool first_column_printed = false;
	AttrNumber default_value_index = 0;
	for (int i = 0; i < tuple_descriptor->natts; i++) {
		Form_pg_attribute column = TupleDescAttr(tuple_descriptor, i);
		if (column->attisdropped)
			continue;

		const char *column_name = NameStr(column->attname);

		/* Consumer-supplied validation -- e.g. bare NUMERIC rejection. */
		RoutineValidateColumnType(ctx->routine, column);

		const char *column_type_name = format_type_with_typemod(column->atttypid, column->atttypmod);

		if (first_column_printed) {
			appendStringInfoString(&buffer, ", ");
		}
		first_column_printed = true;

		appendStringInfo(&buffer, "%s ", quote_identifier(column_name));
		appendStringInfoString(&buffer, column_type_name);

		if (column->attcompression) {
			elog(ERROR, "Column compression is not supported in DuckDB");
		}
		if (column->attidentity) {
			elog(ERROR, "Identity columns are not supported in DuckDB");
		}

		if (column->atthasdef) {
			Assert(tuple_constraints != nullptr);
			Assert(default_value_list != nullptr);

			AttrDefault *default_value = &(default_value_list[default_value_index]);
			default_value_index++;

			Assert(default_value->adnum == (i + 1));
			Assert(default_value_index <= tuple_constraints->num_defval);

			Node *default_node = (Node *)stringToNode(default_value->adbin);
			char *default_string = pgduckdb_deparse_expression(default_node, relation_context, false, false);

			if (!column->attgenerated) {
				appendStringInfo(&buffer, " DEFAULT %s", default_string);
			} else if (column->attgenerated == ATTRIBUTE_GENERATED_STORED) {
				elog(ERROR, "DuckDB does not support STORED generated columns");
			} else {
				elog(ERROR, "Unknown generated column type");
			}
		}

		if (column->attnotnull) {
			appendStringInfoString(&buffer, " NOT NULL");
		}

		Oid collation = column->attcollation;
		if (collation != InvalidOid && collation != DEFAULT_COLLATION_OID && !pgduckdb::pg::IsCLocale(collation)) {
			elog(ERROR, "DuckDB does not support column collations");
		}
	}

	AttrNumber constraint_count = tuple_constraints ? tuple_constraints->num_check : 0;
	ConstrCheck *check_constraint_list = tuple_constraints ? tuple_constraints->check : nullptr;

	for (AttrNumber i = 0; i < constraint_count; i++) {
		ConstrCheck *check_constraint = &(check_constraint_list[i]);
		Node *check_node = (Node *)stringToNode(check_constraint->ccbin);
		char *check_string = pgduckdb_deparse_expression(check_node, relation_context, false, false);

		if (first_column_printed || i > 0) {
			appendStringInfoString(&buffer, ", ");
		}
		appendStringInfo(&buffer, "CONSTRAINT %s CHECK (%s)", quote_identifier(check_constraint->ccname), check_string);
	}

	appendStringInfoString(&buffer, ")");

	if (relation->rd_options) {
		elog(ERROR, "Storage options are not supported in DuckDB");
	}

	RoutineAssertOwnedRelation(ctx->routine, relation);

	relation_close(relation, AccessShareLock);

	ctx->result = buffer.data;
	return ctx->result;
}

struct AlterTableCtx {
	Oid relation_oid;
	AlterTableStmt *alter_stmt;
	const DeparseRoutine *routine;
	char *result;
};

char *
RunGetAlterTableDef(void *arg) {
	auto *ctx = static_cast<AlterTableCtx *>(arg);
	Relation relation = relation_open(ctx->relation_oid, AccessShareLock);
	const char *relation_name = pgduckdb_relation_name(ctx->relation_oid);

	StringInfoData buffer;
	initStringInfo(&buffer);

	if (get_rel_relkind(ctx->relation_oid) != RELKIND_RELATION) {
		elog(ERROR, "Only regular tables are supported in DuckDB");
	}
	if (list_length(RelationGetFKeyList(relation)) > 0) {
		elog(ERROR, "DuckDB tables do not support foreign keys");
	}

	List *relation_context = pgduckdb_deparse_context_for(relation_name, ctx->relation_oid);
	ParseState *pstate = make_parsestate(nullptr);
	ParseNamespaceItem *nsitem = addRangeTableEntryForRelation(pstate, relation, AccessShareLock, nullptr, false, true);
	addNSItemToQuery(pstate, nsitem, true, true, true);

	foreach_node(AlterTableCmd, cmd, ctx->alter_stmt->cmds) {
		appendStringInfo(&buffer, "ALTER TABLE %s ", relation_name);

		switch (cmd->subtype) {
		case AT_AddColumn: {
			ColumnDef *col = castNode(ColumnDef, cmd->def);
			TupleDesc tupdesc = BuildDescForRelation(list_make1(col));
			Form_pg_attribute attribute = TupleDescAttr(tupdesc, 0);
			const char *column_fq_type = format_type_with_typemod(attribute->atttypid, attribute->atttypmod);

			appendStringInfo(&buffer, "ADD COLUMN %s %s", quote_identifier(col->colname), column_fq_type);
			foreach_node(Constraint, constraint, col->constraints) {
				switch (constraint->contype) {
				case CONSTR_NULL:
					appendStringInfoString(&buffer, " NULL");
					break;
				case CONSTR_NOTNULL:
					appendStringInfoString(&buffer, " NOT NULL");
					break;
				case CONSTR_DEFAULT:
					if (constraint->raw_expr) {
						auto expr = cookDefault(pstate, constraint->raw_expr, attribute->atttypid, attribute->atttypmod,
						                        col->colname, attribute->attgenerated);
						char *default_string = pgduckdb_deparse_expression(expr, relation_context, false, false);
						appendStringInfo(&buffer, " DEFAULT %s", default_string);
					}
					break;
				case CONSTR_CHECK: {
					appendStringInfoString(&buffer, "CHECK ");
					auto expr = CookCheckConstraint(pstate, constraint->raw_expr, RelationGetRelationName(relation));
					char *check_string = pgduckdb_deparse_expression(expr, relation_context, false, false);
					appendStringInfo(&buffer, "(%s); ", check_string);
					break;
				}
				case CONSTR_PRIMARY:
					appendStringInfoString(&buffer, " PRIMARY KEY");
					break;
				case CONSTR_UNIQUE:
					appendStringInfoString(&buffer, " UNIQUE");
					break;
				default:
					elog(ERROR, "DuckDB does not support this ALTER TABLE constraint yet");
				}
			}

			if (col->collClause || col->collOid != InvalidOid) {
				elog(ERROR, "Column collations are not supported in DuckDB");
			}

			appendStringInfoString(&buffer, "; ");
			break;
		}

		case AT_AlterColumnType: {
			const char *column_name = cmd->name;
			ColumnDef *col = castNode(ColumnDef, cmd->def);
			TupleDesc tupdesc = BuildDescForRelation(list_make1(col));
			Form_pg_attribute attribute = TupleDescAttr(tupdesc, 0);
			const char *column_fq_type = format_type_with_typemod(attribute->atttypid, attribute->atttypmod);

			appendStringInfo(&buffer, "ALTER COLUMN %s TYPE %s; ", quote_identifier(column_name), column_fq_type);
			break;
		}

		case AT_DropColumn: {
			appendStringInfo(&buffer, "DROP COLUMN %s", quote_identifier(cmd->name));
			if (cmd->behavior == DROP_CASCADE) {
				appendStringInfoString(&buffer, " CASCADE");
			} else if (cmd->behavior == DROP_RESTRICT) {
				appendStringInfoString(&buffer, " RESTRICT");
			}
			appendStringInfoString(&buffer, "; ");
			break;
		}

		case AT_ColumnDefault: {
			const char *column_name = cmd->name;
			TupleDesc tupdesc = RelationGetDescr(relation);
			Form_pg_attribute attribute = pgduckdb::pg::GetAttributeByName(tupdesc, column_name);
			if (!attribute) {
				elog(ERROR, "Column %s not found in table %s", column_name, relation_name);
			}

			appendStringInfo(&buffer, "ALTER COLUMN %s ", quote_identifier(cmd->name));

			if (cmd->def) {
				auto expr = cookDefault(pstate, cmd->def, attribute->atttypid, attribute->atttypmod, column_name,
				                        attribute->attgenerated);
				char *default_string = pgduckdb_deparse_expression(expr, relation_context, false, false);
				appendStringInfo(&buffer, "SET DEFAULT %s; ", default_string);
			} else {
				appendStringInfoString(&buffer, "DROP DEFAULT; ");
			}
			break;
		}

		case AT_DropNotNull:
			appendStringInfo(&buffer, "ALTER COLUMN %s DROP NOT NULL; ", quote_identifier(cmd->name));
			break;

		case AT_SetNotNull:
			appendStringInfo(&buffer, "ALTER COLUMN %s SET NOT NULL; ", quote_identifier(cmd->name));
			break;

		case AT_AddConstraint: {
			Constraint *constraint = castNode(Constraint, cmd->def);
			appendStringInfoString(&buffer, "ADD ");

			switch (constraint->contype) {
			case CONSTR_CHECK: {
				appendStringInfo(&buffer, "CONSTRAINT %s CHECK ",
				                 quote_identifier(constraint->conname ? constraint->conname : ""));
				auto expr = CookCheckConstraint(pstate, constraint->raw_expr, RelationGetRelationName(relation));
				char *check_string = pgduckdb_deparse_expression(expr, relation_context, false, false);
				appendStringInfo(&buffer, "(%s); ", check_string);
				break;
			}
			case CONSTR_PRIMARY: {
				appendStringInfoString(&buffer, "PRIMARY KEY (");
				ListCell *cell;
				bool first = true;
				foreach (cell, constraint->keys) {
					char *key = strVal(lfirst(cell));
					if (!first)
						appendStringInfoString(&buffer, ", ");
					appendStringInfoString(&buffer, quote_identifier(key));
					first = false;
				}
				appendStringInfoString(&buffer, "); ");
				break;
			}
			case CONSTR_UNIQUE: {
				appendStringInfoString(&buffer, "UNIQUE (");
				ListCell *ucell;
				bool ufirst = true;
				foreach (ucell, constraint->keys) {
					char *key = strVal(lfirst(ucell));
					if (!ufirst)
						appendStringInfoString(&buffer, ", ");
					appendStringInfoString(&buffer, quote_identifier(key));
					ufirst = false;
				}
				appendStringInfoString(&buffer, "); ");
				break;
			}
			default:
				elog(ERROR, "DuckDB does not support this constraint type");
			}
			break;
		}

		case AT_DropConstraint: {
			appendStringInfo(&buffer, "DROP CONSTRAINT %s", quote_identifier(cmd->name));
			if (cmd->behavior == DROP_CASCADE) {
				appendStringInfoString(&buffer, " CASCADE");
			} else if (cmd->behavior == DROP_RESTRICT) {
				appendStringInfoString(&buffer, " RESTRICT");
			}
			appendStringInfoString(&buffer, "; ");
			break;
		}

		case AT_SetRelOptions:
		case AT_ResetRelOptions: {
			List *options = (List *)cmd->def;
			bool is_set = (cmd->subtype == AT_SetRelOptions);
			appendStringInfoString(&buffer, is_set ? "SET (" : "RESET (");

			ListCell *cell;
			bool first = true;
			foreach (cell, options) {
				DefElem *def = (DefElem *)lfirst(cell);
				if (!first)
					appendStringInfoString(&buffer, ", ");
				appendStringInfoString(&buffer, quote_identifier(def->defname));
				if (is_set && def->arg) {
					char *val = nullptr;
					if (IsA(def->arg, String)) {
						val = strVal(def->arg);
						appendStringInfo(&buffer, " = %s", quote_literal_cstr(val));
					} else if (IsA(def->arg, Integer)) {
						val = psprintf("%d", intVal(def->arg));
						appendStringInfo(&buffer, " = %s", val);
					} else {
						elog(ERROR, "Unsupported option value type");
					}
				}
				first = false;
			}
			appendStringInfoString(&buffer, "); ");
			break;
		}

		default:
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			                errmsg("DuckDB does not support this ALTER TABLE command")));
		}
	}
	relation_close(relation, AccessShareLock);

	ctx->result = buffer.data;
	return ctx->result;
}

struct RenameRelationCtx {
	Oid relation_oid;
	RenameStmt *rename_stmt;
	const DeparseRoutine *routine;
	char *result;
};

char *
RunGetRenameRelationDef(void *arg) {
	auto *ctx = static_cast<RenameRelationCtx *>(arg);
	if (ctx->rename_stmt->renameType != OBJECT_TABLE && ctx->rename_stmt->renameType != OBJECT_VIEW &&
	    ctx->rename_stmt->renameType != OBJECT_COLUMN) {
		elog(ERROR, "Only renaming tables, views, and columns is supported");
	}

	Relation relation = relation_open(ctx->relation_oid, AccessShareLock);
	RoutineAssertOwnedRelation(ctx->routine, relation);

	const char *postgres_schema_name = get_namespace_name_or_temp(relation->rd_rel->relnamespace);
	const char *table_am_name = RoutineTableAmName(ctx->routine, relation);
	const char *db_and_schema = pgduckdb_db_and_schema_string(postgres_schema_name, table_am_name);
	const char *old_table_name =
	    psprintf("%s.%s", db_and_schema, quote_identifier(ctx->rename_stmt->relation->relname));

	const char *relation_type = (relation->rd_rel->relkind == RELKIND_VIEW) ? "VIEW" : "TABLE";

	StringInfoData buffer;
	initStringInfo(&buffer);

	if (ctx->rename_stmt->subname) {
		appendStringInfo(&buffer, "ALTER %s %s RENAME COLUMN %s TO %s;", relation_type, old_table_name,
		                 quote_identifier(ctx->rename_stmt->subname), quote_identifier(ctx->rename_stmt->newname));
	} else {
		appendStringInfo(&buffer, "ALTER %s %s RENAME TO %s;", relation_type, old_table_name,
		                 quote_identifier(ctx->rename_stmt->newname));
	}

	relation_close(relation, AccessShareLock);

	ctx->result = buffer.data;
	return ctx->result;
}

} // anonymous namespace

char *
pgduckdb_get_tabledef(Oid relation_oid, const DeparseRoutine *routine) {
	TableDefCtx ctx{relation_oid, routine, nullptr};
	return pgduckdb_deparse_with_routine(routine, RunGetTableDef, &ctx);
}

char *
pgduckdb_get_alter_tabledef(Oid relation_oid, AlterTableStmt *alter_stmt, const DeparseRoutine *routine) {
	AlterTableCtx ctx{relation_oid, alter_stmt, routine, nullptr};
	return pgduckdb_deparse_with_routine(routine, RunGetAlterTableDef, &ctx);
}

char *
pgduckdb_get_rename_relationdef(Oid relation_oid, RenameStmt *rename_stmt, const DeparseRoutine *routine) {
	RenameRelationCtx ctx{relation_oid, rename_stmt, routine, nullptr};
	return pgduckdb_deparse_with_routine(routine, RunGetRenameRelationDef, &ctx);
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
