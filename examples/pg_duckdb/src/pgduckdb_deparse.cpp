/*-------------------------------------------------------------------------
 *
 * pgduckdb_deparse.cpp
 *		pg_duckdb's flavour of the libpgduckdb DeparseRoutine, plus the
 *		DDL-emit helpers that used to live in lib's pgduckdb_ruleutils.cpp.
 *
 * The callbacks here encode pg_duckdb-specific deparse behaviour --
 * duckdb.row / duckdb.unresolved_type handling, "SELECT *" reconstruction
 * around duckdb.row targets, duckdb.view subquery replacement, pg_temp
 * schema mapping, and the "USING duckdb" table-am -> DuckDB schema
 * mapping (including parsing of pgduckdb$db$schema). They are wired up as
 * fields of `pg_duckdb_deparse_routine`; pg_duckdb then passes that
 * routine to `pgduckdb_get_querydef`.
 *
 * The DDL entries at the bottom -- CREATE/ALTER/RENAME TABLE and CREATE
 * VIEW deparse -- are ext-only because the checks they emit depend on
 * pg_duckdb's metadata_cache / table_am / duckdb.view gluing. They scope
 * `pg_duckdb_deparse_routine` across their inner invocations of the
 * vendored walker via `pgduckdb_deparse_with_routine`.
 *
 *-------------------------------------------------------------------------
 */
#include "duckdb.hpp"
#include "pgduckdb/pg/string_utils.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_ddl.hpp"
#include "pgduckdb/pg/relations.hpp"
#include "pgduckdb/pg/locale.hpp"

extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/heap.h"
#include "catalog/pg_collation.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "lib/stringinfo.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "nodes/print.h"
#include "utils/rls.h"
#include "utils/syscache.h"
#include "storage/lockdefs.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
#include "pgduckdb/pgduckdb_ruleutils.h"
#include "pgduckdb/vendor/pg_list.hpp"
}

#include "pgduckdb/pgduckdb_deparse.hpp"
#include "pgduckdb/pgduckdb_table_am.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"

namespace pgduckdb {

/* ------------------------------------------------------------------
 * DeparseRoutine callback bodies. Each is a static function with the
 * same signature as the corresponding DeparseRoutine field. Behaviour
 * matches what the monolithic lib pgduckdb_ruleutils.cpp used to do
 * pre-split; the split is purely mechanical.
 * ------------------------------------------------------------------ */

static char *
PgDuckdbFunctionName(Oid function_oid, bool *use_variadic_p) {
	if (!IsDuckdbOnlyFunction(function_oid)) {
		return nullptr;
	}

	/*
	 * DuckDB currently doesn't support variadic functions, so we can just
	 * always set this pointer to false.
	 */
	if (use_variadic_p) {
		*use_variadic_p = false;
	}

	auto func_name = get_func_name(function_oid);
	return psprintf("system.main.%s", quote_identifier(func_name));
}

static bool
PgDuckdbIsUnresolvedType(Oid type_oid) {
	return type_oid == DuckdbUnresolvedTypeOid();
}

static bool
PgDuckdbIsDuckdbRow(Oid type_oid) {
	return type_oid == DuckdbRowOid();
}

/*
 * We never want to show some of our unresolved types in the DuckDB query.
 * These types only exist to make the Postgres parser and its type resolution
 * happy. DuckDB can simply figure out the correct type itself without an
 * explicit cast.
 */
static bool
PgDuckdbIsFakeType(Oid type_oid) {
	if (PgDuckdbIsUnresolvedType(type_oid)) {
		return true;
	}

	if (PgDuckdbIsDuckdbRow(type_oid)) {
		return true;
	}

	if (DuckdbJsonOid() == type_oid) {
		return true;
	}

	return false;
}

static bool
PgDuckdbIsDuckdbSubscriptType(Oid type_oid) {
	if (PgDuckdbIsUnresolvedType(type_oid)) {
		return true;
	}

	if (PgDuckdbIsDuckdbRow(type_oid)) {
		return true;
	}

	if (DuckdbStructOid() == type_oid) {
		return true;
	}

	if (DuckdbMapOid() == type_oid) {
		return true;
	}

	return false;
}

static bool
PgDuckdbVarIsDuckdbRow(Var *var) {
	if (!var) {
		return false;
	}
	return PgDuckdbIsDuckdbRow(var->vartype);
}

static bool
PgDuckdbFuncReturnsDuckdbRow(RangeTblFunction *rtfunc) {
	if (!rtfunc) {
		return false;
	}

	if (!IsA(rtfunc->funcexpr, FuncExpr)) {
		return false;
	}

	FuncExpr *func_expr = castNode(FuncExpr, rtfunc->funcexpr);

	return PgDuckdbIsDuckdbRow(func_expr->funcresulttype);
}

/*
 * Returns NULL if the expression is not a subscript on a duckdb-specific
 * type. Returns the Var of the duckdb row if it is.
 */
static Var *
PgDuckdbDuckdbSubscriptVar(Expr *expr) {
	if (!expr) {
		return NULL;
	}

	if (!IsA(expr, SubscriptingRef)) {
		return NULL;
	}

	SubscriptingRef *subscript = (SubscriptingRef *)expr;

	if (!IsA(subscript->refexpr, Var)) {
		return NULL;
	}

	Var *refexpr = (Var *)subscript->refexpr;

	if (!PgDuckdbIsDuckdbSubscriptType(refexpr->vartype)) {
		return NULL;
	}

	return refexpr;
}

/*
 * PgDuckdbCheckForStarStart tries to figure out if this tle_cell
 * contains a Var that is the start of a run of Vars that should be
 * reconstructed as a star. If that's the case it sets the varno_star and
 * varattno_star of the ctx.
 */
static void
PgDuckdbCheckForStarStart(StarReconstructionContext *ctx, ListCell *tle_cell) {
	TargetEntry *first_tle = (TargetEntry *)lfirst(tle_cell);

	if (!IsA(first_tle->expr, Var)) {
		/* Not a Var so we're not at the start of a run of Vars. */
		return;
	}

	Var *first_var = (Var *)first_tle->expr;

	if (first_var->varattno != 1) {
		/* If we don't have varattno 1, then we are not at a run of Vars */
		return;
	}

	/*
	 * We found a Var that could potentially be the first of a run of Vars for
	 * which we have to reconstruct the star. To check if this is indeed the
	 * case we see if we can find a duckdb.row in this list of Vars.
	 */
	int varno = first_var->varno;
	int varattno = first_var->varattno;

	do {
		TargetEntry *tle = (TargetEntry *)lfirst(tle_cell);

		if (!IsA(tle->expr, Var)) {
			/*
			 * We found the end of this run of Vars, by finding something else
			 * than a Var.
			 */
			return;
		}

		Var *var = (Var *)tle->expr;

		if (var->varno != varno) {
			/* A Var from a different RTE */
			return;
		}

		if (var->varattno != varattno) {
			/* Not a consecutive Var */
			return;
		}
		if (PgDuckdbVarIsDuckdbRow(var)) {
			/*
			 * If we have a duckdb.row, then we found a run of Vars that we
			 * have to reconstruct the star for.
			 */

			ctx->varno_star = varno;
			ctx->varattno_star = first_var->varattno;
			ctx->added_current_star = false;
			return;
		}

		/* Look for the next Var in the run */
		varattno++;
	} while ((tle_cell = lnext(ctx->target_list, tle_cell)));
}

/*
 * In our DuckDB queries we sometimes want to use "SELECT *", when selecting
 * from a function like read_parquet. That way DuckDB can figure out the actual
 * columns that it should return. Sadly Postgres expands the * character from
 * the original query to a list of columns. So we need to put a star, any time
 * we want to replace duckdb.row columns with a "*" in the duckdb query.
 *
 * Since the original "*" might expand to many columns we need to remove all of
 * those, when putting a "*" back. To do so we try to find a runs of Vars from
 * the same FROM entry, aka RangeTableEntry (RTE) that we expect were created
 * with a *.
 *
 * This function returns true if we should skip writing this tle_cell to the
 * DuckDB query because it is part of a run of Vars that will be reconstructed
 * as a star.
 */
static bool
PgDuckdbReconstructStarStep(StarReconstructionContext *ctx, ListCell *tle_cell) {
	/* Detect start of a Var run that should be reconstructed to a star */
	PgDuckdbCheckForStarStart(ctx, tle_cell);

	/*
	 * If we're not currently reconstructing a star we don't need to do
	 * anything.
	 */
	if (!ctx->varno_star) {
		return false;
	}

	TargetEntry *tle = (TargetEntry *)lfirst(tle_cell);

	/*
	 * Find out if this target entry is the next element in the run of Vars for
	 * the star we're currently reconstructing.
	 */
	if (tle->expr && IsA(tle->expr, Var)) {
		Var *var = castNode(Var, tle->expr);

		if (var->varno == ctx->varno_star && var->varattno == ctx->varattno_star) {
			/*
			 * We're still in the run of Vars, increment the varattno to look
			 * for the next Var on the next call.
			 */
			ctx->varattno_star++;

			/* If we already added star we skip writing this target entry */
			if (ctx->added_current_star) {
				return true;
			}

			/*
			 * If it's not a duckdb row we skip this target entry too. The way
			 * we add a single star is by expanding the first duckdb.row target
			 * entry, which we've defined to expand to a star. So we need to
			 * skip any non duckdb.row Vars that precede the first duckdb.row.
			 */
			if (!PgDuckdbVarIsDuckdbRow(var)) {
				return true;
			}

			ctx->added_current_star = true;
			return false;
		}
	}

	/*
	 * If it was not, that means we've successfully expanded this star and we
	 * should start looking for the next star start. So reset all the state
	 * used for this star reconstruction.
	 */
	ctx->varno_star = 0;
	ctx->varattno_star = 0;
	ctx->added_current_star = false;

	return false;
}

static bool
PgDuckdbReplaceSubqueryWithView(Query *query, StringInfo buf) {
	FuncExpr *func_expr = GetDuckdbViewExprFromQuery(query);
	if (!func_expr) {
		/* Not a duckdb.view query, so we don't need to do anything */
		return false;
	}

	int i = 0;
	foreach_ptr(Expr, expr, func_expr->args) {
		if (i >= 3) {
			break;
		}

		if (!IsA(expr, Const)) {
			elog(ERROR, "Expected only constant argument to the view function");
		}

		Const *const_val = castNode(Const, expr);
		if (const_val->consttype != TEXTOID) {
			elog(ERROR, "Expected text arguments to the view function, got type %s",
			     format_type_be(const_val->consttype));
		}

		if (const_val->constisnull) {
			elog(ERROR, "Expected non-NULL arguments to the view function");
		}

		if (i > 0) {
			appendStringInfoString(buf, ".");
		}
		appendStringInfoString(buf, quote_identifier(TextDatumGetCString(const_val->constvalue)));

		i++;
	}

	return true;
}

/*
 * A wrapper around PgDuckdbIsFakeType that returns -1 if the type of the
 * Const is fake, because that's the type of value that get_const_expr requires
 * in its showtype variable to never show the type.
 */
static int
PgDuckdbShowType(Const *constval, int original_showtype) {
	if (PgDuckdbIsFakeType(constval->consttype)) {
		return -1;
	}
	return original_showtype;
}

static bool
PgDuckdbSubscriptHasCustomAlias(Plan *plan, List *rtable, Var *subscript_var, char *colname) {
	/* The first bit of this logic is taken from get_variable() */
	int varno;
	int varattno;

	/*
	 * If we have a syntactic referent for the Var, and we're working from a
	 * parse tree, prefer to use the syntactic referent.  Otherwise, fall back
	 * on the semantic referent.  (See comments in get_variable().)
	 */
	if (subscript_var->varnosyn > 0 && plan == NULL) {
		varno = subscript_var->varnosyn;
		varattno = subscript_var->varattnosyn;
	} else {
		varno = subscript_var->varno;
		varattno = subscript_var->varattno;
	}

	RangeTblEntry *rte = rt_fetch(varno, rtable);

	/* Custom code starts here */
	char *original_column = strVal(list_nth(rte->eref->colnames, varattno - 1));

	return strcmp(original_column, colname) != 0;
}

/*
 * Subscript expressions that index into the duckdb.row type need to be changed
 * to regular column references in the DuckDB query. The main reason we do this
 * is so that DuckDB generates nicer column names, i.e. without the square
 * brackets: "mycolumn" instead of "r['mycolumn']"
 */
static SubscriptingRef *
PgDuckdbStripFirstSubscript(SubscriptingRef *sbsref, StringInfo buf) {
	if (!IsA(sbsref->refexpr, Var)) {
		return sbsref;
	}

	if (!PgDuckdbVarIsDuckdbRow((Var *)sbsref->refexpr)) {
		return sbsref;
	}

	Assert(sbsref->refupperindexpr);
	Oid typoutput;
	bool typIsVarlena;
	Const *constval = castNode(Const, linitial(sbsref->refupperindexpr));
	getTypeOutputInfo(constval->consttype, &typoutput, &typIsVarlena);

	char *extval = OidOutputFunctionCall(typoutput, constval->constvalue);

	appendStringInfo(buf, ".%s", quote_identifier(extval));

	/*
	 * If there are any additional subscript expressions we should output them.
	 * Subscripts can be used in duckdb to index into arrays or json objects.
	 * It's fine if this results in an empty List, because printSubscripts
	 * handles that case correctly.
	 */
	SubscriptingRef *shorter_sbsref = (SubscriptingRef *)copyObjectImpl(sbsref);
	/* strip the first subscript from the list */
	shorter_sbsref->refupperindexpr = list_delete_first(shorter_sbsref->refupperindexpr);
	if (shorter_sbsref->reflowerindexpr) {
		shorter_sbsref->reflowerindexpr = list_delete_first(shorter_sbsref->reflowerindexpr);
	}
	return shorter_sbsref;
}

/*
 * Writes the refname to the buf in a way that results in the correct output
 * for the duckdb.row type.
 *
 * Returns the "attname" that should be passed back to the caller of
 * get_variable().
 */
static char *
PgDuckdbWriteRowRefname(StringInfo buf, char *refname, bool is_top_level) {
	appendStringInfoString(buf, quote_identifier(refname));

	if (is_top_level) {
		/*
		 * If the duckdb.row is at the top level target list of a select, then
		 * we want to generate r.*, to unpack all the columns instead of
		 * returning a STRUCT from the query.
		 *
		 * Since we use .* there is no attname.
		 */
		appendStringInfoString(buf, ".*");
		return NULL;
	}

	/*
	 * In any other case, we want to simply use the alias of the TargetEntry.
	 */
	return refname;
}

/*
 * Given a postgres schema name, this returns a list of two elements: the first
 * is the DuckDB database name and the second is the duckdb schema name. These
 * are not escaped yet.
 */
static List *
PgDuckdbDbAndSchema(const char *postgres_schema_name, const char *duckdb_table_am_name) {
	if (duckdb_table_am_name == nullptr) {
		return list_make2((void *)"pgduckdb", (void *)postgres_schema_name);
	}

	if (strcmp("duckdb", duckdb_table_am_name) != 0) {
		return list_make2((void *)duckdb_table_am_name, (void *)postgres_schema_name);
	}

	if (strcmp("pg_temp", postgres_schema_name) == 0) {
		return list_make2((void *)"pg_temp", (void *)"main");
	}

	if (strcmp("public", postgres_schema_name) == 0) {
		/* Use the "main" schema in DuckDB for tables in the public schema in Postgres */
		auto dbname = DuckDBManager::Get().GetDefaultDBName().c_str();
		return list_make2((void *)dbname, (void *)"main");
	}

	if (!IsDuckdbSchemaName(postgres_schema_name)) {
		auto dbname = DuckDBManager::Get().GetDefaultDBName().c_str();
		return list_make2((void *)dbname, (void *)postgres_schema_name);
	}

	StringInfoData db_name;
	StringInfoData schema_name;
	initStringInfo(&db_name);
	initStringInfo(&schema_name);
	const char *saveptr = &postgres_schema_name[4];
	const char *dollar;

	while ((dollar = strchr(saveptr, '$'))) {
		appendBinaryStringInfo(&db_name, saveptr, dollar - saveptr);
		saveptr = dollar + 1;
		if (saveptr[0] == '\0') {
			elog(ERROR, "Schema name is invalid");
		}

		if (saveptr[0] == '$') {
			appendStringInfoChar(&db_name, '$');
		} else {
			break;
		}
	}

	if (!dollar) {
		appendStringInfoString(&db_name, saveptr);
		return list_make2((void *)db_name.data, (char *)"main");
	}

	while ((dollar = strchr(saveptr, '$'))) {
		appendBinaryStringInfo(&schema_name, saveptr, dollar - saveptr);
		saveptr = dollar + 1;

		if (saveptr[0] == '$') {
			appendStringInfoChar(&schema_name, '$');
		} else {
			break;
		}
	}
	appendStringInfoString(&schema_name, saveptr);

	return list_make2(db_name.data, schema_name.data);
}

/*
 * Computes the fully-qualified DuckDB name of the relation for the given
 * Postgres OID, including the DuckDB database name.
 */
static char *
PgDuckdbRelationName(Oid relation_oid) {
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relation_oid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relation_oid);
	Form_pg_class relation = (Form_pg_class)GETSTRUCT(tp);
	const char *relname = NameStr(relation->relname);
	const char *postgres_schema_name = get_namespace_name_or_temp(relation->relnamespace);
	const char *duckdb_table_am_name = DuckdbTableAmGetName(relation_oid);

	const char *db_and_schema = pgduckdb_db_and_schema_string(postgres_schema_name, duckdb_table_am_name);

	char *result = psprintf("%s.%s", db_and_schema, quote_identifier(relname));

	ReleaseSysCache(tp);

	return result;
}

/*
 * Recursively check Const nodes and Var nodes for handling more complex
 * DEFAULT clauses.
 */
static bool
PgDuckdbIsNotDefaultExpr(Node *node, void *context) {
	if (node == NULL) {
		return false;
	}

	if (IsA(node, Var)) {
		return true;
	} else if (IsA(node, Const)) {
		/* If location is -1, it comes from the DEFAULT clause */
		Const *con = (Const *)node;
		if (con->location != -1) {
			return true;
		}
	}

#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, PgDuckdbIsNotDefaultExpr, context);
#else
	return expression_tree_walker(node, (bool (*)())((void *)PgDuckdbIsNotDefaultExpr), context);
#endif
}

static void
PgDuckdbAddTablesamplePercent(const char *tsm_name, StringInfo buf, int num_args) {
	if (!(is_system_sampling(tsm_name, num_args) || is_bernoulli_sampling(tsm_name, num_args))) {
		return;
	}
	appendStringInfoChar(buf, '%');
}

/* ------------------------------------------------------------------
 * The pg_duckdb DeparseRoutine itself.
 * ------------------------------------------------------------------ */

const DeparseRoutine pg_duckdb_deparse_routine = {
    /* .relation_name              = */ PgDuckdbRelationName,
    /* .function_name              = */ PgDuckdbFunctionName,
    /* .db_and_schema              = */ PgDuckdbDbAndSchema,
    /* .is_duckdb_row_type         = */ PgDuckdbIsDuckdbRow,
    /* .is_unresolved_type         = */ PgDuckdbIsUnresolvedType,
    /* .is_fake_type               = */ PgDuckdbIsFakeType,
    /* .var_is_duckdb_row          = */ PgDuckdbVarIsDuckdbRow,
    /* .func_returns_duckdb_row    = */ PgDuckdbFuncReturnsDuckdbRow,
    /* .duckdb_subscript_var       = */ PgDuckdbDuckdbSubscriptVar,
    /* .strip_first_subscript      = */ PgDuckdbStripFirstSubscript,
    /* .write_row_refname          = */ PgDuckdbWriteRowRefname,
    /* .subscript_has_custom_alias = */ PgDuckdbSubscriptHasCustomAlias,
    /* .reconstruct_star_step      = */ PgDuckdbReconstructStarStep,
    /* .replace_subquery_with_view = */ PgDuckdbReplaceSubqueryWithView,
    /* .is_not_default_expr        = */ PgDuckdbIsNotDefaultExpr,
    /* .show_type                  = */ PgDuckdbShowType,
    /* .add_tablesample_percent    = */ PgDuckdbAddTablesamplePercent,
};

} // namespace pgduckdb

/* ------------------------------------------------------------------
 * DDL-emit entries moved from lib. Each wraps its body in
 * pgduckdb_deparse_with_routine so that the inner pgduckdb_deparse_expression
 * calls see pg_duckdb_deparse_routine while rendering default values and
 * check constraints.
 * ------------------------------------------------------------------ */

namespace {

struct TableDefCtx {
	Oid relation_oid;
	char *result;
};

static char *
RunGetTableDef(void *arg) {
	auto *ctx = static_cast<TableDefCtx *>(arg);
	Oid relation_oid = ctx->relation_oid;
	Relation relation = relation_open(relation_oid, AccessShareLock);
	const char *relation_name = pgduckdb_relation_name(relation_oid);
	const char *postgres_schema_name = get_namespace_name_or_temp(relation->rd_rel->relnamespace);
	const char *duckdb_table_am_name = pgduckdb::DuckdbTableAmGetName(relation->rd_tableam);
	const char *db_and_schema = pgduckdb_db_and_schema_string(postgres_schema_name, duckdb_table_am_name);

	StringInfoData buffer;
	initStringInfo(&buffer);

	if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("Using duckdb as a table access method on a partitioned table is not supported")));
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

	List *relation_context = pgduckdb_deparse_context_for(relation_name, relation_oid);

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, print the column's name and
	 * its formatted type.
	 */
	TupleDesc tuple_descriptor = RelationGetDescr(relation);
	TupleConstr *tuple_constraints = tuple_descriptor->constr;
	AttrDefault *default_value_list = tuple_constraints ? tuple_constraints->defval : NULL;

	bool first_column_printed = false;
	AttrNumber default_value_index = 0;
	for (int i = 0; i < tuple_descriptor->natts; i++) {
		Form_pg_attribute column = TupleDescAttr(tuple_descriptor, i);

		if (column->attisdropped) {
			continue;
		}

		const char *column_name = NameStr(column->attname);

		/*
		 * Check that this type is known by DuckDB, and throw the appropriate
		 * error otherwise. This is particularly important for NUMERIC without
		 * precision specified. Because that means something very different in
		 * Postgres.
		 */
		auto duck_type = pgduckdb::ConvertPostgresToDuckColumnType(column);
		pgduckdb::GetPostgresDuckDBType(duck_type, true);

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

		/* if this column has a default value, append the default value */
		if (column->atthasdef) {
			Assert(tuple_constraints != NULL);
			Assert(default_value_list != NULL);

			AttrDefault *default_value = &(default_value_list[default_value_index]);
			default_value_index++;

			Assert(default_value->adnum == (i + 1));
			Assert(default_value_index <= tuple_constraints->num_defval);

			/*
			 * convert expression to node tree, and prepare deparse
			 * context
			 */
			Node *default_node = (Node *)stringToNode(default_value->adbin);

			/* deparse default value string */
			char *default_string = pgduckdb_deparse_expression(default_node, relation_context, false, false);

			/*
			 * DuckDB does not support STORED generated columns, it does
			 * support VIRTUAL generated columns though. However, Postgres
			 * currently does not support those, so for now there's no overlap
			 * in generated column support between the two databases.
			 */
			if (!column->attgenerated) {
				appendStringInfo(&buffer, " DEFAULT %s", default_string);
			} else if (column->attgenerated == ATTRIBUTE_GENERATED_STORED) {
				elog(ERROR, "DuckDB does not support STORED generated columns");
			} else {
				elog(ERROR, "Unkown generated column type");
			}
		}

		/* if this column has a not null constraint, append the constraint */
		if (column->attnotnull) {
			appendStringInfoString(&buffer, " NOT NULL");
		}

		/*
		 * XXX: default collation is actually probably not supported by
		 * DuckDB, unless it's C or POSIX. But failing unless people
		 * provide C or POSIX seems pretty annoying. How should we handle
		 * this?
		 */
		Oid collation = column->attcollation;
		if (collation != InvalidOid && collation != DEFAULT_COLLATION_OID && !pgduckdb::pg::IsCLocale(collation)) {
			elog(ERROR, "DuckDB does not support column collations");
		}
	}

	/*
	 * Now check if the table has any constraints. If it does, set the number
	 * of check constraints here. Then iterate over all check constraints and
	 * print them.
	 */
	AttrNumber constraint_count = tuple_constraints ? tuple_constraints->num_check : 0;
	ConstrCheck *check_constraint_list = tuple_constraints ? tuple_constraints->check : NULL;

	for (AttrNumber i = 0; i < constraint_count; i++) {
		ConstrCheck *check_constraint = &(check_constraint_list[i]);

		/* convert expression to node tree, and prepare deparse context */
		Node *check_node = (Node *)stringToNode(check_constraint->ccbin);

		/* deparse check constraint string */
		char *check_string = pgduckdb_deparse_expression(check_node, relation_context, false, false);

		/* if an attribute or constraint has been printed, format properly */
		if (first_column_printed || i > 0) {
			appendStringInfoString(&buffer, ", ");
		}

		appendStringInfo(&buffer, "CONSTRAINT %s CHECK ", quote_identifier(check_constraint->ccname));

		appendStringInfoString(&buffer, "(");
		appendStringInfoString(&buffer, check_string);
		appendStringInfoString(&buffer, ")");
	}

	/* close create table's outer parentheses */
	appendStringInfoString(&buffer, ")");

	if (!pgduckdb::IsDuckdbTableAm(relation->rd_tableam)) {
		/* Shouldn't happen but seems good to check anyway */
		elog(ERROR, "Only a table with the DuckDB can be stored in DuckDB, %d %d", relation->rd_rel->relam,
		     pgduckdb::DuckdbTableAmOid());
	}

	if (relation->rd_options) {
		elog(ERROR, "Storage options are not supported in DuckDB");
	}

	relation_close(relation, AccessShareLock);

	ctx->result = buffer.data;
	return ctx->result;
}

/*
 * Take a raw CHECK constraint expression and convert it to a cooked format
 * ready for storage.
 *
 * Parse state must be set up to recognize any vars that might appear
 * in the expression.
 *
 * Vendored in from src/backend/catalog/heap.c
 */
static Node *
CookConstraint(ParseState *pstate, Node *raw_constraint, char *relname) {
	Node *expr;

	expr = transformExpr(pstate, raw_constraint, EXPR_KIND_CHECK_CONSTRAINT);

	expr = coerce_to_boolean(pstate, expr, "CHECK");

	assign_expr_collations(pstate, expr);

	if (list_length(pstate->p_rtable) != 1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
		                errmsg("only table \"%s\" can be referenced in check constraint", relname)));

	return expr;
}

struct AlterTableCtx {
	Oid relation_oid;
	AlterTableStmt *alter_stmt;
	char *result;
};

static char *
RunGetAlterTableDef(void *arg) {
	auto *ctx = static_cast<AlterTableCtx *>(arg);
	Oid relation_oid = ctx->relation_oid;
	AlterTableStmt *alter_stmt = ctx->alter_stmt;

	Relation relation = relation_open(relation_oid, AccessShareLock);
	const char *relation_name = pgduckdb_relation_name(relation_oid);

	StringInfoData buffer;
	initStringInfo(&buffer);

	if (get_rel_relkind(relation_oid) != RELKIND_RELATION) {
		elog(ERROR, "Only regular tables are supported in DuckDB");
	}

	if (list_length(RelationGetFKeyList(relation)) > 0) {
		elog(ERROR, "DuckDB tables do not support foreign keys");
	}

	List *relation_context = pgduckdb_deparse_context_for(relation_name, relation_oid);
	ParseState *pstate = make_parsestate(NULL);
	ParseNamespaceItem *nsitem = addRangeTableEntryForRelation(pstate, relation, AccessShareLock, NULL, false, true);
	addNSItemToQuery(pstate, nsitem, true, true, true);

	foreach_node(AlterTableCmd, cmd, alter_stmt->cmds) {
		/*
		 * DuckDB does not support doing multiple ALTER TABLE commands in
		 * one statement, so we split them up.
		 */
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
				case CONSTR_NULL: {
					appendStringInfoString(&buffer, " NULL");
					break;
				}
				case CONSTR_NOTNULL: {
					appendStringInfoString(&buffer, " NOT NULL");
					break;
				}
				case CONSTR_DEFAULT: {
					if (constraint->raw_expr) {
						auto expr = cookDefault(pstate, constraint->raw_expr, attribute->atttypid, attribute->atttypmod,
						                        col->colname, attribute->attgenerated);
						char *default_string = pgduckdb_deparse_expression(expr, relation_context, false, false);
						appendStringInfo(&buffer, " DEFAULT %s", default_string);
					}
					break;
				}
				case CONSTR_CHECK: {
					appendStringInfo(&buffer, "CHECK ");

					auto expr = CookConstraint(pstate, constraint->raw_expr, RelationGetRelationName(relation));

					char *check_string = pgduckdb_deparse_expression(expr, relation_context, false, false);

					appendStringInfo(&buffer, "(%s); ", check_string);
					break;
				}
				case CONSTR_PRIMARY: {
					appendStringInfoString(&buffer, " PRIMARY KEY");
					break;
				}
				case CONSTR_UNIQUE: {
					appendStringInfoString(&buffer, " UNIQUE");
					break;
				}
				default:
					elog(ERROR, "pg_duckdb does not support this ALTER TABLE yet");
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
			/* TODO: Disallow after SET DEFAULT/ADD CHECK CONSTRAINT in the same ALTER command */

			appendStringInfo(&buffer, "ALTER COLUMN %s TYPE %s; ", quote_identifier(column_name), column_fq_type);
			break;
		}

		case AT_DropColumn: {
			appendStringInfo(&buffer, "DROP COLUMN %s", quote_identifier(cmd->name));

			/* Add CASCADE or RESTRICT if specified */
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

		case AT_DropNotNull: {
			appendStringInfo(&buffer, "ALTER COLUMN %s DROP NOT NULL; ", quote_identifier(cmd->name));
			break;
		}

		case AT_SetNotNull: {
			appendStringInfo(&buffer, "ALTER COLUMN %s SET NOT NULL; ", quote_identifier(cmd->name));
			break;
		}

		case AT_AddConstraint: {
			Constraint *constraint = castNode(Constraint, cmd->def);

			appendStringInfoString(&buffer, "ADD ");

			switch (constraint->contype) {
			case CONSTR_CHECK: {
				appendStringInfo(&buffer, "CONSTRAINT %s CHECK ",
				                 quote_identifier(constraint->conname ? constraint->conname : ""));

				auto expr = CookConstraint(pstate, constraint->raw_expr, RelationGetRelationName(relation));

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
					if (!first) {
						appendStringInfoString(&buffer, ", ");
					}
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
					if (!ufirst) {
						appendStringInfoString(&buffer, ", ");
					}
					appendStringInfoString(&buffer, quote_identifier(key));
					ufirst = false;
				}
				appendStringInfoString(&buffer, "); ");
				break;
			}

			default: {
				elog(ERROR, "DuckDB does not support this constraint type");
				break;
			}
			}
			break;
		}

		case AT_DropConstraint: {
			appendStringInfo(&buffer, "DROP CONSTRAINT %s", quote_identifier(cmd->name));

			/* Add CASCADE or RESTRICT if specified */
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

			if (is_set) {
				appendStringInfoString(&buffer, "SET (");
			} else {
				appendStringInfoString(&buffer, "RESET (");
			}

			ListCell *cell;
			bool first = true;
			foreach (cell, options) {
				DefElem *def = (DefElem *)lfirst(cell);
				if (!first) {
					appendStringInfoString(&buffer, ", ");
				}

				appendStringInfoString(&buffer, quote_identifier(def->defname));

				if (is_set && def->arg) {
					char *val = NULL;
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
	char *result;
};

static char *
RunGetRenameRelationDef(void *arg) {
	auto *ctx = static_cast<RenameRelationCtx *>(arg);
	Oid relation_oid = ctx->relation_oid;
	RenameStmt *rename_stmt = ctx->rename_stmt;

	if (rename_stmt->renameType != OBJECT_TABLE && rename_stmt->renameType != OBJECT_VIEW &&
	    rename_stmt->renameType != OBJECT_COLUMN) {
		elog(ERROR, "Only renaming tables and columns is supported in DuckDB");
	}

	Relation relation = relation_open(relation_oid, AccessShareLock);
	Assert(pgduckdb::IsDuckdbTable(relation));

	const char *postgres_schema_name = get_namespace_name_or_temp(relation->rd_rel->relnamespace);
	const char *db_and_schema = pgduckdb_db_and_schema_string(postgres_schema_name, "duckdb");
	const char *old_table_name = psprintf("%s.%s", db_and_schema, quote_identifier(rename_stmt->relation->relname));

	const char *relation_type = "TABLE";
	if (relation->rd_rel->relkind == RELKIND_VIEW) {
		relation_type = "VIEW";
	}

	StringInfoData buffer;
	initStringInfo(&buffer);

	if (rename_stmt->subname) {
		appendStringInfo(&buffer, "ALTER %s %s RENAME COLUMN %s TO %s;", relation_type, old_table_name,
		                 quote_identifier(rename_stmt->subname), quote_identifier(rename_stmt->newname));
	} else {
		appendStringInfo(&buffer, "ALTER %s %s RENAME TO %s;", relation_type, old_table_name,
		                 quote_identifier(rename_stmt->newname));
	}

	relation_close(relation, AccessShareLock);

	ctx->result = buffer.data;
	return ctx->result;
}

struct ViewDefCtx {
	const ViewStmt *stmt;
	const char *postgres_schema_name;
	const char *view_name;
	const char *duckdb_query_string;
	char *result;
};

static char *
RunGetViewDef(void *arg) {
	auto *ctx = static_cast<ViewDefCtx *>(arg);
	const ViewStmt *stmt = ctx->stmt;

	StringInfoData buffer;
	initStringInfo(&buffer);

	const char *db_and_schema = pgduckdb_db_and_schema_string(ctx->postgres_schema_name, "duckdb");
	appendStringInfo(&buffer, "CREATE SCHEMA IF NOT EXISTS %s; ", db_and_schema);

	appendStringInfoString(&buffer, "CREATE ");
	if (stmt->replace) {
		appendStringInfoString(&buffer, "OR REPLACE ");
	}
	appendStringInfo(&buffer, "VIEW %s.%s", db_and_schema, quote_identifier(ctx->view_name));
	if (stmt->aliases) {
		appendStringInfoChar(&buffer, '(');
		bool first = true;
#if PG_VERSION_NUM >= 150000
		foreach_node(String, alias, stmt->aliases) {
#else
		foreach_ptr(Value, alias, stmt->aliases) {
#endif
			if (!first) {
				appendStringInfoString(&buffer, ", ");
			} else {
				first = false;
			}

			appendStringInfoString(&buffer, quote_identifier(strVal(alias)));
		}
		appendStringInfoChar(&buffer, ')');
	}
	appendStringInfo(&buffer, " AS %s;", ctx->duckdb_query_string);
	ctx->result = buffer.data;
	return ctx->result;
}

} // namespace

char *
pgduckdb_get_tabledef(Oid relation_oid) {
	TableDefCtx ctx{relation_oid, nullptr};
	return pgduckdb_deparse_with_routine(&pgduckdb::pg_duckdb_deparse_routine, RunGetTableDef, &ctx);
}

char *
pgduckdb_get_alter_tabledef(Oid relation_oid, AlterTableStmt *alter_stmt) {
	AlterTableCtx ctx{relation_oid, alter_stmt, nullptr};
	return pgduckdb_deparse_with_routine(&pgduckdb::pg_duckdb_deparse_routine, RunGetAlterTableDef, &ctx);
}

char *
pgduckdb_get_rename_relationdef(Oid relation_oid, RenameStmt *rename_stmt) {
	RenameRelationCtx ctx{relation_oid, rename_stmt, nullptr};
	return pgduckdb_deparse_with_routine(&pgduckdb::pg_duckdb_deparse_routine, RunGetRenameRelationDef, &ctx);
}

char *
pgduckdb_get_viewdef(const ViewStmt *stmt, const char *postgres_schema_name, const char *view_name,
                     const char *duckdb_query_string) {
	ViewDefCtx ctx{stmt, postgres_schema_name, view_name, duckdb_query_string, nullptr};
	return pgduckdb_deparse_with_routine(&pgduckdb::pg_duckdb_deparse_routine, RunGetViewDef, &ctx);
}
