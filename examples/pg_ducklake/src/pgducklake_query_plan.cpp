/*
 * pgducklake_query_plan.cpp -- planner hook + custom scan node that
 * routes queries touching ducklake tables to pg_ducklake's in-process
 * DuckDB instance.
 *
 * Mirrors pg_duckdb's DuckdbPlannerHook / DuckdbPlanNode / Duckdb_*
 * CustomScan methods pipeline. Stripped-down because pg_ducklake doesn't
 * expose duckdb.row pseudo-types, MotherDuck, duckdb_unresolved_type,
 * etc. -- the only routing rule is "any rtable entry with the ducklake
 * table AM means the whole query goes to DuckDB".
 *
 * Entry points: InitQueryPlan() registers the custom scan methods in
 * _PG_init. DuckLakePlanQuery(parse, cursor_options) is called from
 * DucklakePlannerHook before it forwards to the previous hook.
 */

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/query_result.hpp"

// pgduckdb_types.hpp uses cpp_only_file.hpp to assert it sees no PG
// headers; include it before anything that pulls postgres.h.
#include "pgduckdb/pgduckdb_types.hpp"

#include "pgducklake/pgducklake_hooks.hpp"
#include "pgducklake/utility/cpp_wrapper.hpp"

#include "pgduckdb/pgduckdb_contracts.hpp"
#include "pgduckdb/pgduckdb_ruleutils.h"

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/optimizer.h"
#include "parser/parse_relation.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
}

namespace pgducklake {

// EnsureDuckDBInitialized lives in pgducklake_contracts.cpp.
duckdb::DuckDB &EnsureDuckDBInitialized();

extern const DeparseRoutine ducklake_deparse_routine;

namespace {

// ---- Ownership probe ---------------------------------------------------

bool
IsDuckLakeRelid(Oid relid) {
	if (!OidIsValid(relid)) {
		return false;
	}
	HeapTuple rel_tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(rel_tup)) {
		return false;
	}
	Oid amoid = ((Form_pg_class)GETSTRUCT(rel_tup))->relam;
	ReleaseSysCache(rel_tup);
	if (!OidIsValid(amoid)) {
		return false;
	}
	HeapTuple am_tup = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(am_tup)) {
		return false;
	}
	bool match = strcmp(NameStr(((Form_pg_am)GETSTRUCT(am_tup))->amname), "ducklake") == 0;
	ReleaseSysCache(am_tup);
	return match;
}

bool
RtableHasDuckLakeRelation(List *rtable) {
	foreach_node(RangeTblEntry, rte, rtable) {
		if (rte->rtekind == RTE_RELATION && IsDuckLakeRelid(rte->relid)) {
			return true;
		}
	}
	return false;
}

bool
QueryTreeHasDuckLakeItems(Node *node, void * /*context*/) {
	if (node == nullptr) {
		return false;
	}
	if (IsA(node, Query)) {
		Query *query = (Query *)node;
		if (RtableHasDuckLakeRelation(query->rtable)) {
			return true;
		}
#if PG_VERSION_NUM >= 160000
		return query_tree_walker(query, QueryTreeHasDuckLakeItems, nullptr, 0);
#else
		return query_tree_walker(query, (bool (*)())((void *)QueryTreeHasDuckLakeItems), nullptr, 0);
#endif
	}
#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, QueryTreeHasDuckLakeItems, nullptr);
#else
	return expression_tree_walker(node, (bool (*)())((void *)QueryTreeHasDuckLakeItems), nullptr);
#endif
}

// ---- Per-backend DuckDB connection -------------------------------------
//
// pg_duckdb's DuckDBManager::GetConnection returns a shared duckdb::Connection
// that outlives individual queries. We replicate the lazy-init + reuse
// pattern so multiple CustomScan invocations in one statement share state
// (prepared-statement cache, attached catalog, etc.).

duckdb::Connection *
GetBackendConnection() {
	static std::unique_ptr<duckdb::Connection> cached;
	static bool pgduckdb_attached = false;
	auto &db = EnsureDuckDBInitialized();
	if (!cached) {
		cached = std::make_unique<duckdb::Connection>(db);
	}
	if (!pgduckdb_attached) {
		// Done lazily (not in ducklake_load_extension) because the
		// PostgresCatalog attach path scans pg_class via SPI, which
		// deadlocks if it runs inside DuckDBManager::Initialize during
		// CREATE EXTENSION. By the time we get here, the outer query is
		// past the point that held the conflicting lock.
		auto result = cached->Query("ATTACH DATABASE 'pgduckdb' (TYPE pgduckdb)");
		if (result->HasError()) {
			// Already-attached is fine (e.g. after DROP+CREATE EXTENSION).
			std::string msg = result->GetError();
			if (msg.find("already attached") == std::string::npos &&
			    msg.find("already exists") == std::string::npos) {
				throw duckdb::Exception(duckdb::ExceptionType::CATALOG,
				                        "pg_ducklake: failed to attach pgduckdb PG-scan catalog: " + msg);
			}
		}
		pgduckdb_attached = true;
	}
	return cached.get();
}

// ---- Query deparse + prepare -------------------------------------------

duckdb::unique_ptr<duckdb::PreparedStatement>
PrepareQuery(const Query *query, const char *explain_prefix) {
	Query *copied_query = (Query *)copyObjectImpl(query);
	const char *query_string = pgduckdb_get_querydef(copied_query, &ducklake_deparse_routine);
	if (explain_prefix) {
		query_string = psprintf("%s %s", explain_prefix, query_string);
	}
	elog(DEBUG2, "(pg_ducklake/PrepareQuery) %s", query_string);
	auto *conn = GetBackendConnection();
	return conn->context->Prepare(query_string);
}

// ---- CustomScan methods ------------------------------------------------

CustomScanMethods g_scan_methods;
CustomExecMethods g_exec_methods;

struct DuckLakeScanState {
	CustomScanState css;            // must be first
	const CustomScan *custom_scan;
	const Query *query;
	ParamListInfo params;
	duckdb::Connection *connection;
	duckdb::PreparedStatement *prepared;
	bool executed;
	bool fetch_next;
	duckdb::unique_ptr<duckdb::QueryResult> query_results;
	duckdb::idx_t column_count;
	duckdb::unique_ptr<duckdb::DataChunk> current_chunk;
	duckdb::idx_t current_row;
};

void
ResetScanState(DuckLakeScanState *state) {
	if (state->css.ss.ps.ps_ExprContext) {
		MemoryContextReset(state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
	}
	if (state->css.ss.ss_ScanTupleSlot) {
		ExecClearTuple(state->css.ss.ss_ScanTupleSlot);
	}
	state->query_results.reset();
	state->current_chunk.reset();
	if (state->prepared) {
		delete state->prepared;
		state->prepared = nullptr;
	}
}

Node *
CreateScanState(CustomScan *cscan) {
	auto *state = (DuckLakeScanState *)newNode(sizeof(DuckLakeScanState), T_CustomScanState);
	state->custom_scan = cscan;
	state->query = (const Query *)linitial(cscan->custom_private);
	state->css.methods = &g_exec_methods;
	return (Node *)&state->css;
}

void
BeginScan_Cpp(CustomScanState *cscanstate, EState *estate, int /*eflags*/) {
	auto *state = (DuckLakeScanState *)cscanstate;
	auto prepared = PrepareQuery(state->query, nullptr);
	if (prepared->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR,
		                        "pg_ducklake re-planning failed: " + prepared->GetError());
	}
	state->connection = GetBackendConnection();
	state->prepared = prepared.release();
	state->params = estate->es_param_list_info;
	state->executed = false;
	state->fetch_next = true;
	state->css.ss.ps.ps_ResultTupleDesc = state->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
}

void
BeginScan(CustomScanState *node, EState *estate, int eflags) {
	InvokeCPPFunc(BeginScan_Cpp, node, estate, eflags);
}

void
ExecuteInDuckDB(DuckLakeScanState *state) {
	auto &prepared = *state->prepared;
	duckdb::case_insensitive_map_t<duckdb::BoundParameterData> named_values;
	auto *pg_params = state->params;
	int num_params = pg_params ? pg_params->numParams : 0;
	for (int i = 0; i < num_params; i++) {
		ParamExternData tmp;
		ParamExternData *pg_param = pg_params->paramFetch ? pg_params->paramFetch(pg_params, i + 1, false, &tmp)
		                                                 : &pg_params->params[i];
		auto key = duckdb::to_string(i + 1);
		if (prepared.named_param_map.count(key) == 0) {
			continue;
		}
		duckdb::Value value;
		if (pg_param->isnull) {
			value = duckdb::Value();
		} else if (OidIsValid(pg_param->ptype)) {
			value = pgduckdb::ConvertPostgresParameterToDuckValue(pg_param->value, pg_param->ptype);
		} else {
			throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR,
			                        duckdb::StringUtil::Format("parameter %d has no type", i + 1));
		}
		named_values[key] = duckdb::BoundParameterData(value);
	}
	auto pending = prepared.PendingQuery(named_values, /*allow_stream_result=*/true);
	if (pending->HasError()) {
		pending->ThrowError();
	}
	auto execution = duckdb::PendingExecutionResult::RESULT_NOT_READY;
	while (!duckdb::PendingQueryResult::IsResultReady(execution)) {
		execution = pending->ExecuteTask();
	}
	if (execution == duckdb::PendingExecutionResult::EXECUTION_ERROR) {
		pending->ThrowError();
	}
	state->query_results = pending->Execute();
	state->column_count = state->query_results->ColumnCount();
	state->executed = true;
}

TupleTableSlot *
ExecScan_Cpp(CustomScanState *node) {
	auto *state = (DuckLakeScanState *)node;
	try {
		TupleTableSlot *slot = state->css.ss.ss_ScanTupleSlot;
		if (!state->executed) {
			ExecuteInDuckDB(state);
		}
		if (state->fetch_next) {
			state->current_chunk = state->query_results->Fetch();
			state->current_row = 0;
			state->fetch_next = false;
			if (!state->current_chunk || state->current_chunk->size() == 0) {
				MemoryContextReset(state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
				ExecClearTuple(slot);
				return slot;
			}
		}
		MemoryContextReset(state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
		ExecClearTuple(slot);
		MemoryContext old_ctx = MemoryContextSwitchTo(state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
		for (duckdb::idx_t col = 0; col < state->column_count; col++) {
			auto value = state->current_chunk->GetValue(col, state->current_row);
			if (value.IsNull()) {
				slot->tts_isnull[col] = true;
			} else {
				slot->tts_isnull[col] = false;
				if (!pgduckdb::ConvertDuckToPostgresValue(slot, value, col, /*resolver=*/nullptr)) {
					MemoryContextSwitchTo(old_ctx);
					throw duckdb::Exception(duckdb::ExceptionType::CONVERSION, "value conversion failed");
				}
			}
		}
		MemoryContextSwitchTo(old_ctx);
		state->current_row++;
		if (state->current_row >= state->current_chunk->size()) {
			state->current_chunk.reset();
			state->fetch_next = true;
		}
		ExecStoreVirtualTuple(slot);
		return slot;
	} catch (std::exception &) {
		ResetScanState(state);
		throw;
	}
}

TupleTableSlot *
ExecScan(CustomScanState *node) {
	return InvokeCPPFunc(ExecScan_Cpp, node);
}

void
EndScan_Cpp(CustomScanState *node) {
	ResetScanState((DuckLakeScanState *)node);
}

void
EndScan(CustomScanState *node) {
	InvokeCPPFunc(EndScan_Cpp, node);
}

void
ReScanScan(CustomScanState * /*node*/) {
	// Not yet supported -- callers that rescan will see the first result
	// set again. Matches pg_duckdb's current behaviour (its ReScan is a
	// no-op too).
}

// ---- Plan construction -------------------------------------------------

RangeTblEntry *
MakeScanRangeTableEntry(CustomScan *custom_scan) {
	List *column_names = NIL;
	foreach_node(TargetEntry, te, custom_scan->scan.plan.targetlist) {
		column_names = lappend(column_names, makeString(te->resname));
	}
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	// RTE_NAMEDTUPLESTORE avoids the cluster of asserts RTE_RELATION imposes
	// on fields we don't fill in; pg_duckdb uses the same trick.
	rte->rtekind = RTE_NAMEDTUPLESTORE;
	rte->eref = makeAlias("ducklake_scan", column_names);
	rte->inFromCl = true;
	return rte;
}

Plan *
CreatePlanTree_Cpp(Query *query) {
	auto prepared = PrepareQuery(query, /*explain_prefix=*/nullptr);
	if (prepared->HasError()) {
		elog(ERROR, "(pg_ducklake/CreatePlan) Prepared query returned an error: %s", prepared->GetError().c_str());
	}
	CustomScan *cscan = makeNode(CustomScan);
	auto &result_types = prepared->GetTypes();
	auto &result_names = prepared->GetNames();
	for (size_t i = 0; i < result_types.size(); i++) {
		Oid pg_type = pgduckdb::GetPostgresDuckDBType(result_types[i], /*throw_error=*/true, /*resolver=*/nullptr);
		HeapTuple type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pg_type));
		if (!HeapTupleIsValid(type_tup)) {
			elog(ERROR, "(pg_ducklake/CreatePlan) cache lookup failed for type %u", pg_type);
		}
		Form_pg_type tform = (Form_pg_type)GETSTRUCT(type_tup);
		int32 typmod = pgduckdb::GetPostgresDuckDBTypemod(result_types[i]);
		Var *var = makeVar(1, i + 1, pg_type, typmod, tform->typcollation, 0);
		TargetEntry *target =
		    makeTargetEntry((Expr *)var, i + 1, (char *)pstrdup(result_names[i].c_str()), false);
		cscan->custom_scan_tlist = lappend(cscan->custom_scan_tlist, copyObjectImpl(target));
		var->varno = INDEX_VAR;
		cscan->scan.plan.targetlist = lappend(cscan->scan.plan.targetlist, target);
		ReleaseSysCache(type_tup);
	}
	cscan->custom_private = list_make1(query);
	cscan->methods = &g_scan_methods;
	return (Plan *)cscan;
}

Plan *
CreatePlanTree(Query *query) {
	return InvokeCPPFunc(CreatePlanTree_Cpp, query);
}

} // anonymous namespace

void
InitQueryPlan() {
	memset(&g_scan_methods, 0, sizeof(g_scan_methods));
	g_scan_methods.CustomName = "DuckLakeScan";
	g_scan_methods.CreateCustomScanState = CreateScanState;
	RegisterCustomScanMethods(&g_scan_methods);

	memset(&g_exec_methods, 0, sizeof(g_exec_methods));
	g_exec_methods.CustomName = "DuckLakeScan";
	g_exec_methods.BeginCustomScan = BeginScan;
	g_exec_methods.ExecCustomScan = ExecScan;
	g_exec_methods.EndCustomScan = EndScan;
	g_exec_methods.ReScanCustomScan = ReScanScan;
}

PlannedStmt *
DuckLakePlanQuery(Query *parse, int /*cursor_options*/) {
	if (parse->commandType != CMD_SELECT) {
		/* INSERT/UPDATE/DELETE still ride on TryCreateDirectInsertPlan /
		 * the DDL-trigger path. Routing them through this CustomScan
		 * recurses indefinitely because the CREATE TABLE AS deparse emits
		 * its own INSERT that would land right back here. */
		return nullptr;
	}
	if (!QueryTreeHasDuckLakeItems((Node *)parse, nullptr)) {
		return nullptr;
	}

	Plan *plan = CreatePlanTree(parse);
	CustomScan *cscan = castNode(CustomScan, plan);
	RangeTblEntry *rte = MakeScanRangeTableEntry(cscan);

	PlannedStmt *result = makeNode(PlannedStmt);
	result->commandType = parse->commandType;
	result->queryId = parse->queryId;
	result->hasReturning = (parse->returningList != NIL);
	result->hasModifyingCTE = parse->hasModifyingCTE;
	result->canSetTag = parse->canSetTag;
	result->transientPlan = false;
	result->dependsOnRole = false;
	result->parallelModeNeeded = false;
	result->planTree = plan;
	result->rtable = list_make1(rte);
#if PG_VERSION_NUM >= 160000
	result->permInfos = NULL;
#endif
	result->resultRelations = NULL;
	result->appendRelations = NULL;
	result->subplans = NIL;
	result->rewindPlanIDs = NULL;
	result->rowMarks = NIL;
	result->relationOids = NIL;
	result->invalItems = NIL;
	result->paramExecTypes = NIL;
	result->utilityStmt = parse->utilityStmt;
	result->stmt_location = parse->stmt_location;
	result->stmt_len = parse->stmt_len;
	return result;
}

} // namespace pgducklake
