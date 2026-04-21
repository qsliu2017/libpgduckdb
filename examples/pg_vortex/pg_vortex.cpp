// pg_vortex: minimum-surface consumer of libpgduckdb_core.a.
//
// Registers one SQL-level SRF (read_vortex(path text) RETURNS SETOF record)
// that opens a transient DuckDB connection, forwards the query to DuckDB's
// own read_vortex() (shipped by the Vortex community extension), and
// materializes the result into Postgres tuples. No planner hook, no custom
// scan, no GUCs, no shared_preload.

#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <memory>
#include <sstream>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"
}

extern "C" {

PG_MODULE_MAGIC;

void
_PG_init(void) {
	// Nothing to do at load time. DuckDB is brought up lazily on the first
	// read_vortex() call.
}

} // extern "C"

namespace {

// Lazy in-memory DuckDB instance scoped to the backend. A fresh
// duckdb::Connection is opened per call -- the cost is trivial for this
// minimal example and avoids the connection-cache lifecycle that the
// full-fat pg_duckdb DuckDBManager exists to manage.
//
// On first create we INSTALL+LOAD the Vortex core extension so callers don't
// have to. INSTALL hits the network once per backend lifetime; subsequent
// backends pick up the cached download from ~/.duckdb/extensions.
duckdb::DuckDB &
GetDatabase() {
	static std::unique_ptr<duckdb::DuckDB> instance;
	if (!instance) {
		instance = std::make_unique<duckdb::DuckDB>(nullptr);
		duckdb::Connection bootstrap(*instance);
		pgduckdb::DuckDBQueryOrThrow(bootstrap, "INSTALL vortex");
		pgduckdb::DuckDBQueryOrThrow(bootstrap, "LOAD vortex");
	}
	return *instance;
}

} // namespace

extern "C" {

PG_FUNCTION_INFO_V1(pg_vortex_read_vortex);

Datum
pg_vortex_read_vortex(PG_FUNCTION_ARGS) {
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) || (rsinfo->allowedModes & SFRM_Materialize) == 0) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("read_vortex: materialize mode required")));
	}

	TupleDesc tupdesc = NULL;
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("read_vortex: function returning record called in context that cannot accept type record"),
		                errhint("call as SELECT * FROM read_vortex(...) AS t(col1 type1, ...)")));
	}

	char *path = text_to_cstring(PG_GETARG_TEXT_PP(0));

	// Drive the DuckDB call under the per-query memory context so anything
	// that palloc()s on the Postgres side lives for the right duration.
	MemoryContext per_query = rsinfo->econtext->ecxt_per_query_memory;
	MemoryContext oldctx = MemoryContextSwitchTo(per_query);

	Tuplestorestate *tupstore = tuplestore_begin_heap(true, false, work_mem);
	TupleTableSlot *slot = MakeTupleTableSlot(tupdesc, &TTSOpsVirtual);

	try {
		auto &db = GetDatabase();
		duckdb::Connection conn(db);

		std::ostringstream oss;
		oss << "SELECT * FROM read_vortex(" << duckdb::KeywordHelper::WriteQuoted(path, '\'') << ")";
		auto result = pgduckdb::DuckDBQueryOrThrow(conn, oss.str());

		if (result->ColumnCount() != (idx_t)tupdesc->natts) {
			ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
			                errmsg("read_vortex: column count mismatch: DuckDB returned %u column(s), "
			                       "caller declared %d",
			                       (unsigned)result->ColumnCount(), tupdesc->natts)));
		}

		while (auto chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				ExecClearTuple(slot);
				for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
					duckdb::Value value = chunk->GetValue(col, row);
					pgduckdb::ConvertDuckToPostgresValue(slot, value, col, nullptr);
				}
				ExecStoreVirtualTuple(slot);
				tuplestore_puttupleslot(tupstore, slot);
			}
		}
	} catch (const duckdb::Exception &ex) {
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("read_vortex: %s", ex.what())));
	} catch (const std::exception &ex) {
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("read_vortex: %s", ex.what())));
	}

	ExecDropSingleTupleTableSlot(slot);
	MemoryContextSwitchTo(oldctx);

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	PG_RETURN_NULL();
}

} // extern "C"
