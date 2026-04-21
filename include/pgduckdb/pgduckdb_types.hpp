#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "pgduckdb/pg/declarations.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

struct PostgresScanGlobalState;
struct PostgresScanLocalState;

// DuckDB has date starting from 1/1/1970 while PG starts from 1/1/2000
constexpr int32_t PGDUCKDB_DUCK_DATE_OFFSET = 10957;
constexpr int64_t PGDUCKDB_DUCK_TIMESTAMP_OFFSET =
    static_cast<int64_t>(PGDUCKDB_DUCK_DATE_OFFSET) * static_cast<int64_t>(86400000000) /* USECS_PER_DAY */;

// Check from regress/sql/date.sql
#define PG_MINYEAR  (-4713)
#define PG_MINMONTH (11)
#define PG_MINDAY   (24)
#define PG_MAXYEAR  (5874897)
#define PG_MAXMONTH (12)
#define PG_MAXDAY   (31)

const duckdb::date_t PGDUCKDB_PG_MIN_DATE_VALUE = duckdb::Date::FromDate(PG_MINYEAR, PG_MINMONTH, PG_MINDAY);
const duckdb::date_t PGDUCKDB_PG_MAX_DATE_VALUE = duckdb::Date::FromDate(PG_MAXYEAR, PG_MAXMONTH, PG_MAXDAY);

// Check ValidTimestampOrTimestampTz() for the logic, These values are counted from 1/1/1970
constexpr int64_t PGDUCKDB_MAX_TIMESTAMP_VALUE = 9223371244800000000;
constexpr int64_t PGDUCKDB_MIN_TIMESTAMP_VALUE = -210866803200000000;

/*
 * Struct-of-callbacks (PG FdwRoutine-style) that a consumer passes to the
 * type-conversion entry points. All fields are optional; a NULL function
 * pointer (or a NULL resolver) means "lib default":
 *   - unknown Postgres Oid -> CreateUnsupportedPostgresType
 *   - unknown duckdb::LogicalTypeId (STRUCT / UNION / MAP) -> return InvalidOid
 *   - unsupported NUMERIC -> reject
 *
 * Callbacks are expected to be pure w.r.t. lib state. They may read the
 * Postgres syscache. They MUST NOT call DuckDB from the PG side or PG APIs
 * from the DuckDB side -- the usual two-error-worlds rule applies.
 *
 * See examples/pg_duckdb/src/pgduckdb_lib_hooks.cpp for a filled-in resolver.
 */
struct TypeResolver {
	/*
	 * Ext-first Postgres -> DuckDB mapping. Returning LogicalType::INVALID
	 * (the default ctor) means "not my concern" -- lib will handle via its
	 * built-in switch on pg_type Oids.
	 */
	duckdb::LogicalType (*postgres_to_duckdb)(Form_pg_attribute attribute) = nullptr;

	/*
	 * DuckDB LogicalType -> Postgres Oid. Called by lib for type IDs that
	 * don't have a built-in mapping (STRUCT / UNION / MAP and their array
	 * counterparts). Return InvalidOid if the ext doesn't expose this type.
	 */
	Oid (*duckdb_to_postgres_oid)(const duckdb::LogicalType &type) = nullptr;
	Oid (*duckdb_to_postgres_array_oid)(const duckdb::LogicalType &type) = nullptr;

	/*
	 * Handle Oids that don't land in lib's built-in switch during Duck ->
	 * Postgres value conversion (ext pseudo-types). Return true if handled
	 * (and slot->tts_values[col] has been written), false to let the caller
	 * emit the usual "Unsupported pgduckdb type" warning.
	 */
	bool (*convert_duck_value_to_postgres)(Oid oid, const duckdb::Value &value,
	                                       TupleTableSlot *slot, int col) = nullptr;

	/*
	 * Policy for NUMERICs whose precision/scale DuckDB can't express. Return
	 * true to coerce to DOUBLE, false to reject. nullptr -> reject.
	 */
	bool (*convert_unsupported_numeric_to_double)() = nullptr;
};

void CheckForUnsupportedPostgresType(duckdb::LogicalType type);
duckdb::LogicalType ConvertPostgresToDuckColumnType(Form_pg_attribute &attribute, const TypeResolver *resolver = nullptr);
duckdb::LogicalType ConvertPostgresToBaseDuckColumnType(Form_pg_attribute &attribute,
                                                        const TypeResolver *resolver = nullptr);
Oid GetPostgresDuckDBType(const duckdb::LogicalType &type, bool throw_error = false,
                          const TypeResolver *resolver = nullptr);
Oid GetPostgresArrayDuckDBType(const duckdb::LogicalType &type, bool throw_error = false,
                               const TypeResolver *resolver = nullptr);
int32_t GetPostgresDuckDBTypemod(const duckdb::LogicalType &type);
duckdb::Value ConvertPostgresParameterToDuckValue(Datum value, Oid postgres_type);
void ConvertPostgresToDuckValue(Oid attr_type, Datum value, duckdb::Vector &result, uint64_t offset);
bool ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, uint64_t col,
                                const TypeResolver *resolver = nullptr);

/*
 * Helpers exported for consumer TypeResolver implementations that need to
 * hand back a Postgres Datum for a DuckDB Value carrying an ext pseudo-type
 * (duckdb.struct, duckdb.union, duckdb.map, ...).
 *
 * ConvertDuckValueToStringDatum -- DuckDB Value -> PG text Datum using
 * duckdb::Value::ToString(). Preferred for free-form pseudo-types that PG
 * sees as text internally.
 *
 * ConvertDuckToPostgresRuntimeArray -- walks a DuckDB LIST/ARRAY value,
 * invokes `element_to_datum` for each leaf element, and emits a Postgres
 * multi-dimensional array of the given `element_oid` with the standard
 * variable-length layout (typlen=-1, typbyval=false, typalign='i').
 */
Datum ConvertDuckValueToStringDatum(const duckdb::Value &value);

using DuckValueToDatum = Datum (*)(const duckdb::Value &value);
void ConvertDuckToPostgresRuntimeArray(TupleTableSlot *slot, const duckdb::Value &value, int col, Oid element_oid,
                                       DuckValueToDatum element_to_datum);
void InsertTupleIntoChunk(duckdb::DataChunk &output, PostgresScanLocalState &scan_local_state, TupleTableSlot *slot);
void InsertTuplesIntoChunk(duckdb::DataChunk &output, PostgresScanLocalState &scan_local_state, TupleTableSlot **slots,
                           int num_slots);

} // namespace pgduckdb
