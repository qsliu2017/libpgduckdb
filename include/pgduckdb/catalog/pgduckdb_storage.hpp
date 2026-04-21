#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

struct TypeResolver;

/*
 * Knobs that used to live behind the `threads_for_postgres_scan`,
 * `max_workers_per_postgres_scan`, and `log_pg_explain` lib hooks. Ext
 * fills these in when constructing the PostgresStorageExtension; scan and
 * table-reader code reads them from the attached catalog.
 *
 * Defaults preserve the "lib alone" behavior -- no parallelism, no logging.
 */
struct PostgresStorageOptions {
	/* DuckDB consumer-side concurrency for a Postgres scan. 0 -> DuckDB default
	 * (capped at 1 when the table_reader doesn't fan out to PG workers). */
	int threads_for_postgres_scan = 0;
	/* Cap on PG parallel workers launched per scan. 0 -> no parallel workers. */
	int max_workers_per_postgres_scan = 0;
	/* If true, NOTICE-log the PG plan that backs each scan. */
	bool log_pg_explain = false;
};

/*
 * Storage-info blob attached to the PostgresStorageExtension and forwarded
 * by DuckDB to PostgresCatalog::Attach. Carries the consumer's options and
 * a non-owning pointer to the consumer's TypeResolver (ext-specific
 * pseudo-type glue). Lifetime of `resolver` must outlive the DuckDB
 * database -- in practice ext stores it as a `const` global.
 */
struct PostgresStorageInfo : public duckdb::StorageExtensionInfo {
	PostgresStorageInfo() : options(), resolver(nullptr) {
	}

	// Not copyable: non-owning TypeResolver pointer.
	PostgresStorageInfo(const PostgresStorageInfo &) = delete;
	PostgresStorageInfo &operator=(const PostgresStorageInfo &) = delete;

	PostgresStorageOptions options;
	const TypeResolver *resolver;
};

class PostgresStorageExtension : public duckdb::StorageExtension {
public:
	PostgresStorageExtension();
	PostgresStorageExtension(const PostgresStorageOptions &options, const TypeResolver *resolver);
};

} // namespace pgduckdb
