#pragma once

// Slim, instantiable lazy-singleton-DuckDB base shared by every libpgduckdb
// consumer that wants the "one duckdb::DuckDB per backend, recycle via Reset"
// shape. Concrete subclasses (PgDuckDBManager in pg_duckdb, DuckLakeManager in
// pg_ducklake) layer their own connection caches, refresh state, and static
// accessors on top.
//
// Two no-op virtual hooks (default to no-op so the base is instantiable):
//   OnInitialized(DuckDB&) -- post-construction setup (ATTACH, SET, LOAD, ...)
//   OnReset()              -- cleanup before database deletion (drop conn cache)
//
// The default Initialize() runs `new duckdb::DuckDB(nullptr); OnInitialized;`,
// which matches pg_ducklake's pre-refactor lazy-init behavior. Subclasses that
// need a custom duckdb::DuckDB construction (e.g. pg_duckdb's GUC-driven
// DBConfig) override Initialize() entirely and call OnInitialized themselves.
// We deliberately don't expose a Configure(DBConfig&) hook because passing a
// stack-local DBConfig& to duckdb::DuckDB(...) causes a hang in DuckLake's
// catalog initialization -- DBConfig's encoding/arrow/type subsystems capture
// `*this` by reference at ctor time and dangle when the stack-local goes away.

namespace duckdb {
class DuckDB;
} // namespace duckdb

namespace pgduckdb {

class DuckDBManager {
public:
	DuckDBManager() = default;
	virtual ~DuckDBManager() = default;

	DuckDBManager(const DuckDBManager &) = delete;
	DuckDBManager &operator=(const DuckDBManager &) = delete;

	// Returns the lazy-constructed instance. First call runs Configure ->
	// duckdb::DuckDB ctor -> OnInitialized; subsequent calls hit the cache.
	duckdb::DuckDB &GetDatabase();

	bool
	IsInitialized() const noexcept {
		return database != nullptr;
	}

	// Tear down the cached instance. Next GetDatabase() rebuilds. Calls
	// OnReset() first so subclasses can drop their own caches before the
	// duckdb::DuckDB goes away.
	void Reset();

protected:
	virtual void OnInitialized(duckdb::DuckDB &db);
	virtual void OnReset();

	// Default: `database = new duckdb::DuckDB(nullptr); OnInitialized(*database);`.
	// Subclasses that need a custom duckdb::DuckDB construction (e.g. a
	// GUC-built DBConfig, a thread-signal guard scoped over construction)
	// override this entirely; they assign `database` themselves and call
	// OnInitialized themselves. They MUST NOT call DuckDBManager::Initialize()
	// from inside their override -- the base path uses DuckDB(nullptr) and is
	// not parameterizable by design (see file-level comment for why).
	virtual void Initialize();

	/*
	 * Raw pointer (not unique_ptr) so the destructor never runs at process
	 * exit. Some DuckDB extensions (historically MotherDuck) abort during
	 * their destructor on backend shutdown, which would crash Postgres. The
	 * intentional leak is fine -- the OS reclaims the address space anyway,
	 * and Reset() still runs the destructor when an extension explicitly
	 * recycles. Mirror PgDuckDBManager's longstanding stance.
	 */
	duckdb::DuckDB *database = nullptr;
};

} // namespace pgduckdb
