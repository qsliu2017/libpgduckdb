#pragma once

// pgduckdb.cpp
extern "C" void _PG_init(void);

// pgduckdb_hooks.c
void DuckdbInitHooks(void);

namespace pgduckdb {
// Wires pg_duckdb into libpgduckdb's extension points (DuckDBManager
// configuration, pseudo-type OIDs, scan parallelism, etc.). Defined in
// pgduckdb_lib_hooks.cpp. Called from _PG_init before InitGUCHooks so the
// hooks are live before the GUCs that invalidate secrets can fire.
void RegisterLibHooks();

// Called whenever a duckdb.create_*_secret row changes. Defined in
// pgduckdb_lib_hooks.cpp; uses static state tracked there to drop+reload
// secrets on the next connection refresh.
void InvalidateDuckDBSecretsIfInitialized();
} // namespace pgduckdb
