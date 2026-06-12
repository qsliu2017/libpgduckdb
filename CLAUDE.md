# CLAUDE.md

Guidance for AI coding assistants working in this repository.

## What this repo is

The **libpgddb kernel** plus the PostgreSQL extensions built on it:

- `libpgduckdb/` -- the kernel (`namespace pgddb`): reusable "use DuckDB inside
  a PG extension" infrastructure (DuckDB instance lifecycle, planner-offload
  primitives, PG heap scan from DuckDB, CustomScan execution, PG-side
  wrappers). Not itself a PG extension: no `.control`, no `_PG_init`. Its `.o`
  files are bundled into each extension's shared library at link time.
- `pg_ducklake/` -- the primary extension: DuckLake lakehouse tables in
  PostgreSQL (`namespace pgducklake`). Headers in `include/`, implementation
  in `src/`, tests in `test/`.
- `pg_duckdb/` -- the original "DuckDB inside PostgreSQL" extension.
- `examples/pg_vortex/` -- minimal example extension demonstrating kernel reuse.
- `duckdb/` -- DuckDB submodule shared by all extensions.

## pg_ducklake architecture

### Lifecycle scopes

```
PG extension           CREATE/DROP EXTENSION pg_ducklake
PG backend process     _PG_init, one per connection
  +-- DuckDB instance  [1..N via recycle_ddb]
```

PG extension and PG backend process are parallel -- the extension creates SQL
objects (table AM, functions, metadata tables), while each backend process
independently initializes its own state.

| Scope              | Entrypoint                                                        |
| ------------------ | ----------------------------------------------------------------- |
| PG extension       | `pg_ducklake/sql/pg_ducklake--*.sql`                              |
| PG backend process | `pg_ducklake/src/pgducklake.cpp` (`_PG_init`)                     |
| DuckDB instance    | `pg_ducklake/src/duckdb_manager.cpp` (`DuckDBManager::OnPostInit`)|

1. **PG extension**: created/destroyed by `CREATE/DROP EXTENSION pg_ducklake`.
   SQL catalog objects: `ducklake` schema and metadata tables, table AM
   `ducklake`, index AM `ducklake_sorted`, SQL functions and procedures, event
   triggers, predefined roles.
2. **PG backend process**: created by `_PG_init()` when `pg_ducklake.so` loads,
   destroyed on backend exit. C++ static variables and static class members
   belong here -- they live as long as the process regardless of DuckDB
   instance recycling.
3. **DuckDB instance**: created by `DuckDBManager::Initialize()` on first
   DuckDB query, destroyed by `recycle_ddb()` or backend exit. Everything
   registered on `db.instance` is lost on recycle and re-created by
   `DuckDBManager::OnPostInit`. Per-transaction `PgDuckLakeMetadataManager`
   instances also belong here.
4. **Background maintenance worker**: registered by `_PG_init()`. A persistent
   launcher (`ducklake_maintenance_launcher`) spawns short-lived workers per
   database that flush inlined data, expire snapshots, and compact data files.
   VACUUM on ducklake tables is a no-op; all maintenance goes through the
   worker. See `pg_ducklake/src/maintenance_worker.cpp`.

Each source file declares its scopes in the header comment; these tags are the
source of truth for per-file classification:

```cpp
/*
 * @scope backend: register GUCs
 * @scope duckdb-instance: register wrapper macros in DuckDB catalog
 */
```

### DDL path

DDL is executed by PostgreSQL, then `ducklake_<ddl>_trigger` event triggers
(see `pg_ducklake/src/ducklake_table.cpp`) synchronize the corresponding
DuckDB objects in `PGDUCKLAKE_DUCKDB_CATALOG`.

### DML path

DML referencing ducklake objects is caught by pg_ducklake's planner hook
(`DucklakePlannerHook` in `pg_ducklake/src/hooks.cpp`). When a query references
a ducklake-AM table or a ducklake-only function, the whole query is routed to
the kernel's CustomScan via `pgddb::PlanNode`, which deparses it to DuckDB SQL
and executes it in DuckDB. Ducklake tables are deparsed as
`pgducklake.<schema_name>.<table_name>` via the `pgddb_db_and_schema` hook
(`DbAndSchemaForDucklake` in `pg_ducklake/src/duckdb_manager.cpp`).

## Build and test

See the `setup-dev` skill for full dev environment setup, `coding-rules` for
style/docs rules, and `commit-message-format` for commits.

Supported PostgreSQL versions: 14-18. `PG_CONFIG` is required; usually a local
PostgreSQL is installed under the workdir (e.g.
`PG_CONFIG=$(pwd)/pg-18/bin/pg_config`) to avoid conflicts with other
worktrees. If neither a local nor a global pg is found, stop and ask the user.

The root `Makefile` defaults to pg_ducklake; other components are reached with
`make <dir>/<target>` delegation or `make -C`.

```bash
# macOS: prefix builds with LIBRARY_PATH="$(brew --prefix)/lib"
NCPU=$(nproc 2>/dev/null || sysctl -n hw.ncpu)

PG_CONFIG=<pg_config> make -j"$NCPU"        # build pg_ducklake (default target)
PG_CONFIG=<pg_config> make install          # install pg_ducklake
PG_CONFIG=<pg_config> make installcheck     # pg_ducklake regression + isolation
PG_CONFIG=<pg_config> make check-regression TEST=basic
PG_CONFIG=<pg_config> make check-isolation TEST=concurrent_writes
PG_CONFIG=<pg_config> make check-e2e        # needs uv; optional dockerized MinIO
PG_CONFIG=<pg_config> make format           # clang-format pg_ducklake src/ include/
PG_CONFIG=<pg_config> make check-format

# Other extensions, via delegation:
PG_CONFIG=<pg_config> make pg_duckdb/all -j"$NCPU"
PG_CONFIG=<pg_config> make pg_duckdb/installcheck
PG_CONFIG=<pg_config> make examples/pg_vortex/installcheck
```

pg_ducklake tests live in `pg_ducklake/test/regression/` (SQL regression),
`test/isolation/` (concurrency specs), and `test/e2e/` (external-client
integration). Prefer regression and isolation tests to verify functionality.

## C/C++ header & include rules

PG and DuckDB headers are include-order-sensitive: PG's `elog.h`
`#define FATAL` clobbers DuckDB's `ExceptionType::FATAL`, so DuckDB headers
must precede `postgres.h`.

### Headers (`.h`/`.hpp`)

- `#pragma once` on the first line; a brief header comment only if needed.
- No `postgres.h` in headers -- get PG structs from
  `libpgduckdb/include/pgddb/pg/declarations.hpp` (add forward decls there).
  Exception: a PG type that can't be forward-declared (e.g. plain enums).
- Don't declare what `PG_FUNCTION_INFO_V1` already declares.

### Source `.cpp` include groups

Ordered groups, blank line between, alphabetical within each:

1. own/sibling module headers (`"pgducklake/..."`, or `"pgddb/..."` in the kernel)
2. C/C++ std `<...>`
3. postgres-free `"pgddb/..."` (extensions; these pull DuckDB)
4. DuckDB/DuckLake `<...>`
5. `extern "C"` { `postgres.h` first, then PG headers, then postgres-dependent
   pgddb C headers like `pgddb_ruleutils.h` }

Postgres last keeps the FATAL ordering automatic. Postgres-dependent pgddb
*C++* headers (`utility/cpp_wrapper.hpp`, `pgddb_node.hpp`) go AFTER group 5
(after the `extern "C"` block), without an explanatory comment.

`.clang-format` uses `SortIncludes: false` / `IncludeBlocks: Preserve` -- order
is manual. ASCII only.

## Gotchas

- **ducklake dependency / patch structure**: `pg_ducklake/third_party/ducklake`
  is a git **submodule** pinned to a community `duckdb/ducklake` commit. Our
  divergence is **not** committed into its tree -- it lives as an ordered
  series of patch files `third_party/ducklake-NNN-<desc>.patch` that the
  Makefile applies onto the pristine checkout at build time (apply-once,
  stamp-guarded via `DUCKLAKE_STAMP`; the submodule working tree is
  intentionally left dirty/patched, the gitlink stays pinned). To see what we
  changed vs. community, read those patch files. Change behavior by
  adding/editing patch files, never by committing into the submodule tree.
- **duckdb submodule**: pinned upstream source shared by all extensions; bump
  the gitlink only, never commit edits into its tree.

## Miscellaneous

- When exploring multiple files, run tool calls in parallel whenever possible.
- **Never `cd` into subdirectories** in Bash commands -- it changes the working
  directory for subsequent calls. Use subshells
  (`(cd third_party/ducklake && git ...)`) or `pushd`/`popd`.
