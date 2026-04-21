# libpgduckdb

A reusable library distilled from [pg_duckdb][pg_duckdb_upstream]: the mechanical plumbing for embedding [DuckDB](https://duckdb.org) inside a Postgres extension -- connection management, Postgres heap scanning from DuckDB, type conversion, plan deparse -- without the opinionated layer on top (query routing hooks, MotherDuck, the `duckdb.*` SQL surface, etc.).

This repo is **not itself a Postgres extension**. It has no `_PG_init`, no `.control`, no SQL scripts, no GUCs. It is a source-level library that downstream extensions link into their own `.so`.

## Status

Work in progress. See [`GOAL.md`](GOAL.md) for the scope rules and [`CLAUDE.md`](CLAUDE.md) for the map from subsystem to file. Today:

- `make core-lib` at root is the intended target but is currently unbuildable -- several lib files still `#include` headers that have moved out. Fixing this is the next step.
- `examples/pg_duckdb/` is the reference consumer: a full-fat rebuild of the upstream extension on top of this library. `make -C examples/pg_duckdb check` exercises the same regression + pytest suites that upstream ships.
- `examples/pg_parquet/` is planned: a minimum-surface consumer that proves the library can be used without inheriting pg_duckdb's opinionated layer.

## Layout

    src/, include/      Library sources and public headers.
    third_party/duckdb  DuckDB submodule (pinned in Makefile).
    examples/           Downstream extensions built on the library.
    Makefile            Produces libpgduckdb_core.a.

## Upstream

Tracked against [`duckdb/pg_duckdb`][pg_duckdb_upstream] `main`. Changes specific to this branch are limited to the distill work; feature development continues to happen upstream.

## License

MIT, same as upstream. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

[pg_duckdb_upstream]: https://github.com/duckdb/pg_duckdb
