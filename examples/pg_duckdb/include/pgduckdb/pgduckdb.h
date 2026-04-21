#pragma once

// pgduckdb.cpp
extern "C" void _PG_init(void);

// pgduckdb_hooks.c
void DuckdbInitHooks(void);

// InvalidateDuckDBSecretsIfInitialized is declared in pgduckdb_duckdb.hpp
// (ext-side) -- see that header for the full declaration and comment.
