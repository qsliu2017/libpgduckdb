# DuckDB extensions baked into libduckdb for the pg_vortex consumer.
# vortex is statically linked so pg_vortex.cpp doesn't need runtime
# INSTALL/LOAD. Pinned to the "chore: bump extension to duckdb 1.5.2"
# commit on vortex-data/duckdb-vortex#main; v0.56.0 targets DuckDB
# v1.4.x and its C++ plugin (table_filter.cpp, table_function.cpp)
# fails to link against v1.5.2 with undefined references to changed
# TableFunction / ExceptionFormatValue signatures.

duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 9de3296f40ed03e8e063394887f0d6a46144e847
)
duckdb_extension_load(vortex
    GIT_URL https://github.com/vortex-data/duckdb-vortex
    GIT_TAG b5fc172130020adcb28b4fe78665cf4ed0069ad0
    SUBMODULES vortex
    APPLY_PATCHES
)
