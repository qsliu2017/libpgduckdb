# DuckDB extensions baked into libduckdb for the pg_vortex consumer.
# vortex is statically linked so pg_vortex.cpp doesn't need runtime
# INSTALL/LOAD. The v0.56.0 tag is what DuckDB v1.4.3 itself pairs with
# (see third_party/duckdb/.github/config/extensions/vortex.cmake).

duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 9de3296f40ed03e8e063394887f0d6a46144e847
)
duckdb_extension_load(vortex
    GIT_URL https://github.com/vortex-data/duckdb-vortex
    GIT_TAG v0.56.0
    SUBMODULES vortex
    APPLY_PATCHES
)
