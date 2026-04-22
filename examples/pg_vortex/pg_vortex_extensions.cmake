# DuckDB extensions baked into libduckdb for the pg_vortex consumer.
#
# Baseline (json/icu/httpfs) mirrors the pg_duckdb set so the standard SQL
# surface works out of the box. vortex is intended to land here as well so
# that pg_vortex.cpp can drop its runtime INSTALL/LOAD bootstrap -- add the
# duckdb_extension_load(vortex ...) entry once the upstream repo + a
# DuckDB-v1.4.3-compatible tag are pinned.

duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 9c7d34977b10346d0b4cbbde5df807d1dab0b2bf
)
# TODO: statically link vortex here once we have a concrete GIT_URL/GIT_TAG.
# duckdb_extension_load(vortex
#     GIT_URL https://github.com/spiraldb/vortex-duckdb-extension
#     GIT_TAG <pinned>
# )
