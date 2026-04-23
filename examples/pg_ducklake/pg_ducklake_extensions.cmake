# DuckDB extensions baked into libduckdb for the pg_ducklake consumer.
#
# ducklake is loaded from the in-tree SOURCE_DIR (examples/pg_ducklake/
# third_party/ducklake) because the vendored copy carries local patches
# (metadata-manager virtual interface, transaction lifecycle) that the
# pg_ducklake sources rely on. Do NOT replace with GIT_URL/GIT_TAG against
# upstream duckdb/ducklake -- the APIs will not match.
#
# json/icu/httpfs are pulled from upstream; pg_ducklake's regression suite
# and default initialization path expect them to be statically available.

duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 13e18b3c9f3810334f5972b76a3acc247b28e537
)
duckdb_extension_load(ducklake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/third_party/ducklake
)
