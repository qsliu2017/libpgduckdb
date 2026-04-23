# DuckDB extensions baked into libduckdb for the pg_ducklake consumer.
#
# ducklake is statically linked so pg_ducklake can call
# db.LoadStaticExtension<duckdb::DucklakeExtension>() without runtime
# INSTALL/LOAD. Pinned to the same SHA DuckDB v1.4.3 itself ships with
# (see third_party/duckdb/.github/config/extensions/ducklake.cmake).
#
# json/icu/httpfs come along because pg_ducklake's regression suite and
# default initialization path expect them to be available.

duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 9c7d34977b10346d0b4cbbde5df807d1dab0b2bf
)
duckdb_extension_load(ducklake
    GIT_URL https://github.com/duckdb/ducklake
    GIT_TAG de813ff4d052bffe3e9e7ffcdc31d18ca38e5ecd
)
