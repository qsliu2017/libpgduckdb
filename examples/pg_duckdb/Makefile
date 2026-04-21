# examples/pg_duckdb/Makefile -- STUB.
#
# This Postgres extension is the product-shaped consumer of libpgduckdb
# (archive at ../../libpgduckdb_core.a). After the step-2 file move it is
# intentionally unbuildable: its sources were written against the old
# monolithic layout and reach into code that has since moved out of root
# (CoreInit, GUC registration, hook registration) or still needs lib-side
# API work (DuckDBManager setters replacing GUC reads, xact callback hooks).
#
# Step 3 wires this up:
#   1. PGXS-based Makefile mirroring the old root Makefile's compile flags
#      (PG_CPPFLAGS, PG_CXXFLAGS, DUCKDB include paths, lz4, stdc++).
#   2. MODULE_big = pg_duckdb; EXTENSION = pg_duckdb; DATA = pg_duckdb.control
#      + sql/pg_duckdb--*.sql.
#   3. SHLIB_LINK += ../../libpgduckdb_core.a -lduckdb (via -L...).
#   4. installcheck / pycheck / schedulecheck targets that used to live at
#      root, pointed at this directory's test/.
#   5. A trigger that runs `make -C ../.. core-lib` before linking.
#
# Until then, `make` here errors out to make the missing wiring obvious.

$(error pg_duckdb example not yet wired up; see step 3 of the distill plan)
