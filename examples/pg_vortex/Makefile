# examples/pg_vortex/Makefile -- minimum-surface libpgduckdb consumer.
# Pulls $(CORE_OBJS) from the root Makefile into its own shlib.

.PHONY: duckdb install check-regression-vortex clean-regression installcheck

MODULE_big = pg_vortex
EXTENSION = pg_vortex
EXTENSION_CONFIGS = $(CURDIR)/pg_vortex_extensions.cmake
DATA = pg_vortex--0.1.0.sql
OBJS = pg_vortex.o $(CORE_OBJS)

include ../../Makefile

ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	PG_VORTEX_DUCKDB_LINK = $(FULL_DUCKDB_LIB) -lcurl
else
	PG_VORTEX_DUCKDB_LINK = -lduckdb
endif

SHLIB_LINK += -L$(DUCKDB_BUILD_DIR)/src $(PG_VORTEX_DUCKDB_LINK) \
	-Wl,-rpath,$(PG_LIB)/ -L$(PG_LIB) -lstdc++ -llz4

$(shlib): $(FULL_DUCKDB_LIB)

install: install-duckdb

NO_INSTALLCHECK = 1

check-regression-vortex:
	$(MAKE) -C test/regression check-regression-vortex

clean-regression:
	$(MAKE) -C test/regression clean-regression

installcheck: all install
	$(MAKE) check-regression-vortex
