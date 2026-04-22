# examples/pg_duckdb/Makefile -- pg_duckdb, the full-fat consumer of libpgduckdb.
# Compiles $(CORE_OBJS) (from the root Makefile) alongside its own src/ into pg_duckdb.so.

.PHONY: duckdb install check-regression-duckdb clean-regression pycheck schedulecheck check

MODULE_big = pg_duckdb
EXTENSION = pg_duckdb
EXTENSION_CONFIGS = $(CURDIR)/pg_duckdb_extensions.cmake
DATA = pg_duckdb.control $(wildcard sql/pg_duckdb--*.sql)

OBJS = $(patsubst %.cpp,%.o,$(wildcard src/*.cpp src/*/*.cpp)) \
       $(patsubst %.c,%.o,$(wildcard src/*.c src/*/*.c)) \
       $(CORE_OBJS)

override PG_CPPFLAGS += -Iinclude

include ../../Makefile

PG_DUCKDB_VERSION ?= $(shell git -C $(ROOT_DIR) describe --always --dirty 2>/dev/null || echo "unknown")

ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	PG_DUCKDB_LINK = $(FULL_DUCKDB_LIB) -lcurl
else
	# Pass the explicit path (not -lduckdb) so the linker doesn't pick up a
	# stale copy from postgres/inst/lib that PGXS puts earlier in -L order.
	PG_DUCKDB_LINK = $(FULL_DUCKDB_LIB)
endif

SHLIB_LINK += $(PG_DUCKDB_LINK) -Wl,-rpath,$(PG_LIB)/ -lstdc++ -llz4

# Only the TU that uses it gets the version define, so ccache doesn't
# invalidate every object on every commit.
src/pgduckdb.o: PG_CPPFLAGS += -DPG_DUCKDB_VERSION="\"$(PG_DUCKDB_VERSION)\""

$(OBJS): $(ROOT_DIR)/.git/modules/third_party/duckdb/HEAD

$(shlib): $(FULL_DUCKDB_LIB)

install: install-duckdb

NO_INSTALLCHECK = 1
PYTEST_CONCURRENCY = auto

check-regression-duckdb:
	$(MAKE) -C test/regression check-regression-duckdb

clean-regression:
	$(MAKE) -C test/regression clean-regression

# AWS_REGION pinned so S3-referencing regression output is stable.
installcheck: all install
	AWS_REGION=us-east-1 $(MAKE) check-regression-duckdb

# testpaths in pyproject.toml drives discovery; an explicit arg here breaks xdist workers.
pycheck: all install
	LD_LIBRARY_PATH=$(PG_LIBDIR):${LD_LIBRARY_PATH} pytest -n $(PYTEST_CONCURRENCY)

schedulecheck:
	./scripts/schedule-check.sh

check: installcheck pycheck schedulecheck
