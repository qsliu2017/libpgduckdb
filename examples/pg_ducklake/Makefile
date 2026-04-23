# examples/pg_ducklake/Makefile -- pg_ducklake rebuilt on libpgduckdb.
#
# Pulls $(CORE_OBJS) from the root Makefile into its own shlib and statically
# links DuckDB's ducklake community extension via EXTENSION_CONFIGS. The
# pg_ducklake sources reference duckdb::DucklakeExtension and ducklake
# storage headers directly, so the build adds the ducklake extension source
# tree (populated by DuckDB's FetchContent) to the include path.
#
# Runtime dependency: pg_ducklake still expects pg_duckdb's own extension to
# be loaded in the same backend. pg_ducklake calls into pg_duckdb's
# DuckDBManager (RegisterDuckdbLoadExtension, DuckdbRecycleDuckDB, etc.)
# declared in include/pgduckdb/pgduckdb_contracts.hpp. These symbols are
# resolved lazily at .so load time against the loaded pg_duckdb.so -- on
# Darwin via -undefined,dynamic_lookup; on ELF via lazy binding.

.PHONY: duckdb install check-regression-ducklake check-isolation-ducklake clean-regression

MODULE_big = pg_ducklake
EXTENSION = pg_ducklake
EXTENSION_CONFIGS = $(CURDIR)/pg_ducklake_extensions.cmake
DATA = pg_ducklake.control $(wildcard sql/pg_ducklake--*.sql)

OBJS = $(patsubst %.cpp,%.o,$(wildcard src/*.cpp src/*/*.cpp)) \
       $(patsubst %.c,%.o,$(wildcard src/*.c src/*/*.c)) \
       $(CORE_OBJS)

# Local pg_ducklake headers. The ducklake extension source tree is dropped
# into $(DUCKDB_BUILD_DIR)/_deps/ducklake_extension_fc-src/ by DuckDB's
# FetchContent step -- add that to the include path so our TUs can
# #include "ducklake_extension.hpp" and "storage/ducklake_metadata_manager.hpp".
DUCKLAKE_EXT_SRC = $(DUCKDB_BUILD_DIR)/_deps/ducklake_extension_fc-src
override PG_CPPFLAGS += -I$(CURDIR)/include -I$(DUCKLAKE_EXT_SRC)/src/include

include ../../Makefile

PG_DUCKLAKE_VERSION ?= $(shell git -C $(ROOT_DIR) describe --always --dirty 2>/dev/null || echo "unknown")

ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	PG_DUCKLAKE_DUCKDB_LINK = $(FULL_DUCKDB_LIB) -lcurl
else
	# Pass the explicit path (not -lduckdb) so the linker doesn't pick up a
	# stale copy from postgres/inst/lib that PGXS puts earlier in -L order.
	PG_DUCKLAKE_DUCKDB_LINK = $(FULL_DUCKDB_LIB)
endif

SHLIB_LINK += $(PG_DUCKLAKE_DUCKDB_LINK) -Wl,-rpath,$(PG_LIB)/ -lstdc++ -llz4

# pg_duckdb's runtime "contract" symbols (DuckdbRecycleDuckDB, etc.) are
# resolved against the already-loaded pg_duckdb.so at backend load time.
ifeq ($(shell uname -s), Darwin)
	SHLIB_LINK += -Wl,-undefined,dynamic_lookup
endif

# Pin the version define to the one TU that references it so ccache doesn't
# invalidate everything else on every commit.
src/pgducklake.o: PG_CPPFLAGS += -DPG_DUCKLAKE_VERSION="\"$(PG_DUCKLAKE_VERSION)\""

$(OBJS): $(ROOT_DIR)/.git/modules/third_party/duckdb/HEAD $(FULL_DUCKDB_LIB)

$(shlib): $(FULL_DUCKDB_LIB)

install: install-duckdb

NO_INSTALLCHECK = 1

check-regression-ducklake:
	$(MAKE) -C test/regression check-regression

check-isolation-ducklake:
	$(MAKE) -C test/isolation check-isolation

clean-regression:
	$(MAKE) -C test/regression clean-regression
	$(MAKE) -C test/isolation clean-isolation

installcheck: all install
	$(MAKE) check-regression-ducklake
	$(MAKE) check-isolation-ducklake
