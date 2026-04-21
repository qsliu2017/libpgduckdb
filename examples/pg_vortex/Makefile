# examples/pg_vortex/Makefile -- minimum-surface libpgduckdb consumer.
#
# Links the prebuilt ../../libpgduckdb_core.a archive instead of recompiling
# the library sources inline. This is the layout the root Makefile was
# designed for; examples/pg_duckdb is transitional by comparison.

.PHONY: all duckdb install install-duckdb check-regression-vortex clean-regression installcheck

ROOT_DIR = ../..

MODULE_big = pg_vortex
EXTENSION = pg_vortex
DATA = pg_vortex--0.1.0.sql
OBJS = pg_vortex.o

LIBPGDUCKDB_CORE = $(ROOT_DIR)/libpgduckdb_core.a

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	DLSUFFIX = .dylib
else
	DLSUFFIX = .so
endif
DUCKDB_BUILD_DIR = $(ROOT_DIR)/third_party/duckdb/build/release
FULL_DUCKDB_LIB = $(DUCKDB_BUILD_DIR)/src/libduckdb$(DLSUFFIX)

override PG_CPPFLAGS += -I$(ROOT_DIR)/include \
	-isystem $(ROOT_DIR)/third_party/duckdb/src/include \
	-isystem $(INCLUDEDIR_SERVER) \
	-Wno-register
override PG_CXXFLAGS += -std=c++17

SHLIB_LINK += $(LIBPGDUCKDB_CORE) \
	-L$(DUCKDB_BUILD_DIR)/src -lduckdb \
	-Wl,-rpath,$(PG_LIB)/ -L$(PG_LIB) -lstdc++ -llz4

include Makefile.global

COMPILE.cc.bc += $(PG_CPPFLAGS)
COMPILE.cxx.bc += $(PG_CXXFLAGS)

$(shlib): $(LIBPGDUCKDB_CORE) $(FULL_DUCKDB_LIB)

$(LIBPGDUCKDB_CORE):
	$(MAKE) -C $(ROOT_DIR) core-lib

$(FULL_DUCKDB_LIB):
	$(MAKE) -C $(ROOT_DIR) duckdb

duckdb: $(FULL_DUCKDB_LIB)

install-duckdb: $(FULL_DUCKDB_LIB)
	$(install_bin) -m 755 $(FULL_DUCKDB_LIB) $(DESTDIR)$(PG_LIB)

install: install-duckdb

NO_INSTALLCHECK = 1

check-regression-vortex:
	$(MAKE) -C test/regression check-regression-vortex

clean-regression:
	$(MAKE) -C test/regression clean-regression

installcheck: all install
	$(MAKE) check-regression-vortex
