# examples/pg_duckdb/Makefile -- pg_duckdb, the full-fat consumer of libpgduckdb.
#
# Transitional wiring: compiles lib sources (../../src/) alongside ext sources
# (src/) into one pg_duckdb.so. This is equivalent to the pre-split monolithic
# build, just run from a different directory. A later step breaks the lib/ext
# coupling so that this Makefile can link ../../libpgduckdb_core.a instead of
# recompiling the lib every time, and examples/pg_parquet becomes buildable
# against just the archive.

.PHONY: duckdb install-duckdb check-regression-duckdb clean-regression pycheck schedulecheck check

ROOT_DIR = ../..

PG_DUCKDB_VERSION ?= $(shell cd $(ROOT_DIR) && git describe --always --dirty 2>/dev/null || echo "unknown")

MODULE_big = pg_duckdb
EXTENSION = pg_duckdb
DATA = pg_duckdb.control $(wildcard sql/pg_duckdb--*.sql)

# Lib sources live at ../../src/; ext sources live at ./src/. We compile both
# sets into pg_duckdb.so.
LIB_SRCS = $(wildcard $(ROOT_DIR)/src/*.cpp $(ROOT_DIR)/src/*/*.cpp)
EXT_SRCS = $(wildcard src/*.cpp src/*/*.cpp)
SRCS = $(LIB_SRCS) $(EXT_SRCS)
OBJS = $(subst .cpp,.o, $(SRCS))

LIB_C_SRCS = $(wildcard $(ROOT_DIR)/src/*.c $(ROOT_DIR)/src/*/*.c)
EXT_C_SRCS = $(wildcard src/*.c src/*/*.c)
OBJS += $(subst .c,.o, $(LIB_C_SRCS) $(EXT_C_SRCS))

# DuckDB build config -- delegates actual build to root Makefile.
DUCKDB_BUILD_CXX_FLAGS=
DUCKDB_BUILD_TYPE=
ifeq ($(DUCKDB_BUILD), Debug)
	DUCKDB_BUILD_CXX_FLAGS = -g -O0 -D_GLIBCXX_ASSERTIONS
	DUCKDB_BUILD_TYPE = debug
else ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	DUCKDB_BUILD_CXX_FLAGS =
	DUCKDB_BUILD_TYPE = release
else
	DUCKDB_BUILD_CXX_FLAGS =
	DUCKDB_BUILD_TYPE = release
endif

DUCKDB_BUILD_DIR = $(ROOT_DIR)/third_party/duckdb/build/$(DUCKDB_BUILD_TYPE)

ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	FULL_DUCKDB_LIB = $(DUCKDB_BUILD_DIR)/libduckdb_bundle.a
	PG_DUCKDB_LINK_FLAGS = $(FULL_DUCKDB_LIB) -lcurl
else
	FULL_DUCKDB_LIB = $(DUCKDB_BUILD_DIR)/src/libduckdb$(DLSUFFIX)
	PG_DUCKDB_LINK_FLAGS = -lduckdb
endif

PG_DUCKDB_LINK_FLAGS += -Wl,-rpath,$(PG_LIB)/ -L$(DUCKDB_BUILD_DIR)/src -L$(PG_LIB) -lstdc++ -llz4

# Ensure -lstdc++fs is included for GCC 8 builds
CXX ?= c++
IS_GCC := $(shell $(CXX) --version 2>/dev/null | grep -q "Free Software Foundation" && echo true || echo false)
ifeq ($(IS_GCC),true)
  GCC_MAJOR := $(shell $(CXX) -dumpversion 2>/dev/null | cut -d. -f1)
  ifeq ($(GCC_MAJOR),8)
    PG_DUCKDB_LINK_FLAGS += -lstdc++fs
  endif
endif

ERROR_ON_WARNING ?=
ifeq ($(ERROR_ON_WARNING), 1)
	ERROR_ON_WARNING = -Werror
else
	ERROR_ON_WARNING =
endif

COMPILER_FLAGS=-Wno-sign-compare -Wshadow -Wswitch -Wunused-parameter -Wunreachable-code -Wno-unknown-pragmas -Wall -Wextra ${ERROR_ON_WARNING}

# Lib headers at ../../include, ext headers at ./include. Both on the path.
override PG_CPPFLAGS += -I$(ROOT_DIR)/include -Iinclude -isystem $(ROOT_DIR)/third_party/duckdb/src/include -isystem $(ROOT_DIR)/third_party/duckdb/third_party/re2 -isystem $(INCLUDEDIR_SERVER) ${COMPILER_FLAGS}
override PG_CXXFLAGS += -std=c++17 ${DUCKDB_BUILD_CXX_FLAGS} ${COMPILER_FLAGS} -Wno-register -Weffc++
override PG_CFLAGS += -Wno-declaration-after-statement

SHLIB_LINK += $(PG_DUCKDB_LINK_FLAGS)

include Makefile.global

# Only the TU that uses it gets the version define, so ccache doesn't
# invalidate every object on every commit.
src/pgduckdb.o: PG_CPPFLAGS += -DPG_DUCKDB_VERSION="\"$(PG_DUCKDB_VERSION)\""

# Rebuild all .o files whenever the DuckDB submodule HEAD moves.
$(OBJS): $(ROOT_DIR)/.git/modules/third_party/duckdb/HEAD

COMPILE.cc.bc += $(PG_CPPFLAGS)
COMPILE.cxx.bc += $(PG_CXXFLAGS)

$(shlib): $(FULL_DUCKDB_LIB) $(OBJS)

# Delegate DuckDB build to the root Makefile so there's only one build tree.
duckdb: $(FULL_DUCKDB_LIB)

$(FULL_DUCKDB_LIB):
	$(MAKE) -C $(ROOT_DIR) duckdb

$(ROOT_DIR)/.git/modules/third_party/duckdb/HEAD:
	$(MAKE) -C $(ROOT_DIR) duckdb

ifeq ($(DUCKDB_BUILD), ReleaseStatic)
install-duckdb: $(FULL_DUCKDB_LIB) $(shlib)
else
install-duckdb: $(FULL_DUCKDB_LIB) $(shlib)
	$(install_bin) -m 755 $(FULL_DUCKDB_LIB) $(DESTDIR)$(PG_LIB)
endif

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

# The test directory is set via pyproject.toml's testpaths; passing an explicit
# arg here makes xdist workers fail to resolve the relative path.
pycheck: all install
	LD_LIBRARY_PATH=$(PG_LIBDIR):${LD_LIBRARY_PATH} pytest -n $(PYTEST_CONCURRENCY)

schedulecheck:
	./scripts/schedule-check.sh

check: installcheck pycheck schedulecheck
