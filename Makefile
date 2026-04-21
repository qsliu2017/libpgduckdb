# libpgduckdb: static archive of reusable pg_duckdb internals. Consumed by
# Postgres extensions that want to embed DuckDB (see examples/pg_duckdb for the
# full-fat consumer, examples/pg_parquet for a minimal one).
#
# This is NOT a Postgres extension. It has no `_PG_init`, no `.control`, no
# `sql/`, no GUCs. Deliberately does not go through PGXS -- PGXS is scaffolding
# for building extensions, and including it here would be a layering
# violation. We read `pg_config` directly for server headers and write our own
# compile rules.

.PHONY: all core-lib duckdb install-duckdb clean-lib clean-duckdb clean-all lintcheck format format-all

PG_CONFIG ?= pg_config
AR ?= ar

# PG version gate -- lib only compiles against PG 14-18 headers.
PG_MIN_VER = 14
PG_MAX_VER ?= 18
PG_VER := $(shell $(PG_CONFIG) --version | sed "s/^[^ ]* \([0-9]*\).*$$/\1/" 2>/dev/null)
ifeq ($(shell expr "$(PG_MIN_VER)" \<= "$(PG_VER)"), 0)
$(error Minimum PostgreSQL version is $(PG_MIN_VER) (but have $(PG_VER)))
endif
ifeq ($(shell expr "$(PG_MAX_VER)" \>= "$(PG_VER)"), 0)
$(error Maximum PostgreSQL version is $(PG_MAX_VER) (but have $(PG_VER)))
endif

PG_LIB := $(shell $(PG_CONFIG) --pkglibdir)
INCLUDEDIR_SERVER := $(shell $(PG_CONFIG) --includedir-server)
PG_CPPFLAGS_BASE := $(shell $(PG_CONFIG) --cppflags)
PG_CFLAGS_BASE := $(shell $(PG_CONFIG) --cflags)
PG_CFLAGS_SL := $(shell $(PG_CONFIG) --cflags_sl)

# Shared-library suffix (.so on Linux, .dylib on macOS) -- only needed to point
# install-duckdb at the right file.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	DLSUFFIX = .dylib
else
	DLSUFFIX = .so
endif

# DuckDB submodule config.
DUCKDB_GEN ?= ninja
DUCKDB_VERSION = v1.4.3
DUCKDB_CMAKE_VARS = -DCXX_EXTRA=-fvisibility=default -DBUILD_SHELL=0 -DBUILD_PYTHON=0 -DBUILD_UNITTESTS=0
DUCKDB_DISABLE_ASSERTIONS ?= 0

DUCKDB_BUILD_CXX_FLAGS=
ifeq ($(DUCKDB_BUILD), Debug)
	DUCKDB_BUILD_CXX_FLAGS = -g -O0 -D_GLIBCXX_ASSERTIONS
	DUCKDB_BUILD_TYPE = debug
	DUCKDB_MAKE_TARGET = debug
else ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	DUCKDB_BUILD_TYPE = release
	DUCKDB_MAKE_TARGET = bundle-library
else
	DUCKDB_BUILD_TYPE = release
	DUCKDB_MAKE_TARGET = release
endif

DUCKDB_BUILD_DIR = third_party/duckdb/build/$(DUCKDB_BUILD_TYPE)

ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	FULL_DUCKDB_LIB = $(DUCKDB_BUILD_DIR)/libduckdb_bundle.a
else
	FULL_DUCKDB_LIB = $(DUCKDB_BUILD_DIR)/src/libduckdb$(DLSUFFIX)
endif

ERROR_ON_WARNING ?=
ifeq ($(ERROR_ON_WARNING), 1)
	ERROR_ON_WARNING_FLAG = -Werror
else
	ERROR_ON_WARNING_FLAG =
endif

COMPILER_FLAGS = -Wno-sign-compare -Wshadow -Wswitch -Wunused-parameter -Wunreachable-code -Wno-unknown-pragmas -Wall -Wextra $(ERROR_ON_WARNING_FLAG)

# Mirror what PGXS would emit for an extension object. `-fPIC` via
# $(PG_CFLAGS_SL) so the archive's objects are relocatable and can be pulled
# into downstream .so / .dylib builds.
BASE_CPPFLAGS = $(PG_CPPFLAGS_BASE) -Iinclude -isystem third_party/duckdb/src/include -isystem third_party/duckdb/third_party/re2 -isystem $(INCLUDEDIR_SERVER) $(COMPILER_FLAGS)
BASE_CFLAGS = $(PG_CFLAGS_BASE) $(PG_CFLAGS_SL) -Wno-declaration-after-statement
BASE_CXXFLAGS = $(PG_CFLAGS_BASE) $(PG_CFLAGS_SL) -std=c++17 $(DUCKDB_BUILD_CXX_FLAGS) $(COMPILER_FLAGS) -Wno-register -Weffc++

SRCS = $(wildcard src/*.cpp src/*/*.cpp)
OBJS = $(subst .cpp,.o, $(SRCS))
C_SRCS = $(wildcard src/*.c src/*/*.c)
OBJS += $(subst .c,.o, $(C_SRCS))

DEPDIR = .deps

# Compile rules with autodepend (matches the pattern the old Makefile.global
# vendored from Postgres -- produces `.deps/<basename>.Po` alongside each .o).
%.o: %.cpp
	@mkdir -p $(DEPDIR)
	$(CXX) $(BASE_CXXFLAGS) $(BASE_CPPFLAGS) -c -o $@ $< -MMD -MP -MF $(DEPDIR)/$(*F).Po

%.o: %.c
	@mkdir -p $(DEPDIR)
	$(CC) $(BASE_CFLAGS) $(BASE_CPPFLAGS) -c -o $@ $< -MMD -MP -MF $(DEPDIR)/$(*F).Po

# Pull in per-object dependency info if it exists.
Po_files := $(wildcard $(DEPDIR)/*.Po)
ifneq (,$(Po_files))
include $(Po_files)
endif

# Rebuild when the DuckDB submodule HEAD moves.
$(OBJS): .git/modules/third_party/duckdb/HEAD

LIBPGDUCKDB_CORE_A = libpgduckdb_core.a

$(LIBPGDUCKDB_CORE_A): $(OBJS) $(FULL_DUCKDB_LIB)
	$(AR) rcs $@ $(OBJS)

all: $(LIBPGDUCKDB_CORE_A)
core-lib: $(LIBPGDUCKDB_CORE_A)

duckdb: $(FULL_DUCKDB_LIB)

.git/modules/third_party/duckdb/HEAD:
	git submodule update --init --recursive

$(FULL_DUCKDB_LIB): .git/modules/third_party/duckdb/HEAD third_party/pg_duckdb_extensions.cmake
ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	mkdir -p third_party/duckdb/build/release/vcpkg_installed
endif
	OVERRIDE_GIT_DESCRIBE=$(DUCKDB_VERSION) \
	GEN=$(DUCKDB_GEN) \
	CMAKE_VARS="$(DUCKDB_CMAKE_VARS)" \
	DISABLE_SANITIZER=1 \
	DISABLE_ASSERTIONS=$(DUCKDB_DISABLE_ASSERTIONS) \
	EXTENSION_CONFIGS="../pg_duckdb_extensions.cmake" \
	$(MAKE) -C third_party/duckdb \
	$(DUCKDB_MAKE_TARGET)

# Install libduckdb into Postgres' libdir so downstream extensions (built
# against libpgduckdb_core.a) can dlopen it at backend startup.
ifeq ($(DUCKDB_BUILD), ReleaseStatic)
install-duckdb: $(FULL_DUCKDB_LIB)
else
install-duckdb: $(FULL_DUCKDB_LIB)
	install -m 755 $(FULL_DUCKDB_LIB) $(DESTDIR)$(PG_LIB)/
endif

clean-lib:
	rm -f $(LIBPGDUCKDB_CORE_A) $(OBJS)
	rm -rf $(DEPDIR)

clean-duckdb:
	rm -rf third_party/duckdb/build

clean-all: clean-lib clean-duckdb

lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR_SERVER) -Iinclude $(PG_CPPFLAGS_BASE) -std=c++17
	ruff check

format:
	find src include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs git clang-format origin/main
	ruff format

format-all:
	find src include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs clang-format -i
	ruff format
