# libpgduckdb: static archive of reusable pg_duckdb internals. Consumed by
# Postgres extensions that want to embed DuckDB (see examples/pg_duckdb for the
# full-fat consumer, examples/pg_parquet for a minimal one).
#
# This is NOT a Postgres extension. It has no `_PG_init`, no `.control`, no
# `sql/`, no GUCs. Downstream extensions provide those themselves and link the
# archive produced here.

.PHONY: all core-lib duckdb install-duckdb clean-lib clean-duckdb clean-all lintcheck format format-all

SRCS = $(wildcard src/*.cpp src/*/*.cpp)
OBJS = $(subst .cpp,.o, $(SRCS))

C_SRCS = $(wildcard src/*.c src/*/*.c)
OBJS += $(subst .c,.o, $(C_SRCS))

# set to `make` to disable ninja
DUCKDB_GEN ?= ninja
# used to know what version of extensions to download
DUCKDB_VERSION = v1.4.3
# duckdb build tweaks
DUCKDB_CMAKE_VARS = -DCXX_EXTRA=-fvisibility=default -DBUILD_SHELL=0 -DBUILD_PYTHON=0 -DBUILD_UNITTESTS=0
# set to 1 to disable asserts in DuckDB. This is particularly useful in combinition with MotherDuck.
# When asserts are enabled the released motherduck extension will fail some of
# those asserts. By disabling asserts it's possible to run a debug build of
# DuckDB agains the release build of MotherDuck.
DUCKDB_DISABLE_ASSERTIONS ?= 0

DUCKDB_BUILD_CXX_FLAGS=
DUCKDB_BUILD_TYPE=
ifeq ($(DUCKDB_BUILD), Debug)
	DUCKDB_BUILD_CXX_FLAGS = -g -O0 -D_GLIBCXX_ASSERTIONS
	DUCKDB_BUILD_TYPE = debug
	DUCKDB_MAKE_TARGET = debug
else ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	DUCKDB_BUILD_CXX_FLAGS =
	DUCKDB_BUILD_TYPE = release
	DUCKDB_MAKE_TARGET = bundle-library
else
	DUCKDB_BUILD_CXX_FLAGS =
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
	ERROR_ON_WARNING = -Werror
else
	ERROR_ON_WARNING =
endif

COMPILER_FLAGS=-Wno-sign-compare -Wshadow -Wswitch -Wunused-parameter -Wunreachable-code -Wno-unknown-pragmas -Wall -Wextra ${ERROR_ON_WARNING}

override PG_CPPFLAGS += -Iinclude -isystem third_party/duckdb/src/include -isystem third_party/duckdb/third_party/re2 -isystem $(INCLUDEDIR_SERVER) ${COMPILER_FLAGS}
override PG_CXXFLAGS += -std=c++17 ${DUCKDB_BUILD_CXX_FLAGS} ${COMPILER_FLAGS} -Wno-register -Weffc++
# Ignore declaration-after-statement warnings in our code. Postgres enforces
# this because their ancient style guide requires it, but we don't care. It
# would only apply to C files anyway, and we don't have many of those. The only
# ones that we do have are vendored in from Postgres (ruleutils), and allowing
# declarations to be anywhere is even a good thing for those as we can keep our
# changes to the vendored code in one place.
override PG_CFLAGS += -Wno-declaration-after-statement

# PGXS is included only for its %.o compile rules and PG_CPPFLAGS/PG_CXXFLAGS
# plumbing into CFLAGS/CXXFLAGS. We do NOT set MODULE_big/EXTENSION/DATA: this
# build produces a library, not an installable extension.
include Makefile.global

# We need the DuckDB header files to build our own .o files. We depend on the
# duckdb submodule HEAD, because that target pulls in the submodule which
# includes those header files. This does mean that we rebuild our .o files
# whenever we change the DuckDB version, but that seems like a fairly
# reasonable thing to do anyway, even if not always strictly necessary always.
$(OBJS): .git/modules/third_party/duckdb/HEAD

COMPILE.cc.bc += $(PG_CPPFLAGS)
COMPILE.cxx.bc += $(PG_CXXFLAGS)

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
	$(install_bin) -m 755 $(FULL_DUCKDB_LIB) $(DESTDIR)$(PG_LIB)
endif

clean-lib:
	rm -f $(LIBPGDUCKDB_CORE_A) $(OBJS)

clean-duckdb:
	rm -rf third_party/duckdb/build

clean-all: clean-lib clean-duckdb

lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -Iinclude $(CPPFLAGS) -std=c++17
	ruff check

format:
	find src include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs git clang-format origin/main
	ruff format

format-all:
	find src include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs clang-format -i
	ruff format
