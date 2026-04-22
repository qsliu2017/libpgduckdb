# Root Makefile -- shared build infra for every libpgduckdb consumer. Consumer
# extensions include this via `include ../../Makefile` after setting EXTENSION
# (PGXS name, also namespaces the per-extension DuckDB build tree) and
# EXTENSION_CONFIGS (cmake list of DuckDB extensions to statically link).
# Consumers then `OBJS += $(CORE_OBJS)` to pull the lib sources into their shlib.

.PHONY: duckdb install-duckdb clean-core clean-duckdb clean-all lintcheck format format-all

ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

ifndef EXTENSION
$(error EXTENSION must be set before including this Makefile)
endif
ifndef EXTENSION_CONFIGS
$(error EXTENSION_CONFIGS must be set before including this Makefile)
endif

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

DUCKDB_BUILD_CXX_FLAGS =
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

# Per-extension DuckDB build tree -- passed to DuckDB's Makefile via BUILD_DIR
# so switching EXTENSION picks up a disjoint build-<EXTENSION>/<type>/ output.
DUCKDB_BUILD_NAME = build-$(EXTENSION)
DUCKDB_BUILD_ROOT = $(ROOT_DIR)/third_party/duckdb/$(DUCKDB_BUILD_NAME)
DUCKDB_BUILD_DIR = $(DUCKDB_BUILD_ROOT)/$(DUCKDB_BUILD_TYPE)

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

# Route lib-side compile flags through PGXS's PG_CPPFLAGS / PG_CXXFLAGS /
# PG_CFLAGS conventions. PG_CPPFLAGS flows into both C and C++ compiles, so
# COMPILER_FLAGS lives there once; PG_CXXFLAGS only adds C++-specific flags.
override PG_CPPFLAGS += -I$(ROOT_DIR)/include -isystem $(ROOT_DIR)/third_party/duckdb/src/include -isystem $(ROOT_DIR)/third_party/duckdb/third_party/re2 $(COMPILER_FLAGS)
override PG_CXXFLAGS += -std=c++17 $(DUCKDB_BUILD_CXX_FLAGS) -Wno-register -Weffc++
override PG_CFLAGS += -Wno-declaration-after-statement

# Reusable lib sources; consumers set `OBJS = ... $(CORE_OBJS)` before
# including this Makefile. Variables are defined before Makefile.pg (which
# includes PGXS) so the list is visible when PGXS parses `$(shlib): $(OBJS)`
# -- without this ordering, touching a core source would not trigger the
# consumer shlib to relink. The rule attaching the submodule HEAD prereq,
# on the other hand, lives AFTER the include so it doesn't preempt PGXS's
# `all:` as the default goal.
CORE_SRCS := $(wildcard $(ROOT_DIR)/src/*.cpp $(ROOT_DIR)/src/*/*.cpp)
CORE_C_SRCS := $(wildcard $(ROOT_DIR)/src/*.c $(ROOT_DIR)/src/*/*.c)
CORE_OBJS := $(CORE_SRCS:.cpp=.o) $(CORE_C_SRCS:.c=.o)

include $(ROOT_DIR)/Makefile.pg

$(CORE_OBJS): $(ROOT_DIR)/.git/modules/third_party/duckdb/HEAD

duckdb: $(FULL_DUCKDB_LIB)

$(ROOT_DIR)/.git/modules/third_party/duckdb/HEAD:
	git -C $(ROOT_DIR) submodule update --init --recursive

# Patches layered on top of the submodule's pinned commit -- local until the
# features land upstream. Re-runs on patch edits or submodule HEAD moves;
# resets the working tree first so an updated series never stacks on an old one.
DUCKDB_PATCHES := $(sort $(wildcard $(ROOT_DIR)/third_party/duckdb-patches/*.patch))
DUCKDB_PATCH_STAMP = $(ROOT_DIR)/third_party/.duckdb-patched

$(DUCKDB_PATCH_STAMP): $(DUCKDB_PATCHES) $(ROOT_DIR)/.git/modules/third_party/duckdb/HEAD
	git -C $(ROOT_DIR)/third_party/duckdb checkout -- .
	for p in $(DUCKDB_PATCHES); do \
		git -C $(ROOT_DIR)/third_party/duckdb apply --whitespace=nowarn $$p || exit 1; \
	done
	touch $@

$(FULL_DUCKDB_LIB): $(ROOT_DIR)/.git/modules/third_party/duckdb/HEAD $(EXTENSION_CONFIGS) $(DUCKDB_PATCH_STAMP)
ifeq ($(DUCKDB_BUILD), ReleaseStatic)
	mkdir -p $(DUCKDB_BUILD_DIR)/vcpkg_installed
endif
	mkdir -p $(DUCKDB_BUILD_ROOT)
	OVERRIDE_GIT_DESCRIBE=$(DUCKDB_VERSION) \
	GEN=$(DUCKDB_GEN) \
	BUILD_DIR=$(DUCKDB_BUILD_NAME) \
	CMAKE_VARS="$(DUCKDB_CMAKE_VARS)" \
	DISABLE_SANITIZER=1 \
	DISABLE_ASSERTIONS=$(DUCKDB_DISABLE_ASSERTIONS) \
	EXTENSION_CONFIGS="$(abspath $(EXTENSION_CONFIGS))" \
	$(MAKE) -C $(ROOT_DIR)/third_party/duckdb $(DUCKDB_MAKE_TARGET)

# Install libduckdb alongside the consumer shlib. Static builds embed it, so no-op.
ifeq ($(DUCKDB_BUILD), ReleaseStatic)
install-duckdb: $(FULL_DUCKDB_LIB)
else
install-duckdb: $(FULL_DUCKDB_LIB)
	$(install_bin) -m 755 $(FULL_DUCKDB_LIB) $(DESTDIR)$(PG_LIB)/
endif

clean-core:
	rm -f $(CORE_OBJS)
	rm -rf $(ROOT_DIR)/.deps

clean-duckdb:
	rm -rf $(ROOT_DIR)/third_party/duckdb/build-*
	rm -f $(DUCKDB_PATCH_STAMP)
	git -C $(ROOT_DIR)/third_party/duckdb checkout -- . 2>/dev/null || true

clean-all: clean-core clean-duckdb

lintcheck:
	clang-tidy $(CORE_SRCS) -- -I$(INCLUDEDIR_SERVER) -I$(ROOT_DIR)/include $(shell $(PG_CONFIG) --cppflags) -std=c++17
	ruff check

format:
	find $(ROOT_DIR)/src $(ROOT_DIR)/include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs git clang-format origin/main
	ruff format

format-all:
	find $(ROOT_DIR)/src $(ROOT_DIR)/include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs clang-format -i
	ruff format
