#pragma once

extern "C" {
#include "postgres.h"
#include "nodes/extensible.h"
}

/*
 * DuckdbInitNode populates the consumer-owned scan_methods and exec_methods
 * slots with libpgddb's CustomScan callbacks and registers scan_methods in
 * PG's process-global CustomScanMethods table under custom_scan_name.
 *
 * The name MUST be unique per consumer in a backend; RegisterCustomScanMethods
 * keys on it. Each consumer's .dylib has its own copy of libpgddb's internal
 * pointer slots, so two consumers can coexist as long as they pass distinct
 * names.
 *
 * The consumer owns the lifetime of both slots and uses scan_methods's address
 * for identity checks (e.g. "is this plan mine?").
 */
extern "C" void DuckdbInitNode(const char *custom_scan_name,
                               CustomScanMethods *scan_methods,
                               CustomExecMethods *exec_methods);

/* libpgddb-internal accessor used by CreatePlan to set CustomScan->methods. */
CustomScanMethods *DuckdbGetScanMethods(void);
