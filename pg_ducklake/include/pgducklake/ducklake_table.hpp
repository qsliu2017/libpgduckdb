#pragma once

/* ducklake_table.hpp -- Table lifecycle: AM handler, DDL triggers, sync. */

#include "pgddb/pg/declarations.hpp"

extern "C" {
void EnsureDuckLakeTable(Oid relid);
}

namespace pgducklake {

/* Caller must have an active SPI connection. */
void SyncNewTables(const char *snapshot_id);
void SyncDroppedTables(const char *snapshot_id);

} // namespace pgducklake
