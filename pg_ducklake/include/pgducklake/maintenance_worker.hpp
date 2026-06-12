#pragma once

/* maintenance_worker.hpp -- launcher/worker for periodic DuckLake table
 * maintenance (flush inlined data, rewrite/merge files, expire snapshots,
 * clean up old files). */

/* Hard cap on concurrent maintenance workers (GUC max bound) */
#define DUCKLAKE_MAX_MAINTENANCE_WORKERS 8
