#include "pgduckdb/catalog/pgduckdb_storage.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction_manager.hpp"

#include "pgduckdb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgduckdb {

static duckdb::unique_ptr<duckdb::TransactionManager>
CreateTransactionManager(duckdb::optional_ptr<duckdb::StorageExtensionInfo>, duckdb::AttachedDatabase &db,
                         duckdb::Catalog &catalog) {
	return duckdb::make_uniq<PostgresTransactionManager>(db, catalog.Cast<PostgresCatalog>());
}

PostgresStorageExtension::PostgresStorageExtension() {
	attach = PostgresCatalog::Attach;
	create_transaction_manager = CreateTransactionManager;
	storage_info = duckdb::make_shared_ptr<PostgresStorageInfo>();
}

PostgresStorageExtension::PostgresStorageExtension(const PostgresStorageOptions &options,
                                                   const TypeResolver *resolver) {
	attach = PostgresCatalog::Attach;
	create_transaction_manager = CreateTransactionManager;
	auto info = duckdb::make_shared_ptr<PostgresStorageInfo>();
	info->options = options;
	info->resolver = resolver;
	storage_info = std::move(info);
}

PostgresStorageExtension::PostgresStorageExtension(PostgresStorageOptionsProvider options_provider,
                                                   const TypeResolver *resolver) {
	attach = PostgresCatalog::Attach;
	create_transaction_manager = CreateTransactionManager;
	auto info = duckdb::make_shared_ptr<PostgresStorageInfo>();
	info->options_provider = options_provider;
	info->resolver = resolver;
	storage_info = std::move(info);
}

} // namespace pgduckdb
