# Makefile for isolation tests

ROOT_DIR = ../../..

include $(ROOT_DIR)/Makefile.pgxs

# pg_duckdb stays in shared_preload_libraries (isolation.conf) so the
# suite still covers library-level coexistence (both _PG_init's, chained
# hooks, two kernel copies in one backend), but its EXTENSION is not
# created: CREATE EXTENSION pg_ducklake after pg_duckdb fails on the
# overlapping public read_csv/read_parquet stubs (SQL-level coexistence
# gap, tracked separately), and no isolation spec uses pg_duckdb objects.
check-isolation:
	TEST_DIR=$(CURDIR) $(pg_isolation_regress_installcheck) \
	--encoding=UTF8 \
	--no-locale \
	--temp-instance=./tmp_check \
	--temp-config isolation.conf \
	--load-extension=pg_ducklake \
	$(if $(TEST),$(TEST),--schedule=schedule)

clean-isolation:
	rm -fr $(CURDIR)/tmp_check
	rm -fr $(CURDIR)/output_iso
	rm -f  $(CURDIR)/regression.diffs
	rm -f  $(CURDIR)/regression.out
