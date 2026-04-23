# Makefile for pg_ducklake isolation tests.
# Delegates PGXS/pg_config discovery to the repo-root Makefile.pg (shared
# across libpgduckdb consumer extensions).

include ../../../../Makefile.pg

check-isolation:
	TEST_DIR=$(CURDIR) $(pg_isolation_regress_installcheck) \
	--encoding=UTF8 \
	--no-locale \
	--temp-instance=./tmp_check \
	--temp-config isolation.conf \
	--load-extension=pg_duckdb \
	--load-extension=pg_ducklake \
	$(if $(TEST),$(TEST),--schedule=schedule)

clean-isolation:
	rm -fr $(CURDIR)/tmp_check
	rm -fr $(CURDIR)/output_iso
	rm -f  $(CURDIR)/regression.diffs
	rm -f  $(CURDIR)/regression.out
