EXT_DIR = ../..

include $(EXT_DIR)/Makefile.global

check-regression-vortex:
	TEST_DIR=$(CURDIR) $(pg_regress_installcheck) \
	    --encoding=UTF8 \
	    --no-locale \
	    --temp-instance=./tmp_check \
	    --temp-config regression.conf \
	    --load-extension=pg_vortex \
	    $(if $(TEST),$(TEST),--schedule=schedule)

clean-regression:
	rm -fr $(CURDIR)/tmp_check $(CURDIR)/log $(CURDIR)/results
	rm -f  $(CURDIR)/regression.diffs $(CURDIR)/regression.out
