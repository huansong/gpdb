MODULES = gp_storage_suggestion

EXTENSION = gp_storage_suggestion
DATA = gp_storage_suggestion--1.0.sql

REGRESS = inject_fault_test

PG_CPPFLAGS = -I$(libpq_srcdir)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/gp_storage_suggestion
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
