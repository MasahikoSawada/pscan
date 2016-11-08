# pscan

MODULE_big = pscan
OBJS = pscan.o $(WIN32RES)

EXTENSION = pscan
DATA = pscan--1.0.sql
PGFILEDESC = "pscan - parallel scan test"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = pscan
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
