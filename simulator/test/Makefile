include ../../Makefile.inc

CFLAGS = $(COMMON_CFLAGS) $(FLUX_CFLAGS) $(RDL_CFLAGS)
LIBS = $(FLUX_LIBS) $(RDL_LIBS) $(KVS_LIBS)

TESTSIM_INPUT_FILE := $(abs_topdir)/conf/hype-io.lua

LUA_PATH = $(abs_topdir)/rdl/?.lua;$(FLUX_SRCDIR)/src/bindings/lua/?.lua;;;
LUA_CPATH = $(abs_topdir)/rdl/?.so;$(FLUX_SRCDIR)/src/bindings/lua/.libs/?.so;;;

COMMON_OBJS = ../simulator.o ../xzmalloc.o ../log.o ../jsonutil.o
SCHEDULER_OBJS = ../scheduler.o

export CFLAGS TESTSIM_INPUT_FILE LUA_PATH LUA_CPATH

LDADD_CLI = $(FLUX_SRCDIR)/src/common/libutil/.libs/libutil.a \
            $(FLUX_SRCDIR)/src/common/liblsd/.libs/liblsd.a \
            $(FLUX_SRCDIR)/src/common/libtap/.libs/libtap.a

test_cppflags = \
	-I$(FLUX_SRCDIR)/src/common/libtap

BUILD = tsched_lib

all: $(BUILD)

tsched_lib: tsched_lib.c $(COMMON_OBJS) $(SCHEDULER_OBJS)
	$(CC)  $(CFLAGS) $(test_cppflags) -o $@ $^ $(LIBS) $(LDADD_CLI)

check: tsched_lib
	./tsched_lib

clean:
	rm -f *.o a.out
	rm -f $(BUILD)
