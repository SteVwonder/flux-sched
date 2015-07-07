
include ../Makefile.inc

CFLAGS = $(COMMON_CFLAGS) $(FLUX_CFLAGS) $(RDL_CFLAGS)
GLPK_LIBS = -lglpk -lgmp -lm
KVS_LIBS = -Wl,-rpath,$(FLUX_BUILDDIR)/src/modules/kvs/.libs \
         -L$(FLUX_BUILDDIR)/src/modules/kvs/.libs -lkvs
LIBS = $(FLUX_LIBS) $(RDL_LIBS) $(KVS_LIBS)

CONF=../conf/cab-io.lua
JOBDATA=job_data/cab.ilp.csv
SAVEPATH=/nfs/tmp2/herbein1

COMMON_OBJS = simulator.o xzmalloc.o log.o jsonutil.o
LP_OBJS = flux_ilp.o

SCHEDULERS_C = $(wildcard sim_fcfs*.c) $(wildcard sim_easy*.c) $(wildcard sim_ilp.c)
SCHEDULERS_SO = $(SCHEDULERS_C:sim_%.c=sim_sched_%.so)
SCHEDULER_OBJS = scheduler.o

BUILD = sim.so submit.so sim_exec.so $(SCHEDULERS_SO)

all:  $(BUILD)

sim_sched_ilp.so: sim_ilp.c $(COMMON_OBJS) $(SCHEDULER_OBJS) $(LP_OBJS)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LIBS) $(GLPK_LIBS)

sim_sched_ilp%.so: sim_ilp%.c $(COMMON_OBJS) $(SCHEDULER_OBJS) $(LP_OBJS)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LIBS) $(GLPK_LIBS)

sim_sched_%.so: sim_%.c $(COMMON_OBJS) $(SCHEDULER_OBJS)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LIBS)

%.so : %srv.o $(COMMON_OBJS)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LIBS)

start:
	$(FLUX) -B ../.build/src/broker/flux-broker start
load:
	$(FLUX) module load ./sim.so save-path=$(SAVEPATH)
	$(FLUX) module load ./submit.so job-csv=$(JOBDATA)
	$(FLUX) module load ./sim_exec.so
unload:
	$(FLUX) module load sim_exec
	$(FLUX) module load sim_sched
	$(FLUX) module load submit
	$(FLUX) module load sim
sim_sched_%: sim_sched_%.so load
	$(FLUX) module load ./$@.so rdl-conf=$(CONF)

check:
	echo "This is an empty depenendency"

clean:
	rm -f *.a *.o *.so
	#for subdir in $(SUBDIRS); do make -C $$subdir $@; done

## Debugging conveinences
SCHED=sim_sched_fcfs_aware.so
EXEC_MOD=--module ./sim_exec.so
SIM_MOD=--module ./sim.so --modopt sim:save-path=${SAVEPATH}
SUBMIT_MOD=--module ./submit.so --modopt submit:job-csv=${JOBDATA}
SCHED_MOD=--module ./${SCHED} --modopt sim_sched:rdl-conf=${CONF}
BROKER_OPTS=-Lstderr ${EXEC_MOD} ${SIM_MOD} ${SUBMIT_MOD} ${SCHED_MOD}
BROKER=$(FLUX_BUILDDIR)/src/broker/.libs/flux-broker

VALGRIND=$(shell which valgrind)
TOTALVIEW=$(shell which totalview)
GDB=$(shell which gdb)

callgrind:
	$(FLUX) $(VALGRIND) --tool=callgrind --trace-children=yes $(BROKER) $(BROKER_OPTS)
valgrind:
	$(FLUX)	$(VALGRIND) --log-file="%p.valgrind.out" --tool=memcheck --leak-check=full --show-leak-kinds=all --trace-children=yes $(BROKER) $(BROKER_OPTS)
gdb:
	$(FLUX) $(GDB) --args $(BROKER) $(BROKER_OPTS)
totalview:
	$(FLUX) $(TOTALVIEW) --args $(BROKER) $(BROKER_OPTS)


.PHONY: all clean load unload start
