include ../Makefile.inc

CFLAGS = $(COMMON_CFLAGS) $(FLUX_CFLAGS) $(RDL_CFLAGS)
GLPK_LIBS = -lglpk -lgmp -lm
LIBS = $(FLUX_LIBS) $(RDL_LIBS)

CONF=../conf/cab-io.lua
JOBDATA=job_data/cab.ilp.csv
SAVEPATH=/nfs/tmp2/herbein1

COMMON_OBJS = simulator.o xzmalloc.o log.o jsonutil.o
LP_OBJS = flux_ilp.o

SCHEDULERS_C = $(wildcard sim_fcfs*.c) $(wildcard sim_easy*.c) $(wildcard sim_ilp*.c)
SCHEDULERS_SO = $(SCHEDULERS_C:sim_%.c=sim_sched_%.so)

BUILD = sim.so submit.so sim_exec.so $(SCHEDULERS_SO)

all:  $(BUILD)

sim_sched_ilp.so: sim_ilp.c $(COMMON_OBJS) $(LP_OBJS)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LIBS) $(GLPK_LIBS)

sim_sched_ilp%.so: sim_ilp%.c $(COMMON_OBJS) $(LP_OBJS)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LIBS) $(GLPK_LIBS)

sim_sched_%.so: sim_%.c $(COMMON_OBJS)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LIBS)

%.so : %srv.o $(COMMON_OBJS)
	$(CC) $(CFLAGS) -shared -o $@ $^ $(LIBS)

# N.B. "race fix" in job module is conditionally disabled
# turn it on there if needed until there is a proper fix for the race

start:
	$(FLUX) start
callgrind:
	valgrind --tool=callgrind --trace-children=yes $(FLUX) start
valgrind:
	valgrind --log-file="%p.valgrind.out" --tool=memcheck --leak-check=full --show-leak-kinds=all --trace-children=yes $(FLUX) start
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

.PHONY: all clean load unload start
