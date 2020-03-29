#!/bin/sh

test_description='Test flux-tree correctness'

ORIG_HOME=${HOME}

. `dirname $0`/sharness.sh

#
# sharness modifies $HOME environment variable, but this interferes
# with python's package search path, in particular its user site package.
#
HOME=${ORIG_HOME}

test_under_flux 1

if test -z "${FLUX_SCHED_TEST_INSTALLED}" || test -z "${FLUX_SCHED_CO_INST}"
 then
     export FLUX_RC_EXTRA="${SHARNESS_TEST_SRCDIR}/rc"
fi

remove_prefix(){
    awk '{for (i=3; i<=NF; i++) printf $i " "; print ""}' ${1} > ${2}
}

test_expect_success 'flux-tree: --dry-run works' '
    flux tree --dry-run -N 1 -c 1 /bin/hostname
'

test_expect_success 'flux-tree: relative jobscript command path works' '
    flux tree --dry-run -N 1 -c 1 hostname
'

test_expect_success 'flux-tree: --leaf works' '
    cat >cmp.01 <<-EOF &&
	FLUX_TREE_ID=tree
	FLUX_TREE_JOBSCRIPT_INDEX=1
	FLUX_TREE_NCORES_PER_NODE=1
	FLUX_TREE_NGPUS_PER_NODE=0
	FLUX_TREE_NNODES=1
	eval /bin/hostname
EOF
    flux tree --dry-run --leaf -N 1 -c 1 /bin/hostname > out.01 &&
    remove_prefix out.01 out.01.a &&
    sed -i "s/[ \t]*$//g" out.01.a &&
    test_cmp cmp.01 out.01.a
'

test_expect_success 'flux-tree: -- option works' '
    cat >cmp.01.1 <<-EOF &&
	FLUX_TREE_ID=tree
	FLUX_TREE_JOBSCRIPT_INDEX=1
	FLUX_TREE_NCORES_PER_NODE=1
	FLUX_TREE_NGPUS_PER_NODE=0
	FLUX_TREE_NNODES=1
	eval hostname -h
EOF
    flux tree --dry-run --leaf -N 1 -c 1 -- hostname -h > out.01.1 &&
    remove_prefix out.01.1 out.01.1.a &&
    sed -i "s/[ \t]*$//g" out.01.1.a &&
    test_cmp cmp.01.1 out.01.1.a
'

test_expect_success 'flux-tree: multi cmdline works' '
    cat >cmp.01.2 <<-EOF &&
	FLUX_TREE_ID=tree
	FLUX_TREE_JOBSCRIPT_INDEX=1
	FLUX_TREE_NCORES_PER_NODE=1
	FLUX_TREE_NGPUS_PER_NODE=0
	FLUX_TREE_NNODES=1
	eval python hostname.py
EOF
    flux tree --dry-run --leaf -N 1 -c 1 python hostname.py > out.01.2 &&
    remove_prefix out.01.2 out.01.2.a &&
    sed -i "s/[ \t]*$//g" out.01.2.a &&
    test_cmp cmp.01.2 out.01.2.a
'

test_expect_success 'flux-tree: nonexistant script can be detected' '
    test_must_fail flux tree --dry-run -N 1 -c 1 ./nonexistant
'

test_expect_success 'flux-tree: --njobs works' '
    cat >cmp.02 <<-EOF &&
	FLUX_TREE_ID=tree
	FLUX_TREE_JOBSCRIPT_INDEX=1
	FLUX_TREE_NCORES_PER_NODE=1
	FLUX_TREE_NGPUS_PER_NODE=0
	FLUX_TREE_NNODES=1
	eval /bin/hostname
	FLUX_TREE_ID=tree
	FLUX_TREE_JOBSCRIPT_INDEX=2
	FLUX_TREE_NCORES_PER_NODE=1
	FLUX_TREE_NGPUS_PER_NODE=0
	FLUX_TREE_NNODES=1
	eval /bin/hostname
EOF
    flux tree --dry-run --leaf -N 1 -c 1 -J 2 /bin/hostname > out.02 &&
    remove_prefix out.02 out.02.2 &&
    sed -i "s/[ \t]*$//g" out.02.2 &&
    test_cmp cmp.02 out.02.2
'

test_expect_success 'flux-tree: --prefix to detect level != toplevel works' '
    cat >cmp.03 <<-EOF &&
	FLUX_TREE_ID=tree.1
	FLUX_TREE_JOBSCRIPT_INDEX=1
	FLUX_TREE_NCORES_PER_NODE=1
	FLUX_TREE_NGPUS_PER_NODE=0
	FLUX_TREE_NNODES=1
	eval /bin/hostname
EOF
    flux tree --dry-run --leaf -N 1 -c 1 -X tree.1 /bin/hostname > out.03 &&
    remove_prefix out.03 out.03.2 &&
    sed -i "s/[ \t]*$//g" out.03.2 &&
    test_cmp cmp.03 out.03.2
'

test_expect_success 'flux-tree: --topology and --leaf cannot be given together' '
    test_must_fail flux tree --dry-run -T 2 -l -N 1 -c 1 /bin/hostname
'

# cmp.04 contains parameters to use to spawn a child instance
test_expect_success 'flux-tree: --topology=1 works' '
    cat >cmp.04 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=1
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=1 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=1 /bin/hostname > out.04 &&
    remove_prefix out.04 out.04.2 &&
    sed -i "s/[ \t]*$//g" out.04.2 && 
    test_cmp cmp.04 out.04.2
'

test_expect_success 'flux-tree: -T2 on 1 node/1 core is equal to -T1' '
    cat >cmp.05 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=1
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=1 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=2 /bin/hostname > out.05 &&
    remove_prefix out.05 out.05.2 &&
    sed -i "s/[ \t]*$//g" out.05.2 &&
    test_cmp cmp.05 out.05.2
'

test_expect_success 'flux-tree: -T3 on 1 node/1 core is equal to -T1' '
    cat >cmp.06 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=1
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=1 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=3 /bin/hostname > out.06 &&
    remove_prefix out.06 out.06.2 &&
    sed -i "s/[ \t]*$//g" out.06.2 &&
    test_cmp cmp.06 out.06.2
'

test_expect_success 'flux-tree: -T1 on 2 nodes/2 cores work' '
    cat >cmp.07 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=2 c=2
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=1 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=1 -N 2 -c 2 /bin/hostname > out.07 &&
    remove_prefix out.07 out.07.2 &&
    sed -i "s/[ \t]*$//g" out.07.2 &&
    test_cmp cmp.07 out.07.2
'

test_expect_success 'flux-tree: -T2 on 2 nodes/2 cores work' '
    cat >cmp.08 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=2
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=5 S=/bin/hostname
	
	Rank=2: N=1 c=2
	Rank=2: T=--leaf
	Rank=2:
	Rank=2: X=--prefix=tree.2 J=--njobs=5 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=2 -N 2 -c 2 -J 10 /bin/hostname > out.08 &&
    remove_prefix out.08 out.08.2 &&
    sed -i "s/[ \t]*$//g" out.08.2 &&
    test_cmp cmp.08 out.08.2
'

# Not all instance will run simultaneously
test_expect_success 'flux-tree: -T3 on 4 nodes/4 cores work' '
    cat >cmp.09 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=2 c=3
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=4 S=/bin/hostname
	
	Rank=2: N=2 c=3
	Rank=2: T=--leaf
	Rank=2:
	Rank=2: X=--prefix=tree.2 J=--njobs=3 S=/bin/hostname
	
	Rank=3: N=2 c=3
	Rank=3: T=--leaf
	Rank=3:
	Rank=3: X=--prefix=tree.3 J=--njobs=3 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=3 -N 4 -c 4 -J 10 /bin/hostname > out.09 &&
    remove_prefix out.09 out.09.2 &&
    sed -i "s/[ \t]*$//g" out.09.2 &&
    test_cmp cmp.09 out.09.2
'

test_expect_success 'flux-tree: -T4 on 4 nodes/4 cores work' '
    cat >cmp.10 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=4
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=3 S=/bin/hostname
	
	Rank=2: N=1 c=4
	Rank=2: T=--leaf
	Rank=2:
	Rank=2: X=--prefix=tree.2 J=--njobs=3 S=/bin/hostname
	
	Rank=3: N=1 c=4
	Rank=3: T=--leaf
	Rank=3:
	Rank=3: X=--prefix=tree.3 J=--njobs=2 S=/bin/hostname
	
	Rank=4: N=1 c=4
	Rank=4: T=--leaf
	Rank=4:
	Rank=4: X=--prefix=tree.4 J=--njobs=2 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=4 -N 4 -c 4 -J 10 /bin/hostname > out.10 &&
    remove_prefix out.10 out.10.2 &&
    sed -i "s/[ \t]*$//g" out.10.2 &&
    test_cmp cmp.10 out.10.2
'

test_expect_success 'flux-tree: --perf-out generates a perf output file' '
    flux tree --dry-run -T 4 -N 4 -c 4 -J 10 -o p.out /bin/hostname &&
    test -f p.out &&
    lcount=$(wc -l p.out | awk "{print \$1}") &&
    test ${lcount} -eq 2 && 
    wcount=$(wc -w p.out | awk "{print \$1}") &&
    test ${wcount} -eq 18
'

test_expect_success 'flux-tree: --perf-format works with custom format' '
    flux tree --dry-run -T 2 -N 2 -c 4 -J 2 -o perf.out \
         --perf-format="{treeid}\ {elapse:f}\ {my_nodes:d}" \
         /bin/hostname &&
    test -f perf.out &&
    lcount=$(wc -l perf.out | awk "{print \$1}") &&
    test ${lcount} -eq 2 && 
    wcount=$(wc -w perf.out | awk "{print \$1}") &&
    test ${wcount} -eq 6
'

test_expect_success 'flux-tree: -T4x2 on 4 nodes/4 cores work' '
    cat >cmp.11 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=4
	Rank=1: T=--topology=2
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=3 S=/bin/hostname
	
	Rank=2: N=1 c=4
	Rank=2: T=--topology=2
	Rank=2:
	Rank=2: X=--prefix=tree.2 J=--njobs=3 S=/bin/hostname
	
	Rank=3: N=1 c=4
	Rank=3: T=--topology=2
	Rank=3:
	Rank=3: X=--prefix=tree.3 J=--njobs=2 S=/bin/hostname
	
	Rank=4: N=1 c=4
	Rank=4: T=--topology=2
	Rank=4:
	Rank=4: X=--prefix=tree.4 J=--njobs=2 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run -T 4x2 -N 4 -c 4 -J 10 /bin/hostname > out.11 &&
    remove_prefix out.11 out.11.2 &&
    sed -i "s/[ \t]*$//g" out.11.2 &&
    test_cmp cmp.11 out.11.2
'

test_expect_success 'flux-tree: -T4x2 -Q fcfs:easy works' '
    cat >cmp.12 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:queue-policy=fcfs
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=4
	Rank=1: T=--topology=2
	Rank=1: Q=--queue-policy=easy
	Rank=1: X=--prefix=tree.1 J=--njobs=3 S=/bin/hostname
	
	Rank=2: N=1 c=4
	Rank=2: T=--topology=2
	Rank=2: Q=--queue-policy=easy
	Rank=2: X=--prefix=tree.2 J=--njobs=3 S=/bin/hostname
	
	Rank=3: N=1 c=4
	Rank=3: T=--topology=2
	Rank=3: Q=--queue-policy=easy
	Rank=3: X=--prefix=tree.3 J=--njobs=2 S=/bin/hostname
	
	Rank=4: N=1 c=4
	Rank=4: T=--topology=2
	Rank=4: Q=--queue-policy=easy
	Rank=4: X=--prefix=tree.4 J=--njobs=2 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run -T 4x2 -Q fcfs:easy -N 4 -c 4 -J 10 /bin/hostname \
> out.12 &&
    remove_prefix out.12 out.12.2 &&
    sed -i "s/[ \t]*$//g" out.12.2 &&
    test_cmp cmp.12 out.12.2
'

test_expect_success 'flux-tree: -T4x2x3 -Q fcfs:fcfs:easy works' '
    cat >cmp.13 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:queue-policy=fcfs
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=4
	Rank=1: T=--topology=2x3
	Rank=1: Q=--queue-policy=fcfs:easy
	Rank=1: X=--prefix=tree.1 J=--njobs=3 S=/bin/hostname
	
	Rank=2: N=1 c=4
	Rank=2: T=--topology=2x3
	Rank=2: Q=--queue-policy=fcfs:easy
	Rank=2: X=--prefix=tree.2 J=--njobs=3 S=/bin/hostname
	
	Rank=3: N=1 c=4
	Rank=3: T=--topology=2x3
	Rank=3: Q=--queue-policy=fcfs:easy
	Rank=3: X=--prefix=tree.3 J=--njobs=2 S=/bin/hostname
	
	Rank=4: N=1 c=4
	Rank=4: T=--topology=2x3
	Rank=4: Q=--queue-policy=fcfs:easy
	Rank=4: X=--prefix=tree.4 J=--njobs=2 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run -T 4x2x3 -Q fcfs:fcfs:easy -N 4 -c 4 -J 10 /bin/hostname \
> out.13 &&
    remove_prefix out.13 out.13.2 &&
    sed -i "s/[ \t]*$//g" out.13.2 &&
    test_cmp cmp.13 out.13.2
'

test_expect_success 'flux-tree: combining -T -Q -P works (I)' '
    cat >cmp.14 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:queue-policy=fcfs,queue-params=queue-depth=23
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=4
	Rank=1: T=--topology=2
	Rank=1: Q=--queue-policy=conservative P=--queue-params=reservation-depth=24
	Rank=1: X=--prefix=tree.1 J=--njobs=3 S=/bin/hostname
	
	Rank=2: N=1 c=4
	Rank=2: T=--topology=2
	Rank=2: Q=--queue-policy=conservative P=--queue-params=reservation-depth=24
	Rank=2: X=--prefix=tree.2 J=--njobs=3 S=/bin/hostname
	
	Rank=3: N=1 c=4
	Rank=3: T=--topology=2
	Rank=3: Q=--queue-policy=conservative P=--queue-params=reservation-depth=24
	Rank=3: X=--prefix=tree.3 J=--njobs=2 S=/bin/hostname
	
	Rank=4: N=1 c=4
	Rank=4: T=--topology=2
	Rank=4: Q=--queue-policy=conservative P=--queue-params=reservation-depth=24
	Rank=4: X=--prefix=tree.4 J=--njobs=2 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run -T 4x2 -Q fcfs:conservative \
-P queue-depth=23:reservation-depth=24 -N 4 -c 4 -J 10 /bin/hostname > out.14 &&
    remove_prefix out.14 out.14.2 &&
    sed -i "s/[ \t]*$//g" out.14.2 &&
    test_cmp cmp.14 out.14.2
'

test_expect_success 'flux-tree: combining -T -Q -P works (II)' '
    cat >cmp.15 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:queue-policy=fcfs,queue-params=queue-depth=7,reservation-depth=8
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=4
	Rank=1: T=--topology=2
	Rank=1: Q=--queue-policy=conservative P=--queue-params=queue-depth=24
	Rank=1: X=--prefix=tree.1 J=--njobs=3 S=/bin/hostname
	
	Rank=2: N=1 c=4
	Rank=2: T=--topology=2
	Rank=2: Q=--queue-policy=conservative P=--queue-params=queue-depth=24
	Rank=2: X=--prefix=tree.2 J=--njobs=3 S=/bin/hostname
	
	Rank=3: N=1 c=4
	Rank=3: T=--topology=2
	Rank=3: Q=--queue-policy=conservative P=--queue-params=queue-depth=24
	Rank=3: X=--prefix=tree.3 J=--njobs=2 S=/bin/hostname
	
	Rank=4: N=1 c=4
	Rank=4: T=--topology=2
	Rank=4: Q=--queue-policy=conservative P=--queue-params=queue-depth=24
	Rank=4: X=--prefix=tree.4 J=--njobs=2 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run -T 4x2 -Q fcfs:conservative \
-P queue-depth=7,reservation-depth=8:queue-depth=24 -N 4 -c 4 -J 10 \
/bin/hostname > out.15 &&
    remove_prefix out.15 out.15.2 &&
    sed -i "s/[ \t]*$//g" out.15.2 &&
    test_cmp cmp.15 out.15.2
'

test_expect_success 'flux-tree: combining -T -M works' '
    cat >cmp.16 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:hwloc-whitelist=node,core,gpu policy=low
	Rank=1: N=1 c=4
	Rank=1: T=--topology=2
	Rank=1: M=--match-policy=high
	Rank=1: X=--prefix=tree.1 J=--njobs=3 S=/bin/hostname
	
	Rank=2: N=1 c=4
	Rank=2: T=--topology=2
	Rank=2: M=--match-policy=high
	Rank=2: X=--prefix=tree.2 J=--njobs=3 S=/bin/hostname
	
	Rank=3: N=1 c=4
	Rank=3: T=--topology=2
	Rank=3: M=--match-policy=high
	Rank=3: X=--prefix=tree.3 J=--njobs=2 S=/bin/hostname
	
	Rank=4: N=1 c=4
	Rank=4: T=--topology=2
	Rank=4: M=--match-policy=high
	Rank=4: X=--prefix=tree.4 J=--njobs=2 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run -T 4x2 -M low:high -N 4 -c 4 -J 10 /bin/hostname \
> out.16 &&
    remove_prefix out.16 out.16.2 &&
    sed -i "s/[ \t]*$//g" out.16.2 &&
    test_cmp cmp.16 out.16.2
'

test_expect_success 'flux-tree: existing FLUX_QMANAGER_OPTIONS is respected' '
    cat >cmp.17 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:queue-policy=conservative
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=1
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=1 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:queue-policy=easy
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    export FLUX_QMANAGER_OPTIONS=queue-policy=easy &&
    flux tree --dry-run -T 1 -Q conservative /bin/hostname > out.17 &&
    unset FLUX_QMANAGER_OPTIONS &&
    remove_prefix out.17 out.17.2 &&
    sed -i "s/[ \t]*$//g" out.17.2 &&
    test_cmp cmp.17 out.17.2
'

test_expect_success 'flux-tree: existing FLUX_RESOURCE_OPTIONS is respected' '
    cat >cmp.18 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:hwloc-whitelist=node,core,gpu policy=locality
	Rank=1: N=1 c=1
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=1 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:high
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    export FLUX_RESOURCE_OPTIONS=high &&
    flux tree --dry-run -T 1 -M locality /bin/hostname > out.18 &&
    unset FLUX_RESOURCE_OPTIONS &&
    remove_prefix out.18 out.18.2 &&
    sed -i "s/[ \t]*$//g" out.18.2 &&
    test_cmp cmp.18 out.18.2 
'

test_expect_success 'flux-tree: -T4 on 4 nodes/4 cores/4 GPUs work' '
    cat >cmp.19 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=4 g=-g 4
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=3 S=/bin/hostname
	
	Rank=2: N=1 c=4 g=-g 4
	Rank=2: T=--leaf
	Rank=2:
	Rank=2: X=--prefix=tree.2 J=--njobs=3 S=/bin/hostname
	
	Rank=3: N=1 c=4 g=-g 4
	Rank=3: T=--leaf
	Rank=3:
	Rank=3: X=--prefix=tree.3 J=--njobs=2 S=/bin/hostname
	
	Rank=4: N=1 c=4 g=-g 4
	Rank=4: T=--leaf
	Rank=4:
	Rank=4: X=--prefix=tree.4 J=--njobs=2 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=4 -N 4 -c 4 -g 4 -J 10 \
/bin/hostname > out.19 &&
    remove_prefix out.19 out.19.2 &&
    sed -i "s/[ \t]*$//g" out.19.2 &&
    test_cmp cmp.19 out.19.2
'

test_expect_success 'flux-tree: -T4 on 4 nodes/4 cores/2 GPUs work' '
    cat >cmp.20 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=2 c=4 g=-g 2
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=5 S=/bin/hostname
	
	Rank=2: N=2 c=4 g=-g 2
	Rank=2: T=--leaf
	Rank=2:
	Rank=2: X=--prefix=tree.2 J=--njobs=5 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=4 -N 4 -c 4 -g 2 -J 10 \
/bin/hostname > out.20 &&
    remove_prefix out.20 out.20.2 &&
    sed -i "s/[ \t]*$//g" out.20.2 &&
    test_cmp cmp.20 out.20.2
'

test_expect_success 'flux-tree: -T4 on 4 nodes/4 cores/2 GPUs 1 job works' '
    cat >cmp.21 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=2 c=4 g=-g 2
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=1 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=4 -N 4 -c 4 -g 2 -J 1 \
/bin/hostname > out.21 &&
    remove_prefix out.21 out.21.2 &&
    sed -i "s/[ \t]*$//g" out.21.2 &&
    test_cmp cmp.21 out.21.2
'

test_expect_success 'flux-tree: correct job count under a unbalanced tree' '
    cat >cmp.22 <<-EOF &&
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	Rank=1: N=1 c=1
	Rank=1: T=--leaf
	Rank=1:
	Rank=1: X=--prefix=tree.1 J=--njobs=5 S=/bin/hostname
	
	Rank=2: N=1 c=1
	Rank=2: T=--leaf
	Rank=2:
	Rank=2: X=--prefix=tree.2 J=--njobs=5 S=/bin/hostname
	
	FLUX_QMANAGER_OPTIONS:
	FLUX_RESOURCE_OPTIONS:
	FLUX_QMANAGER_RC_NOOP:1
	FLUX_RESOURCE_RC_NOOP:1
EOF
    flux tree --dry-run --topology=3 -N 1 -c 2 -J 10 \
/bin/hostname > out.22 &&
    remove_prefix out.22 out.22.2 &&
    sed -i "s/[ \t]*$//g" out.22.2 &&
    test_cmp cmp.22 out.22.2
'

test_expect_success 'flux-tree: prep for testing in real mode works' '
    flux module remove sched-simple &&
    flux module load resource prune-filters=ALL:core \
subsystems=containment policy=low load-whitelist=node,core,gpu &&
    flux module load qmanager
'

test_expect_success 'flux-tree: --leaf in real mode' '
    flux tree --leaf -N 1 -c 1 -J 1 -o p.out2 hostname &&
    test -f p.out2 &&
    lcount=$(wc -l p.out2 | awk "{print \$1}") &&
    test ${lcount} -eq 2 &&
    wcount=$(wc -w p.out2 | awk "{print \$1}") &&
    test ${wcount} -eq 18
'

test_expect_success 'flux-tree: -T1 in real mode' '
    flux tree -T1 -N 1 -c 1 -J 1 -o p.out3 hostname &&
    test -f p.out3 &&
    lcount=$(wc -l p.out3 | awk "{print \$1}") &&
    test ${lcount} -eq 3 &&
    wcount=$(wc -w p.out3 | awk "{print \$1}") &&
    test ${wcount} -eq 27
'

test_expect_success 'flux-tree: -T1x1 in real mode' '
    flux tree -T1x1 -N 1 -c 1 -J 1 -o p.out4 hostname &&
    test -f p.out4 &&
    lcount=$(wc -l p.out4 | awk "{print \$1}") &&
    test ${lcount} -eq 4 &&
    wcount=$(wc -w p.out4 | awk "{print \$1}") &&
    test ${wcount} -eq 36
'

test_expect_success 'flux-tree: -T2 with exit code rollup works' '
    cat >jobscript.sh <<EOF &&
#! /bin/bash
echo \${FLUX_TREE_ID}
if [[ \${FLUX_TREE_ID} = "tree.2" ]]
then
	exit 4
else
	exit 1
fi
EOF

    cat >cmp.23 <<EOF &&
tree.2
flux-tree: warning: ./jobscript.sh: exited with exit code (4)
flux-tree: warning: invocation id: tree.2@index[1]
flux-tree: warning: output displayed above, if any
EOF
    chmod u+x jobscript.sh &&
    test_expect_code 4 flux tree -T2 -N 1 -c 2 -J 2 \
./jobscript.sh > out.23 &&
    test_cmp cmp.23 out.23
'

PERF_FORMAT="{treeid}"
PERF_BLOB='{"treeid":"tree", "perf": {}}'
JOB_NAME="foobar"
test_expect_success 'flux-tree: successfully runs alongside other jobs' '
    flux tree -T 1 -N 1 -c 1 -J 1 -o p.out5 --perf-format="$PERF_FORMAT" \
         --job-name="${JOB_NAME}" -- hostname &&
    flux mini run -N1 -c1 hostname &&
    echo "$PERF_BLOB" | run_timeout 5 flux tree-helper --perf-out=p.out6 \
         --perf-format="$PERF_FORMAT" 1 "tree-perf" "${JOB_NAME}" &&
    test_cmp p.out5 p.out6
'

JOB_NAME="foobar2"
test_expect_success 'flux-tree: successfully runs alongside other flux-trees' '
    run_timeout 20 \
    flux tree -T 1x1 -N 1 -c 1 -J 1 -o p.out7 --perf-format="$PERF_FORMAT" \
         --job-name="${JOB_NAME}" -- hostname &&
    flux tree -T 1 -N 1 -c 1 -J 1 -- hostname &&
    echo "$PERF_BLOB" | run_timeout 5 flux tree-helper --perf-out=p.out8 \
         --perf-format="$PERF_FORMAT" 1 "tree-perf" "${JOB_NAME}" &&
    test_cmp p.out7 p.out8
'

test_expect_success 'flux-tree: removing qmanager/resource works' '
     flux module remove resource &&
     flux module remove qmanager
'

test_done