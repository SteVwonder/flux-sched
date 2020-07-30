#!/bin/sh

test_description='Test node-local storage cases'

. $(dirname $0)/sharness.sh

cmd_dir="${SHARNESS_TEST_SRCDIR}/data/resource/commands/node_local_storage"
exp_dir="${SHARNESS_TEST_SRCDIR}/data/resource/expected/node_local_storage"
xml_dir="${SHARNESS_TEST_SRCDIR}/data/hwloc-data/001N/node_local_storage/"
query="../../resource/utilities/resource-query"


corona_cmds="${cmd_dir}/corona.in"
corona_xml="${xml_dir}/corona/0.xml"
corona_desc="match allocate with corona hwloc and corona jobspec"
test_expect_success "${corona_desc}" '
    sed "s~@TEST_SRCDIR@~${SHARNESS_TEST_SRCDIR}~g" ${corona_cmds} > corona_cmds &&
    ${query} -L ${corona_xml} -f hwloc -W node,socket,core,memory,storage -S CA -P low -t corona.R.out < corona_cmds &&
    test_cmp ${exp_dir}/corona.R.out corona.R.out
'

test_done
