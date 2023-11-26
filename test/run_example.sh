#!/bin/bash
BUILDDIR=$(dirname "$0")/../build/
function Run() {
    dbname=$1
    loadnum=$2
    opnum=$3

    rm -rf /mnt/pmem1/lbl/*
    date | tee example_output.txt
    numactl --cpubind=1 --membind=1 ${BUILDDIR}/example --load-size ${loadnum} \
    --put-size ${opnum} --get-size ${opnum} \
    | tee -a example_output.txt
}

function main() {
    dbname="letree"
    loadnum=400000000
    opnum=10000000

    if [ $# -ge 1 ]; then
        dbname=$1
    fi
    if [ $# -ge 2 ]; then
        loadnum=$2
    fi
    if [ $# -ge 3 ]; then
        opnum=$3
    fi

    echo "Run $dbname $loadnum $opnum"
    Run $dbname $loadnum $opnum
}

main letree 400000000 10000000