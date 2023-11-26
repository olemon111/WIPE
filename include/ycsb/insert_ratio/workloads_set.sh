#!/bin/bash
RecordCount=10000000
OpCount=1000000
Distribute="zipfian"
WorkLoads="0 10 20 30 40 50 60 70 80 90 100"
if [ $# -ge 1 ]; then
    RecordCount=$1
fi

if [ $# -ge 2 ]; then
    OpCount=$2
fi

for workload in $WorkLoads; do
    sed -r -i "s/recordcount=.*/recordcount=$RecordCount/1" workloada_insert_${workload}.spec
    sed -r -i "s/operationcount=.*/operationcount=$OpCount/1" workloada_insert_${workload}.spec
done

if [ $# -ge 3 ]; then
    Distribute=$3
    sed -r -i "s/requestdistribution=.*/requestdistribution=$Distribute/1" workloada_insert_${workload}.spec
fi