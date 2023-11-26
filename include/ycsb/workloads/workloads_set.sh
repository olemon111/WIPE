#!/bin/bash
RecordCount=10000000
OpCount=1000000
WorkLoads="a b c d e f"
if [ $# -ge 1 ]; then
    RecordCount=$1
fi

if [ $# -ge 2 ]; then
    OpCount=$2
fi

for workload in $WorkLoads; do
    sed -r -i "s/recordcount=.*/recordcount=$RecordCount/1" workload${workload}.spec
    sed -r -i "s/operationcount=.*/operationcount=$OpCount/1" workload${workload}.spec
done