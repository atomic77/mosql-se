#!/bin/bash

if [ $# -lt 3 ]; then
	echo "Usage: $0 <Label> <WH> <Time>"
	echo "Assumes that local mysql server is running on localhost:3307"
	exit 1;
fi

set -e
#LABEL="3x2-newcache"; 
LABEL=$1
WH=$2; 
TM=$3; 
THREADCNTS="1 2 4 8 12 16"
TPCCDIR=/home/tomic/branches/tpcc-mysql/src

cd $TPCCDIR
export LD_LIBRARY_PATH=/home/tomic/local/mysql/lib/mysql

for T in $THREADCNTS; do 
	echo "`hostname` : $T"
	./tpcc_start 127.0.0.1:3307 tpcc tpcc tpcc $WH $T 0 $TM 2> /dev/null > logs/`hostname`-$LABEL-$WH-$T.log; 
	sleep 10;
done

cd -

