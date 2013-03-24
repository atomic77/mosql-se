#!/bin/bash
set -e

if [ $# -lt 4 ]; then
	echo "Usage: $0 <Start Server> <Num Physical Servers> <Nodes Per Server> <Additional Servers>"
	echo "Assumes that each physical server will correspond to one mysql instances and write"
	echo "to a sequentially ordered data directory in ~/db/mysql/data<x>"
	echo "Additional node configuration files will be written assuming the same number of nodes"
	echo "per physical server. Assumes that a base configuration file exists"
	exit 1;
fi

set -e 
START=$1
PHYS=$2
VIRT=$3
ADDITIONAL=$4
END=$(( $2 + $START - 1 ))
TOTADDNODES=$(( $4 * $3 ))
BASECFGNAME=$PHYS\x$VIRT
BASECFGFILE=configs/$BASECFGNAME\.cfg

STARTADD=$(( $END + 1 ))
ENDADD=$(( $END + $ADDITIONAL ))

cd ~/exp/

NODEID=$(( $PHYS * $VIRT ))
PHYSID=$PHYS
CFGPORT=$(( 4800 + $NODEID ))
INTNODES=$(( $PHYS * $VIRT + 1 ))
echo "Generating files for $TOTADDNODES additional nodes from servers node$STARTADD to node$ENDADD"

for s in `seq $STARTADD $ENDADD`; do
	for v in `seq 1 $VIRT`; do
		INTCFGFILE=configs/$BASECFGNAME-node$s-id$NODEID\.cfg
		cp $BASECFGFILE $INTCFGFILE
		perl -pi -e "s/NumberOfNodes.*/NumberOfNodes $INTNODES/g" $INTCFGFILE
		echo "node $NODEID 192.168.3.$s $CFGPORT  " >> $INTCFGFILE	
		CFGPORT=$(( $CFGPORT + 1 ))
		INTNODES=$(( $INTNODES + 1 ))
		NODEID=$(( $NODEID + 1 ))
		BASECFGFILE=$INTCFGFILE
	done

	PHYSID=$(( $PHYSID + 1 ))
done

cd -

