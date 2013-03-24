#!/bin/bash
set -e

if [ $# -lt 3 ]; then
	echo ""
	echo "Usage: $0 <Start Server> <Num Physical Servers> <Nodes Per Server>"
	echo ""
	echo "Assumes that each physical server will correspond to one mysql instances and write"
	echo "to a sequentially ordered data directory in ~/db/mysql/data<x>"
	echo ""
	echo "Requires that header.cfg and header.sh exist for the given sequence of nodes in ./config/"
	echo ""
	echo "Also generates mysql launch files to be used with mysql_launch.sh"
	exit 1;
fi

START=$1
PHYS=$2
VIRT=$3
END=$(( $2 + $START - 1 ))
TOTNODES=$(( $2 * $3 ))
BASHFILE=configs/$PHYS\x$VIRT.sh
CFGNAME=$PHYS\x$VIRT.cfg
CFGFILE=configs/$CFGNAME
MYSQLLAUNCHFILE=configs/mysql.$PHYS
MYSQLCFGBASE=/home/tomic/db/mysql
TAPIOCAPORT=55500

cd ~/exp/

NODEID=0
PHYSID=0
CFGPORT=4800
echo "Generating files for $TOTNODES tapioca nodes from servers node$START to node$END"

rm -f $MYSQLLAUNCHFILE
cp configs/header.cfg $CFGFILE
perl -pi -e "s/NumberOfNodes.*/NumberOfNodes $TOTNODES/g" $CFGFILE

echo "#!/bin/bash" > $BASHFILE
echo ". configs/header.sh" >> $BASHFILE
echo "config_file=\"$CFGNAME\"" >> $BASHFILE
echo "" >> $BASHFILE

for s in `seq $START $END`; do
	MYSQLTAPIOCACFG="$MYSQLCFGBASE/data$PHYSID/tapioca.cfg"
	rm -f $MYSQLTAPIOCACFG

	# Write out mysql launch configuration 
	# TODO Modify to support multiple mysqld per box
	echo "data$PHYSID 127.0.0.1 $TAPIOCAPORT $PHYSID node$s 3307" >> $MYSQLLAUNCHFILE

	# Write bash and configuration data for tapioca to be able to launch"
	for v in `seq 1 $VIRT`; do
		echo "dbnode[$NODEID]=\"node$s\"" >> $BASHFILE	
		echo "node $NODEID 192.168.3.$s $CFGPORT  " >> $CFGFILE	
		echo "127.0.0.1 $TAPIOCAPORT $NODEID" >> $MYSQLTAPIOCACFG
		CFGPORT=$(( $CFGPORT + 1 ))
		NODEID=$(( $NODEID + 1 ))
		TAPIOCAPORT=$(( $TAPIOCAPORT + 1 ))
	done

#data1 127.0.0.1 5555 1 node45 3307

	
	PHYSID=$(( $PHYSID + 1 ))
done

cd -

