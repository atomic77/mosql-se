#!/bin/bash
BASEDIR=/home/atomic/local/mysql
BUILDDIR=/home/atomic/Documents/USI/workspace/TapiocaMySQL/src/.libs
LIB=libTapiocaMySQL.so
MYSQLCNF=my.cnf
if [ $# -lt 2 ]; then
	echo "Usage: $0 <NodeId> <Port> <Kill mysqld with -k>"
	exit 1;
fi

NODE=$1
PORT=$2
SOCKET=/tmp/mysql-debug-node$NODE.sock
TS=`date +%s`
TRACEDIR=/tmp
DEBUGPARAMS=d:t:i:O,$TRACEDIR/mysql-$TS-node$NODE.trace
#DEBUGPARAMS=d:g:n:t:i:O,$TRACEDIR/mysql-$TS.trace
#DEBUGPARAMS=d,ha_tapioca:g:n:t:i:O,$TRACEDIR/mysql-$TS.trace
#DEBUGPARAMS=d:f,open:i:O,$TRACEDIR/mysql-$TS.trace
#DEBUGPARAMS=
BASEDATADIR=/home/tomic/data/mysql/

if [ "$3" = "-k" ]; then 
	kill -9 `cat $BASEDATADIR/data$NODE/*.pid`
else
	$BASEDIR/bin/mysqladmin --socket $SOCKET -u root shutdown
fi
#cp $MYSQLCNF $BASEDIR
cp $BUILDDIR/$LIB $BASEDIR/lib/mysql/plugin
set -e

cd $BASEDIR
#bin/mysqld_safe --basedir=$BASEDIR --datadir=$BASEDIR/data --defaults-file=$BASEDIR/$MYSQLCNF 
xterm -e "bin/mysqld_safe --defaults-file=$BASEDATADIR/data$NODE/$MYSQLCNF --debug=$DEBUGPARAMS --skip-syslog --basedir=$BASEDIR --datadir=$BASEDATADIR/data$NODE --port=$PORT --log-error=$BASEDATADIR/data$NODE/debug.err  " &
#xterm -e "bin/mysqld_safe --defaults-file=$BASEDIR/$MYSQLCNF  --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err  " &
#xterm -e "bin/mysqld --defaults-file=$BASEDIR/$MYSQLCNF --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err  " &
#bin/mysqld_safe --defaults-file $BASEDIR/$MYSQLCNF 

