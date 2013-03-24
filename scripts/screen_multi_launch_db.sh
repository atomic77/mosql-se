#!/bin/bash
#set -x
#BASEDIR=/home/atomic/dev/mysql-5.1.51-linux-i686-glibc23
if [ $# -lt 2 ]; then
	echo "Usage: $0 <NodeId> <Port> <Kill mysqld with -k>"
	exit 1;
fi

NODE=$1
PORT=$2

###
# This script needs to modified to ensure that mysql runs with minimal impact to /home
# by copying everything over to /tmp before execution

BASEDIR=/home/tomic/local/mysql
BUILDDIR=/home/tomic/branches/buffering/src/.libs
LIB=libTapiocaMySQL.so
MYSQLCNF=my.cnf
SOCKET=/tmp/tomic-mysql.sock
if [ $PORT -ne 3307 ]; then
	SOCKET=/tmp/tomic-mysql-$NODE.sock
fi

TS=`date +%s`
TRACEDIR=/tmp
SRCDATADIR=/home/tomic/db/mysql
BASEDATADIR=/home/tomic/db/mysql
#BASEDATADIR=/tmp/tomic

#DEBUGPARAMS=d:t:i:O,$TRACEDIR/mysql-$TS.trace
#DEBUGPARAMS=g
DEBUGPARAMS=

#if [ -d $BASEDATADIR ]; then
#	echo "Found existing $BASEDATADIR, removing it."
#	rm -rf $BASEDATADIR
#fi

if [ "$3" = "-k" ]; then 
	killall -9 -u tomic mysqld_safe mysqld
else
	$BASEDIR/bin/mysqladmin --socket $SOCKET -u root shutdown
fi

#cp $MYSQLCNF $BASEDIR
if [ -e $BASEDIR/lib/mysql/plugin ]; then # 5.1 build
	cp $BUILDDIR/$LIB $BASEDIR/lib/mysql/plugin
else  # 5.5 build
	cp $BUILDDIR/$LIB $BASEDIR/lib/plugin
fi


cd $BASEDIR
rm -rf $BASEDATADIR/data$NODE/test1
screen -d -m bin/mysqld_safe --defaults-file=$BASEDATADIR/data$NODE/$MYSQLCNF  --skip-syslog --basedir=$BASEDIR --datadir=$BASEDATADIR/data$NODE --log-error=$BASEDATADIR/data$NODE/debug.err --port=$PORT --socket=$SOCKET  --lower_case_table_names=1  #--debug=$DEBUGPARAMS 


