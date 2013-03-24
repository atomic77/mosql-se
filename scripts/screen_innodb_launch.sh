#!/bin/bash

set -x

#BASEDIR=/home/atomic/dev/mysql-5.1.51-linux-i686-glibc23
BASEDIR=/home/tomic/local/mysql
BUILDDIR=/home/tomic/branches/buffering/src/.libs
LIB=libTapiocaMySQL.so
MYSQLCNF=my-innodb.cnf
SOCKET=/tmp/tomic-mysql.sock
TS=`date +%s`
TRACEDIR=/tmp
#DEBUGPARAMS=d:t:i:O,$TRACEDIR/mysql-$TS.trace
DEBUGPARAMS=

#set -x
if [ "$1" = "-k" ]; then 
	pkill -9 -u tomic mysqld
else
	$BASEDIR/bin/mysqladmin --socket $SOCKET -u root shutdown
fi
mkdir /tmp/tomic
mkdir /tmp/tomic/innodb

set -e
cp $MYSQLCNF $BASEDIR
cp $BUILDDIR/$LIB $BASEDIR/lib/mysql/plugin

cd $BASEDIR

screen -d -m bin/mysqld_safe --defaults-file=$BASEDIR/$MYSQLCNF   --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err # --debug=$DEBUGPARAMS  

