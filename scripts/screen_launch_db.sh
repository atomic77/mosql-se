#!/bin/bash

#BASEDIR=/home/atomic/dev/mysql-5.1.51-linux-i686-glibc23
BASEDIR=/home/tomic/local/mysql
BUILDDIR=/home/tomic/branches/buffering/src/.libs
LIB=libTapiocaMySQL.so
MYSQLCNF=my.cnf
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
set -e
cp $MYSQLCNF $BASEDIR
cp $BUILDDIR/$LIB $BASEDIR/lib/mysql/plugin

cd $BASEDIR
#bin/mysqld_safe --basedir=$BASEDIR --datadir=$BASEDIR/data --defaults-file=$BASEDIR/$MYSQLCNF 
screen -d -m bin/mysqld_safe --defaults-file=$BASEDIR/$MYSQLCNF --debug=$DEBUGPARAMS --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err   
#xterm -e "bin/mysqld --defaults-file=$BASEDIR/$MYSQLCNF --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err  " &
#bin/mysqld_safe --defaults-file $BASEDIR/$MYSQLCNF 

