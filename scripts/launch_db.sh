#!/bin/bash
BASEDIR=/home/atomic/local/mysql
BUILDDIR=/home/atomic/branches/buffering/src/.libs
LIB=libTapiocaMySQL.so
MYSQLCNF=my.cnf
SOCKET=/tmp/mysql-debug.sock
TS=`date +%s`
TRACEDIR=/tmp
#DEBUGPARAMS=d:t:i:O,$TRACEDIR/mysql-$TS.trace
#DEBUGPARAMS=d:g:n:t:i:O,$TRACEDIR/mysql-$TS.trace
#DEBUGPARAMS=d,ha_tapioca:g:n:t:i:O,$TRACEDIR/mysql-$TS.trace
#DEBUGPARAMS=d:f,open:i:O,$TRACEDIR/mysql-$TS.trace
DEBUGPARAMS=


if [ "$1" = "-k" ]; then 
	pkill -9 -f mysqld
	rm -rf $BASEDIR/data/test1
else
	$BASEDIR/bin/mysqladmin --socket $SOCKET -u root shutdown
fi

cp $MYSQLCNF $BASEDIR
if [ -e $BASEDIR/lib/mysql/plugin ]; then # 5.1 build
	echo "Copying to 5.1 plugin dir"
	cp $BUILDDIR/$LIB $BASEDIR/lib/mysql/plugin
else 
	echo "Copying to 5.5 plugin dir"
	cp $BUILDDIR/$LIB $BASEDIR/lib/plugin/
fi

set -e

cd $BASEDIR
#bin/mysqld_safe --basedir=$BASEDIR --datadir=$BASEDIR/data --defaults-file=$BASEDIR/$MYSQLCNF 
#xterm -e "bin/mysqld_safe --defaults-file=$BASEDIR/$MYSQLCNF --debug=$DEBUGPARAMS --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err  " &
#xterm -e "bin/mysqld_safe --defaults-file=$BASEDIR/$MYSQLCNF  --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err --debug=$DEBUGPARAMS --lower_case_table_names=1  " 2> /dev/null &
nohup bin/mysqld_safe --defaults-file=$BASEDIR/$MYSQLCNF  --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err --debug=$DEBUGPARAMS --lower_case_table_names=1   2> /dev/null &
#xterm -e "bin/mysqld_safe --defaults-file=$BASEDIR/$MYSQLCNF  --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err  --lower_case_table_names=1   " 2> /dev/null &
#bin/mysqld_safe --defaults-file $BASEDIR/$MYSQLCNF 

