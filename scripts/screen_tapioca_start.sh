#!/bin/bash

#BASEDIR=/home/atomic/dev/mysql-5.1.51-linux-i686-glibc23
TAPIOCADIR=/home/atomic//Documents/USI/workspace/tapioca_trunk

#set -x
#set -e
cd $TAPIOCADIR
echo Killing any existing tapioca processes
if [ "$1" = "-k" ]; then 
	killall -s 9 tapioca cm acceptor
else
	./stop.sh	
fi
echo Launching tapioca in a detached screen session
cd -
screen -d -m scripts/tapioca_start_wrapper.sh 

#xterm -e "bin/mysqld --defaults-file=$BASEDIR/$MYSQLCNF --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err  " &
#bin/mysqld_safe --defaults-file $BASEDIR/$MYSQLCNF 

