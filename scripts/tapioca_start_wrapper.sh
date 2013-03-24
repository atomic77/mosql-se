#!/bin/bash

#BASEDIR=/home/atomic/dev/mysql-5.1.51-linux-i686-glibc23
TAPIOCADIR=/home/atomic//Documents/USI/workspace/tapioca_trunk

cd $TAPIOCADIR
./start.sh
read -p 'Press enter to finish'

#xterm -e "bin/mysqld --defaults-file=$BASEDIR/$MYSQLCNF --skip-syslog --basedir=$BASEDIR --datadir=$BASEDIR/data --log-error=$BASEDIR/debug.err  " &
#bin/mysqld_safe --defaults-file $BASEDIR/$MYSQLCNF 

