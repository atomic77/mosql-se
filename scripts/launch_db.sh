#!/bin/bash
# Script for launching mysqld process with mosql-se shared lib
PROGNAME=${0##*/} 
# Options for script, in order given in LONGOPTS
CLEARDB="n"
KILL="n"
NOREC="n"

SHORTOPTS="hb:tksp:l:d:"
LONGOPTS="help,basedir:,trace,kill-all,shutdown,purge-schema-dir:,launch-in:,datadir:"

BASEDIR=~/local/mysql
DATADIR=~/local/mysql/data
BUILDDIR=~/local/mosql-se/
LIB=libmosqlse.so
MYSQLCNF=my.cnf
SOCKET=/tmp/mysql-debug.sock
TS=`date +%s`
TRACEDIR=/tmp
TRACE="n"
LAUNCHWITH="nohup" 
#DEBUGPARAMS=d:g:n:t:i:O,$TRACEDIR/mysql-$TS.trace
DEBUGPARAMS=d:g:n:t:i:o,$TRACEDIR/mysql-$TS.trace

FINALPARAMS=
PURGESCHEMADIR=
DOPURGESCHEMA="n"

usage () {
	echo "$0 <options>"
	echo "Long options: $LONGOPTS"
	echo "Short options: $SHORTOPTS"
}

check_env () {
	if [ ! -e lib/$LIB ]; then
		echo "Could not find $LIB library in ./lib"
		echo "Please run this script from the root mosql-se install folder"
		exit 1
	fi
}

ARGS=$(getopt -s bash --options $SHORTOPTS  \
  --longoptions $LONGOPTS --name $PROGNAME -- "$@" )

eval set -- "$ARGS"

while true; do
   case $1 in
      -h|--help)
         usage
         exit 0
         ;;
      -p|--purge-schema-dir) 
		shift
		DOPURGESCHEMA="y"
		PURGESCHEMADIR=$1
		;;
      -b|--basedir) 
		shift
		BASEDIR=$1
		;;
      -d|--datadir) 
		shift
		DATADIR=$1
		;;
      -t|--trace) 
		EXTRAPARAMS="$EXTRAPARAMS --debug=$DEBUGPARAMS"
		;;
      -k|--kill-all) 
		KILL="y"
		;;
      *)
         break
         ;; 
	esac
	shift
done

check_env

if [ "$KILL" = "y" ]; then 
	echo "Giving mysqld 2 seconds to shutdown with SIGTERM"
	killall -q -u $USER mysqld mysqld_safe
	sleep 2
	echo "Now -9'ing."
	killall -q -9 -u $USER mysqld mysqld_safe
	#rm -rf $BASEDIR/data/test1
else
	echo "Shutting down mysqld with mysqladmin"
	$BASEDIR/bin/mysqladmin --socket $SOCKET -u root shutdown
fi

cp $BUILDDIR/config/$MYSQLCNF $BASEDIR

if [ "$DOPURGESCHEMA" = "y" ]; then
	if [ -e $BASEDIR/data/$PURGESCHEMADIR/db.opt ]; then
		echo "Blowing away $BASEDIR/data/$PURGESCHEMADIR"
		rm -rf $BASEDIR/data/$PURGESCHEMADIR
	fi
fi

if [ -e $BASEDIR/lib/mysql/plugin ]; then # 5.1 build
	echo "Copying to 5.1 plugin dir"
	cp $BUILDDIR/lib/$LIB $BASEDIR/lib/mysql/plugin
else 
	echo "Copying to 5.5+ plugin dir"
	cp $BUILDDIR/lib/$LIB $BASEDIR/lib/plugin/
fi

set -e

BASEPARAMS="--defaults-file=$BASEDIR/$MYSQLCNF  --basedir=$BASEDIR --datadir=$DATADIR --log-error=$DATADIR/debug.err --lower_case_table_names=1 " #  2> /dev/null &" 

cd $BASEDIR
nohup bin/mysqld_safe $BASEPARAMS $EXTRAPARAMS 2> /dev/null &


