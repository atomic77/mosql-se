#!/bin/bash

set -e
set -x

make all
scripts/loc_restart.sh
sleep 3
mysql < sql/percona_tpcc.ddl.sql
# mysql < data/
tail -15 ~/local/mysql/debug.err
