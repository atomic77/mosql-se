#!/bin/bash

cd ~/branches/tapioca_bptree
./stop.sh
sleep 1;
./start_multi.sh $2
cd -
sleep 1;
scripts/launch_db.sh $1

