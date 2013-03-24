#!/bin/bash


mysql -D perftests -e "truncate perfresults"
set -e
cd ~/data/results
for dname in `ls`; do
	cd $dname
	for i in `ls *.out`; do 
		mysql -D perftests -e "truncate rawperftests"
		mysql -D perftests -e "load data local infile '$i' into table rawperftests"; 
		mysql -D perftests -e "INSERT INTO perfresults 
			SELECT 
			'$dname' as global_test,
			FROM_UNIXTIME(SUBSTRING_INDEX(SUBSTRING_INDEX( test_name , '-', 2 ),'-',-1)) AS b,
			SUBSTRING_INDEX(SUBSTRING_INDEX( test_name , '-', 3 ),'-',-1) AS c,
			SUBSTRING_INDEX(SUBSTRING_INDEX( test_name , '-', 4 ),'-',-1) AS d,
			SUBSTRING_INDEX(SUBSTRING_INDEX( test_name , '-', 5 ),'-',-1) AS e,
			r.*
			FROM rawperftests r;"
	done
	cd ..
done
