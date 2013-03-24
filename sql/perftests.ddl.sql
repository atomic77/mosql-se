USE perftests;
DROP TABLE IF EXISTS rawperftests;
CREATE TABLE rawperftests (
	test_name varchar(1024),
	thread_id int,
	test_id int,
	start_key int,
	end_key int,
	elapsed DECIMAL(10,3),
	op_rate DECIMAL(10,1),
	aborts INT,
	resp_min INT,
	resp_5pct INT,
	resp_med INT,
	resp_95pct INT,
	resp_max INT
) ENGINE = MyISAM;

DROP TABLE IF EXISTS perfresults;
CREATE TABLE perfresults (
	global_test varchar(1024),
	test_time datetime,
	num_threads int,
	num_keys_per_thread int,
	mysql_host varchar(1024),
	test_name varchar(1024),
	thread_id int,
	test_id int,
	start_key int,
	end_key int,
	elapsed DECIMAL(10,3),
	op_rate DECIMAL(10,1),
	aborts INT,
	resp_min INT,
	resp_5pct INT,
	resp_med INT,
	resp_95pct INT,
	resp_max INT
) ENGINE = MyISAM;

-- for `ls *.out`; do mysql -D perftests -e "load data local infile '$i' into table rawperftests; done

INSERT INTO perfresults 
SELECT 
'3n3m' as `global_test`,
FROM_UNIXTIME(SUBSTRING_INDEX(SUBSTRING_INDEX( `test_name` , '-', 2 ),'-',-1)) AS b,
SUBSTRING_INDEX(SUBSTRING_INDEX( `test_name` , '-', 3 ),'-',-1) AS c,
SUBSTRING_INDEX(SUBSTRING_INDEX( `test_name` , '-', 4 ),'-',-1) AS d,
SUBSTRING_INDEX(SUBSTRING_INDEX( `test_name` , '-', 5 ),'-',-1) AS e,
r.*
FROM rawperftests r;