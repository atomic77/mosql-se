SET AUTOCOMMIT = on;
DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

CREATE TABLE t1 (c int, primary key (c)) ENGINE=MoSQL;
CREATE TABLE t2 (c float, primary key (c)) ENGINE=MoSQL;

INSERT INTO t1 VALUES (1),(2),(3),(4),(5);

-- Transaction A

SET AUTOCOMMIT = off;
BEGIN;

SELECT AVG(c) FROM t1 INTO @asdf;
INSERT INTO t2 VALUES (@asdf);

COMMIT;


-- Transaction B

SET AUTOCOMMIT = off;
BEGIN;

INSERT INTO t1 VALUES (6);
SELECT AVG(c) FROM t1 INTO @asdf;
INSERT INTO t2  VALUES (@asdf);

COMMIT;
