
DROP SCHEMA IF EXISTS test1;
CREATE SCHEMA test1;
USE test1;

create table c (i int, t varchar(3000), 
primary key (i)) ENGINE=MoSQL;


SET AUTOCOMMIT=0;
BEGIN;
INSERT INTO c VALUES (1,repeat("a",3000));
INSERT INTO c VALUES (2,repeat("a",3000));
INSERT INTO c VALUES (3,repeat("a",3000));
INSERT INTO c VALUES (4,repeat("a",3000));
INSERT INTO c VALUES (5,repeat("a",3000));
INSERT INTO c VALUES (6,repeat("a",3000));
INSERT INTO c VALUES (7,repeat("a",3000));
INSERT INTO c VALUES (8,repeat("a",3000));
INSERT INTO c VALUES (9,repeat("a",3000));
INSERT INTO c VALUES (10,repeat("a",3000));
INSERT INTO c VALUES (11,repeat("a",3000));
INSERT INTO c VALUES (12,repeat("a",3000));
INSERT INTO c VALUES (13,repeat("a",3000));
INSERT INTO c VALUES (14,repeat("a",3000));
INSERT INTO c VALUES (15,repeat("a",3000));
INSERT INTO c VALUES (16,repeat("a",3000));
INSERT INTO c VALUES (17,repeat("a",3000));
INSERT INTO c VALUES (18,repeat("a",3000));
INSERT INTO c VALUES (19,repeat("a",3000));
INSERT INTO c VALUES (20,repeat("a",3000));
INSERT INTO c VALUES (21,repeat("a",3000));
INSERT INTO c VALUES (22,repeat("a",3000));
INSERT INTO c VALUES (23,repeat("a",3000));
INSERT INTO c VALUES (24,repeat("a",3000));
INSERT INTO c VALUES (25,repeat("a",3000));
INSERT INTO c VALUES (26,repeat("a",3000));
INSERT INTO c VALUES (27,repeat("a",3000));
COMMIT;

BEGIN;
SELECT i, length(t) from c;
COMMIT;

