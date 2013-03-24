

DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;



CREATE TABLE s
(i int, t char(16), 
primary key (i,t)) engine = tapioca;

insert into s values (254, 'ABCD');
insert into s values (254*254, 'ABCDE');


INSERT INTO s VALUES (1,'put something');
INSERT INTO s VALUES (2,'put something');
INSERT INTO s VALUES (2,'rut something');
INSERT INTO s VALUES (2,'sut tpmething');
INSERT INTO s VALUES (2,'tut tpmething');
INSERT INTO s VALUES (2,'vut tpmething');
INSERT INTO s VALUES (2,'sut tomething');
INSERT INTO s VALUES (2,'sut tonething');


DROP TABLE IF EXISTS t;

CREATE TABLE t 
(t char(8), i int, u char(6), j int, primary key (t,u)) 
engine = tapioca;


INSERT INTO t VALUES ('aaaaaaaa', 123,  'AAAA', 432);
INSERT INTO t VALUES ('aaaaaaaa', 123,  'ABAA', 432);
INSERT INTO t VALUES ('baaaaaaa', 123,  'AAAA', 432);
INSERT INTO t VALUES ('acaaaaaa', 123,  'AAAA', 432);
INSERT INTO t VALUES ('caaaaaaa', 123,  'AAAA', 432);
INSERT INTO t VALUES ('daaaaaaa', 123,  'AAAA', 432);
INSERT INTO t VALUES ('aeaaaaaa', 123,  'AAAA', 432);
INSERT INTO t VALUES ('adaaaaaa', 123,  'AAAA', 432);

select * from t;
select * from t where t = 'aaaaaaaa' and u = 'AAAA';
select * from t where t = 'aaaaaaaa' ;


DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;
DROP TABLE IF EXISTS u;

CREATE TABLE u (
c1 char(17), 
i INT NOT NULL, 
c2 CHAR(11), 
j INT NOT NULL, 
k INT NOT NULL,
PRIMARY KEY (c1,c2),
KEY `idx_1` (j,k,c2)) ENGINE = TAPIOCA;

INSERT INTO u VALUES ('AAAABBBBCCCCDDDDE', 123,  'ffffgggghhh', 432, 976);
INSERT INTO u VALUES ('AAAABBBBCCCCDDDEE', 123,  'ffffgggghhh', 412, 76);
INSERT INTO u VALUES ('AAAABBBBCCCCDDEEE', 123,  'ffffggghhhh', 442, 96);
INSERT INTO u VALUES ('AAAABBBBDDCCDDDDE', 1257, 'ffffggghhhh', 442, 946);
INSERT INTO u VALUES ('AAAABBBBDCCCDDDDE', 1256, 'ffffggghhhh', 432, 196);

INSERT INTO u VALUES ('AAAABBBBCCCDDDDDE', 1257, 'ffffggghhhh', 452, 96);
INSERT INTO u VALUES ('AAAABBBBCCCDDDDDE', 1258, 'ffffhgghhhh', 452, 96);
INSERT INTO u VALUES ('AAAABBBBCCCDDDDDE', 1259, 'ffffhgghihh', 472, 96);
INSERT INTO u VALUES ('AAAABBBBCCCDDDDDE', 1267, 'ffffhgghjhh', 422, 96);
INSERT INTO u VALUES ('AAAABBBBCCCDDDDDE', 1268, 'ffffhgghkhh', 401, 96);

INSERT INTO u VALUES ('AAAABBBBCCDDDDDDE', 1269, 'ffffggghhhh', 452, 96);
INSERT INTO u VALUES ('AAAABBBBCCDDDDDDE', 1269, 'ffffggghhhh', 452, 96);

INSERT INTO u VALUES ('AAAABBBBCDDDDDDDE', 1278, 'ffgffgghhhh', 452, 96);
INSERT INTO u VALUES ('AAAABBBBCDDDDDDDE', 1270, 'ffggffghhhh', 452, 96);


DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;
DROP TABLE IF EXISTS u;


CREATE TABLE v (
i INT NOT NULL,
j INT NOT NULL,
PRIMARY KEY (i),
KEY `implicit_pk` (j)
) ENGINE = TAPIOCA;

-- Generate script: 
-- rm /tmp/ins.sql; for i in `seq 1 1000`; 
--    do echo "INSERT INTO v VALUES ($i, $(( i / 2)) ); " >> /tmp/ins.sql; done
-- Failing testcase:
INSERT INTO v VALUES (1, 0 ); 
INSERT INTO v VALUES (2, 1 ); 
INSERT INTO v VALUES (3, 1 ); 
INSERT INTO v VALUES (4, 2 ); 
INSERT INTO v VALUES (5, 2 ); 
INSERT INTO v VALUES (6, 3 ); 
INSERT INTO v VALUES (7, 3 ); 
INSERT INTO v VALUES (8, 4 ); 
INSERT INTO v VALUES (9, 4 ); 
INSERT INTO v VALUES (10, 5 ); 


DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;
DROP TABLE IF EXISTS u;

CREATE TABLE v (
i CHAR(3) NOT NULL,
j CHAR(3) NOT NULL,
PRIMARY KEY (i),
KEY `implicit_pk` (j)
) ENGINE = TAPIOCA;

INSERT INTO v VALUES ('aaa', 'aaa' ); 
INSERT INTO v VALUES ('aab', 'aaa' ); 
INSERT INTO v VALUES ('aac', 'aac' ); 
INSERT INTO v VALUES ('aad', 'aac' ); 
INSERT INTO v VALUES ('aae', 'aae' ); 
INSERT INTO v VALUES ('aaf', 'aae' ); 
INSERT INTO v VALUES ('aag', 'aag' ); 
INSERT INTO v VALUES ('aah', 'aag' ); 
INSERT INTO v VALUES ('aai', 'aai' ); 
INSERT INTO v VALUES ('aaj', 'aao' ); 
INSERT INTO v VALUES ('aak', 'aao' ); 
INSERT INTO v VALUES ('aal', 'aao' ); 
INSERT INTO v VALUES ('aam', 'aao' ); 
INSERT INTO v VALUES ('aan', 'aao' ); 
INSERT INTO v VALUES ('aao', 'aao' ); 
INSERT INTO v VALUES ('aap', 'aao' ); 
INSERT INTO v VALUES ('aaq', 'aao' ); 
INSERT INTO v VALUES ('aar', 'aao' ); 
INSERT INTO v VALUES ('aas', 'aao' ); 
INSERT INTO v VALUES ('aat', 'aao' ); 
