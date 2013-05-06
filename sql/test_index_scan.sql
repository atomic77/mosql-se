
DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table t (a int, b int, c int, v text, primary key (a,b,c)) ENGINE=MoSQL;

insert into t values (1,2,2,'a');
insert into t values (1,2,4,'b');
insert into t values (1,3,3,'c');
insert into t values (2,2,2,'d');
insert into t values (2,3,3,'e');
insert into t values (3,3,3,'f');
insert into t values (3,5,4,'g');
insert into t values (3,5,5,'h');
insert into t values (3,5,6,'i');
insert into t values (3,5,7,'j');
insert into t values (3,5,8,'k');
insert into t values (4,6,7,'l');
insert into t values (4,6,8,'m');
insert into t values (4,6,9,'n');
insert into t values (4,7,9,'o');

SELECT * FROM t;
select * from t where a = 1 and b = 4;

-- the key query (pardon the pun...)	
select * from t where a = 1;

DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;


create table v (a char(4), b char(4), v text, primary key (a,b)) ENGINE=MoSQL;

insert into v values ('aaaa','yyyy','a');
insert into v values ('aaaa','yyzz','a');
insert into v values ('aaab','tttt','a');
insert into v values ('aaab','ttzz','a');
insert into v values ('aaac','tttt','a');



create table t (a int, b int, c int, v text, primary key (a,b,c)) ENGINE=MoSQL;

insert into t values (2,3,3,'e');
insert into t values (3,3,3,'f');
insert into t values (3,5,4,'g');
insert into t values (1,2,2,'a');
insert into t values (1,2,4,'b');
insert into t values (3,5,6,'i');
insert into t values (3,5,7,'j');
insert into t values (3,5,5,'h');
insert into t values (4,6,7,'l');
insert into t values (3,5,8,'k');
insert into t values (1,3,3,'c');
insert into t values (2,2,2,'d');
