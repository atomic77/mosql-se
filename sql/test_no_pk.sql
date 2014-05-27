

DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table t (a int, v text) ENGINE=MoSQL;


insert into t values (1,'a');
insert into t values (2,'d');
insert into t values (3,'f');
insert into t values (4,'g');
insert into t values (5,'h');

select * from t;
update t set v = 'huyasdfa4awefasdfawefasdfasdfasfd' where a = 1;
select * from t;

create table v (a int, b int, v text, KEY (b) ) ENGINE=MoSQL;


insert into t values (1,'a');
insert into t values (2,'d');
insert into t values (3,'f');
insert into t values (4,'g');
insert into t values (5,'h');

select * from t;
update t set v = 'huyasdfa4awefasdfawefasdfasdfasfd' where a = 1;
select * from t;

