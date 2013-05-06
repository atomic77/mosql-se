DROP SCHEMA IF EXISTS test1;
CREATE SCHEMA test1;
USE test1;

create table test1 (i integer , primary key (i)) ENGINE=MoSQL;
create table test2 (i integer , primary key (i)) ENGINE=MoSQL;

-- create table test3 (i integer , primary key (i)) ENGINE=MoSQL;

insert into test1 values (1);
insert into test2 values (1);
-- insert into tests3 values (1);
insert into test2 values (2);
insert into test2 values (3);
insert into test2 values (4);
insert into test2 values (5);
insert into test2 values (6);
insert into test2 values (7);
insert into test2 values (8);
insert into test2 values (9);
insert into test2 values (10);
insert into test2 values (11);
insert into test2 values (12);
