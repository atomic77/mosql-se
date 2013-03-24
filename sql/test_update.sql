

DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table t (a int, v text, primary key (a)) engine = tapioca;


insert into t values (1,'a');
insert into t values (2,'d');
insert into t values (3,'f');
insert into t values (4,'g');
insert into t values (5,'h');

select * from t;
update t set v = 'huyasdfa4awefasdfawefasdfasdfasfd' where a = 1;
select * from t;



create table district (a int, v text, primary key (a)) engine = tapioca;


insert into district values (1,'a');
insert into district values (2,'d');
insert into district values (3,'f');
insert into district values (4,'g');
insert into district values (5,'h');

select * from district;
update district set v = 'huyasdfa4awefasdfawefasdfasdfasfd' where a = 1;
update district set v = 'huyasdfa4awefasdfawefasdfasdfasfd' where a = 2;
select * from district;

