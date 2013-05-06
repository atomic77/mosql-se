DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table test_mpart (
 a integer not null,
 b integer not null,
 v varchar(50),
 primary key (a,b)) ENGINE=MoSQL;
 
insert into test_mpart values (1,1,'a');
insert into test_mpart values (1,2,'b');
insert into test_mpart values (1,3,'c');
insert into test_mpart values (2,1,'d');
insert into test_mpart values (2,2,'e');

DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table test_midx (
 a integer not null,
 b integer not null,
 c integer not null,
 v varchar(50),
 primary key (a,b),
 key idx1 (b,c)) ENGINE=MoSQL;
 
insert into test_midx values (1,1,2,'asdf');
insert into test_midx values (1,2,3,'asdf');
insert into test_midx values (2,1,5,'asdf');


DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table test_mpart2 (
 a integer not null,
 b integer not null,
 c char(4) not null,
 v varchar(50),
 primary key (a,b,c)) ENGINE=MoSQL;
 
insert into test_mpart2 values (1,1,'asdf', 'blah');
insert into test_mpart2 values (1,2,'asdf', 'blah');
insert into test_mpart2 values (1,3,'asdf', 'blah');
insert into test_mpart2 values (2,1,'asdf', 'blah');
insert into test_mpart2 values (2,2,'asdf', 'blah');

DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table test_mpart2 (
 a integer not null,
 b integer not null,
 c char(4) not null,
 v varchar(50),
 primary key (a,b,c)) ENGINE=MoSQL;
 
 create index `asdfa` on test_mpart2 (b,c);
 
DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table t (a int not null, 
b int not null, 
c int not null, 
d int not null, 
v text, 
primary key (a,b,c),
key (c,d)
) ENGINE=MoSQL;

insert into t values (1,2,2,1,'a');
insert into t values (1,2,4,1,'b');
insert into t values (1,3,3,1,'c');
insert into t values (2,2,2,2,'d');
insert into t values (65560,2,2,2,'d');
insert into t values (65560,65530,2,2,'d');
insert into t values (65560,65560,2,2,'d');

insert into t values (2,3,3,2,'e');
insert into t values (3,3,3,3,'f');
insert into t values (3,5,4,3,'g');
insert into t values (3,5,5,3,'h');
insert into t values (3,5,6,3,'i');
insert into t values (3,5,7,3,'j');
insert into t values (3,5,8,3,'k');
insert into t values (4,6,7,4,'l');
insert into t values (4,6,8,4,'m');
insert into t values (4,6,9,4,'n');
insert into t values (4,7,9,4,'o');
insert into t values (5,6,9,10,'p');
insert into t values (6,6,9,11,'q');

select * from t use index (b) where a = 4 and  b = 6 and c = 9 and d = 4;
select * from t where b = 6 and c = 9 ;
select * from t where a = 4 and b = 6 ;
select * from t;

create table v (a int not null, 
b int not null, 
c int not null, 
d int not null, 
e varchar(10) not null, 
v text,
primary key (a,b,c),
key (c,d),
key (e)
) ENGINE=MoSQL;

insert into v values (1,2,2,1,'a', 'a');
insert into v values (1,2,4,1,'a', 'b');
insert into v values (1,3,3,1,'b', 'c');
insert into v values (2,2,2,2,'d', 'd');
insert into v values (2,3,3,2,'d', 'e');
insert into v values (3,3,3,3,'e', 'f');
insert into v values (3,5,4,3,'e', 'g');
insert into v values (3,5,5,3,'f', 'h');
insert into v values (3,5,6,3,'g', 'i');
insert into v values (3,5,7,3,'g', 'j');
insert into v values (3,5,8,3,'h', 'k');
insert into v values (4,6,7,4,'h', 'l');
insert into v values (4,6,8,4,'k', 'm');
insert into v values (4,6,9,4,'k', 'n');
insert into v values (4,7,9,4,'o', 'o');
insert into v values (5,6,9,10,'o','p');
insert into v values (6,6,9,11,'p','q');

