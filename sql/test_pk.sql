DROP SCHEMA IF EXISTS test1;
CREATE SCHEMA test1;
USE test1;

drop table if exists test_pk;
create table test_pk (i char(4) not null, t char(4) not null, 
	v char(4) not null, primary key (i)) engine = MoSQL;
insert into test_pk values ("aaaa", "bbbb", "cccc");
insert into test_pk values ("dddd", "bbbb", "cccc");

select * from test_pk where i = "aaaa";
select * from test_pk where i = "bbbb";

drop table if exists test_pk_int;
create table test_pk_int (i integer not null, t char(4) not null, 
	v char(4) not null, primary key (i)) engine = MoSQL;
insert into test_pk_int values (10, "bbbb", "cccc");
insert into test_pk_int values (20, "bbbb", "cccc");
insert into test_pk_int values (30, "bbbb", "cccc");

select * from test_pk_int where i = 20;


DROP SCHEMA IF EXISTS test1;
CREATE SCHEMA test1;
USE test1;

drop table if exists test_pk_complex;
create table test_pk_complex (
 i integer not null,
 t char(4) not null, 
 v varchar(50),
 d datetime,
 deca decimal(10,4), 
 primary key (i)) engine = MoSQL;
 
 insert into test_pk_complex values (10, "asdf","adsfasfdasdf","2011-02-05 00:00:00", 10.4);
 insert into test_pk_complex values (20, "fdsa","fdssfd","2011-01-02 00:00:00", 3210.4);
 insert into test_pk_complex values (40, "fdsa",NULL,"2011-01-02 00:00:00", 3210.4);

  
 select * from test_pk_int JOIN test_pk_complex c USING (i) where c.i = 10;
  
