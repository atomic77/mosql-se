DROP SCHEMA test1;
USE test1;

drop table if exists test_pk_complex;
create table test_pk_complex (
 i integer not null,
 t char(4) not null, 
 v varchar(50),
 d datetime,
 deca decimal(10,4), 
 primary key (i)) ENGINE=MoSQL;
 
 insert into test_pk_complex values (10, "asdf","adsfasfdasdf","2011-02-05 00:00:00", 10.4);
 insert into test_pk_complex values (20, "fdsa","fdssfd","2011-01-02 00:00:00", 3210.4);
 insert into test_pk_complex values (30, "fdsa","fdssfd","2011-01-02 00:00:00", 3210.4);
 insert into test_pk_complex values (40, "fdsa",NULL,"2011-01-02 00:00:00", 3210.4);
 insert into test_pk_complex values (50, "fdsa",NULL,"2011-01-02 00:00:00", 3210.4);
 insert into test_pk_complex values (60, "fdsa","aaf","2011-01-02 00:00:00", 3210.4);
 insert into test_pk_complex values (45, "fdsa",NULL,"2011-01-02 00:00:00", 3210.4);
 insert into test_pk_complex values (25, "fdsa","fss","2011-01-02 00:00:00", 3210.4);
 
 SELECT * FROM test_pk_complex;
 
