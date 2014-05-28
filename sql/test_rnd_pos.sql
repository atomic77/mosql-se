
DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table t (k char(12), v text, PRIMARY KEY (k)) ENGINE=MoSQL;

insert into t values ("aaaaaaaaaaaa", 'asdfasdfasdf');
insert into t values ("bbbbbbbbbbbb", 'asdfasdfasdf');
insert into t values ("cccccccccccc", 'asdfasdfasdf');
insert into t values ("dddddddddddd", 'asdfasdfasdf');
insert into t values ("eeeeeeeeeeee", 'asdfasdfasdf');

SELECT "Basic table scan";
SELECT * FROM t ;
-- SELECT "Reverse order";
-- SELECT * FROM t ORDER BY k DESC ;

create table v (k varchar(12), v text, PRIMARY KEY (k)) ENGINE=MoSQL;

insert into v values ("aaaaaaaaaaaa", 'asdfasdfasdf');
insert into v values ("bbbbbbbbbbbb", 'asdfasdfasdf');
insert into v values ("cccccccccccc", 'asdfasdfasdf');
insert into v values ("dddddddddddd", 'asdfasdfasdf');
insert into v values ("eeeeeeeeeeee", 'asdfasdfasdf');

SELECT "Basic table scan";
SELECT * FROM v ;
