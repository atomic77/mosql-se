
-- client 1

DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;

create table t (i int, v text, primary key (i)) ENGINE=MoSQL;
set autocommit = off;

begin;

insert into t values (1, 'asdf');






commit;

select * from t;


-- client 2

use test1;

set autocommit = off;

begin;

insert into t values (1, 'fdsa');

commit;

select * from t;
