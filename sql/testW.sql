USE test1;
drop table if exists test1;
create table test1 (k char(4) not null, t char(4) not null, v char(4) not null) ENGINE=MoSQL;
insert into test1 values ("fooo", "barr", "yayy");
select * from test1;

