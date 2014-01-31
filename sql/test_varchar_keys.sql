

DROP SCHEMA test1;
CREATE SCHEMA test1;
USE test1;


CREATE TABLE v
(c VARCHAR(32) NOT NULL, 
i integer, 
primary key (c)) 
ENGINE=MoSQL;

INSERT INTO v VALUES ('AAAABBBB',123);
INSERT INTO v VALUES ('CCCCBBBB',123);
INSERT INTO v VALUES ('EEEEBBBB',123);
INSERT INTO v VALUES ('FFFFBBBB',123);
INSERT INTO v VALUES ('BBBBBBBB',123);



CREATE TABLE w
(v VARCHAR(32) NOT NULL,
w VARCHAR(64) NOT NULL,
 i integer, 
primary key (v,w)) 
ENGINE=MoSQL;

INSERT INTO w VALUES ('AAAABBBB','aaaaaaasaaa',123);
INSERT INTO w VALUES ('EEEEBBBB','aaaaaaaaaaa',123);
INSERT INTO w VALUES ('ASDFBBBB','aaaaaadddaa',123);
INSERT INTO w VALUES ('AAAAABCB','aaaasdfasdf',123);
INSERT INTO w VALUES ('AABBBB','aaaaaaaaasdfa',123);



CREATE TABLE t5
(v VARCHAR(32) NOT NULL,
w VARCHAR(64) NOT NULL,,
primary key (v),
key `asdf` (w)) 
ENGINE=MoSQL;

INSERT INTO t5 VALUES ('AAAABBBB','aaaaaaasaaa',123);
INSERT INTO t5 VALUES ('EEEEBBBB','aaaaaaaaaaa',123);
INSERT INTO t5 VALUES ('ASDFBBBB','aaaaaadddaa',123);
INSERT INTO t5 VALUES ('AAA','aaaaaaasdfasdf',123);
INSERT INTO t5 VALUES ('AABBBB','aaaaaaaaasdfa',123);




DROP TABLE IF EXISTS `oc_appconfig`;

CREATE TABLE `x` (
  `a` varchar(32) NOT NULL DEFAULT ' ',
  `b` varchar(64) NOT NULL DEFAULT ' ',
  `c` text,
  -- KEY `appconfig_appid_key_index` (`appid`,`configkey`),
  PRIMARY KEY (`a`,`b`),
  KEY `idx` (`b`)
) ENGINE=MoSQL DEFAULT CHARSET=latin1;

insert into x values ('123','aaa','asdfasdf');
insert into x values ('423','aaa','asdfasdf');
insert into x values ('1423','bbb','asdfasdf');

