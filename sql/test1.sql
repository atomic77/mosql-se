UNINSTALL PLUGIN tapioca;
install plugin tapioca SONAME 'libTapiocaMySQL.so';
DROP SCHEMA IF EXISTS test1;
CREATE SCHEMA test1;
USE test1;
CREATE TABLE test1 (i integer) ENGINE = tapioca;
