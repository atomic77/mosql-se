
DROP SCHEMA testk;
CREATE SCHEMA testk;
USE testk;

DROP TABLE IF EXISTS kettleout ;
CREATE TABLE kettleout
(
  v VARCHAR(255)
, numb DOUBLE
, k INT
, k_10 INT
, PRIMARY KEY(k_10,k)
) ENGINE= TAPIOCA
;

