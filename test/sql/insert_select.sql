CREATE DATABASE test;
USE test;
CREATE TABLE t1 (a INT, b INT);
SELECT * FROM t1;
INSERT INTO t1 VALUES (1, 2), (3, 4);
SELECT a, b FROM t1;
SELECT * FROM t1;