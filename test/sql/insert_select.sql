CREATE DATABASE test;
USE test;
CREATE TABLE t1 (a INT, b FLOAT, c VARCHAR(32));
INSERT INTO t1 VALUES (1, 2, 'a'), (3, 4.0, 'b');
SELECT * FROM t1;
SELECT * FROM t1 WHERE c = 'a';
UPDATE t1 SET c = 'c' WHERE a = 1;
SELECT * FROM t1;
DELETE FROM t1 WHERE c = 'b';
SELECT * FROM t1;
