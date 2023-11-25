CREATE DATABASE table_test;
USE table_test;
CREATE TABLE table1(a INT, b FLOAT, c VARCHAR(10));
CREATE TABLE table2(a INT NOT NULL, b FLOAT NOT NULL, c VARCHAR(10) NOT NULL);
CREATE TABLE table3(a INT NOT NULL DEFAULT 233, b FLOAT NOT NULL DEFAULT 2.33, c VARCHAR(10) NOT NULL DEFAULT '233');
CREATE TABLE table4(a INT NOT NULL DEFAULT 2.33);
SHOW TABLES;
DESC table1;
DESC table2;
DESC table3;
DROP TABLE table1;
SHOW TABLES;
SELECT a, table2.b FROM table2, table3;
SELECT table2.a, table3.b FROM table2, table3;