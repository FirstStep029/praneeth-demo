-- Databricks notebook source
-- MAGIC %md # Insert Statement

-- COMMAND ----------

INSERT INTO [ TABLE ] table_identifier [ partition_spec ] [ ( column_list ) ] { VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ] | query }

-- COMMAND ----------

-- MAGIC %md ## Create Table

-- COMMAND ----------

-- MAGIC %fs rm -r /FileStore/tables/employees

-- COMMAND ----------

DROP TABLE IF EXISTS Employees;
CREATE EXTERNAL TABLE IF NOT EXISTS Employees (
  EmployeeId STRING,
  EmployeeName STRING,
  EmployeeGender STRING,
  EmployeeEMail STRING,
  EmployeePhone STRING,
  EmployeeAge INT,
  EmployeeManager STRING,
  EmployeeState STRING,
  EmployeeCountry STRING,
  IsActive INT
) USING DELTA 
PARTITIONED BY (EmployeeCountry, EmployeeState)
LOCATION '/FileStore/tables/employees'

-- COMMAND ----------

DROP TABLE IF EXISTS Employees_tgt;
CREATE EXTERNAL TABLE IF NOT EXISTS Employees_tgt (
  EmployeeId STRING,
  EmployeeName STRING,
  EmployeeGender STRING,
  EmployeeEMail STRING,
  EmployeePhone STRING,
  EmployeeAge INT,
  EmployeeManager STRING,
  EmployeeState STRING,
  EmployeeCountry STRING,
  IsActive INT
) USING DELTA 
PARTITIONED BY (EmployeeCountry, EmployeeState)
LOCATION '/FileStore/tables/employees_tgt'

-- COMMAND ----------

DROP TABLE IF EXISTS Employees_copy;
CREATE EXTERNAL TABLE IF NOT EXISTS Employees_copy (
  EmployeeId STRING,
  EmployeeName STRING,
  EmployeeGender STRING,
  EmployeeEMail STRING,
  EmployeePhone STRING,
  EmployeeAge INT,
  EmployeeManager STRING,
  EmployeeState STRING,
  EmployeeCountry STRING,
  IsActive INT
) USING DELTA 
PARTITIONED BY (EmployeeCountry, EmployeeState)
LOCATION '/FileStore/tables/employees_copy'

-- COMMAND ----------

DROP TABLE IF EXISTS Employees_tbl;
CREATE EXTERNAL TABLE IF NOT EXISTS Employees_tbl (
  EmployeeId STRING,
  EmployeeName STRING,
  EmployeeGender STRING,
  EmployeeEMail STRING,
  EmployeePhone STRING,
  EmployeeAge INT,
  EmployeeManager STRING,
  EmployeeState STRING,
  EmployeeCountry STRING,
  IsActive INT
) USING DELTA 
PARTITIONED BY (EmployeeCountry, EmployeeState)
LOCATION '/FileStore/tables/employees_tbl'

-- COMMAND ----------

DROP TABLE IF EXISTS Employees_from;
CREATE EXTERNAL TABLE IF NOT EXISTS Employees_from (
  EmployeeId STRING,
  EmployeeName STRING,
  EmployeeGender STRING,
  EmployeeEMail STRING,
  EmployeePhone STRING,
  EmployeeAge INT,
  EmployeeManager STRING,
  EmployeeState STRING,
  EmployeeCountry STRING,
  IsActive INT
) USING DELTA 
PARTITIONED BY (EmployeeCountry, EmployeeState)
LOCATION '/FileStore/tables/employees_from'

-- COMMAND ----------

DROP TABLE IF EXISTS Employees_ow;
CREATE EXTERNAL TABLE IF NOT EXISTS Employees_ow (
  EmployeeId STRING,
  EmployeeName STRING,
  EmployeeGender STRING,
  EmployeeEMail STRING,
  EmployeePhone STRING,
  EmployeeAge INT,
  EmployeeManager STRING,
  EmployeeState STRING,
  EmployeeCountry STRING,
  IsActive INT
) USING DELTA 
PARTITIONED BY (EmployeeCountry, EmployeeState)
LOCATION '/FileStore/tables/employees_ow'

-- COMMAND ----------

-- MAGIC %md ## Insert Single Row

-- COMMAND ----------

SELECT * FROM Employees;

-- COMMAND ----------

INSERT INTO Employees VALUES ('EMP001', 'Manikandan Jeyabal', 'M', 'Manikandan.J@Gmail.com', '9876543210', 31, 'Paul', 'TN', 'IN', 1);
SELECT * FROM Employees;

-- COMMAND ----------

-- MAGIC %md ## Insert Multipile Rows

-- COMMAND ----------

INSERT INTO Employees VALUES 
('EMP002', 'Rahul', 'M', 'Rahul.R@Gmail.com', '9692758392', 25, 'Paul', 'TN', 'IN', 1),
('EMP003', 'Praneeth', 'M', 'Praneeth.A@Gmail.com', '9876123450', 25, 'Paul', 'TN', 'IN', 1),
('EMP004', 'Yogesh R', 'M', 'Yogesh.R@Gmail.com', '9192939495', 29, 'Paul', 'TN', 'IN', 1);
SELECT * FROM Employees ORDER BY employeeID;

-- COMMAND ----------

-- DBTITLE 1,TODO: Need to Update on this
INSERT INTO Employees (EmployeeID, EmployeeName, EmployeeGender, EmployeeManager, EmployeeCountry) VALUES ('EMP005', 'Newbee', 'F', 'Yogesh R', 'US')

-- COMMAND ----------

-- MAGIC %md ## Insert using Select Stmt

-- COMMAND ----------

TRUNCATE TABLE Employees_Copy;
INSERT INTO Employees_Copy SELECT * FROM Employees; -- SELECT * FROM Employees;
SELECT * FROM Employees_Copy;

-- COMMAND ----------

-- MAGIC %md ## Inset using Table

-- COMMAND ----------

TRUNCATE TABLE Employees_tbl;
INSERT INTO Employees_tbl TABLE Employees;
SELECT * FROM Employees_tbl;

-- COMMAND ----------

-- MAGIC %md ## Insert Using FROM

-- COMMAND ----------

TRUNCATE TABLE Employees_from;
INSERT INTO Employees_from FROM Employees SELECT *;
SELECT * FROM Employees_from 

-- COMMAND ----------

-- MAGIC %md ## Insert Overwrite

-- COMMAND ----------

INSERT INTO Employees VALUES ('EMP001', 'Manikandan Jeyabal', 'M', 'Manikandan.J@Gmail.com', '9876543210', 31, 'Paul', 'TN', 'IN', 1);
INSERT OVERWRITE Employees PARTITION(EmployeeCountry='US') VALUES 
('EMP002', 'Rahul', 'M', 'Rahul.R@Gmail.com', '9692758392', 25, 'Paul', 'TN', 'IN', 1),
('EMP003', 'Praneeth', 'M', 'Praneeth.A@Gmail.com', '9876123450', 25, 'Paul', 'TN', 'IN', 1),
('EMP004', 'Yogesh R', 'M', 'Yogesh.R@Gmail.com', '9192939495', 29, 'Paul', 'TN', 'IN', 1);
SELECT * FROM Employees ORDER BY employeeID;

-- COMMAND ----------

-- MAGIC %md # Truncate Table

-- COMMAND ----------

TRUNCATE TABLE table_identifier [ partition_spec ]

-- COMMAND ----------

TRUNCATE TABLE Employees PARTITION (EmployeeCountry='IN');
SELECT * FROM Employees;

-- COMMAND ----------

DELETE FROM Employees WHERE EmployeeCountry='IN';
SELECT * FROM Employees;

-- COMMAND ----------

TRUNCATE TABLE Employees;
SELECT * FROM Employees;

-- COMMAND ----------

-- MAGIC %md # Delete from Table
-- MAGIC
-- MAGIC #### Limitations:
-- MAGIC The WHERE predicate supports subqueries, including IN, NOT IN, EXISTS, NOT EXISTS, and scalar subqueries. The following types of subqueries are not supported:
-- MAGIC
-- MAGIC Nested subqueries, that is, an subquery inside another subquery
-- MAGIC
-- MAGIC NOT IN subquery inside an OR, for example, a = 3 OR b NOT IN (SELECT c from t)
-- MAGIC
-- MAGIC In most cases, you can rewrite NOT IN subqueries using NOT EXISTS. We recommend using NOT EXISTS whenever possible, as DELETE with NOT IN subqueries can be slow.

-- COMMAND ----------

DELETE FROM table_name [table_alias] [WHERE predicate]

-- COMMAND ----------

DELETE FROM Employees_copy WHERE EmployeeId = 'EMP004';
SELECT * FROM Employees_copy;

-- COMMAND ----------

-- MAGIC %md # Update Table

-- COMMAND ----------

SELECT * FROM Employees_copy;

-- COMMAND ----------

UPDATE Employees_copy
SET EmployeeCountry = 'IN', IsActive = 0
WHERE EmployeeCountry = 'IND';
SELECT * FROM employees_copy;

-- COMMAND ----------

-- MAGIC %md # Merge Table

-- COMMAND ----------

SELECT * FROM Employees;

-- COMMAND ----------

SELECT * FROM Employees_tgt;

-- COMMAND ----------

MERGE INTO employees_tgt AS tgt
USING employees AS src
ON tgt.EmployeeId = src.EmployeeId
-- UPDATING OLD RECORDS
WHEN MATCHED THEN UPDATE SET *
-- INESERT NEW RECORDS 
WHEN NOT MATCHED THEN INSERT *
