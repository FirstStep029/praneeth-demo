-- Databricks notebook source
SHOW PARTITIONS Employees;

-- COMMAND ----------

ANALYZE TABLE Employees COMPUTE STATISTICS;

-- COMMAND ----------

DESC EXTENDED Employees;

-- COMMAND ----------

CACHE TABLE c_emp OPTIONS ('storageLevel' 'MEMORY_ONLY') SELECT * FROM employees;

-- COMMAND ----------

UNCACHE TABLE IF EXISTS c_emp;

-- COMMAND ----------

CLEAR CACHE;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED praneeth_demo;

-- COMMAND ----------

SHOW COLUMNS IN Employees;

-- COMMAND ----------

SHOW TABLES IN praneeth_demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

SHOW CREATE TABLE employees;
