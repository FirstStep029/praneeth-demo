-- Databricks notebook source
-- MAGIC %md # Prep Work

-- COMMAND ----------

-- MAGIC %md ## Creating Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS DQLDB;

-- COMMAND ----------

-- MAGIC %md ## Create Tables

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dqldb.customer_master (
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  gender STRING,
  age INT,
  email_id STRING,
  country_code STRING,
  phone_no BIGINT,
  membership_id STRING,
  created_date TIMESTAMP,
  updated_data TIMESTAMP
) USING DELTA
PARTITIONED BY (country_code, membership_id)
LOCATION '/FileStore/tables/dql/customer_master'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dqldb.orders (
  order_id BIGINT,
  order_date TIMESTAMP,
  order_status STRING,
  customer_id STRING,
  item_id STRING,
  delivery_date DATE,
  source_loc_id STRING,
  destination_loc_id STRING,
  payment_status STRING,
  delivery_status STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP
) USING DELTA
PARTITIONED BY (order_date)
LOCATION '/FileStore/tables/dql/orders'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dqldb.loc_master (
  loc_id STRING,
  loc_name STRING,
  country_code STRING,
  country_name STRING,
  created_date TIMESTAMP,
  updated_date TIMESTAMP
) USING DELTA
PARTITIONED BY (country_code)
LOCATION '/FileStore/tables/dql/loc_master'

-- COMMAND ----------

-- MAGIC %md ## Data Preparation

-- COMMAND ----------

INSERT INTO dqldb.customer_master VALUES 
("2023/11/001", "Manikandan", "Jeyabal", "M", 30, "Manikandan.J@Gmail.com", "IN", "9688514333", "P", current_timestamp(), current_timestamp()),
("2023/11/002", "Iswarya", "Murali", "F", 25, "Iswarya.M@Gmail.com", "IN", "9791432482", "S", current_timestamp(), current_timestamp()),
("2023/11/003", "Yogesh", "Ramesh", "M", 32, "Yogesh.R@Gmail.com", "IN", "9885111442", "B", current_timestamp(), current_timestamp()),
("2023/11/004", "Praneeth", "Agam", "M", 21, "Praneeth.A@Gmail.com", "UK", "6712348926", "P", current_timestamp(), current_timestamp()),
("2023/11/005", "Naveen", "Kumar", "M", 40, "Naveen.K@Gmail.com", "UK", "9812763457", "P", current_timestamp(), current_timestamp()),
("2023/11/006", "Aleesha", "Khanna", "F", 29, "Aleesha.K@Gmail.com", "US", "783944628", "P", current_timestamp(), current_timestamp()),
("2023/11/007", "Adarsh", "Nandhan", "M", 19, "Adarsh.N@Gmail.com", "CN", "9876125437", "S", current_timestamp(), current_timestamp())

-- COMMAND ----------

INSERT INTO dqldb.orders VALUES
(202312020000000001, current_timestamp(), 'In-Transist', '2023/11/001', 'ITM-E-0001', date_add(current_date(), 10), 'L0001', 'L0005', 'Success', 'In-Transist', current_timestamp(), current_timestamp()),
(202312030000000001, date_add(current_timestamp(), 1), 'Ordered', '2023/11/001', 'ITM-E-0003', date_add(current_date(), 10), 'L0002', 'L0005', 'Success', 'Yet-To-Dispatch', date_add(current_timestamp(), 1), date_add(current_timestamp(), 1)),

(202312020000000002, current_timestamp(), 'In-Transist', '2023/11/002', 'ITM-H-0002', date_add(current_date(), 10), 'L0004', 'L0006', 'Success', 'In-Transist', current_timestamp(), current_timestamp()),

(202311300000000001, date_add(current_timestamp(), -3), 'Delivered', '2023/11/005', 'ITM-E-0005', current_date(), 'L0004', 'L0007', 'Success', 'Delivered', date_add(current_timestamp(), -3), date_add(current_timestamp(), -3)),
(202312010000000001, date_add(current_timestamp(), -1), 'Delivery', '2023/11/005', 'ITM-E-0001', current_date(), 'L0004', 'L0007', 'Success', 'Yet-To-Deliver', current_timestamp(), current_timestamp()),
(202312020000000002, current_timestamp(), 'Ordered', '2023/11/005', 'ITM-E-0002', date_add(current_date(), 10), 'L0004', 'L0007', 'Success', 'Yet-To-Dispatch', current_timestamp(), current_timestamp()),

(202312020000000003, current_timestamp(), 'Ordered', '2023/11/003', 'ITM-E-0001', date_add(current_date(), 10), 'L0001', 'L0003', 'Success', 'Yet-To-Dispatch', current_timestamp(), current_timestamp()),

(202312020000000004, current_timestamp(), 'Ordered', '2023/11/008', 'ITM-E-0001', date_add(current_date(), 10), 'L0001', 'L0005', 'Success', 'Yet-To-Dispatch', current_timestamp(), current_timestamp());

-- COMMAND ----------

INSERT INTO dqldb.loc_master VALUES 
('L0001', 'TamilNadu', 'IN', 'India', date_add(current_timestamp(), -30), date_add(current_timestamp(), -30)),
('L0002', 'Karnataka', 'IN', 'India', date_add(current_timestamp(), -30), date_add(current_timestamp(), -30)),
('L0003', 'Andhra Pradesh', 'IN', 'India', date_add(current_timestamp(), -30), date_add(current_timestamp(), -30)),
('L0004', 'London', 'UK', 'United Kingdom', date_add(current_timestamp(), -30), date_add(current_timestamp(), -30)),
('L0005', 'Kerala', 'IN', 'India', date_add(current_timestamp(), -30), date_add(current_timestamp(), -30)),
('L0006', 'Delhi', 'IN', 'India', date_add(current_timestamp(), -30), date_add(current_timestamp(), -30)),
('L0007', 'Paris', 'UK', 'United Kingdom', date_add(current_timestamp(), -30), date_add(current_timestamp(), -30))
;

-- COMMAND ----------

-- MAGIC %md # SQL STMTS

-- COMMAND ----------

-- MAGIC %md ## SELECT - SQL

-- COMMAND ----------

SELECT * FROM dqldb.customer_master;

-- COMMAND ----------

SELECT * FROM dqldb.orders;

-- COMMAND ----------

SELECT * FROM dqldb.loc_master LIMIT 2;

-- COMMAND ----------

-- MAGIC %md ## Filters

-- COMMAND ----------

SELECT * 
FROM dqldb.customer_master
WHERE country_code = 'IN';

-- COMMAND ----------

SELECT * 
FROM dqldb.customer_master
WHERE country_code = 'IN' OR country_code = 'US';

-- COMMAND ----------

SELECT * 
FROM dqldb.customer_master
WHERE country_code LIKE '%IN%' OR country_code LIKE '%US%';

-- COMMAND ----------

SELECT * 
FROM dqldb.customer_master
WHERE age BETWEEN 19 AND 30;

-- COMMAND ----------

SELECT * 
FROM dqldb.customer_master
WHERE age >= 19 AND age <= 30;

-- COMMAND ----------

SELECT order_status, count(*) AS NUM_REC FROM dqldb.orders GROUP BY order_status ORDER BY order_status;

-- COMMAND ----------

SELECT *
FROM dqldb.orders
WHERE order_status IN ('Delivery', 'Delivered', 'Ordered')

-- COMMAND ----------

SELECT * FROM dqldb.customer_master
WHERE CASE WHEN gender = 'M' THEN age > 30 ELSE 1 = 1 END

-- COMMAND ----------

SELECT 
  CASE WHEN gender = 'M' THEN 'MALE' ELSE 'FEMALE' END AS gender, *
  FROM dqldb.customer_master;

-- COMMAND ----------

WITH CTE_FILTERD AS (
SELECT 
  CASE WHEN age <= 20 THEN 'Teen' 
    WHEN age > 20 AND age <= 45 THEN 'Adult'
    WHEN age > 45 THEN 'Old Aged' END AS cus_split, *,
    concat_ws('-', age, gender, membership_id) AS concating
  FROM dqldb.customer_master)

SELECT * FROM CTE_FILTERD WHERE cus_split = 'Teen';

-- COMMAND ----------

-- MAGIC %md ## Group/Order/Sort

-- COMMAND ----------

SELECT * FROM dqldb.orders ORDER BY order_date DESC, delivery_date DESC;

-- COMMAND ----------

SELECT * FROM dqldb.orders SORT BY 2;

-- COMMAND ----------

SELECT order_date, COUNT(*) AS TOTAL_ORDERS FROM dqldb.orders GROUP BY order_date;

-- COMMAND ----------

SELECT order_date, COUNT(*) AS TOTAL_ORDERS FROM dqldb.orders GROUP BY order_date HAVING order_date <= current_timestamp() ORDER BY order_date;

-- COMMAND ----------

SELECT order_date, COUNT(*) AS TOTAL_ORDERS FROM dqldb.orders GROUP BY order_date HAVING order_date >= current_timestamp();

-- COMMAND ----------

SELECT GENDER, COUNT(*) AS TOTAL_CUS FROM dqldb.customer_master GROUP BY GENDER HAVING TOTAL_CUS >= 5;

-- COMMAND ----------

-- MAGIC %md ## Joins

-- COMMAND ----------

SELECT DISTINCT customer_id FROM dqldb.orders;

-- COMMAND ----------

WITH CTE_TABLE_A AS (
  SELECT 'EMP01' AS EMPLOYEE_ID, 'MJ029' AS EMP_NAME
  UNION ALL
  SELECT 'EMP01' AS EMPLOYEE_ID, 'MJ0029' AS EMP_NAME
  UNION ALL
  SELECT 'EMP02' AS EMPLOYEE_ID, 'PA029' AS EMP_NAME
  UNION ALL 
  SELECT 'EMP03' AS EMPLOYEE_ID, 'YR029' AS EMP_NAME
  UNION ALL
  SELECT NULL AS EMPLOYEE_ID, NULL AS EMP_NAME
),

CTE_TABLE_B AS (
  SELECT 'EMP01' AS EMPLOYEE_ID, 'MJ029' AS EMP_NAME
  UNION ALL
  SELECT 'EMP02' AS EMPLOYEE_ID, 'PA029' AS EMP_NAME
  UNION ALL 
  SELECT 'EMP03' AS EMPLOYEE_ID, 'YR029' AS EMP_NAME
  UNION ALL
  SELECT NULL AS EMPLOYEE_ID, NULL AS EMP_NAME
)

SELECT  DISTINCT A.* 
FROM CTE_TABLE_A AS A
FULL OUTER JOIN CTE_TABLE_B AS B
  ON A.EMPLOYEE_ID = B.EMPLOYEE_ID

-- COMMAND ----------

WITH CTE_GENDER_COUNT AS (
SELECT GENDER AS COL1, COUNT(*) AS COL2 FROM dqldb.customer_master GROUP BY GENDER),

CTE_COUNTTRY_COUNT AS (
SELECT country_code AS COL1, COUNT(*) AS COL2 FROM dqldb.customer_master GROUP BY country_code)

SELECT * FROM CTE_GENDER_COUNT
UNION
SELECT * FROM CTE_COUNTTRY_COUNT

-- COMMAND ----------

SELECT *,
  COUNT(GENDER) OVER(PARTITION BY GENDER) AS GENDER_COUNT,
  COUNT(country_code) OVER(PARTITION BY country_code) AS COUNTRY_COUNT
 FROM dqldb.customer_master;

-- COMMAND ----------

SELECT LAG(order_id) OVER (ORDER BY order_id) AS LAG_ORDER, LEAD(order_ID) OVER (ORDER BY order_id) AS LEAD_ORDER, * FROM dqldb.orders ORDER BY order_date;

-- COMMAND ----------

SELECT * FROM dqldb.customer_master;

-- COMMAND ----------

WITH CTE_ARRAY AS (SELECT collect_set(c.first_name) AS ARR, c.gender FROM dqldb.customer_master as c
LEFT ANTI JOIN DQLDB.orders as o
ON c.customer_id = o.customer_id
GROUP BY c.gender)

SELECT posexplode(ARR) FROM CTE_ARRAY;

-- COMMAND ----------

SELECT * FROM dqldb.customer_master PIVOT (COUNT(customer_id) FOR country_code IN ('IN' AS India));

-- COMMAND ----------

SELECT * FROM VALUES 
(101,'A',1000.01),
(101,'B',2000),
(101,'C',5000),
(102,'A',2000.01),
(102,'B',4000.1),
(103,'A',2000.01),
(103,'B',4000.1),
(101,'A',3000.01)
AS Sales(Employee,Product,Amount)
PIVOT (
SUM(Amount) AS amt, COUNT(Amount) AS cnt
FOR Product IN ( 'A' AS a, 'B' as b, 'C' AS c)
)
;
