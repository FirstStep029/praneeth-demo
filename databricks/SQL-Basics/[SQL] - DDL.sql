-- Databricks notebook source
-- MAGIC %md # Create Table Statement

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS praneeth_demo.electric_vehicle_data_m_csv (
  vin_number STRING,
  Country STRING,
  City STRING,
  State STRING,
  Postal_Code INT,
  Model_Year INT,
  Make STRING,
  Model STRING,
  Electric_Vehichle_Type STRING,
  Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility STRING,
  Electric_Range INT,
  Base_MSRP INT,
  Legislative_District INT,
  DOL_Vehicle_ID BIGINT,
  Vehicle_Location STRING,
  Electric_Utility STRING,
  2020_Census_Tract BIGINT
) USING CSV

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS praneeth_demo.electric_vehicle_data_m_delta_as 
AS SELECT * FROM praneeth_demo.electric_vehicle_data_e_csv;

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS praneeth_demo.electric_vehicle_data_e_csv (
  vin_number STRING,
  Country STRING,
  City STRING,
  State STRING,
  Postal_Code INT,
  Model_Year INT,
  Make STRING,
  Model STRING,
  Electric_Vehichle_Type STRING,
  Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility STRING,
  Electric_Range INT,
  Base_MSRP INT,
  Legislative_District INT,
  DOL_Vehicle_ID BIGINT,
  Vehicle_Location STRING,
  Electric_Utility STRING,
  2020_Census_Tract BIGINT
) USING CSV
LOCATION '/FileStore/tables/electric_vehicle_data_e_csv'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS praneeth_demo.electric_vehicle_data_e_csv_like LIKE praneeth_demo.electric_vehicle_data_e_csv USING CSV LOCATION '/FileStore/tables/electric_vehicle_data_e_csv_like';

-- COMMAND ----------

-- MAGIC %md # Alter Table Statement

-- COMMAND ----------

ALTER TABLE praneeth_demo.electric_vehicle_data_m_delta_as SET TBLPROPERTIES ("delta.columnMapping.mode" = "name");
ALTER TABLE praneeth_demo.electric_vehicle_data_m_delta_as SET TBLPROPERTIES ("delta.minReaderVersion" = 2);
ALTER TABLE praneeth_demo.electric_vehicle_data_m_delta_as SET TBLPROPERTIES ("delta.minWriterVersion" = 5);
ALTER TABLE praneeth_demo.electric_vehicle_data_m_delta_as RENAME COLUMN Country TO CountryName;
SELECT * FROM praneeth_demo.electric_vehicle_data_m_delta_as

-- COMMAND ----------

ALTER TABLE praneeth_demo.electric_vehicle_data_m_delta_as ADD COLUMNS (col1 STRING);
SELECT * FROM praneeth_demo.electric_vehicle_data_m_delta_as

-- COMMAND ----------

SELECT COUNT(*) FROM praneeth_demo.electric_vehicle_data_m_delta_as@V8;

-- COMMAND ----------

DELETE FROM praneeth_demo.electric_vehicle_data_m_delta_as WHERE COUNTRYNAME = 'King'

-- COMMAND ----------

SELECT DISTINCT COUNTRYNAME, COUNT(*) FROM praneeth_demo.electric_vehicle_data_m_delta_as@v8 GROUP BY COUNTRYNAME;

-- COMMAND ----------

SHOW PARTITIONS praneeth_demo.electric_vehicle_data_m_delta_as

-- COMMAND ----------

DESC EXTENDED praneeth_demo.electric_vehicle_data_m_delta_as

-- COMMAND ----------

SHOW TABLES DROPPED IN praneeth_demo;
