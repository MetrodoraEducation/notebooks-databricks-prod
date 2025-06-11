# Databricks notebook source
# DBTITLE 1,Created view institucion_Sales
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_institucion_view AS
# MAGIC SELECT DISTINCT
# MAGIC     TRIM(institucion) AS nombre_Institucion
# MAGIC FROM silver_lakehouse.entidad_legal
# MAGIC union
# MAGIC SELECT 'n/a' AS nombre_Institucion;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Merge
# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_institucion AS target
# MAGIC USING dim_institucion_view AS source
# MAGIC ON target.nombre_Institucion = source.nombre_Institucion
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Institucion, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_Institucion, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
