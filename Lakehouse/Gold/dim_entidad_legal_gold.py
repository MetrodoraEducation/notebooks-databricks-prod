# Databricks notebook source
# MAGIC %md
# MAGIC ### **DIM_ENTIDAD_LEGAL**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dim_entidad_legal_view AS
# MAGIC SELECT DISTINCT
# MAGIC     TRIM(entidad_legal) AS nombre_Institucion, codigo as codigo_entidad_legal
# MAGIC FROM silver_lakehouse.entidad_legal
# MAGIC union
# MAGIC SELECT 'n/a' AS nombre_Institucion, 'n/a' AS codigo_Entidad_Legal;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_lakehouse.dim_entidad_legal AS target
# MAGIC USING dim_entidad_legal_view AS source
# MAGIC ON target.nombre_Institucion = source.nombre_Institucion
# MAGIC
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.codigo_Entidad_Legal = source.codigo_Entidad_Legal,
# MAGIC         target.ETLupdatedDate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (nombre_Institucion, codigo_Entidad_Legal, ETLcreatedDate, ETLupdatedDate)
# MAGIC     VALUES (source.nombre_Institucion, source.codigo_Entidad_Legal,  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
