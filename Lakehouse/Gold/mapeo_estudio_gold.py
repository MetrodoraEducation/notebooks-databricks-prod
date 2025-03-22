# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW mapeo_estudio_view
# MAGIC     AS SELECT * FROM silver_lakehouse.mapeo_estudio

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_source AS (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY TRIM(UPPER(estudio)) ORDER BY estudio) AS row_num
# MAGIC     FROM mapeo_estudio_view
# MAGIC )
# MAGIC MERGE INTO gold_lakehouse.mapeo_estudio AS target
# MAGIC USING (SELECT * FROM deduplicated_source WHERE row_num = 1) AS source
# MAGIC ON TRIM(UPPER(target.estudio)) = TRIM(UPPER(source.estudio))
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET target.estudio = source.estudio, target.estudio_norm = source.estudio_norm
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT (estudio, estudio_norm) 
# MAGIC VALUES (source.estudio, source.estudio_norm);
# MAGIC
