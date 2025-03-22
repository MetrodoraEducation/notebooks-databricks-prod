# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

mapeo_estudio_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/mapeo_estudio/{current_date}")

# COMMAND ----------

mapeo_estudio_df.createOrReplaceTempView("mapeo_estudio_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_source AS (
# MAGIC     SELECT *, 
# MAGIC            ROW_NUMBER() OVER (PARTITION BY TRIM(UPPER(estudio_norm)) ORDER BY estudio) AS row_num
# MAGIC     FROM mapeo_estudio_view
# MAGIC )
# MAGIC MERGE INTO silver_lakehouse.mapeo_estudio AS target
# MAGIC USING (SELECT * FROM deduplicated_source WHERE row_num = 1) AS source
# MAGIC ON TRIM(UPPER(target.estudio)) = TRIM(UPPER(source.estudio)) -- Normaliza espacios y mayúsculas
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET target.estudio = source.estudio,
# MAGIC            target.estudio_norm = source.estudio_norm
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT (estudio, estudio_norm) 
# MAGIC VALUES (source.estudio, source.estudio_norm);
