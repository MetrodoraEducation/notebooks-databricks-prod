# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

dim_estudio_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/dim_estudio/{current_date}")

# COMMAND ----------

for t in dim_estudio_df.dtypes:
    if t[1] == 'string':
        dim_estudio_df = dim_estudio_df.withColumn(t[0], coalesce(t[0], lit('n/a')))

# COMMAND ----------

dim_estudio_df = dim_estudio_df.dropDuplicates()

# COMMAND ----------

dim_estudio_df.createOrReplaceTempView("dim_estudio_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.dim_estudio
# MAGIC USING dim_estudio_source_view 
# MAGIC ON silver_lakehouse.dim_estudio.cod_estudio = dim_estudio_source_view.cod_estudio
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
