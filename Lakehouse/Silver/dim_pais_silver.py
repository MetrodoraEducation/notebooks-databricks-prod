# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

dimpais_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/dim_pais/{current_date}")

# COMMAND ----------

dimpais_df = dimpais_df.withColumn("id", col("id").cast(IntegerType()))


# COMMAND ----------

for t in dimpais_df.dtypes:
    if t[1] == 'string':
        dimpais_df = dimpais_df.withColumn(t[0], coalesce(t[0], lit('')))

# COMMAND ----------

dimpais_df.createOrReplaceTempView("dimpais_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dimpais_source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.dim_pais
# MAGIC USING dimpais_source_view 
# MAGIC ON silver_lakehouse.dim_pais.id = dimpais_source_view.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
