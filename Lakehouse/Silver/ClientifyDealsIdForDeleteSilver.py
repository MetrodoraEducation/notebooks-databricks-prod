# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

clientifyidfordelete_df = spark.read.json(f"{bronze_folder_path}/lakehouse/clientify/dealsidfordelete/{current_date}")

# COMMAND ----------

clientifyidfordelete_df = clientifyidfordelete_df.select("results")

# COMMAND ----------

clientifyidfordelete_df = flatten(clientifyidfordelete_df)

# COMMAND ----------

clientifyidfordelete_df = clientifyidfordelete_df.dropDuplicates()

# COMMAND ----------

clientifyidfordelete_df = clientifyidfordelete_df.withColumn("results_id", col("results_id").cast(StringType()))

# COMMAND ----------

clientifyidfordelete_df = clientifyidfordelete_df.withColumnRenamed("results_id", "id")

# COMMAND ----------

clientifyidfordelete_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .saveAsTable("silver_lakehouse.clientifydealsidfordelete")
