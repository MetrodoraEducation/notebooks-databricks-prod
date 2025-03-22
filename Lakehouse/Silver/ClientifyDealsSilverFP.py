# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

clientify_df = spark.read.json(f"{bronze_folder_path}/lakehouse/clientify/deals/{current_date}/clientifydealsfp.json")

# COMMAND ----------

clientify_df = clientify_df.select("results")

# COMMAND ----------

clientify_df = flatten(clientify_df)

# COMMAND ----------

clientify_df_result = clientify_df.drop("results_tags","results_custom_fields_field","results_custom_fields_id","results_custom_fields_value")

# COMMAND ----------

clientify_df_result = clientify_df_result.dropDuplicates()

# COMMAND ----------

clientify_df_custom_fields = clientify_df.select("results_id","results_custom_fields_field","results_custom_fields_id","results_custom_fields_value")

#display(clientify_df_custom_fields)

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.dropDuplicates()

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.filter(clientify_df_custom_fields.results_custom_fields_field != "Año Académico")

# COMMAND ----------

# MAGIC %md
# MAGIC Problema nombre columna custum_field, haciendo pivot sale solo el valor en el nombre de la columna, luego pasa que tenemos el id de result y el id de custumfield

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.withColumn(
    "results_custom_fields_field",
    concat(lit("custom_fields_"), clientify_df_custom_fields["results_custom_fields_field"])
)

#display(clientify_df_custom_fields)

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.groupBy("results_id").pivot("results_custom_fields_field").agg(first(col("results_custom_fields_value")))

#first("results_custom_fields_value")

# COMMAND ----------

for col in clientify_df_custom_fields.columns:
    clientify_df_custom_fields = clientify_df_custom_fields.withColumnRenamed(col, col.replace(" ", "_"))

# COMMAND ----------

for col in clientify_df_custom_fields.columns:
    clientify_df_custom_fields = clientify_df_custom_fields.withColumnRenamed(col, col.lower())

# COMMAND ----------

for col in clientify_df_custom_fields.columns:
    clientify_df_custom_fields = clientify_df_custom_fields.withColumnRenamed(col, col.replace("-", "_"))

# COMMAND ----------

clientify_df_custom_fields = clientify_df_custom_fields.withColumnRenamed("results_id","results_id_custom_fields")

#display(clientify_df_custom_fields)

# COMMAND ----------

clientify_df_final = clientify_df_result.join(
    clientify_df_custom_fields, 
    on=clientify_df_result.results_id == clientify_df_custom_fields.results_id_custom_fields, 
    how='outer'
)

#display(clientify_df_final)

# COMMAND ----------

clientify_df_final = clientify_df_final.drop("results_id_custom_fields")

#display(clientify_df_final)

# COMMAND ----------

for col in clientify_df_final.columns:
    clientify_df_final = clientify_df_final.withColumnRenamed(col, col.replace("results_", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion amount_user**

# COMMAND ----------

display (clientify_df_final)

# COMMAND ----------

#clientify_df_final = clientify_df_final.withColumn("amount_user",regexp_replace(regexp_replace(col("amount_user"), '[^a-zA-Z]', ''), '', '').cast(DoubleType()))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
clientify_df_final = clientify_df_final \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("Clientify FP")) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("probability", col("probability").cast(IntegerType())) \
    .withColumn("status", col("status").cast(IntegerType())) \
    .withColumn("amount", col("amount").cast(DoubleType())) \
    .withColumn("custom_fields_descuento", col("custom_fields_descuento").cast(DoubleType())) \
    .withColumn("custom_fields_descuento_matricula", col("custom_fields_descuento_matricula").cast(DoubleType())) \
    .withColumn("custom_fields_matricula", regexp_replace(
        regexp_replace(col("custom_fields_matricula"), "\.", ""), ",", ".").cast(DoubleType())) \
    .withColumn("custom_fields_mensualidad", regexp_replace(
        regexp_replace(col("custom_fields_mensualidad"), "\.", ""), ",", ".").cast(DoubleType())) \
    .withColumn("custom_fields_byratings_score", col("custom_fields_byratings_score").cast(DoubleType()) if "custom_fields_byratings_score" in clientify_df_final.columns else lit(None).cast(DoubleType())) \
    .withColumn("actual_closed_date", to_timestamp(col("actual_closed_date"))) \
    .withColumn("created", to_timestamp(col("created"))) \
    .withColumn("expected_closed_date", to_timestamp(col("expected_closed_date"))) \
    .withColumn("modified", to_timestamp(col("modified"))) \
    .withColumn("custom_fields_fecha_inscripcion", to_timestamp(col("custom_fields_fecha_inscripcion")) if "custom_fields_fecha_inscripcion" in clientify_df_final.columns else lit(None).cast(TimestampType())) \
    .withColumn("probability_desc", regexp_replace(col("probability_desc"), "%", "").cast(DoubleType())) \
    .withColumn("amount_user", col("amount_user").cast(DoubleType()))
#    .withColumn("date_action_last", to_timestamp(col("date_action_last"), 'yyyy-MM-dd HH:mm:ss')) \
#agregado amount user para que no sea string

#14/01/2025 - sourcesystem se ha cambiado de Clientify a Clientify FP

display(clientify_df_final)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

# Define expected_columns with appropriate column names and data types
expected_columns = {
    'column1': DoubleType(),
    'column2': IntegerType(),
    'column3': StringType(),
    'column4': TimestampType()
}

for column, data_type in expected_columns.items():
    if column in clientify_df_final.columns:  # Verify if the column exists
        if isinstance(data_type, DoubleType):
            clientify_df_final = clientify_df_final.withColumn(column, col(column).cast(DoubleType()))
        elif isinstance(data_type, IntegerType):
            clientify_df_final = clientify_df_final.withColumn(column, col(column).cast(IntegerType()))
        elif isinstance(data_type, StringType):
            clientify_df_final = clientify_df_final.withColumn(column, col(column).cast(StringType()))
        elif isinstance(data_type, TimestampType):
            clientify_df_final = clientify_df_final.withColumn(column, to_timestamp(col(column)))

display(clientify_df_final)

# COMMAND ----------

clientify_df_final = clientify_df_final.dropDuplicates()

display(clientify_df_final)

# COMMAND ----------

clientify_df_final.createOrReplaceTempView("clientifydeals_source_view")

display(spark.sql("SELECT * FROM clientifydeals_source_view"))

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.clientifydeals AS target
# MAGIC USING clientifydeals_source_view AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.actual_closed_date = source.actual_closed_date,
# MAGIC         target.amount = source.amount,
# MAGIC         target.sourcesystem = source.sourcesystem
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (id, actual_closed_date, amount, sourcesystem) 
# MAGIC     VALUES (source.id, source.actual_closed_date, source.amount, source.sourcesystem)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lakehouse.clientifydeals where sourcesystem LIKE '%Clientify FP'
