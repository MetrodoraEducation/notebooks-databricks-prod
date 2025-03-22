# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

odoo_df = spark.read.json(f"{bronze_folder_path}/lakehouse/odoo/lead/{current_date}")

# COMMAND ----------

odoo_df = odoo_df.select("result")

# COMMAND ----------

odoo_df = flatten(odoo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion campos sin user_id y company_id**

# COMMAND ----------

odoo_df_result = odoo_df.drop("result_company_id","result_user_id")

# COMMAND ----------

odoo_df_result = odoo_df_result.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion user_id**

# COMMAND ----------

odoo_df_user_id = odoo_df.select("result_id","result_user_id")

# COMMAND ----------

odoo_df_user_id = odoo_df_user_id.dropDuplicates()

# COMMAND ----------

odoo_df_user_id = odoo_df_user_id.orderBy(odoo_df_user_id.result_id.desc())

# COMMAND ----------

odoo_df_user_id = odoo_df_user_id.withColumn("valuetype",when(col('result_user_id').rlike('^(\\d)+$'), lit("numeric")).otherwise(lit("non-numeric")))

# COMMAND ----------

odoo_df_user_id_id = odoo_df_user_id.filter(odoo_df_user_id.valuetype == "numeric")

# COMMAND ----------

odoo_df_user_id_id = odoo_df_user_id_id.withColumnRenamed("result_id","result_id_user_id")

# COMMAND ----------

odoo_df_user_id_id = odoo_df_user_id_id.drop("valuetype")

# COMMAND ----------

odoo_df_user_id_value = odoo_df_user_id.filter(odoo_df_user_id.valuetype == "non-numeric")

# COMMAND ----------

odoo_df_user_id_value = odoo_df_user_id_value.withColumnRenamed("result_user_id","result_user_value") \
.withColumnRenamed("result_id","result_id_user_value")

# COMMAND ----------

odoo_df_user_id_value = odoo_df_user_id_value.drop("valuetype")

# COMMAND ----------

odoo_df_result_with_user_id = odoo_df_result.join(
    odoo_df_user_id_id, 
    on=odoo_df_result.result_id == odoo_df_user_id_id.result_id_user_id, 
    how='outer'
)

# COMMAND ----------

odoo_df_result_with_user_id_value = odoo_df_result_with_user_id.join(
    odoo_df_user_id_value, 
    on=odoo_df_result_with_user_id.result_id == odoo_df_user_id_value.result_id_user_value, 
    how='outer'
)

# COMMAND ----------

odoo_df_result_with_user_id_value = odoo_df_result_with_user_id_value.drop("result_id_user_id","result_id_user_value")

# COMMAND ----------

# MAGIC %md
# MAGIC **Gestion company_id**

# COMMAND ----------

odoo_df_company_id = odoo_df.select("result_id","result_company_id")

# COMMAND ----------

odoo_df_company_id = odoo_df_company_id.dropDuplicates()

# COMMAND ----------

odoo_df_company_id = odoo_df_company_id.orderBy(odoo_df_company_id.result_id.desc())

# COMMAND ----------

odoo_df_company_id = odoo_df_company_id.withColumn("valuetype",when(col('result_company_id').rlike('^(\\d)+$'), lit("numeric")).otherwise(lit("non-numeric")))

# COMMAND ----------

odoo_df_company_id_id = odoo_df_company_id.filter(odoo_df_company_id.valuetype == "numeric")

# COMMAND ----------

odoo_df_company_id_id = odoo_df_company_id_id.withColumnRenamed("result_id","result_id_company_id")

# COMMAND ----------

odoo_df_company_id_id = odoo_df_company_id_id.drop("valuetype")

# COMMAND ----------

odoo_df_company_id_value = odoo_df_company_id.filter(odoo_df_company_id.valuetype == "non-numeric")

# COMMAND ----------

odoo_df_company_id_value = odoo_df_company_id_value.withColumnRenamed("result_company_id","result_company_value") \
.withColumnRenamed("result_id","result_id_company_value")

# COMMAND ----------

odoo_df_company_id_value = odoo_df_company_id_value.drop("valuetype")

# COMMAND ----------

odoo_df_result_with_user_id_value_with_company_id = odoo_df_result_with_user_id_value.join(
    odoo_df_company_id_id, 
    on=odoo_df_result_with_user_id_value.result_id == odoo_df_company_id_id.result_id_company_id, 
    how='outer'
)

# COMMAND ----------

odoo_df_result_with_user_id_value_with_company_id_value = odoo_df_result_with_user_id_value_with_company_id.join(
    odoo_df_company_id_value, 
    on=odoo_df_result_with_user_id_value_with_company_id.result_id == odoo_df_company_id_value.result_id_company_value, 
    how='outer'
)

# COMMAND ----------

odoo_df_final = odoo_df_result_with_user_id_value_with_company_id_value.drop("result_id_company_id","result_id_company_value")
#odoo_df_result_with_stage_id_value_with_company_id_value = odoo_df_result_with_stage_id_value_with_company_id_value.drop("result_id_company_id","result_id_company_value")

# COMMAND ----------

for col in odoo_df_final.columns:
    odoo_df_final = odoo_df_final.withColumnRenamed(col, col.replace("result_", ""))

# COMMAND ----------

for col in odoo_df_final.columns:
    odoo_df_final = odoo_df_final.withColumnRenamed(col, col.lower())

# COMMAND ----------

# MAGIC %md
# MAGIC Gestion country_id

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
odoo_df_final = odoo_df_final.withColumn(
    "country_id_temp", 
    regexp_replace(split(col("country_id"), ",")[0],"\\[","")
).withColumn(
    "country_value", 
    regexp_extract(split(col("country_id"), ",")[1],'"([^"]*)',1)
).drop("country_id").withColumnRenamed(
    "country_id_temp", 
    "country_id"
)


# COMMAND ----------

# MAGIC %md
# MAGIC Gestion state_id

# COMMAND ----------

odoo_df_final = odoo_df_final.withColumn(
    "state_id_temp", 
    regexp_replace(split(col("state_id"), ",")[0],"\\[","")
).withColumn(
    "state_value", 
    regexp_extract(split(col("state_id"), ",")[1],'"([^"]*)',1)
).drop("state_id").withColumnRenamed(
    "state_id_temp", 
    "state_id"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Gestion stage_id 

# COMMAND ----------

odoo_df_final = odoo_df_final.withColumn(
    "stage_id_temp", 
    regexp_replace(split(col("stage_id"), ",")[0],"\\[","")
).withColumn(
    "stage_value", 
    regexp_extract(split(col("stage_id"), ",")[1],'"([^"]*)',1)
).drop("stage_id").withColumnRenamed(
    "stage_id_temp", 
    "stage_id"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Gestion x_curso_id 

# COMMAND ----------

odoo_df_final = odoo_df_final.withColumn(
    "x_curso_id_temp", 
    regexp_replace(split(col("x_curso_id"), ",")[0],"\\[","")
).withColumn(
    "x_curso_value", 
    regexp_extract(split(col("x_curso_id"), ",")[1],'"([^"]*)',1)
).drop("x_curso_id").withColumnRenamed(
    "x_curso_id_temp", 
    "x_curso_id"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Gestion x_modalidad_id 

# COMMAND ----------

odoo_df_final = odoo_df_final.withColumn(
    "x_modalidad_id_temp", 
    regexp_replace(split(col("x_modalidad_id"), ",")[0],"\\[","")
).withColumn(
    "x_modalidad_value", 
    regexp_extract(split(col("x_modalidad_id"), ",")[1],'"([^"]*)',1)
).drop("x_modalidad_id").withColumnRenamed(
    "x_modalidad_id_temp", 
    "x_modalidad_id"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Gestion x_sede_id

# COMMAND ----------

odoo_df_final = odoo_df_final.withColumn(
    "x_sede_id_temp", 
    regexp_replace(split(col("x_sede_id"), ",")[0],"\\[","")
).withColumn(
    "x_sede_value", 
    regexp_extract(split(col("x_sede_id"), ",")[1],'"([^"]*)',1)
).drop("x_sede_id").withColumnRenamed(
    "x_sede_id_temp", 
    "x_sede_id"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Gestion lost_reason

# COMMAND ----------

odoo_df_final = odoo_df_final.withColumn(
    "lost_reason_id", 
    regexp_replace(split(col("lost_reason"), ",")[0],"\\[","")
).withColumn(
    "lost_reason_value", 
    regexp_extract(split(col("lost_reason"), ",")[1],'"([^"]*)',1)
).drop("lost_reason")

# COMMAND ----------

odoo_df_final = odoo_df_final \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("Odoo")) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("date_action_last", to_timestamp(col("date_action_last"), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("date_closed", to_timestamp(col("date_closed"), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("date_conversion", to_timestamp(col("date_conversion"), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("date_last_stage_update", to_timestamp(col("date_last_stage_update"), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("write_date", to_timestamp(col("write_date"), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC Gestion pais para mapeo

# COMMAND ----------

odoo_df_final = odoo_df_final.withColumn(
    "country_value", split(col("country_value"), "] ")[1]
)

# COMMAND ----------

# MAGIC %md
# MAGIC Gestion string null

# COMMAND ----------

for t in odoo_df_final.dtypes:
    if t[1] == 'string':
        odoo_df_final = odoo_df_final.withColumn(t[0], coalesce(t[0], lit('')))

# COMMAND ----------

odoo_df_final = odoo_df_final.withColumn(
    "user_id_temp", 
    regexp_replace(split(col("user_value"), ",")[0],"\\[","")
).withColumn(
    "user_value", 
    regexp_extract(split(col("user_value"), ",")[1],'"([^"]*)',1)
).drop("user_id").withColumnRenamed(
    "user_id_temp", 
    "user_id"
)

# COMMAND ----------

odoo_df_final = odoo_df_final.dropDuplicates()

# COMMAND ----------

odoo_df_final.createOrReplaceTempView("odoolead_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE WITH SCHEMA EVOLUTION
# MAGIC INTO silver_lakehouse.odoolead
# MAGIC USING odoolead_source_view 
# MAGIC ON silver_lakehouse.odoolead.id = odoolead_source_view.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
