# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date, date_format, lit, to_date, replace, regexp_replace

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

current_date = spark.sql("SELECT date_format(CURRENT_DATE(),'yyyy/MM/dd')").collect()[0][0]
#display(current_date)

# COMMAND ----------

budget_df = spark.read.parquet(f"{bronze_folder_path}/lakehouse/budget/{current_date}")\
.withColumnRenamed("Fecha", "fecha") \
.withColumnRenamed("Escenario", "escenario") \
.withColumnRenamed("Titulacion", "titulacion") \
.withColumnRenamed("Centro", "centro") \
.withColumnRenamed("Sede", "sede") \
.withColumnRenamed("Modalidad", "modalidad") \
.withColumnRenamed("numLeadsNetos", "num_leads_netos") \
.withColumnRenamed("numLeadsBrutos", "num_leads_brutos") \
.withColumnRenamed("NewEnrollment", "new_enrollment") \
.withColumnRenamed("importeVentaNeta", "importe_venta_neta") \
.withColumnRenamed("importeVentaBruta", "importe_venta_bruta") \
.withColumnRenamed("importeCaptacion", "importe_captacion") \
.withColumn("processdate", current_timestamp())

# COMMAND ----------

budget_df = budget_df\
.withColumn("importe_venta_neta",regexp_replace("importe_venta_neta",",",".")) \
.withColumn("importe_venta_bruta",regexp_replace("importe_venta_bruta",",",".")) \
.withColumn("importe_captacion",regexp_replace("importe_captacion",",",".")) 

# COMMAND ----------

budget_df = budget_df\
.withColumn("fecha",to_date(budget_df.fecha,'dd/MM/yyyy')) \
.withColumn("num_leads_netos",budget_df.num_leads_netos.cast(IntegerType())) \
.withColumn("num_leads_brutos",budget_df.num_leads_brutos.cast(IntegerType())) \
.withColumn("new_enrollment",budget_df.new_enrollment.cast(IntegerType())) \
.withColumn("importe_venta_neta",budget_df.importe_venta_neta.cast(DoubleType())) \
.withColumn("importe_venta_bruta",budget_df.importe_venta_bruta.cast(DoubleType())) \
.withColumn("importe_captacion",budget_df.importe_captacion.cast(DoubleType())) 


# COMMAND ----------

budget_df.write.mode("overwrite").format("delta").saveAsTable("silver_lakehouse.budget")
