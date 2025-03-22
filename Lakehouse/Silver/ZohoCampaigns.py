# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

table_name = "JsaZohoCampaigns"

zohocampaigns_df = spark.read.json(f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_name}.json")
zohocampaigns_df

# COMMAND ----------

zohocampaigns_df = zohocampaigns_df.select("data")

# COMMAND ----------

zohocampaigns_df = flatten(zohocampaigns_df)

# COMMAND ----------

# Imprime las columnas disponibles antes de procesarlas
print("Columnas disponibles en el DataFrame:")
print(zohocampaigns_df.columns)

# Renombra columnas, asegurándote de que las columnas existen
for col_name in zohocampaigns_df.columns:
    new_col_name = col_name.replace("data_", "")#.replace("users_", "")
    zohocampaigns_df = zohocampaigns_df.withColumnRenamed(col_name, new_col_name)

# Muestra el DataFrame procesado
display(zohocampaigns_df)

# COMMAND ----------

# DBTITLE 1,Columnas a minusculas
for col in zohocampaigns_df.columns:
    zohocampaigns_df = zohocampaigns_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Crear un diccionario para renombrar columnas con doble guion bajo o nombres más claros
rename_columns = {
    "actual_cost": "actual_cost",
    "budgeted_cost": "budgeted_cost",
    "campaign_name": "campaign_name",
    "currency": "currency",
    "description": "description",
    "end_date": "end_date",
    "exchange_rate": "exchange_rate",
    "expected_response": "expected_response",
    "expected_revenue": "expected_revenue",
    "linea_de_negocio": "business_line",
    "native__campaigns__extn__campaign_subject": "campaign_subject",
    "native__campaigns__extn__reply_to_address": "reply_to_address",
    "native__campaigns__extn__sender_address": "sender_address",
    "native__campaigns__extn__sender_name": "sender_name",
    "native__survey__extn__department_id": "departmen_tid",
    "native__survey__extn__survey": "survey",
    "native__survey__extn__survey_department": "survey_department",
    "native__survey__extn__survey_type": "survey_type",
    "native__survey__extn__survey_url": "survey_url",
    "native__webinar__extn__webinar_duration": "webinar_duration",
    "native__webinar__extn__webinar_launch_url": "webinar_launch_url",
    "native__webinar__extn__webinar_registration_url": "webinar_registration_url",
    "native__webinar__extn__webinar_schedule": "webinar_schedule",
    "num_sent": "num_sent",
    "parent_campaign": "parent_campaign",
    "start_date": "start_date",
    "status": "status",
    "type": "type",
    "id": "id",
    "created_by_email": "created_by_email",
    "created_by_id": "created_by_id",
    "created_by_name": "created_by_name",
    "layout_id": "layout_id",
    "layout_name": "layout_name",
    "modified_by_email": "modified_by_email",
    "modified_by_id": "modified_by_id",
    "modified_by_name": "modified_by_name",
    "owner_email": "owner_email",
    "owner_id": "owner_id",
    "owner_name": "owner_name",
    "tag_color_code": "tag_color_code",
    "tag_id": "tag_id",
    "tag_name": "tag_name"
}

# Ajustar las columnas del DataFrame
zohocampaigns_df = zohocampaigns_df
for col_name, new_name in rename_columns.items():
    zohocampaigns_df = zohocampaigns_df.withColumnRenamed(col_name, new_name)

# Agregar columnas adicionales de procesamiento
zohocampaigns_df = zohocampaigns_df \
    .withColumn("processDate", current_timestamp()) \
    .withColumn("sourceSystem", lit("zoho_Contacts"))

# Mostrar el DataFrame renombrado y ajustado
display(zohocampaigns_df)

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, col

# Reemplaza valores nulos en columnas basadas en sus tipos de datos para zohocampaigns_df
for t in zohocampaigns_df.dtypes:
    column_name = t[0]
    column_type = t[1]
    
    if column_type == 'string':
        zohocampaigns_df = zohocampaigns_df.withColumn(column_name, coalesce(col(column_name), lit('')))
    elif column_type in ['double', 'float']:
        zohocampaigns_df = zohocampaigns_df.withColumn(column_name, coalesce(col(column_name), lit(0.0)))
    elif column_type in ['int', 'bigint']:
        zohocampaigns_df = zohocampaigns_df.withColumn(column_name, coalesce(col(column_name), lit(0)))
    elif column_type == 'boolean':
        zohocampaigns_df = zohocampaigns_df.withColumn(column_name, coalesce(col(column_name), lit(False)))
    elif column_type in ['timestamp', 'date']:
        # Para fechas y timestamps dejamos `None` explícitamente
        zohocampaigns_df = zohocampaigns_df.withColumn(column_name, coalesce(col(column_name), lit(None)))

# Mostrar el DataFrame resultante
display(zohocampaigns_df)

# COMMAND ----------

zohocampaigns_df = zohocampaigns_df.dropDuplicates()

# COMMAND ----------

zohocampaigns_df.createOrReplaceTempView("zohocampaigns_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.zohocampaigns
# MAGIC USING zohocampaigns_source_view
# MAGIC ON silver_lakehouse.zohocampaigns.id = zohocampaigns_source_view.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
