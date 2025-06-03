# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

from pyspark.sql import SparkSession

table_prefix = "JsaZohoLeads_"
file_pattern = f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_prefix}*.json"

print(f"Leyendo archivos desde: {file_pattern}")

zoholeads_df = spark.read.json(file_pattern)

# COMMAND ----------

zoholeads_df = zoholeads_df.select("data")

# COMMAND ----------

zoholeads_df = flatten(zoholeads_df)

# COMMAND ----------

# Imprime las columnas disponibles antes de procesarlas
print("Columnas disponibles en el DataFrame:")
print(zoholeads_df.columns)

# Renombra columnas, asegurándote de que las columnas existen
for col_name in zoholeads_df.columns:
    new_col_name = col_name.replace(" ", "_")
    zoholeads_df = zoholeads_df.withColumnRenamed(col_name, new_col_name)

# COMMAND ----------

for col in zoholeads_df.columns:
    zoholeads_df = zoholeads_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

for col in zoholeads_df.columns:
    zoholeads_df = zoholeads_df.withColumnRenamed(col, col.replace("-", "_"))

# COMMAND ----------

# Diccionario para mapear las columnas con nombres más entendibles
columns_mapping = {
    "data_apellido_2": "apellido_2",
    "data_description": "description",
    "data_email": "email",
    "data_first_name": "first_name",
    "data_last_name": "last_name",
    "data_lead_source": "lead_source",
    "data_lead_status": "lead_status",
    "data_mobile": "mobile",
    "data_modified_time": "modified_time",
    "data_created_time": "Created_Time",
    "data_motivos_de_perdida": "motivos_perdida",
    "data_nacionalidad": "nacionalidad",
    "data_phone": "phone",
    "data_provincia": "provincia",
    "data_residencia": "residencia",
    "data_sexo": "sexo",
    "data_tipolog_a_de_cliente": "tipologia_cliente",
    "data_typo_conversion": "tipo_conversion",
    "data_visitor_score": "visitor_score",
    "data_device": "device",
    "data_fbclid": "facebook_click_id",
    "data_gclid1": "google_click_id",
    "data_id": "id",
    "data_id_producto": "id_producto",
    "data_id_programa": "id_programa",
    "data_lead_correlation_id": "lead_correlation_id",
    "data_lead_rating": "lead_rating",
    "data_lead_scoring": "lead_scoring",
    "data_source": "source",
    "data_utm_ad_id": "utm_ad_id",
    "data_utm_adset_id": "utm_adset_id",
    "data_utm_campaign_id": "utm_campaign_id",
    "data_utm_campaign_name": "utm_campaign_name",
    "data_utm_channel": "utm_channel",
    "data_utm_estrategia": "utm_strategy",
    "data_utm_medium": "utm_medium",
    "data_utm_perfil": "utm_profile",
    "data_utm_source": "utm_source",
    "data_utm_term": "utm_term",
    "data_utm_type": "utm_type",
    "data_owner_email": "owner_email",
    "data_owner_id": "owner_id",
    "data_owner_name": "owner_name"
}


# Renombrar columnas dinámicamente
for old_col, new_col in columns_mapping.items():
    if old_col in zoholeads_df.columns:
        zoholeads_df = zoholeads_df.withColumnRenamed(old_col, new_col)

# COMMAND ----------

# DBTITLE 1,Nombrar columnas
from pyspark.sql.types import *
from pyspark.sql.functions import *

zoholeads_df = zoholeads_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("zoho_Leads")) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("first_name", col("first_name").cast(StringType())) \
    .withColumn("last_name", col("last_name").cast(StringType())) \
    .withColumn("apellido_2", col("apellido_2").cast(StringType())) \
    .withColumn("email", col("email").cast(StringType())) \
    .withColumn("mobile", col("mobile").cast(StringType())) \
    .withColumn("modified_time", to_timestamp(col("modified_time"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("Created_Time", to_timestamp(col("Created_Time"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("lead_source", col("lead_source").cast(StringType())) \
    .withColumn("lead_status", col("lead_status").cast(StringType())) \
    .withColumn("lead_rating", col("lead_rating").cast(StringType())) \
    .withColumn("lead_scoring", col("lead_scoring").cast(StringType())) \
    .withColumn("visitor_score", col("visitor_score").cast(StringType())) \
    .withColumn("sexo", col("sexo").cast(StringType())) \
    .withColumn("tipologia_cliente", col("tipologia_cliente").cast(StringType())) \
    .withColumn("tipo_conversion", col("tipo_conversion").cast(StringType())) \
    .withColumn("residencia", col("residencia").cast(StringType())) \
    .withColumn("provincia", col("provincia").cast(StringType())) \
    .withColumn("motivos_perdida", col("motivos_perdida").cast(StringType())) \
    .withColumn("nacionalidad", col("nacionalidad").cast(StringType())) \
    .withColumn("utm_source", col("utm_source").cast(StringType())) \
    .withColumn("utm_medium", col("utm_medium").cast(StringType())) \
    .withColumn("utm_campaign_id", col("utm_campaign_id").cast(StringType())) \
    .withColumn("utm_campaign_name", col("utm_campaign_name").cast(StringType())) \
    .withColumn("utm_ad_id", col("utm_ad_id").cast(StringType())) \
    .withColumn("utm_adset_id", col("utm_adset_id").cast(StringType())) \
    .withColumn("utm_term", col("utm_term").cast(StringType())) \
    .withColumn("utm_channel", col("utm_channel").cast(StringType())) \
    .withColumn("utm_type", col("utm_type").cast(StringType())) \
    .withColumn("utm_strategy", col("utm_strategy").cast(StringType())) \
    .withColumn("utm_profile", col("utm_profile").cast(StringType())) \
    .withColumn("google_click_id", col("google_click_id").cast(StringType())) \
    .withColumn("facebook_click_id", col("facebook_click_id").cast(StringType())) \
    .withColumn("id_producto", col("id_producto").cast(StringType())) \
    .withColumn("id_programa", col("id_programa").cast(StringType())) \
    .withColumn("lead_correlation_id", col("lead_correlation_id").cast(StringType())) \
    .withColumn("description", col("description").cast(StringType())) \
    .withColumn("phone", col("phone").cast(StringType())) \
    .withColumn("device", col("device").cast(StringType())) \
    .withColumn("source", col("source").cast(StringType())) \
    .withColumn("owner_email", col("owner_email").cast(StringType())) \
    .withColumn("owner_id", col("owner_id").cast(StringType())) \
    .withColumn("linea_de_negocio", col("data_l_nea_de_negocio").cast(StringType())).drop("data_l_nea_de_negocio") \
    .withColumn("owner_name", col("owner_name").cast(StringType())) 

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, col

# Reemplaza valores nulos en columnas basadas en sus tipos de datos
for t in zoholeads_df.dtypes:
    if t[1] == 'string':
        zoholeads_df = zoholeads_df.withColumn(t[0], coalesce(col(t[0]), lit('')))
    elif t[1] == 'double':
        zoholeads_df = zoholeads_df.withColumn(t[0], coalesce(col(t[0]), lit(0.0)))
    elif t[1] == 'int' or t[1] == 'bigint':
        zoholeads_df = zoholeads_df.withColumn(t[0], coalesce(col(t[0]), lit(0)))
    elif t[1] == 'boolean':
        zoholeads_df = zoholeads_df.withColumn(t[0], coalesce(col(t[0]), lit(False)))
    elif t[1] == 'timestamp':
        zoholeads_df = zoholeads_df.withColumn(t[0], coalesce(col(t[0]), lit(None)))

# COMMAND ----------

zoholeads_df = zoholeads_df.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Filter FisioFocus, CESIF, ISEP
zoholeads_df_filtered = zoholeads_df.filter(
    (col("linea_de_negocio").isin("FisioFocus")) &  # Solo esos valores , "CESIF", "ISEP"
    (col("linea_de_negocio").isNotNull()) &  # Que no sea NULL
    (col("linea_de_negocio") != "")  # Que no sea blanco
)

# Crear la vista temporal con datos ya filtrados
zoholeads_df_filtered.createOrReplaceTempView("zoholeads_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.zoholeads AS target
# MAGIC USING zoholeads_source_view AS source
# MAGIC ON target.id = source.id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.Description IS DISTINCT FROM source.Description
# MAGIC  OR target.First_Name IS DISTINCT FROM source.First_Name
# MAGIC  OR target.Last_Name IS DISTINCT FROM source.Last_Name  
# MAGIC  OR target.Apellido_2 IS DISTINCT FROM source.Apellido_2
# MAGIC  OR target.Email IS DISTINCT FROM source.Email
# MAGIC  OR target.Mobile IS DISTINCT FROM source.Mobile
# MAGIC  OR target.Nacionalidad IS DISTINCT FROM source.Nacionalidad
# MAGIC  OR target.Phone IS DISTINCT FROM source.Phone
# MAGIC  OR target.Provincia IS DISTINCT FROM source.Provincia
# MAGIC  OR target.Residencia IS DISTINCT FROM source.Residencia
# MAGIC  OR target.Sexo IS DISTINCT FROM source.Sexo
# MAGIC  OR target.lead_rating IS DISTINCT FROM source.lead_rating
# MAGIC  OR target.lead_scoring IS DISTINCT FROM source.lead_scoring
# MAGIC  OR target.Lead_Status IS DISTINCT FROM source.Lead_Status
# MAGIC  OR target.Motivos_perdida IS DISTINCT FROM source.Motivos_perdida
# MAGIC  OR target.Tipologia_cliente IS DISTINCT FROM source.Tipologia_cliente
# MAGIC  OR target.tipo_conversion IS DISTINCT FROM source.tipo_conversion
# MAGIC  OR target.utm_ad_id IS DISTINCT FROM source.utm_ad_id
# MAGIC  OR target.utm_adset_id IS DISTINCT FROM source.utm_adset_id
# MAGIC  OR target.utm_campaign_id IS DISTINCT FROM source.utm_campaign_id
# MAGIC  OR target.utm_campaign_name IS DISTINCT FROM source.utm_campaign_name
# MAGIC  OR target.utm_channel IS DISTINCT FROM source.utm_channel
# MAGIC  OR target.utm_strategy IS DISTINCT FROM source.utm_strategy
# MAGIC  OR target.utm_medium IS DISTINCT FROM source.utm_medium
# MAGIC  OR target.utm_profile IS DISTINCT FROM source.utm_profile
# MAGIC  OR target.utm_source IS DISTINCT FROM source.utm_source
# MAGIC  OR target.utm_term IS DISTINCT FROM source.utm_term
# MAGIC  OR target.utm_type IS DISTINCT FROM source.utm_type
# MAGIC  OR target.Owner_id IS DISTINCT FROM source.Owner_id
# MAGIC  OR target.id_producto IS DISTINCT FROM source.id_producto
# MAGIC  OR target.lead_correlation_id IS DISTINCT FROM source.lead_correlation_id
# MAGIC  OR target.Created_Time IS DISTINCT FROM source.Created_Time
# MAGIC  OR target.Modified_Time IS DISTINCT FROM source.Modified_Time
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.Description = source.Description,
# MAGIC     target.First_Name = source.First_Name,
# MAGIC     target.Last_Name = source.Last_Name,
# MAGIC     target.Apellido_2 = source.Apellido_2,
# MAGIC     target.Email = source.Email,
# MAGIC     target.Mobile = source.Mobile,
# MAGIC     target.Nacionalidad = source.Nacionalidad,
# MAGIC     target.Phone = source.Phone,
# MAGIC     target.Provincia = source.Provincia,
# MAGIC     target.Residencia = source.Residencia,
# MAGIC     target.Sexo = source.Sexo,
# MAGIC     target.lead_rating = source.lead_rating,
# MAGIC     target.lead_scoring = source.lead_scoring,
# MAGIC     target.Lead_Status = source.Lead_Status,
# MAGIC     target.Motivos_perdida = source.Motivos_perdida,
# MAGIC     target.Tipologia_cliente = source.Tipologia_cliente,
# MAGIC     target.tipo_conversion = source.tipo_conversion,
# MAGIC     target.utm_ad_id = source.utm_ad_id,
# MAGIC     target.utm_adset_id = source.utm_adset_id,
# MAGIC     target.utm_campaign_id = source.utm_campaign_id,
# MAGIC     target.utm_campaign_name = source.utm_campaign_name,
# MAGIC     target.utm_channel = source.utm_channel,
# MAGIC     target.utm_strategy = source.utm_strategy,
# MAGIC     target.utm_medium = source.utm_medium,
# MAGIC     target.utm_profile = source.utm_profile,
# MAGIC     target.utm_source = source.utm_source,
# MAGIC     target.utm_term = source.utm_term,
# MAGIC     target.utm_type = source.utm_type,
# MAGIC     target.Owner_id = source.Owner_id,
# MAGIC     target.id_producto = source.id_producto,
# MAGIC     target.lead_correlation_id = source.lead_correlation_id,
# MAGIC     target.Created_Time = source.Created_Time,
# MAGIC     target.Modified_Time = source.Modified_Time
# MAGIC
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;
