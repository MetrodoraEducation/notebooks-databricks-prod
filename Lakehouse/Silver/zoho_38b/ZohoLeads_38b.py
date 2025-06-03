# Databricks notebook source
# MAGIC %run "../configuration"

# COMMAND ----------

from pyspark.sql import SparkSession

table_prefix = "JsaZohoLeads_"
file_pattern = f"{bronze_folder_path}/lakehouse/zoho_38b/{current_date}/{table_prefix}*.json"

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

display(zoholeads_df)

# COMMAND ----------

from pyspark.sql.functions import col

# Diccionario para mapear las columnas con nombres más entendibles
columns_mapping = {
    "data_apellido_2": "apellido_2",
    "data_created_time": "fecha_creacion",
    "data_description": "descripcion",
    "data_email": "email",
    "data_first_name": "nombre",
    "data_l_nea_de_negocio": "linea_de_negocio",
    "data_last_name": "apellido_1",
    "data_lead_source": "lead_source",
    "data_lead_status": "lead_status",
    "data_mobile": "telefono_movil",
    "data_modified_time": "fecha_modificacion",
    "data_motivos_de_perdida": "motivos_perdida",
    "data_nacionalidad": "nacionalidad",
    "data_phone": "telefono",
    "data_provincia": "provincia",
    "data_residencia": "residencia",
    "data_sexo": "sexo",
    "data_tipolog_a_de_cliente": "tipologia_cliente",
    "data_typo_conversion": "tipo_conversion",
    "data_device": "dispositivo",
    "data_fbclid": "fbclid",
    "data_gclid1": "gclid",
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
    "data_utm_estrategia": "utm_estrategia",
    "data_utm_medium": "utm_medium",
    "data_utm_perfil": "utm_perfil",
    "data_utm_source": "utm_source",
    "data_utm_term": "utm_term",
    "data_utm_type": "utm_type",
    "data_owner_email": "owner_email",
    "data_owner_id": "owner_id",
    "data_owner_name": "owner_name"
}

# Añadir columnas de trazabilidad
zoholeads_df = zoholeads_df \
    .withColumn("sourcesystem", lit("zoho_Leads_38b")) \
    .withColumn("processdate", current_timestamp()) 
    
# Renombrar columnas si existen
for old_col, new_col in columns_mapping.items():
    if old_col in zoholeads_df.columns:
        zoholeads_df = zoholeads_df.withColumnRenamed(old_col, new_col)

# Mostrar DataFrame con nombres normalizados
display(zoholeads_df)

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

# Muestra el DataFrame resultante
display(zoholeads_df)

# COMMAND ----------

zoholeads_df = zoholeads_df.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Filter FisioFocus, CESIF, ISEP
zoholeads_df_filtered = zoholeads_df.filter(
    (col("linea_de_negocio").isin("MetrodoraFP", "Océano")) &  # Solo esos valores
    (col("linea_de_negocio").isNotNull()) &  # Que no sea NULL
    (col("linea_de_negocio") != "")  # Que no sea blanco
)

# Crear la vista temporal con datos ya filtrados
zoholeads_df_filtered.createOrReplaceTempView("zoholeads_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.ZohoLeads_38b AS target
# MAGIC USING zoholeads_source_view AS source
# MAGIC ON target.id = source.id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.apellido_2 IS DISTINCT FROM source.apellido_2 OR
# MAGIC     target.fecha_creacion IS DISTINCT FROM source.fecha_creacion OR
# MAGIC     target.descripcion IS DISTINCT FROM source.descripcion OR
# MAGIC     target.email IS DISTINCT FROM source.email OR
# MAGIC     target.nombre IS DISTINCT FROM source.nombre OR
# MAGIC     target.linea_de_negocio IS DISTINCT FROM source.linea_de_negocio OR
# MAGIC     target.apellido_1 IS DISTINCT FROM source.apellido_1 OR
# MAGIC     target.lead_source IS DISTINCT FROM source.lead_source OR
# MAGIC     target.lead_status IS DISTINCT FROM source.lead_status OR
# MAGIC     target.telefono_movil IS DISTINCT FROM source.telefono_movil OR
# MAGIC     target.fecha_modificacion IS DISTINCT FROM source.fecha_modificacion OR
# MAGIC     target.motivos_perdida IS DISTINCT FROM source.motivos_perdida OR
# MAGIC     target.nacionalidad IS DISTINCT FROM source.nacionalidad OR
# MAGIC     target.telefono IS DISTINCT FROM source.telefono OR
# MAGIC     target.provincia IS DISTINCT FROM source.provincia OR
# MAGIC     target.residencia IS DISTINCT FROM source.residencia OR
# MAGIC     target.sexo IS DISTINCT FROM source.sexo OR
# MAGIC     target.tipologia_cliente IS DISTINCT FROM source.tipologia_cliente OR
# MAGIC     target.tipo_conversion IS DISTINCT FROM source.tipo_conversion OR
# MAGIC     target.dispositivo IS DISTINCT FROM source.dispositivo OR
# MAGIC     target.fbclid IS DISTINCT FROM source.fbclid OR
# MAGIC     target.gclid IS DISTINCT FROM source.gclid OR
# MAGIC     target.id_producto IS DISTINCT FROM source.id_producto OR
# MAGIC     target.id_programa IS DISTINCT FROM source.id_programa OR
# MAGIC     target.lead_correlation_id IS DISTINCT FROM source.lead_correlation_id OR
# MAGIC     target.lead_rating IS DISTINCT FROM source.lead_rating OR
# MAGIC     target.lead_scoring IS DISTINCT FROM source.lead_scoring OR
# MAGIC     target.source IS DISTINCT FROM source.source OR
# MAGIC     target.utm_ad_id IS DISTINCT FROM source.utm_ad_id OR
# MAGIC     target.utm_adset_id IS DISTINCT FROM source.utm_adset_id OR
# MAGIC     target.utm_campaign_id IS DISTINCT FROM source.utm_campaign_id OR
# MAGIC     target.utm_campaign_name IS DISTINCT FROM source.utm_campaign_name OR
# MAGIC     target.utm_channel IS DISTINCT FROM source.utm_channel OR
# MAGIC     target.utm_estrategia IS DISTINCT FROM source.utm_estrategia OR
# MAGIC     target.utm_medium IS DISTINCT FROM source.utm_medium OR
# MAGIC     target.utm_perfil IS DISTINCT FROM source.utm_perfil OR
# MAGIC     target.utm_source IS DISTINCT FROM source.utm_source OR
# MAGIC     target.utm_term IS DISTINCT FROM source.utm_term OR
# MAGIC     target.utm_type IS DISTINCT FROM source.utm_type OR
# MAGIC     target.owner_email IS DISTINCT FROM source.owner_email OR
# MAGIC     target.owner_id IS DISTINCT FROM source.owner_id OR
# MAGIC     target.owner_name IS DISTINCT FROM source.owner_name
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.apellido_2 = source.apellido_2,
# MAGIC     target.fecha_creacion = source.fecha_creacion,
# MAGIC     target.descripcion = source.descripcion,
# MAGIC     target.email = source.email,
# MAGIC     target.nombre = source.nombre,
# MAGIC     target.linea_de_negocio = source.linea_de_negocio,
# MAGIC     target.apellido_1 = source.apellido_1,
# MAGIC     target.lead_source = source.lead_source,
# MAGIC     target.lead_status = source.lead_status,
# MAGIC     target.telefono_movil = source.telefono_movil,
# MAGIC     target.fecha_modificacion = source.fecha_modificacion,
# MAGIC     target.motivos_perdida = source.motivos_perdida,
# MAGIC     target.nacionalidad = source.nacionalidad,
# MAGIC     target.telefono = source.telefono,
# MAGIC     target.provincia = source.provincia,
# MAGIC     target.residencia = source.residencia,
# MAGIC     target.sexo = source.sexo,
# MAGIC     target.tipologia_cliente = source.tipologia_cliente,
# MAGIC     target.tipo_conversion = source.tipo_conversion,
# MAGIC     target.dispositivo = source.dispositivo,
# MAGIC     target.fbclid = source.fbclid,
# MAGIC     target.gclid = source.gclid,
# MAGIC     target.id_producto = source.id_producto,
# MAGIC     target.id_programa = source.id_programa,
# MAGIC     target.lead_correlation_id = source.lead_correlation_id,
# MAGIC     target.lead_rating = source.lead_rating,
# MAGIC     target.lead_scoring = source.lead_scoring,
# MAGIC     target.source = source.source,
# MAGIC     target.utm_ad_id = source.utm_ad_id,
# MAGIC     target.utm_adset_id = source.utm_adset_id,
# MAGIC     target.utm_campaign_id = source.utm_campaign_id,
# MAGIC     target.utm_campaign_name = source.utm_campaign_name,
# MAGIC     target.utm_channel = source.utm_channel,
# MAGIC     target.utm_estrategia = source.utm_estrategia,
# MAGIC     target.utm_medium = source.utm_medium,
# MAGIC     target.utm_perfil = source.utm_perfil,
# MAGIC     target.utm_source = source.utm_source,
# MAGIC     target.utm_term = source.utm_term,
# MAGIC     target.utm_type = source.utm_type,
# MAGIC     target.owner_email = source.owner_email,
# MAGIC     target.owner_id = source.owner_id,
# MAGIC     target.owner_name = source.owner_name,
# MAGIC     target.processdate = current_timestamp(),
# MAGIC     target.sourcesystem = source.sourcesystem
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lakehouse.ZohoLeads_38b
