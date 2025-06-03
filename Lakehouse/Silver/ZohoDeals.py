# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

from pyspark.sql import SparkSession

table_prefix = "JsaZohoDeals_"
file_pattern = f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_prefix}*.json"

print(f"Leyendo archivos desde: {file_pattern}")

zohodeals_df = spark.read.json(file_pattern)

# COMMAND ----------

zohodeals_df = zohodeals_df.select("data")

# COMMAND ----------

zohodeals_df = flatten(zohodeals_df)

# COMMAND ----------

# Imprime las columnas disponibles antes de procesarlas
print("Columnas disponibles en el DataFrame:")
print(zohodeals_df.columns)

# Renombra columnas, asegurándote de que las columnas existen
for col_name in zohodeals_df.columns:
    new_col_name = col_name.replace(" ", "_")
    zohodeals_df = zohodeals_df.withColumnRenamed(col_name, new_col_name)

# COMMAND ----------

for col in zohodeals_df.columns:
    zohodeals_df = zohodeals_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

for col in zohodeals_df.columns:
    zohodeals_df = zohodeals_df.withColumnRenamed(col, col.replace("-", "_"))

# COMMAND ----------

from pyspark.sql.functions import col

# Diccionario para mapear las columnas con nombres más entendibles
columns_mapping = {
    "data_amount": "importe",
    "data_c_digo_descuento": "codigo_descuento",
    "data_closing_date": "fecha_cierre",
    "data_competencia": "competencia",
    "data_currency": "currency",
    "data_deal_name": "deal_name",
    "data_descuento": "descuento",
    "data_exchange_rate": "exchange_rate",
    "data_fecha_hora_anulaci_n": "fecha_hora_anulacion",
    "data_fecha_hora_documentaci_n_completada": "fecha_hora_documentacion_completada",
    "data_fecha_hora_pagado_ne": "fecha_hora_pagado",
    "data_id_classlife": "id_classlife",
    "data_id_lead": "id_lead",
    "data_id_producto": "id_producto",
    "data_importe_pagado": "importe_pagado",
    "data_modified_time": "modified_time",
    "data_created_time": "Created_Time",
    "data_motivo_p_rdida_b2b": "motivo_perdida_b2b",
    "data_motivo_p_rdida_b2c": "motivo_perdida_b2c",
    "data_pipeline": "pipeline",
    "data_probability": "probabilidad",
    "data_profesion_estudiante": "profesion_estudiante",
    "data_residencia1": "residencia1",
    "data_stage": "etapa",
    "data_tipolog_a_de_cliente": "tipologia_cliente",
    "data_br_rating": "br_rating",
    "data_br_score": "br_score",
    "data_id": "id",
    "data_network": "network",
    "data_tipo_conversion": "tipo_conversion",
    "data_utm_ad_id": "utm_ad_id",
    "data_utm_adset_id": "utm_adset_id",
    "data_utm_campaign_id": "utm_campana_id",
    "data_utm_campaign_name": "utm_campana_nombre",
    "data_utm_channel": "utm_canal",
    "data_utm_estrategia": "utm_estrategia",
    "data_utm_medium": "utm_medio",
    "data_utm_perfil": "utm_perfil",
    "data_utm_source": "utm_fuente",
    "data_utm_term": "utm_termino",
    "data_utm_type": "utm_tipo",
    "data_owner_email": "owner_email",
    "data_owner_id": "owner_id",
    "data_owner_name": "owner_name",
    "data_lead_correlation_id": "lead_correlation_id",
    "data_nacionalidad1": "nacionalidad1",
    "data_id_unico": "id_unico",
    "data_tipolog_a_del_alumno1": "Tipologia_alumno1",
    "data_contact_name_id": "Contact_Name_id",
    "data_l_nea_de_negocio": "linea_de_negocio"
}

# Filtrar solo las columnas que existen en el DataFrame antes de renombrarlas
existing_columns = [col for col in columns_mapping.keys() if col in zohodeals_df.columns]

# Aplicar renombrado solo a las columnas que existen
for old_col in existing_columns:
    zohodeals_df = zohodeals_df.withColumnRenamed(old_col, columns_mapping[old_col])

# Seleccionar solo las columnas renombradas
zohodeals_df = zohodeals_df.select([col(new_col) for new_col in columns_mapping.values() if new_col in zohodeals_df.columns])

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Ajuste del DataFrame con validación de columnas
zohodeals_df = zohodeals_df \
    .withColumn("importe", col("importe").cast(DoubleType())) \
    .withColumn("codigo_descuento", col("codigo_descuento").cast(StringType())) \
    .withColumn("fecha_cierre", to_date(col("fecha_cierre"), "yyyy-MM-dd")) \
    .withColumn("competencia", col("competencia").cast(StringType())) \
    .withColumn("currency", col("currency").cast(StringType())) \
    .withColumn("deal_name", col("deal_name").cast(StringType())) \
    .withColumn("descuento", col("descuento").cast(DoubleType())) \
    .withColumn("tipo_cambio", col("exchange_rate").cast(DoubleType())) \
    .withColumn("fecha_hora_anulacion", to_timestamp(col("fecha_hora_anulacion"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("fecha_hora_documentacion_completada", to_timestamp(col("fecha_hora_documentacion_completada"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("fecha_hora_pagado", to_timestamp(col("fecha_hora_pagado"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("id_classlife", col("id_classlife").cast(StringType())) \
    .withColumn("id_lead", col("id_lead").cast(StringType())) \
    .withColumn("id_producto", col("id_producto").cast(StringType())) \
    .withColumn("importe_pagado", col("importe_pagado").cast(DoubleType())) \
    .withColumn("modified_time", to_timestamp(col("modified_time"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("Created_Time", to_timestamp(col("Created_Time"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("motivo_perdida_b2b", col("motivo_perdida_b2b").cast(StringType())) \
    .withColumn("motivo_perdida_b2c", col("motivo_perdida_b2c").cast(StringType())) \
    .withColumn("pipeline", col("pipeline").cast(StringType())) \
    .withColumn("probabilidad", col("probabilidad").cast(IntegerType())) \
    .withColumn("profesion_estudiante", col("profesion_estudiante").cast(StringType())) \
    .withColumn("residencia1", col("residencia1").cast(StringType())) \
    .withColumn("etapa", col("etapa").cast(StringType())) \
    .withColumn("tipologia_cliente", col("tipologia_cliente").cast(StringType())) \
    .withColumn("br_rating", col("br_rating").cast(StringType())) \
    .withColumn("br_score", col("br_score").cast(DoubleType())) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("network", col("network").cast(StringType())) \
    .withColumn("tipo_conversion", col("tipo_conversion").cast(StringType())) \
    .withColumn("utm_ad_id", col("utm_ad_id").cast(StringType())) \
    .withColumn("utm_adset_id", col("utm_adset_id").cast(StringType())) \
    .withColumn("utm_campaign_id", col("utm_campana_id").cast(StringType())) \
    .withColumn("utm_campaign_name", col("utm_campana_nombre").cast(StringType())) \
    .withColumn("utm_channel", col("utm_canal").cast(StringType())) \
    .withColumn("utm_strategy", col("utm_estrategia").cast(StringType())) \
    .withColumn("utm_medium", col("utm_medio").cast(StringType())) \
    .withColumn("utm_profile", col("utm_perfil").cast(StringType())) \
    .withColumn("utm_source", col("utm_fuente").cast(StringType())) \
    .withColumn("utm_term", col("utm_termino").cast(StringType())) \
    .withColumn("utm_type", col("utm_tipo").cast(StringType())) \
    .withColumn("owner_email", col("owner_email").cast(StringType())) \
    .withColumn("owner_id", col("owner_id").cast(StringType())) \
    .withColumn("owner_name", col("owner_name").cast(StringType())) \
    .withColumn("lead_correlation_id", col("lead_correlation_id").cast(StringType())) \
    .withColumn("nacionalidad1", col("nacionalidad1").cast(StringType())) \
    .withColumn("id_unico", col("id_unico")) \
    .withColumn("Contact_Name_id", col("Contact_Name_id").cast(StringType())) \
    .withColumn("Tipologia_alumno1", col("Tipologia_alumno1").cast(StringType())) \
    .withColumn("linea_de_negocio", col("linea_de_negocio").cast(StringType())) \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("zoho_Deals")) 

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, col

# Reemplaza valores nulos en columnas basadas en sus tipos de datos
for t in zohodeals_df.dtypes:
    column_name = t[0]
    column_type = t[1]
    
    if column_type == 'string':
        zohodeals_df = zohodeals_df.withColumn(column_name, coalesce(col(column_name), lit('')))
    elif column_type in ['double', 'float']:
        zohodeals_df = zohodeals_df.withColumn(column_name, coalesce(col(column_name), lit(0.0)))
    elif column_type in ['int', 'bigint']:
        zohodeals_df = zohodeals_df.withColumn(column_name, coalesce(col(column_name), lit(0)))
    elif column_type == 'boolean':
        zohodeals_df = zohodeals_df.withColumn(column_name, coalesce(col(column_name), lit(False)))
    elif column_type == 'timestamp' or column_type == 'date':
        zohodeals_df = zohodeals_df.withColumn(column_name, coalesce(col(column_name), lit(None)))

# COMMAND ----------

zohodeals_df = zohodeals_df.dropDuplicates()

# COMMAND ----------

zohodeals_df.createOrReplaceTempView("zohodeals_source_view")

zohodeals_df_filtered = zohodeals_df.filter(
    (col("linea_de_negocio").isin("FisioFocus")) &  # Solo esos valores
    (col("linea_de_negocio").isNotNull()) &  # Que no sea NULL
    (col("linea_de_negocio") != "")  # Que no sea blanco
)

# Crear la vista temporal con datos ya filtrados
zohodeals_df_filtered.createOrReplaceTempView("zohodeals_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.zohodeals AS target
# MAGIC USING zohodeals_source_view AS source
# MAGIC ON target.id = source.id
# MAGIC AND target.id_producto = source.id_producto
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        target.fecha_hora_pagado IS DISTINCT FROM source.fecha_hora_pagado
# MAGIC     OR target.Nacionalidad1 IS DISTINCT FROM source.Nacionalidad1
# MAGIC     OR target.Residencia1 IS DISTINCT FROM source.Residencia1
# MAGIC     OR target.br_rating IS DISTINCT FROM source.br_rating
# MAGIC     OR target.br_score IS DISTINCT FROM source.br_score
# MAGIC     OR target.Motivo_perdida_B2B IS DISTINCT FROM source.Motivo_perdida_B2B
# MAGIC     OR target.Motivo_perdida_B2C IS DISTINCT FROM source.Motivo_perdida_B2C
# MAGIC     OR target.Probabilidad IS DISTINCT FROM source.Probabilidad
# MAGIC     OR target.Pipeline IS DISTINCT FROM source.Pipeline
# MAGIC     OR target.Profesion_Estudiante IS DISTINCT FROM source.Profesion_Estudiante
# MAGIC     OR target.Competencia IS DISTINCT FROM source.Competencia
# MAGIC     OR target.Tipologia_cliente IS DISTINCT FROM source.Tipologia_cliente
# MAGIC     OR target.utm_ad_id IS DISTINCT FROM source.utm_ad_id
# MAGIC     OR target.utm_adset_id IS DISTINCT FROM source.utm_adset_id
# MAGIC     OR target.utm_campaign_id IS DISTINCT FROM source.utm_campaign_id
# MAGIC     OR target.utm_campaign_name IS DISTINCT FROM source.utm_campaign_name
# MAGIC     OR target.utm_channel IS DISTINCT FROM source.utm_channel
# MAGIC     OR target.utm_strategy IS DISTINCT FROM source.utm_strategy
# MAGIC     OR target.utm_medium IS DISTINCT FROM source.utm_medium
# MAGIC     OR target.utm_profile IS DISTINCT FROM source.utm_profile
# MAGIC     OR target.utm_source IS DISTINCT FROM source.utm_source
# MAGIC     OR target.utm_term IS DISTINCT FROM source.utm_term
# MAGIC     OR target.utm_type IS DISTINCT FROM source.utm_type
# MAGIC     OR target.deal_name IS DISTINCT FROM source.deal_name
# MAGIC     OR target.fecha_Cierre IS DISTINCT FROM source.fecha_Cierre
# MAGIC     OR target.Exchange_Rate IS DISTINCT FROM source.Exchange_Rate
# MAGIC     OR target.Currency IS DISTINCT FROM source.Currency
# MAGIC     OR target.Importe_pagado IS DISTINCT FROM source.Importe_pagado
# MAGIC     OR target.Codigo_descuento IS DISTINCT FROM source.Codigo_descuento
# MAGIC     OR target.Descuento IS DISTINCT FROM source.Descuento
# MAGIC     OR target.importe IS DISTINCT FROM source.importe
# MAGIC     OR target.Tipologia_alumno1 IS DISTINCT FROM source.Tipologia_alumno1
# MAGIC     OR target.tipo_conversion IS DISTINCT FROM source.tipo_conversion
# MAGIC     OR target.Created_Time IS DISTINCT FROM source.Created_Time
# MAGIC     OR target.Modified_Time IS DISTINCT FROM source.Modified_Time
# MAGIC     OR target.fecha_hora_anulacion IS DISTINCT FROM source.fecha_hora_anulacion
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.fecha_hora_pagado = source.fecha_hora_pagado,
# MAGIC     target.Nacionalidad1 = source.Nacionalidad1,
# MAGIC     target.Residencia1 = source.Residencia1,
# MAGIC     target.br_rating = source.br_rating,
# MAGIC     target.br_score = source.br_score,
# MAGIC     target.Motivo_perdida_B2B = source.Motivo_perdida_B2B,
# MAGIC     target.Motivo_perdida_B2C = source.Motivo_perdida_B2C,
# MAGIC     target.Probabilidad = source.Probabilidad,
# MAGIC     target.Pipeline = source.Pipeline,
# MAGIC     target.Profesion_Estudiante = source.Profesion_Estudiante,
# MAGIC     target.Competencia = source.Competencia,
# MAGIC     target.Tipologia_cliente = source.Tipologia_cliente,
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
# MAGIC     target.deal_name = source.deal_name,
# MAGIC     target.fecha_Cierre = source.fecha_Cierre,
# MAGIC     target.Exchange_Rate = source.Exchange_Rate,
# MAGIC     target.Currency = source.Currency,
# MAGIC     target.Importe_pagado = source.Importe_pagado,
# MAGIC     target.Codigo_descuento = source.Codigo_descuento,
# MAGIC     target.Descuento = source.Descuento,
# MAGIC     target.importe = source.importe,
# MAGIC     target.Tipologia_alumno1 = source.Tipologia_alumno1,
# MAGIC     target.tipo_conversion = source.tipo_conversion,
# MAGIC     target.Created_Time = source.Created_Time,
# MAGIC     target.Modified_Time = source.Modified_Time,
# MAGIC     target.fecha_hora_anulacion = source.fecha_hora_anulacion
# MAGIC
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;
