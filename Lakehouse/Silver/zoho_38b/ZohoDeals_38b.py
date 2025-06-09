# Databricks notebook source
# MAGIC %run "../configuration" 

# COMMAND ----------

from pyspark.sql import SparkSession

table_prefix = "JsaZohoDeals_"
file_pattern = f"{bronze_folder_path}/lakehouse/zoho_38b/{current_date}/{table_prefix}*.json"

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

# Muestra el DataFrame procesado
display(zohodeals_df)

# COMMAND ----------

for col in zohodeals_df.columns:
    zohodeals_df = zohodeals_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

for col in zohodeals_df.columns:
    zohodeals_df = zohodeals_df.withColumnRenamed(col, col.replace("-", "_"))

display(zohodeals_df)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StringType

columns_mapping = {
    "data_id": "id",
    "data_id_producto": "id_producto",
    "data_amount": "importe",
    "data_c_digo_descuento": "codigo_descuento",
    "data_closing_date": "fecha_cierre",
    "data_competencia": "competencia",
    "data_created_time": "fecha_creacion",
    "data_deal_name": "nombre_oportunidad",
    "data_descuento": "descuento",
    "data_fecha_hora_anulaci_n": "fecha_hora_anulacion",
    "data_fecha_hora_documentaci_n_completada": "fecha_hora_documentacion_completada",
    "data_fecha_hora_pagado_ne": "fecha_hora_pagado",
    "data_id_classlife": "id_classlife",
    "data_id_lead": "id_lead",
    "data_importe_pagado": "importe_pagado",
    "data_l_nea_de_negocio": "linea_de_negocio",
    "data_modified_time": "fecha_modificacion",
    "data_motivo_p_rdida_b2b": "motivo_perdida_b2b",
    "data_motivo_p_rdida_b2c": "motivo_perdida_b2c",
    "data_nacionalidad1": "nacionalidad",
    "data_pipeline": "flujo_venta",
    "data_probability": "probabilidad_conversion",
    "data_profesion_estudiante": "profesion_estudiante",
    "data_residencia1": "residencia",
    "data_stage": "etapa",
    "data_tipolog_a_de_cliente": "tipologia_cliente",
    "data_tipolog_a_del_alumno1": "tipologia_alumno",
    "data_br_rating": "rating",
    "data_br_score": "scoring",
    "data_id_unico": "id_unico",
    "data_lead_correlation_id": "lead_correlation_id",
    "data_network": "network",
    "data_tipo_conversion": "tipo_conversion",
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
    "data_contact_name_id": "contact_name_id",
    "data_contact_name_name": "contact_name",
    "data_owner_email": "owner_email",
    "data_owner_id": "owner_id",
    "data_owner_name": "owner_name"
}

# Renombrar columnas si existen
for old, new in columns_mapping.items():
    if old in zohodeals_df.columns:
        zohodeals_df = zohodeals_df.withColumnRenamed(old, new)

# Cast y columnas adicionales
for col_name in columns_mapping.values():
    zohodeals_df = zohodeals_df.withColumn(col_name, col(col_name).cast(StringType()))

zohodeals_df = zohodeals_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("zoho_deals_38b")) \
    .withColumn("linea_de_negocio",when(lower(col("linea_de_negocio")) == "metrodorafp", lit("METRODORA FP")).otherwise(col("linea_de_negocio"))) \
    .withColumn("linea_de_negocio",when(lower(col("linea_de_negocio")) == "océano", lit("OCEANO")).otherwise(col("linea_de_negocio")))

display(zohodeals_df)

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
    (col("linea_de_negocio").isin("METRODORA FP", "OCEANO")) &  # Solo esos valores
    (col("linea_de_negocio").isNotNull()) &  # Que no sea NULL
    (col("linea_de_negocio") != "")  # Que no sea blanco
)

# Crear la vista temporal con datos ya filtrados
zohodeals_df_filtered.createOrReplaceTempView("zohodeals_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.zohodeals_38b AS target
# MAGIC USING zohodeals_source_view AS source
# MAGIC ON target.id = source.id
# MAGIC --AND target.id_producto = source.id_producto
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     source.importe IS DISTINCT FROM target.importe OR
# MAGIC     source.codigo_descuento IS DISTINCT FROM target.codigo_descuento OR
# MAGIC     source.fecha_cierre IS DISTINCT FROM target.fecha_cierre OR
# MAGIC     source.competencia IS DISTINCT FROM target.competencia OR
# MAGIC     source.fecha_creacion IS DISTINCT FROM target.fecha_creacion OR
# MAGIC     source.nombre_oportunidad IS DISTINCT FROM target.nombre_oportunidad OR
# MAGIC     source.descuento IS DISTINCT FROM target.descuento OR
# MAGIC     source.fecha_hora_anulacion IS DISTINCT FROM target.fecha_hora_anulacion OR
# MAGIC     source.fecha_hora_documentacion_completada IS DISTINCT FROM target.fecha_hora_documentacion_completada OR
# MAGIC     source.fecha_hora_pagado IS DISTINCT FROM target.fecha_hora_pagado OR
# MAGIC     source.id_classlife IS DISTINCT FROM target.id_classlife OR
# MAGIC     source.id_lead IS DISTINCT FROM target.id_lead OR
# MAGIC     source.importe_pagado IS DISTINCT FROM target.importe_pagado OR
# MAGIC     source.linea_de_negocio IS DISTINCT FROM target.linea_de_negocio OR
# MAGIC     source.fecha_modificacion IS DISTINCT FROM target.fecha_modificacion OR
# MAGIC     source.motivo_perdida_b2b IS DISTINCT FROM target.motivo_perdida_b2b OR
# MAGIC     source.motivo_perdida_b2c IS DISTINCT FROM target.motivo_perdida_b2c OR
# MAGIC     source.nacionalidad IS DISTINCT FROM target.nacionalidad OR
# MAGIC     source.flujo_venta IS DISTINCT FROM target.flujo_venta OR
# MAGIC     source.probabilidad_conversion IS DISTINCT FROM target.probabilidad_conversion OR
# MAGIC     source.profesion_estudiante IS DISTINCT FROM target.profesion_estudiante OR
# MAGIC     source.residencia IS DISTINCT FROM target.residencia OR
# MAGIC     source.etapa IS DISTINCT FROM target.etapa OR
# MAGIC     source.tipologia_cliente IS DISTINCT FROM target.tipologia_cliente OR
# MAGIC     source.tipologia_alumno IS DISTINCT FROM target.tipologia_alumno OR
# MAGIC     source.rating IS DISTINCT FROM target.rating OR
# MAGIC     source.scoring IS DISTINCT FROM target.scoring OR
# MAGIC     source.id_unico IS DISTINCT FROM target.id_unico OR
# MAGIC     source.lead_correlation_id IS DISTINCT FROM target.lead_correlation_id OR
# MAGIC     source.network IS DISTINCT FROM target.network OR
# MAGIC     source.tipo_conversion IS DISTINCT FROM target.tipo_conversion OR
# MAGIC     source.utm_ad_id IS DISTINCT FROM target.utm_ad_id OR
# MAGIC     source.utm_adset_id IS DISTINCT FROM target.utm_adset_id OR
# MAGIC     source.utm_campaign_id IS DISTINCT FROM target.utm_campaign_id OR
# MAGIC     source.utm_campaign_name IS DISTINCT FROM target.utm_campaign_name OR
# MAGIC     source.utm_channel IS DISTINCT FROM target.utm_channel OR
# MAGIC     source.utm_estrategia IS DISTINCT FROM target.utm_estrategia OR
# MAGIC     source.utm_medium IS DISTINCT FROM target.utm_medium OR
# MAGIC     source.utm_perfil IS DISTINCT FROM target.utm_perfil OR
# MAGIC     source.utm_source IS DISTINCT FROM target.utm_source OR
# MAGIC     source.utm_term IS DISTINCT FROM target.utm_term OR
# MAGIC     source.utm_type IS DISTINCT FROM target.utm_type OR
# MAGIC     source.contact_name_id IS DISTINCT FROM target.contact_name_id OR
# MAGIC     source.contact_name IS DISTINCT FROM target.contact_name OR
# MAGIC     source.owner_email IS DISTINCT FROM target.owner_email OR
# MAGIC     source.owner_id IS DISTINCT FROM target.owner_id OR
# MAGIC     source.owner_name IS DISTINCT FROM target.owner_name
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.importe = source.importe,
# MAGIC     target.codigo_descuento = source.codigo_descuento,
# MAGIC     target.fecha_cierre = source.fecha_cierre,
# MAGIC     target.competencia = source.competencia,
# MAGIC     target.fecha_creacion = source.fecha_creacion,
# MAGIC     target.nombre_oportunidad = source.nombre_oportunidad,
# MAGIC     target.descuento = source.descuento,
# MAGIC     target.fecha_hora_anulacion = source.fecha_hora_anulacion,
# MAGIC     target.fecha_hora_documentacion_completada = source.fecha_hora_documentacion_completada,
# MAGIC     target.fecha_hora_pagado = source.fecha_hora_pagado,
# MAGIC     target.id_classlife = source.id_classlife,
# MAGIC     target.id_lead = source.id_lead,
# MAGIC     target.importe_pagado = source.importe_pagado,
# MAGIC     target.linea_de_negocio = source.linea_de_negocio,
# MAGIC     target.fecha_modificacion = source.fecha_modificacion,
# MAGIC     target.motivo_perdida_b2b = source.motivo_perdida_b2b,
# MAGIC     target.motivo_perdida_b2c = source.motivo_perdida_b2c,
# MAGIC     target.nacionalidad = source.nacionalidad,
# MAGIC     target.flujo_venta = source.flujo_venta,
# MAGIC     target.probabilidad_conversion = source.probabilidad_conversion,
# MAGIC     target.profesion_estudiante = source.profesion_estudiante,
# MAGIC     target.residencia = source.residencia,
# MAGIC     target.etapa = source.etapa,
# MAGIC     target.tipologia_cliente = source.tipologia_cliente,
# MAGIC     target.tipologia_alumno = source.tipologia_alumno,
# MAGIC     target.rating = source.rating,
# MAGIC     target.scoring = source.scoring,
# MAGIC     target.id_unico = source.id_unico,
# MAGIC     target.lead_correlation_id = source.lead_correlation_id,
# MAGIC     target.network = source.network,
# MAGIC     target.tipo_conversion = source.tipo_conversion,
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
# MAGIC     target.contact_name_id = source.contact_name_id,
# MAGIC     target.contact_name = source.contact_name,
# MAGIC     target.owner_email = source.owner_email,
# MAGIC     target.owner_id = source.owner_id,
# MAGIC     target.owner_name = source.owner_name,
# MAGIC     target.processdate = current_timestamp(),
# MAGIC     target.sourcesystem = source.sourcesystem
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;

# COMMAND ----------

#%sql select distinct linea_de_negocio from silver_lakehouse.zohodeals_38b 
