# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

table_name = "JsaZohoDeals"

zohodeals_df = spark.read.json(f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_name}.json")
zohodeals_df

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

# Mostrar el DataFrame resultante
display(zohodeals_df)

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

# Mostrar el DataFrame final
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

# Muestra el DataFrame resultante
display(zohodeals_df)

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
# MAGIC MERGE INTO silver_lakehouse.zohodeals
# MAGIC USING zohodeals_source_view
# MAGIC ON silver_lakehouse.zohodeals.id = zohodeals_source_view.id
# MAGIC AND silver_lakehouse.zohodeals.id_producto = zohodeals_source_view.id_producto
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
