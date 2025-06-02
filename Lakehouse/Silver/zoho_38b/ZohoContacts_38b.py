# Databricks notebook source
# MAGIC %run "../configuration" 

# COMMAND ----------

from pyspark.sql import SparkSession

table_prefix = "JsaZohoContacts_"
file_pattern = f"{bronze_folder_path}/lakehouse/zoho_38b/{current_date}/{table_prefix}*.json"

print(f"Leyendo archivos desde: {file_pattern}")

zohocontacts_df = spark.read.json(file_pattern)

# COMMAND ----------

zohocontacts_df = zohocontacts_df.select("data")

# COMMAND ----------

zohocontacts_df = flatten(zohocontacts_df)

# COMMAND ----------

# Imprime las columnas disponibles antes de procesarlas
print("Columnas disponibles en el DataFrame:")
print(zohocontacts_df.columns)

# Renombra columnas, asegurándote de que las columnas existen
for col_name in zohocontacts_df.columns:
    new_col_name = col_name.replace("data_", "")#.replace("users_", "")
    zohocontacts_df = zohocontacts_df.withColumnRenamed(col_name, new_col_name)

# COMMAND ----------

# DBTITLE 1,Columnas a minusculas
for col in zohocontacts_df.columns:
    zohocontacts_df = zohocontacts_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

# Diccionario de mapeo de columnas a nombres más entendibles
columns_mapping = {
    "apellidos_2": "last_name",
    "dni": "dni",
    "date_of_birth": "date_birth",
    "email": "email",
    "first_name": "first_name",
    "id_classlife": "id_classlife",
    "mailing_city": "mailing_city",
    "mailing_street": "mailing_street",
    "mailing_zip": "mailing_zip",
    "mobile": "mobile",
    "modified_time": "modified_time",
    "nacionalidad": "nacionalidad",
    "other_city": "other_city",
    "other_country": "other_country",
    "other_state": "other_state",
    "other_street": "other_street",
    "other_zip": "other_zip",
    "phone": "phone",
    "residencia": "residencia",
    "sexo": "sexo",
    "tipo_de_cliente": "tipo_cliente",
    "tipo_de_contacto": "tipo_contacto",
    "id": "id",
    "ltima_l_nea_de_negocio": "ultima_linea_de_negocio"
}

# Añadir columnas de trazabilidad
zohocontacts_df = zohocontacts_df \
    .withColumn("sourcesystem", lit("zoho_contacts_38b")) \
    .withColumn("processdate", current_timestamp())

# Renombrar las columnas dinámicamente si existen en el DataFrame
for old_col, new_col in columns_mapping.items():
    if old_col in zohocontacts_df.columns:
        zohocontacts_df = zohocontacts_df.withColumnRenamed(old_col, new_col)

columnas_finales = list(columns_mapping.values()) + ["sourcesystem", "processdate"]

# Seleccionar solo esas columnas
zohocontacts_df = zohocontacts_df.select(*columnas_finales)

# Mostrar resultado final
display(zohocontacts_df)

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, col

# Reemplaza valores nulos en columnas basadas en sus tipos de datos para zohocontacts_df
for t in zohocontacts_df.dtypes:
    column_name = t[0]
    column_type = t[1]
    
    if column_type == 'string':
        zohocontacts_df = zohocontacts_df.withColumn(column_name, coalesce(col(column_name), lit('')))
    elif column_type in ['double', 'float']:
        zohocontacts_df = zohocontacts_df.withColumn(column_name, coalesce(col(column_name), lit(0.0)))
    elif column_type in ['int', 'bigint']:
        zohocontacts_df = zohocontacts_df.withColumn(column_name, coalesce(col(column_name), lit(0)))
    elif column_type == 'boolean':
        zohocontacts_df = zohocontacts_df.withColumn(column_name, coalesce(col(column_name), lit(False)))
    elif column_type in ['timestamp', 'date']:
        # Para fechas y timestamps dejamos `None` explícitamente
        zohocontacts_df = zohocontacts_df.withColumn(column_name, coalesce(col(column_name), lit(None)))

# Mostrar el DataFrame resultante
display(zohocontacts_df)

# COMMAND ----------

zohocontacts_df = zohocontacts_df.dropDuplicates()

# COMMAND ----------

zohocontacts_df.createOrReplaceTempView("zohocontacts_source_view")

zohocontacts_df_filtered = zohocontacts_df.filter(
    (col("ultima_linea_de_negocio").isin("MetrodoraFP", "Oceano")) &  # Solo esos valores
    (col("ultima_linea_de_negocio").isNotNull()) &  # Que no sea NULL
    (col("ultima_linea_de_negocio") != "")  # Que no sea blanco
)

# Crear la vista temporal con datos ya filtrados
zohocontacts_df_filtered.createOrReplaceTempView("zohocontacts_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.ZohoContacts_38b AS target
# MAGIC USING zohocontacts_source_view AS source
# MAGIC ON target.id = source.id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        target.last_name IS DISTINCT FROM source.last_name
# MAGIC     OR target.dni IS DISTINCT FROM source.dni
# MAGIC     OR target.date_birth IS DISTINCT FROM source.date_birth
# MAGIC     OR target.email IS DISTINCT FROM source.email
# MAGIC     OR target.first_name IS DISTINCT FROM source.first_name
# MAGIC     OR target.id_classlife IS DISTINCT FROM source.id_classlife
# MAGIC     OR target.mailing_city IS DISTINCT FROM source.mailing_city
# MAGIC     OR target.mailing_street IS DISTINCT FROM source.mailing_street
# MAGIC     OR target.mailing_zip IS DISTINCT FROM source.mailing_zip
# MAGIC     OR target.mobile IS DISTINCT FROM source.mobile
# MAGIC     OR target.modified_time IS DISTINCT FROM source.modified_time
# MAGIC     OR target.nacionalidad IS DISTINCT FROM source.nacionalidad
# MAGIC     OR target.other_city IS DISTINCT FROM source.other_city
# MAGIC     OR target.other_country IS DISTINCT FROM source.other_country
# MAGIC     OR target.other_state IS DISTINCT FROM source.other_state
# MAGIC     OR target.other_street IS DISTINCT FROM source.other_street
# MAGIC     OR target.other_zip IS DISTINCT FROM source.other_zip
# MAGIC     OR target.phone IS DISTINCT FROM source.phone
# MAGIC     OR target.residencia IS DISTINCT FROM source.residencia
# MAGIC     OR target.sexo IS DISTINCT FROM source.sexo
# MAGIC     OR target.tipo_cliente IS DISTINCT FROM source.tipo_cliente
# MAGIC     OR target.tipo_contacto IS DISTINCT FROM source.tipo_contacto
# MAGIC     OR target.ultima_linea_de_negocio IS DISTINCT FROM source.ultima_linea_de_negocio
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     last_name = source.last_name,
# MAGIC     dni = source.dni,
# MAGIC     date_birth = source.date_birth,
# MAGIC     email = source.email,
# MAGIC     first_name = source.first_name,
# MAGIC     id_classlife = source.id_classlife,
# MAGIC     mailing_city = source.mailing_city,
# MAGIC     mailing_street = source.mailing_street,
# MAGIC     mailing_zip = source.mailing_zip,
# MAGIC     mobile = source.mobile,
# MAGIC     modified_time = source.modified_time,
# MAGIC     nacionalidad = source.nacionalidad,
# MAGIC     other_city = source.other_city,
# MAGIC     other_country = source.other_country,
# MAGIC     other_state = source.other_state,
# MAGIC     other_street = source.other_street,
# MAGIC     other_zip = source.other_zip,
# MAGIC     phone = source.phone,
# MAGIC     residencia = source.residencia,
# MAGIC     sexo = source.sexo,
# MAGIC     tipo_cliente = source.tipo_cliente,
# MAGIC     tipo_contacto = source.tipo_contacto,
# MAGIC     ultima_linea_de_negocio = source.ultima_linea_de_negocio,
# MAGIC     sourcesystem = source.sourcesystem,
# MAGIC     processdate = source.processdate
# MAGIC     
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lakehouse.ZohoContacts_38b
