# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

table_name = "JsaZohoContacts"

zohocontacts_df = spark.read.json(f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_name}.json")
zohocontacts_df

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

# Muestra el DataFrame procesado
display(zohocontacts_df)

# COMMAND ----------

# DBTITLE 1,Columnas a minusculas
for col in zohocontacts_df.columns:
    zohocontacts_df = zohocontacts_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

# Diccionario para mapear las columnas directamente, manteniendo los nombres originales
columns_mapping = {
    "apellidos_2": "last_name",
    "dni": "dni",
    "date_of_birth": "date_birth",
    "email": "email",
    "estudios": "estudios",
    "first_name": "first_name",
    "home_phone1": "home_phone",
    "id_classlife": "id_classlife",
    "mailing_city": "mailing_city",
    "mailing_country": "mailing_country",
    "mailing_street": "mailing_street",
    "mailing_zip": "mailing_zip",
    "mobile": "mobile",
    "nacionalidad": "nacionalidad",
    "other_city": "other_city",
    "other_country": "other_country",
    "other_state": "other_state",
    "other_street": "other_street",
    "other_zip": "other_zip",
    "phone": "phone",
    "profesion": "profesion",
    "provincia": "provincia",
    "residencia": "residencia",
    "secondary_email": "secondary_email",
    "sexo": "sexo",
    "tipo_de_cliente": "tipo_cliente",
    "tipo_de_contacto": "tipo_contacto",
    "id": "id",
    "recibir_comunicacion": "recibir_comunicacion",
    "woztellplatformintegration__whatsapp_opt_out": "woztellplatform_whatsapp_out"
}

# Renombrar columnas dinámicamente (en este caso, no cambiarán porque el mapeo es 1 a 1)
for old_col, new_col in columns_mapping.items():
    if old_col in zohocontacts_df.columns:
        zohocontacts_df = zohocontacts_df.withColumnRenamed(old_col, new_col)

# Mostrar el DataFrame resultante
display(zohocontacts_df)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Ajuste del DataFrame con validación de columnas para zohocontacts
zohocontacts_df = zohocontacts_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("zoho_Contacts")) \
    .withColumn("last_name", col("last_name").cast(StringType())) \
    .withColumn("dni", col("dni").cast(StringType())) \
    .withColumn("date_birth", to_date(col("date_birth"), "yyyy-MM-dd")) \
    .withColumn("email", col("email").cast(StringType())) \
    .withColumn("estudios", col("estudios").cast(StringType())) \
    .withColumn("first_name", col("first_name").cast(StringType())) \
    .withColumn("home_phone", col("home_phone").cast(StringType())) \
    .withColumn("id_classlife", col("id_classlife").cast(StringType())) \
    .withColumn("mailing_city", col("mailing_city").cast(StringType())) \
    .withColumn("mailing_country", col("mailing_country").cast(StringType())) \
    .withColumn("mailing_street", col("mailing_street").cast(StringType())) \
    .withColumn("mailing_zip", col("mailing_zip").cast(StringType())) \
    .withColumn("mobile", col("mobile").cast(StringType())) \
    .withColumn("nacionalidad", col("nacionalidad").cast(StringType())) \
    .withColumn("other_city", col("other_city").cast(StringType())) \
    .withColumn("other_country", col("other_country").cast(StringType())) \
    .withColumn("other_state", col("other_state").cast(StringType())) \
    .withColumn("other_street", col("other_street").cast(StringType())) \
    .withColumn("other_zip", col("other_zip").cast(StringType())) \
    .withColumn("phone", col("phone").cast(StringType())) \
    .withColumn("profesion", col("profesion").cast(StringType())) \
    .withColumn("provincia", col("provincia").cast(StringType())) \
    .withColumn("residencia", col("residencia").cast(StringType())) \
    .withColumn("secondary_email", col("secondary_email").cast(StringType())) \
    .withColumn("sexo", col("sexo").cast(StringType())) \
    .withColumn("tipo_cliente", col("tipo_cliente").cast(StringType())) \
    .withColumn("tipo_contacto", col("tipo_contacto").cast(StringType())) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("recibir_comunicacion", col("recibir_comunicacion").cast(StringType())) \
    .withColumn("ultima_linea_de_negocio", col("ltima_l_nea_de_negocio").cast(StringType())).drop("ltima_l_nea_de_negocio") \
    .withColumn("woztellplatform_whatsapp_out", col("woztellplatform_whatsapp_out").cast(BooleanType()))

# Mostrar el DataFrame final
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
    (col("ultima_linea_de_negocio").isin("FisioFocus")) &  # Solo esos valores
    (col("ultima_linea_de_negocio").isNotNull()) &  # Que no sea NULL
    (col("ultima_linea_de_negocio") != "")  # Que no sea blanco
)

# Crear la vista temporal con datos ya filtrados
zohocontacts_df_filtered.createOrReplaceTempView("zohocontacts_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.zohocontacts
# MAGIC USING zohocontacts_source_view
# MAGIC ON silver_lakehouse.zohocontacts.id = zohocontacts_source_view.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
