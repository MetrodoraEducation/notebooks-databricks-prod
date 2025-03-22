# Databricks notebook source
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

table_name = "JsaZohoUsers"

zohousers_df = spark.read.json(f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_name}.json")
zohousers_df

print(f"{bronze_folder_path}/lakehouse/zoho/{current_date}/{table_name}.json")

# COMMAND ----------

zohousers_df = zohousers_df.select("users")

# COMMAND ----------

zohousers_df = flatten(zohousers_df)

# COMMAND ----------

# Imprime las columnas disponibles antes de procesarlas
print("Columnas disponibles en el DataFrame:")
print(zohousers_df.columns)

# Renombra columnas, asegurándote de que las columnas existen
for col_name in zohousers_df.columns:
    new_col_name = col_name.replace("users_$", "").replace("users_", "")
    zohousers_df = zohousers_df.withColumnRenamed(col_name, new_col_name)

# Muestra el DataFrame procesado
display(zohousers_df)

# COMMAND ----------

# DBTITLE 1,Columnas a minusculas
for col in zohousers_df.columns:
    zohousers_df = zohousers_df.withColumnRenamed(col, col.lower())

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType, BooleanType, LongType

# 📌 Lista de columnas con sus tipos de datos correspondientes
columnas_con_tipo = [
    ("processdate", current_timestamp()),
    ("sourcesystem", lit("zoho_Users")),
    ("next_shift", col("next_shift").cast(StringType())),
    ("shift_effective_from", to_timestamp(col("shift_effective_from"), "yyyy-MM-dd'T'HH:mm:ssXXX")),
    ("currency", col("currency").cast(StringType())),
    ("isonline", col("isonline").cast(BooleanType())),
    ("modified_time", to_timestamp(col("modified_time"), "yyyy-MM-dd'T'HH:mm:ssXXX")),
    ("alias", col("alias").cast(StringType())),
    ("city", col("city").cast(StringType())),
    ("confirm", col("confirm").cast(BooleanType())),
    ("country", col("country").cast(StringType())),
    ("country_locale", col("country_locale").cast(StringType())),
    ("created_time", to_timestamp(col("created_time"), "yyyy-MM-dd'T'HH:mm:ssXXX")),
    ("date_format", col("date_format").cast(StringType())),
    ("decimal_separator", col("decimal_separator").cast(StringType())),
    ("default_tab_group", col("default_tab_group").cast(StringType())),
    ("dob", to_date(col("dob"), "yyyy-MM-dd")),
    ("email", col("email").cast(StringType())),
    ("fax", col("fax").cast(StringType())),
    ("first_name", col("first_name").cast(StringType())),
    ("full_name", col("full_name").cast(StringType())),
    ("id", col("id").cast(StringType())),
    ("language", col("language").cast(StringType())),
    ("last_name", col("last_name").cast(StringType())),
    ("locale", col("locale").cast(StringType())),
    ("microsoft", col("microsoft").cast(BooleanType())),
    ("mobile", col("mobile").cast(StringType())),
    ("number_separator", col("number_separator").cast(StringType())),
    ("offset", col("offset").cast(LongType())),
    ("personal_account", col("personal_account").cast(BooleanType())),
    ("phone", col("phone").cast(StringType())),
    ("sandboxdeveloper", col("sandboxdeveloper").cast(BooleanType())),
    ("signature", col("signature").cast(StringType())),
    ("state", col("state").cast(StringType())),
    ("status", col("status").cast(StringType())),
    ("street", col("street").cast(StringType())),
    ("time_format", col("time_format").cast(StringType())),
    ("time_zone", col("time_zone").cast(StringType())),
    ("website", col("website").cast(StringType())),
    ("zip", col("zip").cast(StringType())),
    ("zuid", col("zuid").cast(StringType())),
    ("modified_by_id", col("modified_by_id").cast(StringType())),
    ("modified_by_name", col("modified_by_name").cast(StringType())),
    ("created_by_id", col("created_by_id").cast(StringType())),
    ("created_by_name", col("created_by_name").cast(StringType())),
    ("profile_id", col("profile_id").cast(StringType())),
    ("profile_name", col("profile_name").cast(StringType())),
    ("role_id", col("role_id").cast(StringType())),
    ("role_name", col("role_name").cast(StringType())),
]

# 📌 Aplicar transformaciones y seleccionar solo las columnas especificadas
zohousers_df = zohousers_df.select(
    [expr.alias(nombre) for nombre, expr in columnas_con_tipo]
)

# 📌 Mostrar el DataFrame final
display(zohousers_df)

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit, col

# Reemplaza valores nulos en columnas basadas en sus tipos de datos para zohousers_df
for t in zohousers_df.dtypes:
    column_name = t[0]
    column_type = t[1]
    
    if column_type == 'string':
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit('')))
    elif column_type in ['double', 'float']:
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit(0.0)))
    elif column_type in ['int', 'bigint']:
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit(0)))
    elif column_type == 'boolean':
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit(False)))
    elif column_type in ['timestamp', 'date']:
        # Para fechas y timestamps dejamos `None` explícitamente
        zohousers_df = zohousers_df.withColumn(column_name, coalesce(col(column_name), lit(None)))

# Mostrar el DataFrame resultante
display(zohousers_df)

# COMMAND ----------

zohousers_df = zohousers_df.dropDuplicates()

# COMMAND ----------

zohousers_df.createOrReplaceTempView("zohousers_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.zohousers
# MAGIC USING zohousers_source_view
# MAGIC ON silver_lakehouse.zohousers.id = zohousers_source_view.id
# MAGIC    --AND silver_lakehouse.zohousers.email = zohousers_source_view.email
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
