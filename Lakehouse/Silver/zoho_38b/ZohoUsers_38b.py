# Databricks notebook source
# MAGIC %run "../configuration"

# COMMAND ----------

table_name = "JsaZohoUsers"

#zohousers_df = spark.read.json(f"{bronze_folder_path}/lakehouse/zoho_38b/{current_date}/{table_name}.json")
zohousers_df = spark.read.json(f"{bronze_folder_path}/lakehouse/zoho_38b/Users/{current_date}/{table_name}.json")
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

from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType, BooleanType

# 🧱 Aplicar transformaciones y seleccionar SOLO columnas indicadas
zohousers_df = zohousers_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("zoho_Users_38b")) \
    .withColumn("status", col("Estado").cast(StringType())) \
    .withColumn("isonline", col("Isonline").cast(BooleanType())) \
    .withColumn("linea_de_negocio", col("L_nea_de_Negocio").cast(StringType())) \
    .withColumn("linea_de_negocio", when(lower(col("linea_de_negocio")) == "metrodorafp", lit("METRODORA FP"))
                                  .when(lower(col("linea_de_negocio")) == "océano", lit("OCEANO"))
                                  .when(lower(col("linea_de_negocio")) == "oceano", lit("OCEANO"))
                                  .otherwise(col("linea_de_negocio"))) \
    .withColumn("modified_time", to_timestamp(col("Modified_Time"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("created_time", to_timestamp(col("created_time"), "yyyy-MM-dd'T'HH:mm:ssXXX")) \
    .withColumn("city", col("city").cast(StringType())) \
    .withColumn("confirm", col("confirm").cast(BooleanType())) \
    .withColumn("country", col("country").cast(StringType())) \
    .withColumn("country_locale", col("country_locale").cast(StringType())) \
    .withColumn("email", col("email").cast(StringType())) \
    .withColumn("first_name", col("first_name").cast(StringType())) \
    .withColumn("full_name", col("full_name").cast(StringType())) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("language", col("language").cast(StringType())) \
    .withColumn("last_name", col("last_name").cast(StringType())) \
    .withColumn("mobile", col("mobile").cast(StringType())) \
    .withColumn("modified_by_id", col("Modified_By_id").cast(StringType())) \
    .withColumn("modified_by_name", col("Modified_By_name").cast(StringType())) \
    .withColumn("created_by_id", col("created_by_id").cast(StringType())) \
    .withColumn("created_by_name", col("created_by_name").cast(StringType())) \
    .withColumn("role_id", col("role_id").cast(StringType())) \
    .withColumn("role_name", col("role_name").cast(StringType())) \
    .withColumn("Sede", col("Sede").cast(StringType())) \
    .select(
         "id", "status", "isonline", "linea_de_negocio", "modified_time", "created_time", "city", "confirm",
        "country", "country_locale", "email", "first_name", "full_name", "language", "last_name",
        "mobile", "modified_by_id", "modified_by_name", "created_by_id", "created_by_name",
        "role_id", "role_name", "processdate", "sourcesystem", "Sede"
    )

# 👁️ Mostrar resultado
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

zohousers_df = zohousers_df.filter(
    (col("linea_de_negocio").isin("METRODORA FP", "OCEANO")) &  # Solo esos valores
    (col("linea_de_negocio").isNotNull()) &  # Que no sea NULL
    (col("linea_de_negocio") != "")  # Que no sea blanco
)

# Crear la vista temporal con datos ya filtrados
zohousers_df.createOrReplaceTempView("zohousers_source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.ZohoUsers_38b AS target
# MAGIC USING zohousers_source_view AS source
# MAGIC ON target.id = source.id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.status IS DISTINCT FROM source.status OR
# MAGIC     target.isonline IS DISTINCT FROM source.isonline OR
# MAGIC     target.linea_de_negocio IS DISTINCT FROM source.linea_de_negocio OR
# MAGIC     target.modified_time IS DISTINCT FROM source.modified_time OR
# MAGIC     target.created_time IS DISTINCT FROM source.created_time OR
# MAGIC     target.city IS DISTINCT FROM source.city OR
# MAGIC     target.confirm IS DISTINCT FROM source.confirm OR
# MAGIC     target.country IS DISTINCT FROM source.country OR
# MAGIC     target.country_locale IS DISTINCT FROM source.country_locale OR
# MAGIC     target.email IS DISTINCT FROM source.email OR
# MAGIC     target.first_name IS DISTINCT FROM source.first_name OR
# MAGIC     target.full_name IS DISTINCT FROM source.full_name OR
# MAGIC     target.language IS DISTINCT FROM source.language OR
# MAGIC     target.last_name IS DISTINCT FROM source.last_name OR
# MAGIC     target.mobile IS DISTINCT FROM source.mobile OR
# MAGIC     target.modified_by_id IS DISTINCT FROM source.modified_by_id OR
# MAGIC     target.modified_by_name IS DISTINCT FROM source.modified_by_name OR
# MAGIC     target.created_by_id IS DISTINCT FROM source.created_by_id OR
# MAGIC     target.created_by_name IS DISTINCT FROM source.created_by_name OR
# MAGIC     target.role_id IS DISTINCT FROM source.role_id OR
# MAGIC     target.role_name IS DISTINCT FROM source.role_name OR
# MAGIC     target.sede IS DISTINCT FROM source.sede
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.status = source.status,
# MAGIC     target.isonline = source.isonline,
# MAGIC     target.linea_de_negocio = source.linea_de_negocio,
# MAGIC     target.modified_time = source.modified_time,
# MAGIC     target.created_time = source.created_time,
# MAGIC     target.city = source.city,
# MAGIC     target.confirm = source.confirm,
# MAGIC     target.country = source.country,
# MAGIC     target.country_locale = source.country_locale,
# MAGIC     target.email = source.email,
# MAGIC     target.first_name = source.first_name,
# MAGIC     target.full_name = source.full_name,
# MAGIC     target.language = source.language,
# MAGIC     target.last_name = source.last_name,
# MAGIC     target.mobile = source.mobile,
# MAGIC     target.modified_by_id = source.modified_by_id,
# MAGIC     target.modified_by_name = source.modified_by_name,
# MAGIC     target.created_by_id = source.created_by_id,
# MAGIC     target.created_by_name = source.created_by_name,
# MAGIC     target.role_id = source.role_id,
# MAGIC     target.role_name = source.role_name,
# MAGIC     target.processdate = source.processdate,
# MAGIC     target.sourcesystem = source.sourcesystem,
# MAGIC     target.sede = source.sede
# MAGIC     
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql select * from silver_lakehouse.ZohoUsers_38b
