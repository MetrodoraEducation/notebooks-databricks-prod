# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "admissions"
table_name = "JsaClassLifeAdmissions"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife/{endpoint_process_name}/{current_date}/{table_name}.json")

# COMMAND ----------

# MAGIC %md
# MAGIC **Pasos principales:**
# MAGIC - Limpiar el esquema completo del DataFrame antes de trabajar con las columnas anidadas.
# MAGIC - Desanidar las estructuras una por una asegurando que no existan conflictos.
# MAGIC - Revisar si existen estructuras complejas adicionales que deban manejarse de forma especial.

# COMMAND ----------

# 📌 Función para limpiar nombres de columnas
def clean_column_names(df):
    """
    Limpia los nombres de columnas eliminando espacios, tildes y caracteres especiales.
    """
    for old_col in df.columns:
        new_col = (
            old_col.lower()
            .replace(" ", "_")
            .replace(".", "_")
            .replace("ñ", "n")
            .replace("ó", "o")
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ú", "u")
        )
        df = df.withColumnRenamed(old_col, new_col)
    
    return df

# COMMAND ----------

# 📌 Extraer el contenido de `data` si existe
if "data" in classlifetitulaciones_df.columns:
    classlifetitulaciones_df = classlifetitulaciones_df.selectExpr("data.*")


# COMMAND ----------

# 📌 Explotar `items` si es un array
if "items" in classlifetitulaciones_df.columns:
    print("📌 'items' es una estructura o array. Procedemos a desanidar.")

    # Si `items` es un array de estructuras, lo explotamos
    if isinstance(classlifetitulaciones_df.schema["items"].dataType, ArrayType):
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn("items", explode(col("items")))

# COMMAND ----------

# DBTITLE 1,Desanida Items
# 📌 Extraer subcolumnas de `items`
if "items" in classlifetitulaciones_df.columns:
    subcolumns = classlifetitulaciones_df.select("items.*").columns  # Obtener nombres originales
    
    # 📌 Limpieza de nombres de columnas
    clean_subcolumns = [
        f"items.`{col_name}`" if " " in col_name or "." in col_name else f"items.{col_name}"
        for col_name in subcolumns
    ]

    # 📌 Extraer columnas de `items` y renombrarlas
    classlifetitulaciones_df = classlifetitulaciones_df.select(*[col(c).alias(c.replace("items.", "")) for c in clean_subcolumns])

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType

def clean_column_names(df):
    """
    Limpia los nombres de columnas eliminando espacios, tildes, caracteres especiales,
    y asegurando un formato estándar.
    """
    cleaned_columns = {}
    
    for old_col in df.columns:
        new_col = (
            old_col.lower()
            .strip()  # 📌 Elimina espacios al inicio y fin
            .replace(" ", "_")
            .replace(".", "_")
            .replace("ñ", "n")
            .replace("ó", "o")
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ú", "u")
            .replace("`", "")
            .replace("metas_", "")
            .replace("no__", "no_")
            .replace("admission_", "")
        )

        # Evitar nombres duplicados
        if new_col in cleaned_columns.values():
            print(f"⚠️ Nombre duplicado detectado: {new_col}, renombrando...")
            new_col += "_2"

        cleaned_columns[old_col] = new_col

    # Aplicar los cambios
    for old_col, new_col in cleaned_columns.items():
        df = df.withColumnRenamed(old_col, new_col)

    return df

# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# 📌 Verificar si `tarifa_matricula` existe en el DataFrame después de la limpieza
columnas_actuales = set(classlifetitulaciones_df.columns)

# 📌 Seleccionar solo las columnas válidas si existen
columnas_seleccionadas = list(columnas_actuales)

# 📌 Solución para nombres con comillas invertidas
# Aplicamos alias() para normalizar los nombres con comillas invertidas
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).alias(c.strip().replace("`", "")) for c in columnas_seleccionadas]
)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType, IntegerType, DoubleType

# 📌 Aplicar transformaciones a las columnas con nombres corregidos y tipos de datos adecuados
classlifetitulaciones_df = classlifetitulaciones_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("AdmissionsSystem")) \
    .withColumn("student_phone", col("student_phone").cast(StringType())) \
    .withColumn("comercial", col("comercial").cast(StringType())) \
    .withColumn("student_email", col("student_email").cast(StringType())) \
    .withColumn("ini_date", col("ini_date").cast(StringType())) \
    .withColumn("zoho_deal_id", col("zoho_deal_id").cast(StringType())) \
    .withColumn("enroll_group", col("enroll_group").cast(StringType())) \
    .withColumn("ciclo_title", col("ciclo_title").cast(StringType())) \
    .withColumn("id", col("id").cast(StringType())) \
    .withColumn("student_dni", col("student_dni").cast(StringType())) \
    .withColumn("registration_date", col("registration_date").cast(StringType())) \
    .withColumn("year_id", col("year_id").cast(StringType())) \
    .withColumn("student_full_name", col("student_full_name").cast(StringType())) \
    .withColumn("area_title", col("area_title").cast(StringType())) \
    .withColumn("school_name", col("school_name").cast(StringType())) \
    .withColumn("end_date", col("end_date").cast(StringType())) \
    .select(
        "processdate",
        "sourcesystem",
        "student_phone",
        "comercial",
        "student_email",
        "ini_date",
        "zoho_deal_id",
        "enroll_group",
        "ciclo_title",
        "id",
        "student_dni",
        "registration_date",
        "year_id",
        "student_full_name",
        "area_title",
        "school_name",
        "end_date"
    )

# 📌 Mostrar solo las columnas seleccionadas
#display(classlifetitulaciones_df)

# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.dropDuplicates()

# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, COUNT(*)
# MAGIC FROM classlifetitulaciones_view
# MAGIC GROUP BY id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.ClasslifeAdmissions AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.id = source.id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        target.student_phone IS DISTINCT FROM source.student_phone
# MAGIC     OR target.comercial IS DISTINCT FROM source.comercial
# MAGIC     OR target.student_email IS DISTINCT FROM source.student_email
# MAGIC     OR target.ini_date IS DISTINCT FROM source.ini_date
# MAGIC     OR target.zoho_deal_id IS DISTINCT FROM source.zoho_deal_id
# MAGIC     OR target.enroll_group IS DISTINCT FROM source.enroll_group
# MAGIC     OR target.ciclo_title IS DISTINCT FROM source.ciclo_title
# MAGIC     OR target.student_dni IS DISTINCT FROM source.student_dni
# MAGIC     OR target.registration_date IS DISTINCT FROM source.registration_date
# MAGIC     OR target.year_id IS DISTINCT FROM source.year_id
# MAGIC     OR target.student_full_name IS DISTINCT FROM source.student_full_name
# MAGIC     OR target.area_title IS DISTINCT FROM source.area_title
# MAGIC     OR target.school_name IS DISTINCT FROM source.school_name
# MAGIC     OR target.end_date IS DISTINCT FROM source.end_date
# MAGIC     OR target.sourcesystem IS DISTINCT FROM source.sourcesystem
# MAGIC ) THEN 
# MAGIC     UPDATE SET
# MAGIC        student_phone       = source.student_phone,
# MAGIC        comercial           = source.comercial,
# MAGIC        student_email       = source.student_email,
# MAGIC        ini_date            = source.ini_date,
# MAGIC        zoho_deal_id        = source.zoho_deal_id,
# MAGIC        enroll_group        = source.enroll_group,
# MAGIC        ciclo_title         = source.ciclo_title,
# MAGIC        student_dni         = source.student_dni,
# MAGIC        registration_date   = source.registration_date,
# MAGIC        year_id             = source.year_id,
# MAGIC        student_full_name   = source.student_full_name,
# MAGIC        area_title          = source.area_title,
# MAGIC        school_name         = source.school_name,
# MAGIC        end_date            = source.end_date,
# MAGIC        sourcesystem        = source.sourcesystem
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         student_phone, comercial, student_email, ini_date, zoho_deal_id,
# MAGIC         enroll_group, ciclo_title, id, student_dni, registration_date,
# MAGIC         year_id, student_full_name, area_title, school_name, end_date
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.student_phone, source.comercial, source.student_email, source.ini_date, source.zoho_deal_id,
# MAGIC         source.enroll_group, source.ciclo_title, source.id, source.student_dni, source.registration_date,
# MAGIC         source.year_id, source.student_full_name, source.area_title, source.school_name, source.end_date
# MAGIC     );
