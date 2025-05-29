# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../configuration"

# COMMAND ----------

endpoint_process_name = "enrollments"
table_name = "JsaClassLifeEnrollments"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife_931/{endpoint_process_name}/{current_date}/{table_name}.json")
classlifetitulaciones_df

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

# DBTITLE 1,Desanida Metas
from pyspark.sql.functions import col

# Verificar si 'metas' es una columna y si es de tipo StructType
if "metas" in classlifetitulaciones_df.columns:
    metas_schema = classlifetitulaciones_df.schema["metas"].dataType

    # Si 'metas' es una estructura (StructType), extraemos sus campos
    if hasattr(metas_schema, "fields"):
        metas_cols = [f.name for f in metas_schema.fields]  # Obtener nombres de campos

        # 📌 Filtrar las columnas que NO comienzan con 'wannme.api.request.response'
        metas_cols = [c for c in metas_cols if not c.startswith("wannme")]

        # 📌 Solo desanidamos las columnas que pasaron el filtro
        classlifetitulaciones_df = classlifetitulaciones_df.select(
            "*", 
            *[col(f"metas.{c}").alias(f"metas_{c}") for c in metas_cols]  
        ).drop("metas")

# COMMAND ----------

# DBTITLE 1,Desanida Api-Data
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# 1. Esquema del JSON metas_API-Data
api_data_schema = StructType() \
    .add("enroll_stage_id", StringType()) \
    .add("lastnameend", StringType()) \
    .add("address", StringType()) \
    .add("birthdate", StringType()) \
    .add("id_contacto", StringType()) \
    .add("apiKey", StringType()) \
    .add("city", StringType()) \
    .add("perform", StringType()) \
    .add("asesor", StringType()) \
    .add("id_oportunidad", StringType()) \
    .add("zip_code", StringType()) \
    .add("lastname", StringType()) \
    .add("password", StringType()) \
    .add("enroll_group_id", StringType()) \
    .add("phone", StringType()) \
    .add("name", StringType()) \
    .add("TipusDocument", StringType()) \
    .add("state_id", StringType()) \
    .add("email", StringType()) \
    .add("dni", StringType()) \
    .add("country_id", StringType()) \
    .add("last", StringType()) \
    .add("school_id", StringType())

# 2. Parsear JSON y desanidar campos
df_with_struct = classlifetitulaciones_df.withColumn(
    "metas_APIData_struct", from_json(col("metas_API-Data"), api_data_schema)
)

for field in api_data_schema.fieldNames():
    df_with_struct = df_with_struct.withColumn(
        f"metas_APIData_{field}",
        col(f"metas_APIData_struct.{field}")
    )

df_result = df_with_struct.drop("metas_API-Data", "metas_APIData_struct")

# 3. Función para limpiar nombres de columnas
def clean_column_names(df):
    cleaned_columns = {}

    for old_col in df.columns:
        new_col = (
            old_col.lower()
            .strip()
            .replace(" ", "_")
            .replace(".", "_")
            .replace("ñ", "n")
            .replace("ó", "o")
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ú", "u")
            .replace("metas_", "")  # Elimina prefijo si existe
            .replace("no__", "no_")
            .replace("fees_", "")
            .replace("apidata_", "")
        )

        if new_col in cleaned_columns.values():
            print(f"⚠️ Nombre duplicado detectado: {new_col}, renombrando...")
            new_col += "_2"

        cleaned_columns[old_col] = new_col

    for old_col, new_col in cleaned_columns.items():
        df = df.withColumnRenamed(old_col, new_col)

    return df

# 4. Aplicar limpieza de columnas
print("📌 Columnas antes de renombramiento:")
for col_name in df_result.columns:
    print(f"- {repr(col_name)}")

df_result_clean = clean_column_names(df_result)

print("\n📌 Columnas después de renombramiento:")
for col_name in df_result_clean.columns:
    print(f"- {repr(col_name)}")

# 5. Asegurar que los nombres no tienen caracteres invisibles
df_result_clean = df_result_clean.select(
    *[col(c).alias(c.strip().replace("`", "")) for c in df_result_clean.columns]
)

# COMMAND ----------

# DBTITLE 1,Asignar Columnas
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

# 🧱 Columnas requeridas
columnas_requeridas = [
    "area_id", "area_title", "ciclo_id", "ciclo_title",
    "degree_id", "degree_title", "enroll_alias", "enroll_end", "enroll_group",
    "enroll_id", "enroll_in", "enroll_ini", "enroll_status", "enroll_status_id",
    "school_id", "school_name", "section_id", "section_title", "student_full_name",
    "student_id", "term_id", "term_title", "updated_at", "year",
    "about_agent_code__c", "admisionesmodalidad", "totalenroll", "totalfinal",
    "totalfinaldocencia", "totalfinalmatricula", "id_contacto"
]

# 🧽 1. Limpiar nombres
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# 🧱 2. Crear columnas faltantes como null
for columna in columnas_requeridas:
    if columna not in classlifetitulaciones_df.columns:
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn(
            columna, lit(None).cast(StringType())
        )

# 🧠 3. Castear y seleccionar columnas requeridas
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).cast(StringType()).alias(c) for c in columnas_requeridas]
)

# 🧠 3. Seleccionar columnas y agregar las nuevas
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).cast(StringType()).alias(c) for c in columnas_requeridas],
    current_timestamp().alias("processdate"),
    lit("classlifeEnrollments_931").alias("sourcesystem")
)

# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.dropDuplicates()

# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT enroll_id, COUNT(*)
# MAGIC FROM classlifetitulaciones_view
# MAGIC GROUP BY enroll_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.ClasslifeEnrollments_931 AS target 
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.enroll_id = source.enroll_id
# MAGIC WHEN MATCHED AND (
# MAGIC     target.area_id IS DISTINCT FROM source.area_id OR
# MAGIC     target.area_title IS DISTINCT FROM source.area_title OR
# MAGIC     target.ciclo_id IS DISTINCT FROM source.ciclo_id OR
# MAGIC     target.ciclo_title IS DISTINCT FROM source.ciclo_title OR
# MAGIC     target.degree_id IS DISTINCT FROM source.degree_id OR
# MAGIC     target.degree_title IS DISTINCT FROM source.degree_title OR
# MAGIC     target.enroll_alias IS DISTINCT FROM source.enroll_alias OR
# MAGIC     target.enroll_end IS DISTINCT FROM source.enroll_end OR
# MAGIC     target.enroll_group IS DISTINCT FROM source.enroll_group OR
# MAGIC     target.enroll_in IS DISTINCT FROM source.enroll_in OR
# MAGIC     target.enroll_ini IS DISTINCT FROM source.enroll_ini OR
# MAGIC     target.enroll_status IS DISTINCT FROM source.enroll_status OR
# MAGIC     target.enroll_status_id IS DISTINCT FROM source.enroll_status_id OR
# MAGIC     target.school_id IS DISTINCT FROM source.school_id OR
# MAGIC     target.school_name IS DISTINCT FROM source.school_name OR
# MAGIC     target.section_id IS DISTINCT FROM source.section_id OR
# MAGIC     target.section_title IS DISTINCT FROM source.section_title OR
# MAGIC     target.student_full_name IS DISTINCT FROM source.student_full_name OR
# MAGIC     target.student_id IS DISTINCT FROM source.student_id OR
# MAGIC     target.term_id IS DISTINCT FROM source.term_id OR
# MAGIC     target.term_title IS DISTINCT FROM source.term_title OR
# MAGIC     target.updated_at IS DISTINCT FROM source.updated_at OR
# MAGIC     target.year IS DISTINCT FROM source.year OR
# MAGIC     target.about_agent_code__c IS DISTINCT FROM source.about_agent_code__c OR
# MAGIC     target.admisionesmodalidad IS DISTINCT FROM source.admisionesmodalidad OR
# MAGIC     target.totalenroll IS DISTINCT FROM source.totalenroll OR
# MAGIC     target.totalfinal IS DISTINCT FROM source.totalfinal OR
# MAGIC     target.totalfinaldocencia IS DISTINCT FROM source.totalfinaldocencia OR
# MAGIC     target.totalfinalmatricula IS DISTINCT FROM source.totalfinalmatricula OR
# MAGIC     target.id_contacto IS DISTINCT FROM source.id_contacto
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.area_id = source.area_id,
# MAGIC     target.area_title = source.area_title,
# MAGIC     target.ciclo_id = source.ciclo_id,
# MAGIC     target.ciclo_title = source.ciclo_title,
# MAGIC     target.degree_id = source.degree_id,
# MAGIC     target.degree_title = source.degree_title,
# MAGIC     target.enroll_alias = source.enroll_alias,
# MAGIC     target.enroll_end = source.enroll_end,
# MAGIC     target.enroll_group = source.enroll_group,
# MAGIC     target.enroll_id = source.enroll_id,
# MAGIC     target.enroll_in = source.enroll_in,
# MAGIC     target.enroll_ini = source.enroll_ini,
# MAGIC     target.enroll_status = source.enroll_status,
# MAGIC     target.enroll_status_id = source.enroll_status_id,
# MAGIC     target.school_id = source.school_id,
# MAGIC     target.school_name = source.school_name,
# MAGIC     target.section_id = source.section_id,
# MAGIC     target.section_title = source.section_title,
# MAGIC     target.student_full_name = source.student_full_name,
# MAGIC     target.student_id = source.student_id,
# MAGIC     target.term_id = source.term_id,
# MAGIC     target.term_title = source.term_title,
# MAGIC     target.updated_at = source.updated_at,
# MAGIC     target.year = source.year,
# MAGIC     target.about_agent_code__c = source.about_agent_code__c,
# MAGIC     target.admisionesmodalidad = source.admisionesmodalidad,
# MAGIC     target.totalenroll = source.totalenroll,
# MAGIC     target.totalfinal = source.totalfinal,
# MAGIC     target.totalfinaldocencia = source.totalfinaldocencia,
# MAGIC     target.totalfinalmatricula = source.totalfinalmatricula,
# MAGIC     target.id_contacto = source.id_contacto
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT enroll_id, COUNT(*)
# MAGIC FROM silver_lakehouse.ClasslifeEnrollments_931
# MAGIC GROUP BY enroll_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lakehouse.ClasslifeEnrollments_931;
