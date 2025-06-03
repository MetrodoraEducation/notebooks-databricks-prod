# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../configuration"

# COMMAND ----------

endpoint_process_name = "students"
table_name = "JsaClassLifeStudents"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife_931/{endpoint_process_name}/{current_date}/{table_name}.json")

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

# DBTITLE 1,Explota data
# 📌 Extraer el contenido de `data` si existe
if "data" in classlifetitulaciones_df.columns:
    classlifetitulaciones_df = classlifetitulaciones_df.selectExpr("data.*")

# COMMAND ----------

# DBTITLE 1,Busca items
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

# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Desanida Metas
# 📌 Desanidar estructuras internas (`counters`, `metas`) si existen
if "counters" in classlifetitulaciones_df.columns:
    counters_cols = classlifetitulaciones_df.select("counters.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"counters.{c}").alias(f"counters_{c}") for c in counters_cols]).drop("counters")

if "metas" in classlifetitulaciones_df.columns:
    metas_cols = classlifetitulaciones_df.select("metas.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"metas.{c}").alias(f"metas_{c}") for c in metas_cols]).drop("metas")

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

# 📌 Inspeccionar nombres antes de la limpieza
print("📌 Columnas antes de renombramiento:")
for col_name in classlifetitulaciones_df.columns:
    print(f"- '{repr(col_name)}'")  # 📌 Usa repr() para detectar caracteres invisibles

# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# 📌 Mostrar columnas después de la limpieza para verificar cambios
print("\n📌 Columnas después de renombramiento:")
for col_name in classlifetitulaciones_df.columns:
    print(f"- '{repr(col_name)}'")  # 📌 Usa repr() nuevamente para comparación

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

from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType

# Lista de columnas a seleccionar y normalizar
columnas_requeridas = [
    "paisnacimiento", "zenddesk_id", "factura_correo", "student_language",
    "fiscal_codigo", "language", "nacimiento", "direccion", "ciudad", "factura_telefono",
    "student_phone", "fiscal_iban", "student_email", "student_uid", "fiscal_dpuerta",
    "student_active", "student_lastname", "zoho_id", "factura_dni", "sexo", "student_id",
    "school_id", "edad", "student_registration_date", "student_name", "student_full_name", "pais", "codigo"
]

# Seleccionar y castear columnas
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).cast(StringType()).alias(c) for c in columnas_requeridas],
    current_timestamp().alias("processdate"),
    lit("ClasslifeStudents_931").alias("sourcesystem")
)

# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.dropDuplicates()

# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.ClasslifeStudents_931 AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.student_id = source.student_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.paisnacimiento IS DISTINCT FROM source.paisnacimiento OR
# MAGIC     target.zenddesk_id IS DISTINCT FROM source.zenddesk_id OR
# MAGIC     target.factura_correo IS DISTINCT FROM source.factura_correo OR
# MAGIC     target.student_language IS DISTINCT FROM source.student_language OR
# MAGIC     target.fiscal_codigo IS DISTINCT FROM source.fiscal_codigo OR
# MAGIC     target.language IS DISTINCT FROM source.language OR
# MAGIC     target.nacimiento IS DISTINCT FROM source.nacimiento OR
# MAGIC     target.direccion IS DISTINCT FROM source.direccion OR
# MAGIC     target.ciudad IS DISTINCT FROM source.ciudad OR
# MAGIC     target.factura_telefono IS DISTINCT FROM source.factura_telefono OR
# MAGIC     target.student_phone IS DISTINCT FROM source.student_phone OR
# MAGIC     target.fiscal_iban IS DISTINCT FROM source.fiscal_iban OR
# MAGIC     target.student_email IS DISTINCT FROM source.student_email OR
# MAGIC     target.student_uid IS DISTINCT FROM source.student_uid OR
# MAGIC     target.fiscal_dpuerta IS DISTINCT FROM source.fiscal_dpuerta OR
# MAGIC     target.student_active IS DISTINCT FROM source.student_active OR
# MAGIC     target.student_lastname IS DISTINCT FROM source.student_lastname OR
# MAGIC     target.zoho_id IS DISTINCT FROM source.zoho_id OR
# MAGIC     target.factura_dni IS DISTINCT FROM source.factura_dni OR
# MAGIC     target.sexo IS DISTINCT FROM source.sexo OR
# MAGIC     target.school_id IS DISTINCT FROM source.school_id OR
# MAGIC     target.edad IS DISTINCT FROM source.edad OR
# MAGIC     target.student_registration_date IS DISTINCT FROM source.student_registration_date OR
# MAGIC     target.student_name IS DISTINCT FROM source.student_name OR
# MAGIC     target.student_full_name IS DISTINCT FROM source.student_full_name OR
# MAGIC     target.pais IS DISTINCT FROM source.pais OR
# MAGIC     target.codigo IS DISTINCT FROM source.codigo
# MAGIC )
# MAGIC THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
