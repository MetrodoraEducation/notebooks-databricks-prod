# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../configuration"

# COMMAND ----------

from pyspark.sql.functions import explode, col

endpoint_process_name = "students"
table_name = "JsaClassLifeStudents_"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife_931/{endpoint_process_name}/{current_date}/student_cleaned/{table_name}*.json")

print(f"Total de archivos leídos: {classlifetitulaciones_df.count()}")

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
            .replace("codipais", "codipais_1")
        )
        df = df.withColumnRenamed(old_col, new_col)
    
    return df

# COMMAND ----------

# DBTITLE 1,Explota data
# 📌 Extraer el contenido de `data` si existe
if "data" in classlifetitulaciones_df.columns:
    # Renaming columns to avoid duplicates
    data_columns = [f"data.{col} as data_{col}" for col in classlifetitulaciones_df.select("data.*").columns]
    classlifetitulaciones_df = classlifetitulaciones_df.selectExpr(*data_columns)

#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Busca items
## 📌 Explotar `items` si es un array
#if "data_items" in classlifetitulaciones_df.columns:
#    print("📌 'data_items' es una estructura o array. Procedemos a desanidar.")
#
#    # Si `data_items` es un array de estructuras, lo explotamos
#    if isinstance(classlifetitulaciones_df.schema["data_items"].dataType, ArrayType):
#        classlifetitulaciones_df = classlifetitulaciones_df.withColumn("data_items", explode(col("data_items")))
#
#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Desanida Items
## 📌 Extraer subcolumnas de `items`
#if "items" in classlifetitulaciones_df.columns:
#    subcolumns = classlifetitulaciones_df.select("items.*").columns  # Obtener nombres originales
#    
#    # 📌 Limpieza de nombres de columnas
#    clean_subcolumns = [
#        f"items.`{col_name}`" if " " in col_name or "." in col_name else f"items.{col_name}"
#        for col_name in subcolumns
#    ]
#
#    # 📌 Extraer columnas de `items` y renombrarlas
#    classlifetitulaciones_df = classlifetitulaciones_df.select(*[col(c).alias(c.replace("items.", "")) for c in clean_subcolumns])

# COMMAND ----------

## 📌 Aplicar limpieza de nombres de columnas
#classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Desanida Metas
## 📌 Desanidar estructuras internas (`counters`, `metas`) si existen
#if "counters" in classlifetitulaciones_df.columns:
#    counters_cols = classlifetitulaciones_df.select("counters.*").columns
#    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"counters.{c}").alias(f"counters_{c}") for c in counters_cols]).drop("counters")
#
#if "metas" in classlifetitulaciones_df.columns:
#    metas_cols = classlifetitulaciones_df.select("metas.*").columns
#    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"metas.{c}").alias(f"metas_{c}") for c in metas_cols]).drop("metas")

# COMMAND ----------

#from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp
#from pyspark.sql.types import StringType
#
#def clean_column_names(df):
#    """
#    Limpia los nombres de columnas eliminando espacios, tildes, caracteres especiales,
#    y asegurando un formato estándar.
#    """
#    cleaned_columns = {}
#    
#    for old_col in df.columns:
#        new_col = (
#            old_col.lower()
#            .strip()  # 📌 Elimina espacios al inicio y fin
#            .replace(" ", "_")
#            .replace(".", "_")
#            .replace("ñ", "n")
#            .replace("ó", "o")
#            .replace("á", "a")
#            .replace("é", "e")
#            .replace("í", "i")
#            .replace("ú", "u")
#            .replace("`", "")
#            .replace("metas_", "")
#            .replace("no__", "no_")
#        )
#
#        # Evitar nombres duplicados
#        if new_col in cleaned_columns.values():
#            print(f"⚠️ Nombre duplicado detectado: {new_col}, renombrando...")
#            new_col += "_2"
#
#        cleaned_columns[old_col] = new_col
#
#    # Aplicar los cambios
#    for old_col, new_col in cleaned_columns.items():
#        df = df.withColumnRenamed(old_col, new_col)
#
#    return df
#
## 📌 Inspeccionar nombres antes de la limpieza
#print("📌 Columnas antes de renombramiento:")
#for col_name in classlifetitulaciones_df.columns:
#    print(f"- '{repr(col_name)}'")  # 📌 Usa repr() para detectar caracteres invisibles
#
## 📌 Aplicar limpieza de nombres de columnas
#classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)
#
## 📌 Mostrar columnas después de la limpieza para verificar cambios
#print("\n📌 Columnas después de renombramiento:")
#for col_name in classlifetitulaciones_df.columns:
#    print(f"- '{repr(col_name)}'")  # 📌 Usa repr() nuevamente para comparación
#
## 📌 Verificar si `tarifa_matricula` existe en el DataFrame después de la limpieza
#columnas_actuales = set(classlifetitulaciones_df.columns)
#
## 📌 Seleccionar solo las columnas válidas si existen
#columnas_seleccionadas = list(columnas_actuales)
#
## 📌 Solución para nombres con comillas invertidas
## Aplicamos alias() para normalizar los nombres con comillas invertidas
#classlifetitulaciones_df = classlifetitulaciones_df.select(
#    *[col(c).alias(c.strip().replace("`", "")) for c in columnas_seleccionadas]
#)
#
#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Select the columns
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType

# ✅ Columnas que deseas conservar
columnas_requeridas = [
    "student_id",
    "student_active",
    "student_email",
    "student_full_name",
    "ciudad",
    "codigo",
    "direccion",
    "edad",
    "pais",
    "student_phone",
    "student_registration_date",
    "zoho_id",
    "nacimiento"
]

# 🧼 Selección y casteo de columnas
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).cast(StringType()).alias(c) for c in columnas_requeridas],
    current_timestamp().alias("processdate"),
    lit("ClasslifeStudents_931").alias("sourcesystem")
)

# 👁️ Visualizar resultado
#display(classlifetitulaciones_df)

# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.dropDuplicates()

# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

#%sql select count(*) from classlifetitulaciones_view;

# COMMAND ----------

# DBTITLE 1,Merge ClasslifeStudents_931
# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.ClasslifeStudents_931 AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.student_id = source.student_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.student_active IS DISTINCT FROM source.student_active OR
# MAGIC     target.student_email IS DISTINCT FROM source.student_email OR
# MAGIC     target.student_full_name IS DISTINCT FROM source.student_full_name OR
# MAGIC     target.ciudad IS DISTINCT FROM source.ciudad OR
# MAGIC     target.codigo IS DISTINCT FROM source.codigo OR
# MAGIC     target.direccion IS DISTINCT FROM source.direccion OR
# MAGIC     target.edad IS DISTINCT FROM source.edad OR
# MAGIC     target.pais IS DISTINCT FROM source.pais OR
# MAGIC     target.student_phone IS DISTINCT FROM source.student_phone OR
# MAGIC     target.student_registration_date IS DISTINCT FROM source.student_registration_date OR
# MAGIC     target.zoho_id IS DISTINCT FROM source.zoho_id OR
# MAGIC     target.nacimiento IS DISTINCT FROM source.nacimiento
# MAGIC )
# MAGIC
# MAGIC THEN UPDATE SET
# MAGIC     target.student_active = source.student_active,
# MAGIC     target.student_email = source.student_email,
# MAGIC     target.student_full_name = source.student_full_name,
# MAGIC     target.ciudad = source.ciudad,
# MAGIC     target.codigo = source.codigo,
# MAGIC     target.direccion = source.direccion,
# MAGIC     target.edad = source.edad,
# MAGIC     target.pais = source.pais,
# MAGIC     target.student_phone = source.student_phone,
# MAGIC     target.student_registration_date = source.student_registration_date,
# MAGIC     target.zoho_id = source.zoho_id,
# MAGIC     target.nacimiento = source.nacimiento,
# MAGIC     target.processdate = source.processdate,
# MAGIC     target.sourcesystem = source.sourcesystem
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

#%sql select * from silver_lakehouse.ClasslifeStudents_931;
