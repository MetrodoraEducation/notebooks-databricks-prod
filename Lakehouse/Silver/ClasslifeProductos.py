# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "enroll_groups"
table_name = "JsaClassLifeProductos"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife/{endpoint_process_name}/{current_date}/{table_name}.json")
classlifetitulaciones_df

# 📌 Inspeccionar el esquema inicial
print("📌 Esquema inicial antes de limpieza:")
classlifetitulaciones_df.printSchema()

display(classlifetitulaciones_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Pasos principales:**
# MAGIC - Limpiar el esquema completo del DataFrame antes de trabajar con las columnas anidadas.
# MAGIC - Desanidar las estructuras una por una asegurando que no existan conflictos.
# MAGIC - Revisar si existen estructuras complejas adicionales que deban manejarse de forma especial.

# COMMAND ----------

# 📌 Inspeccionar Esquema Inicial
print("📌 Esquema inicial antes de limpieza:")
classlifetitulaciones_df.printSchema()

display(classlifetitulaciones_df)

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

display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Extraer el contenido de `data` si existe
if "data" in classlifetitulaciones_df.columns:
    classlifetitulaciones_df = classlifetitulaciones_df.selectExpr("data.*")

# 📌 Inspeccionar después de extraer `data`
print("📌 Esquema después de seleccionar `data.*`:")
classlifetitulaciones_df.printSchema()

display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Explotar `items` si es un array
if "items" in classlifetitulaciones_df.columns:
    print("📌 'items' es una estructura o array. Procedemos a desanidar.")

    # Si `items` es un array de estructuras, lo explotamos
    if isinstance(classlifetitulaciones_df.schema["items"].dataType, ArrayType):
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn("items", explode(col("items")))
    
display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Verificar esquema después de explotar `items`
print("📌 Esquema después de explotar `items`:")
classlifetitulaciones_df.printSchema()

display(classlifetitulaciones_df)

# COMMAND ----------

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

    display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Inspeccionar después de desanidar `items`
print("📌 Esquema después de desanidar `items`:")
classlifetitulaciones_df.printSchema()


# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Inspeccionar después de limpiar nombres de columnas
print("📌 Esquema después de limpiar nombres de columnas:")
classlifetitulaciones_df.printSchema()

display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Desanidar estructuras internas (`counters`, `metas`) si existen
if "counters" in classlifetitulaciones_df.columns:
    counters_cols = classlifetitulaciones_df.select("counters.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"counters.{c}").alias(f"counters_{c}") for c in counters_cols]).drop("counters")

if "metas" in classlifetitulaciones_df.columns:
    metas_cols = classlifetitulaciones_df.select("metas.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"metas.{c}").alias(f"metas_{c}") for c in metas_cols]).drop("metas")

display(classlifetitulaciones_df)

# COMMAND ----------

# 📌 Inspeccionar después de expandir estructuras internas
print("📌 Esquema final después de desanidar estructuras:")
classlifetitulaciones_df.printSchema()

display(classlifetitulaciones_df)

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
            .replace("counters_enroll_group_id", "enroll_group_id_2")
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

if "tarifa_matricula" not in columnas_actuales:
    print("⚠️ `tarifa_matricula` NO se encuentra en el DataFrame después del renombramiento.")
    print("🔍 Buscando nombres similares:")
    for col_name in columnas_actuales:
        if "tarifa" in col_name:
            print(f"🔍 Posible coincidencia: {repr(col_name)}")

# 📌 Seleccionar solo las columnas válidas si existen
columnas_seleccionadas = list(columnas_actuales)

# 📌 Solución para nombres con comillas invertidas
# Aplicamos alias() para normalizar los nombres con comillas invertidas
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).alias(c.strip().replace("`", "")) for c in columnas_seleccionadas]
)

# 📌 Mostrar los primeros registros
display(classlifetitulaciones_df)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType, IntegerType, DoubleType

# 📌 Aplicar transformaciones a las columnas con nombres corregidos
classlifetitulaciones_df = classlifetitulaciones_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("classlifetitulaciones")) \
    .withColumn("fecha_inicio_docencia", to_date(col("fecha_inicio_docencia"), "dd/MM/yyyy")) \
    .withColumn("fecha_inicio", to_date(col("fecha_inicio"), "dd/MM/yyyy")) \
    .withColumn("fecha_fin_pago", to_date(col("fecha_fin_pago"), "dd/MM/yyyy")) \
    .withColumn("fecha_inicio_cuotas", to_date(col("fecha_inicio_cuotas"), "dd/MM/yyyy")) \
    .withColumn("fecha_fin", to_date(col("fecha_fin"), "dd/MM/yyyy")) \
    .withColumn("fecha_fin_cuotas", to_date(col("fecha_fin_cuotas"), "dd/MM/yyyy")) \
    .withColumn("fecha_fin_reconocimiento_ingresos", to_date(col("fecha_fin_reconocimiento_ingresos"), "dd/MM/yyyy")) \
    .withColumn("fecha_inicio_reconocimiento_ingresos", to_date(col("fecha_inicio_reconocimiento_ingresos"), "dd/MM/yyyy")) \
    .withColumn("fecha_fin_docencia", to_date(col("fecha_fin_docencia"), "dd/MM/yyyy")) \
    .withColumn("fecha_creacion", to_timestamp(col("fecha_creacion"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("ultima_actualizacion", to_timestamp(col("ultima_actualizacion"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("enroll_end", to_timestamp(col("enroll_end"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("enroll_ini", to_timestamp(col("enroll_ini"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("horas_acreditadas", col("horas_acreditadas").cast(IntegerType())) \
    .withColumn("horas_presenciales", col("horas_presenciales").cast(IntegerType())) \
    .withColumn("horas_presenciales_2", col("horas_presenciales_2").cast(IntegerType())) \
    .withColumn("enroll_group_id", col("enroll_group_id").cast(IntegerType())) \
    .withColumn("enroll_group_id_2", col("enroll_group_id_2").cast(IntegerType())) \
    .withColumnRenamed("counters_pre_enrolled", "pre_enrolled") \
    .withColumn("pre_enrolled", col("pre_enrolled").cast(IntegerType())) \
    .withColumn("tarifa_ampliacion", col("tarifa_ampliacion").cast(DoubleType())) \
    .withColumn("tarifa_euneiz", col("tarifa_euneiz").cast(DoubleType())) \
    .withColumn("tarifa_matricula", col("tarifa_matricula").cast(DoubleType())) \
    .withColumn("tarifa_docencia", col("tarifa_docencia").cast(DoubleType())) \
    .withColumn("total_tarifas", col("total_tarifas").cast(DoubleType())) \
    .withColumn("creditos", col("creditos").cast(DoubleType())) \
    .withColumn("cuotas_docencia", col("cuotas_docencia").cast(IntegerType())) \
    .withColumn("receipts_count", col("receipts_count").cast(IntegerType())) \
    .withColumn("roaster_ind", col("roaster_ind").cast(IntegerType())) \
    .withColumn("admisionsino", col("admisionsino").cast(StringType())) \
    .withColumn("certificado_euneiz_incluido", col("certificado_euneiz_incluido").cast(StringType())) \
    .withColumn("certificado_euneiz_incluido_2", col("certificado_euneiz_incluido_2").cast(StringType())) \
    .withColumn("admisionsino_2", col("admisionsino_2").cast(StringType())) \
    .withColumn("tiponegocio", col("tiponegocio").cast(StringType())) \
    .withColumn("tiponegocio_2", col("tiponegocio_2").cast(StringType())) \
    .withColumn("codigo_antiguo", col("codigo_antiguo").cast(StringType())) \
    .withColumn("codigo_especialidad", col("codigo_especialidad").cast(StringType())) \
    .withColumn("codigo_programa", col("codigo_programa").cast(StringType())) \
    .withColumn("codigo_vertical", col("codigo_vertical").cast(StringType())) \
    .withColumn("codigo_vertical_2", col("codigo_vertical_2").cast(StringType())) \
    .withColumn("codigo_sede", col("codigo_sede").cast(StringType())) \
    .withColumn("codigo_entidad_legal", col("codigo_entidad_legal").cast(StringType())) \
    .withColumn("modalidad_code", col("modalidad_code").cast(StringType())) \
    .withColumn("area_id", col("area_id").cast(IntegerType())) \
    .withColumn("area_title", col("area_title").cast(StringType())) \
    .withColumn("degree_id", col("degree_id").cast(IntegerType())) \
    .withColumn("degree_title", col("degree_title").cast(StringType())) \
    .withColumn("plan_id", col("plan_id").cast(IntegerType())) \
    .withColumn("plan_title", col("plan_title").cast(StringType())) \
    .withColumn("school_id", col("school_id").cast(IntegerType())) \
    .withColumn("school_name", col("school_name").cast(StringType())) \
    .withColumn("section_id", col("section_id").cast(IntegerType())) \
    .withColumn("section_title", col("section_title").cast(StringType())) \
    .withColumn("term_id", col("term_id").cast(IntegerType())) \
    .withColumn("term_title", col("term_title").cast(StringType())) \
    .withColumn("building_id", col("building_id").cast(IntegerType())) \
    .withColumn("building_title", col("building_title").cast(StringType())) \
    .withColumnRenamed("counters_availables", "availables") \
    .withColumn("availables", col("availables").cast(IntegerType())) \
    .withColumnRenamed("counters_enrolled", "enrolled") \
    .withColumn("enrolled", col("enrolled").cast(IntegerType())) \
    .withColumnRenamed("counters_seats", "seats") \
    .withColumn("seats", col("seats").cast(IntegerType())) \
    .withColumn("enroll_group_name", col("enroll_group_name").cast(StringType())) \
    .withColumn("enroll_alias", col("enroll_alias").cast(StringType())) \
    .withColumn("especialidad", col("especialidad").cast(StringType())) \
    .withColumn("destinatarios", col("destinatarios").cast(StringType())) \
    .withColumn("descripcion_calendario", col("descripcion_calendario").cast(StringType())) \
    .withColumn("nombre_antiguo_de_programa", col("nombre_antiguo_de_programa").cast(StringType())) \
    .withColumn("nombre_del_programa_oficial_completo", col("nombre_del_programa_oficial_completo").cast(StringType())) \
    .withColumn("nombreweb", col("nombreweb").cast(StringType()))

# 📌 Mostrar los primeros registros
display(classlifetitulaciones_df)

# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.classlifetitulaciones AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.enroll_group_id = source.enroll_group_id
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT *;
