# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "enroll_groups"
table_name = "JsaClassLifeProductos"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife_931/{endpoint_process_name}/{current_date}/{table_name}.json")
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

# DBTITLE 1,Explota data
# 📌 Extraer el contenido de `data` si existe
if "data" in classlifetitulaciones_df.columns:
    classlifetitulaciones_df = classlifetitulaciones_df.selectExpr("data.*")

# 📌 Inspeccionar después de extraer `data`
print("📌 Esquema después de seleccionar `data.*`:")
classlifetitulaciones_df.printSchema()

display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Explota items
# 📌 Explotar `items` si es un array
if "items" in classlifetitulaciones_df.columns:
    print("📌 'items' es una estructura o array. Procedemos a desanidar.")

    # Si `items` es un array de estructuras, lo explotamos
    if isinstance(classlifetitulaciones_df.schema["items"].dataType, ArrayType):
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn("items", explode(col("items")))
    
display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Extrae subcolumnas de items
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

# DBTITLE 1,Clean columns
# 📌 Inspeccionar después de desanidar `items`
print("📌 Esquema después de desanidar `items`:")
classlifetitulaciones_df.printSchema()


# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Desanida counters and metas
# 📌 Desanidar estructuras internas (`counters`, `metas`) si existen
if "counters" in classlifetitulaciones_df.columns:
    counters_cols = classlifetitulaciones_df.select("counters.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"counters.{c}").alias(f"counters_{c}") for c in counters_cols]).drop("counters")

if "metas" in classlifetitulaciones_df.columns:
    metas_cols = classlifetitulaciones_df.select("metas.*").columns
    classlifetitulaciones_df = classlifetitulaciones_df.select("*", *[col(f"metas.{c}").alias(f"metas_{c}") for c in metas_cols]).drop("metas")

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
from pyspark.sql.types import IntegerType, DoubleType

# Agrega campos de auditoría
classlifetitulaciones_df = classlifetitulaciones_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("classlifetitulaciones"))

# Lista completa de columnas tipo string
string_columns = [
    "modalidad", "fecha_inicio_docencia", "fecha_inicio", "meses_cursos_open", "admisionsino", "grupo",
    "meses_duracion", "horas_acreditadas", "plazas", "horas_presenciales_2", "codigo_programa", "area_title",
    "fecha_inicio_cuotas", "enroll_group_id", "tarifa_euneiz", "term_title", "certificado_euneiz_incluido_2",
    "especialidad", "fecha_fin", "creditos", "enroll_group_id_2", "counters_pre_enrolled", "fecha_fin_cuotas",
    "ano_inicio_docencia", "fecha_fin_reconocimiento_ingresos", "term_id", "fecha_inicio_reconocimiento_ingresos",
    "group_vertical", "tarifa_matricula", "area_id", "admisionsino_2", "year", "codigo_sede", "zoho_id",
    "ano_inicio_docencia_2", "mesesampliacion", "nombre_del_programa_oficial_completo", "codigo_entidad_legal",
    "fecha_fin_docencia", "nombreweb", "area_codigo_vertical", "tiponegocio_2", "enroll_end", "area_entidad_legal",
    "ciclo_title", "school_id", "grupo_2", "counters_availables", "ultima_actualizacion", "mes_inicio_docencia_2",
    "counters_enrolled", "horas_acreditadas_2", "receipts_count", "tiponegocio", "horas_presenciales",
    "enroll_group_name", "enroll_alias", "school_name", "cuotas_docencia", "enroll_ini", "acreditado",
    "descripcion_calendario", "destinatarios", "certificado_euneiz_incluido", "group_entidad_legal", "area_vertical",
    "group_entidad_legal_codigo", "counters_seats", "codigo_especialidad", "descripcion_calendario_2", "ciclo_id",
    "section_id", "codigo_vertical", "mes_inicio_docencia", "section_title", "area_entidad_legal_codigo",
    "group_sede", "fecha_creacion", "tarifa_docencia", "total_tarifas", "group_codigo_vertical", "roaster_ind"
]

# Aplicar el cast
for col_name in string_columns:
    if col_name in classlifetitulaciones_df.columns:
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn(col_name, col(col_name).cast(StringType()))

# Mostrar resultado
display(classlifetitulaciones_df)

# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.filter("enroll_group_name IS NOT NULL")
classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.classlifetitulaciones AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.enroll_group_id = source.enroll_group_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC      target.fecha_inicio_reconocimiento_ingresos IS DISTINCT FROM source.fecha_inicio_reconocimiento_ingresos
# MAGIC   OR target.fecha_fin_reconocimiento_ingresos IS DISTINCT FROM source.fecha_fin_reconocimiento_ingresos
# MAGIC   OR target.fecha_inicio IS DISTINCT FROM source.fecha_inicio
# MAGIC   OR target.fecha_Fin IS DISTINCT FROM source.fecha_Fin
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     target.fecha_inicio_reconocimiento_ingresos = source.fecha_inicio_reconocimiento_ingresos,
# MAGIC     target.fecha_fin_reconocimiento_ingresos = source.fecha_fin_reconocimiento_ingresos,
# MAGIC     target.fecha_inicio = source.fecha_inicio,
# MAGIC     target.fecha_Fin = source.fecha_Fin
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;
# MAGIC
