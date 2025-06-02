# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../configuration"

# COMMAND ----------

endpoint_process_name = "enroll_groups"
table_name = "JsaClassLifeProductos"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife_931/{endpoint_process_name}/{current_date}/{table_name}.json")
classlifetitulaciones_df

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

# COMMAND ----------

# DBTITLE 1,Clean columns
# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Desanida counters and metas
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

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, when
from pyspark.sql.types import StringType

# 🧱 Columnas específicas a mostrar
columnas_requeridas = [
    "admisionsino", "degree_title", "horas_acreditadas", "plazas", "codigo_programa",
    "area_title", "fecha_inicio_cuotas", "enroll_group_id", "tarifa_ampliacion", "tarifa_euneiz",
    "term_title", "especialidad", "fecha_fin", "enroll_group_id_2", "fecha_fin_cuotas",
    "ano_inicio_docencia", "fecha_fin_reconocimiento_ingresos", "term_id", "fecha_inicio_reconocimiento_ingresos",
    "group_vertical", "tarifa_matricula", "zoho_id", "nombre_del_programa_oficial_completo",
    "codigo_entidad_legal", "fecha_fin_docencia", "nombreweb", "area_codigo_vertical", "enroll_end",
    "area_entidad_legal", "ciclo_title", "school_id", "ultima_actualizacion", "horas_acreditadas_2",
    "tiponegocio", "enroll_group_name", "enroll_alias", "school_name", "nombre_antiguo_de_programa",
    "group_entidad_legal", "area_vertical", "section_id", "section_title", "area_entidad_legal_codigo", "group_sede", "fecha_creacion", "tarifa_docencia", "total_tarifas",
    "group_codigo_vertical"
]

# 🧽 1. Limpiar nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# 🧱 2. Crear columnas faltantes como NULL
for columna in columnas_requeridas:
    if columna not in classlifetitulaciones_df.columns:
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn(
            columna, lit(None).cast(StringType())
        )

# 🛠️ 3. Reemplazo en area_entidad_legal
classlifetitulaciones_df = classlifetitulaciones_df.withColumn(
    "area_entidad_legal",
    when(col("area_entidad_legal") == "METRODORA LEARNING", "METRODORA FP").otherwise(col("area_entidad_legal"))
)

# 📋 4. Seleccionar columnas requeridas + processdate + sourcesystem
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).cast(StringType()).alias(c) for c in columnas_requeridas],
    current_timestamp().alias("processdate"),
    lit("classlifetitulaciones_931").alias("sourcesystem")
)

# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.filter("enroll_group_name IS NOT NULL")
classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.classlifetitulaciones_931 AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.enroll_group_id = source.enroll_group_id
# MAGIC WHEN MATCHED AND (
# MAGIC     target.admisionsino IS DISTINCT FROM source.admisionsino OR
# MAGIC     target.degree_title IS DISTINCT FROM source.degree_title OR
# MAGIC     target.horas_acreditadas IS DISTINCT FROM source.horas_acreditadas OR
# MAGIC     target.plazas IS DISTINCT FROM source.plazas OR
# MAGIC     target.codigo_programa IS DISTINCT FROM source.codigo_programa OR
# MAGIC     target.area_title IS DISTINCT FROM source.area_title OR
# MAGIC     target.fecha_inicio_cuotas IS DISTINCT FROM source.fecha_inicio_cuotas OR
# MAGIC     target.tarifa_ampliacion IS DISTINCT FROM source.tarifa_ampliacion OR
# MAGIC     target.tarifa_euneiz IS DISTINCT FROM source.tarifa_euneiz OR
# MAGIC     target.term_title IS DISTINCT FROM source.term_title OR
# MAGIC     target.especialidad IS DISTINCT FROM source.especialidad OR
# MAGIC     target.fecha_fin IS DISTINCT FROM source.fecha_fin OR
# MAGIC     target.enroll_group_id_2 IS DISTINCT FROM source.enroll_group_id_2 OR
# MAGIC     target.fecha_fin_cuotas IS DISTINCT FROM source.fecha_fin_cuotas OR
# MAGIC     target.ano_inicio_docencia IS DISTINCT FROM source.ano_inicio_docencia OR
# MAGIC     target.fecha_fin_reconocimiento_ingresos IS DISTINCT FROM source.fecha_fin_reconocimiento_ingresos OR
# MAGIC     target.term_id IS DISTINCT FROM source.term_id OR
# MAGIC     target.fecha_inicio_reconocimiento_ingresos IS DISTINCT FROM source.fecha_inicio_reconocimiento_ingresos OR
# MAGIC     target.group_vertical IS DISTINCT FROM source.group_vertical OR
# MAGIC     target.tarifa_matricula IS DISTINCT FROM source.tarifa_matricula OR
# MAGIC     target.zoho_id IS DISTINCT FROM source.zoho_id OR
# MAGIC     target.nombre_del_programa_oficial_completo IS DISTINCT FROM source.nombre_del_programa_oficial_completo OR
# MAGIC     target.codigo_entidad_legal IS DISTINCT FROM source.codigo_entidad_legal OR
# MAGIC     target.fecha_fin_docencia IS DISTINCT FROM source.fecha_fin_docencia OR
# MAGIC     target.nombreweb IS DISTINCT FROM source.nombreweb OR
# MAGIC     target.area_codigo_vertical IS DISTINCT FROM source.area_codigo_vertical OR
# MAGIC     target.enroll_end IS DISTINCT FROM source.enroll_end OR
# MAGIC     target.area_entidad_legal IS DISTINCT FROM source.area_entidad_legal OR
# MAGIC     target.ciclo_title IS DISTINCT FROM source.ciclo_title OR
# MAGIC     target.school_id IS DISTINCT FROM source.school_id OR
# MAGIC     target.ultima_actualizacion IS DISTINCT FROM source.ultima_actualizacion OR
# MAGIC     target.horas_acreditadas_2 IS DISTINCT FROM source.horas_acreditadas_2 OR
# MAGIC     target.tiponegocio IS DISTINCT FROM source.tiponegocio OR
# MAGIC     target.enroll_group_name IS DISTINCT FROM source.enroll_group_name OR
# MAGIC     target.enroll_alias IS DISTINCT FROM source.enroll_alias OR
# MAGIC     target.school_name IS DISTINCT FROM source.school_name OR
# MAGIC     target.nombre_antiguo_de_programa IS DISTINCT FROM source.nombre_antiguo_de_programa OR
# MAGIC     target.group_entidad_legal IS DISTINCT FROM source.group_entidad_legal OR
# MAGIC     target.area_vertical IS DISTINCT FROM source.area_vertical OR
# MAGIC     target.section_id IS DISTINCT FROM source.section_id OR
# MAGIC     target.section_title IS DISTINCT FROM source.section_title OR
# MAGIC     target.area_entidad_legal_codigo IS DISTINCT FROM source.area_entidad_legal_codigo OR
# MAGIC     target.group_sede IS DISTINCT FROM source.group_sede OR
# MAGIC     target.fecha_creacion IS DISTINCT FROM source.fecha_creacion OR
# MAGIC     target.tarifa_docencia IS DISTINCT FROM source.tarifa_docencia OR
# MAGIC     target.total_tarifas IS DISTINCT FROM source.total_tarifas OR
# MAGIC     target.group_codigo_vertical IS DISTINCT FROM source.group_codigo_vertical
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.admisionsino = source.admisionsino,
# MAGIC     target.degree_title = source.degree_title,
# MAGIC     target.horas_acreditadas = source.horas_acreditadas,
# MAGIC     target.plazas = source.plazas,
# MAGIC     target.codigo_programa = source.codigo_programa,
# MAGIC     target.area_title = source.area_title,
# MAGIC     target.fecha_inicio_cuotas = source.fecha_inicio_cuotas,
# MAGIC     target.enroll_group_id = source.enroll_group_id,
# MAGIC     target.tarifa_ampliacion = source.tarifa_ampliacion,
# MAGIC     target.tarifa_euneiz = source.tarifa_euneiz,
# MAGIC     target.term_title = source.term_title,
# MAGIC     target.especialidad = source.especialidad,
# MAGIC     target.fecha_fin = source.fecha_fin,
# MAGIC     target.enroll_group_id_2 = source.enroll_group_id_2,
# MAGIC     target.fecha_fin_cuotas = source.fecha_fin_cuotas,
# MAGIC     target.ano_inicio_docencia = source.ano_inicio_docencia,
# MAGIC     target.fecha_fin_reconocimiento_ingresos = source.fecha_fin_reconocimiento_ingresos,
# MAGIC     target.term_id = source.term_id,
# MAGIC     target.fecha_inicio_reconocimiento_ingresos = source.fecha_inicio_reconocimiento_ingresos,
# MAGIC     target.group_vertical = source.group_vertical,
# MAGIC     target.tarifa_matricula = source.tarifa_matricula,
# MAGIC     target.zoho_id = source.zoho_id,
# MAGIC     target.nombre_del_programa_oficial_completo = source.nombre_del_programa_oficial_completo,
# MAGIC     target.codigo_entidad_legal = source.codigo_entidad_legal,
# MAGIC     target.fecha_fin_docencia = source.fecha_fin_docencia,
# MAGIC     target.nombreweb = source.nombreweb,
# MAGIC     target.area_codigo_vertical = source.area_codigo_vertical,
# MAGIC     target.enroll_end = source.enroll_end,
# MAGIC     target.area_entidad_legal = source.area_entidad_legal,
# MAGIC     target.ciclo_title = source.ciclo_title,
# MAGIC     target.school_id = source.school_id,
# MAGIC     target.ultima_actualizacion = source.ultima_actualizacion,
# MAGIC     target.horas_acreditadas_2 = source.horas_acreditadas_2,
# MAGIC     target.tiponegocio = source.tiponegocio,
# MAGIC     target.enroll_group_name = source.enroll_group_name,
# MAGIC     target.enroll_alias = source.enroll_alias,
# MAGIC     target.school_name = source.school_name,
# MAGIC     target.nombre_antiguo_de_programa = source.nombre_antiguo_de_programa,
# MAGIC     target.group_entidad_legal = source.group_entidad_legal,
# MAGIC     target.area_vertical = source.area_vertical,
# MAGIC     target.section_id = source.section_id,
# MAGIC     target.section_title = source.section_title,
# MAGIC     target.area_entidad_legal_codigo = source.area_entidad_legal_codigo,
# MAGIC     target.group_sede = source.group_sede,
# MAGIC     target.fecha_creacion = source.fecha_creacion,
# MAGIC     target.tarifa_docencia = source.tarifa_docencia,
# MAGIC     target.total_tarifas = source.total_tarifas,
# MAGIC     target.group_codigo_vertical = source.group_codigo_vertical,
# MAGIC     target.processdate = source.processdate,
# MAGIC     target.sourcesystem = source.sourcesystem
# MAGIC     
# MAGIC WHEN NOT MATCHED THEN INSERT *;
