# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "enroll_groups"
table_name = "JsaClassLifeProductos"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife/{endpoint_process_name}/{current_date}/{table_name}.json")
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
# MAGIC   target.modalidad IS DISTINCT FROM source.modalidad
# MAGIC   OR target.fecha_inicio_docencia IS DISTINCT FROM source.fecha_inicio_docencia
# MAGIC   OR target.fecha_inicio IS DISTINCT FROM source.fecha_inicio
# MAGIC   OR target.meses_cursos_open IS DISTINCT FROM source.meses_cursos_open
# MAGIC   OR target.admisionsino IS DISTINCT FROM source.admisionsino
# MAGIC   OR target.degree_title IS DISTINCT FROM source.degree_title
# MAGIC   OR target.grupo IS DISTINCT FROM source.grupo
# MAGIC   OR target.degree_id IS DISTINCT FROM source.degree_id
# MAGIC   OR target.meses_duracion IS DISTINCT FROM source.meses_duracion
# MAGIC   OR target.horas_acreditadas IS DISTINCT FROM source.horas_acreditadas
# MAGIC   OR target.plazas IS DISTINCT FROM source.plazas
# MAGIC   OR target.fecha_fin_pago IS DISTINCT FROM source.fecha_fin_pago
# MAGIC   OR target.horas_presenciales_2 IS DISTINCT FROM source.horas_presenciales_2
# MAGIC   OR target.num_plazas_ultimas IS DISTINCT FROM source.num_plazas_ultimas
# MAGIC   OR target.codigo_programa IS DISTINCT FROM source.codigo_programa
# MAGIC   OR target.area_title IS DISTINCT FROM source.area_title
# MAGIC   OR target.fecha_inicio_cuotas IS DISTINCT FROM source.fecha_inicio_cuotas
# MAGIC   OR target.enroll_group_id IS DISTINCT FROM source.enroll_group_id
# MAGIC   OR target.tarifa_ampliacion IS DISTINCT FROM source.tarifa_ampliacion
# MAGIC   OR target.tarifa_euneiz IS DISTINCT FROM source.tarifa_euneiz
# MAGIC   OR target.term_title IS DISTINCT FROM source.term_title
# MAGIC   OR target.certificado_euneiz_incluido_2 IS DISTINCT FROM source.certificado_euneiz_incluido_2
# MAGIC   OR target.especialidad IS DISTINCT FROM source.especialidad
# MAGIC   OR target.fecha_fin IS DISTINCT FROM source.fecha_fin
# MAGIC   OR target.creditos IS DISTINCT FROM source.creditos
# MAGIC   OR target.enroll_group_id_2 IS DISTINCT FROM source.enroll_group_id_2
# MAGIC   OR target.counters_pre_enrolled IS DISTINCT FROM source.counters_pre_enrolled
# MAGIC   OR target.fecha_fin_cuotas IS DISTINCT FROM source.fecha_fin_cuotas
# MAGIC   OR target.ano_inicio_docencia IS DISTINCT FROM source.ano_inicio_docencia
# MAGIC   OR target.fecha_fin_reconocimiento_ingresos IS DISTINCT FROM source.fecha_fin_reconocimiento_ingresos
# MAGIC   OR target.term_id IS DISTINCT FROM source.term_id
# MAGIC   OR target.fecha_inicio_reconocimiento_ingresos IS DISTINCT FROM source.fecha_inicio_reconocimiento_ingresos
# MAGIC   OR target.group_vertical IS DISTINCT FROM source.group_vertical
# MAGIC   OR target.tarifa_matricula IS DISTINCT FROM source.tarifa_matricula
# MAGIC   OR target.area_sede IS DISTINCT FROM source.area_sede
# MAGIC   OR target.area_id IS DISTINCT FROM source.area_id
# MAGIC   OR target.admisionsino_2 IS DISTINCT FROM source.admisionsino_2
# MAGIC   OR target.year IS DISTINCT FROM source.year
# MAGIC   OR target.codigo_sede IS DISTINCT FROM source.codigo_sede
# MAGIC   OR target.plan_title IS DISTINCT FROM source.plan_title
# MAGIC   OR target.no_ultimas_plazas IS DISTINCT FROM source.no_ultimas_plazas
# MAGIC   OR target.zoho_id IS DISTINCT FROM source.zoho_id
# MAGIC   OR target.ano_inicio_docencia_2 IS DISTINCT FROM source.ano_inicio_docencia_2
# MAGIC   OR target.mesesampliacion IS DISTINCT FROM source.mesesampliacion
# MAGIC   OR target.codigo_antiguo IS DISTINCT FROM source.codigo_antiguo
# MAGIC   OR target.nombre_del_programa_oficial_completo IS DISTINCT FROM source.nombre_del_programa_oficial_completo
# MAGIC   OR target.codigo_entidad_legal IS DISTINCT FROM source.codigo_entidad_legal
# MAGIC   OR target.fecha_fin_docencia IS DISTINCT FROM source.fecha_fin_docencia
# MAGIC   OR target.nombreweb IS DISTINCT FROM source.nombreweb
# MAGIC   OR target.area_codigo_vertical IS DISTINCT FROM source.area_codigo_vertical
# MAGIC   OR target.tiponegocio_2 IS DISTINCT FROM source.tiponegocio_2
# MAGIC   OR target.enroll_end IS DISTINCT FROM source.enroll_end
# MAGIC   OR target.modalidad_code IS DISTINCT FROM source.modalidad_code
# MAGIC   OR target.area_entidad_legal IS DISTINCT FROM source.area_entidad_legal
# MAGIC   OR target.ciclo_title IS DISTINCT FROM source.ciclo_title
# MAGIC   OR target.building_title IS DISTINCT FROM source.building_title
# MAGIC   OR target.school_id IS DISTINCT FROM source.school_id
# MAGIC   OR target.grupo_2 IS DISTINCT FROM source.grupo_2
# MAGIC   OR target.building_id IS DISTINCT FROM source.building_id
# MAGIC   OR target.plan_id IS DISTINCT FROM source.plan_id
# MAGIC   OR target.counters_availables IS DISTINCT FROM source.counters_availables
# MAGIC   OR target.ultima_actualizacion IS DISTINCT FROM source.ultima_actualizacion
# MAGIC   OR target.mes_inicio_docencia_2 IS DISTINCT FROM source.mes_inicio_docencia_2
# MAGIC   OR target.counters_enrolled IS DISTINCT FROM source.counters_enrolled
# MAGIC   OR target.horas_acreditadas_2 IS DISTINCT FROM source.horas_acreditadas_2
# MAGIC   OR target.receipts_count IS DISTINCT FROM source.receipts_count
# MAGIC   OR target.tiponegocio IS DISTINCT FROM source.tiponegocio
# MAGIC   OR target.horas_presenciales IS DISTINCT FROM source.horas_presenciales
# MAGIC   OR target.enroll_group_name IS DISTINCT FROM source.enroll_group_name
# MAGIC   OR target.fecha_inicio_pago IS DISTINCT FROM source.fecha_inicio_pago
# MAGIC   OR target.enroll_alias IS DISTINCT FROM source.enroll_alias
# MAGIC   OR target.building IS DISTINCT FROM source.building
# MAGIC   OR target.school_name IS DISTINCT FROM source.school_name
# MAGIC   OR target.cuotas_docencia IS DISTINCT FROM source.cuotas_docencia
# MAGIC   OR target.enroll_ini IS DISTINCT FROM source.enroll_ini
# MAGIC   OR target.acreditado IS DISTINCT FROM source.acreditado
# MAGIC   OR target.descripcion_calendario IS DISTINCT FROM source.descripcion_calendario
# MAGIC   OR target.destinatarios IS DISTINCT FROM source.destinatarios
# MAGIC   OR target.enroll_pago_ini_t IS DISTINCT FROM source.enroll_pago_ini_t
# MAGIC   OR target.nombre_antiguo_de_programa IS DISTINCT FROM source.nombre_antiguo_de_programa
# MAGIC   OR target.certificado_euneiz_incluido IS DISTINCT FROM source.certificado_euneiz_incluido
# MAGIC   OR target.group_entidad_legal IS DISTINCT FROM source.group_entidad_legal
# MAGIC   OR target.area_vertical IS DISTINCT FROM source.area_vertical
# MAGIC   OR target.group_entidad_legal_codigo IS DISTINCT FROM source.group_entidad_legal_codigo
# MAGIC   OR target.counters_seats IS DISTINCT FROM source.counters_seats
# MAGIC   OR target.codigo_especialidad IS DISTINCT FROM source.codigo_especialidad
# MAGIC   OR target.descripcion_calendario_2 IS DISTINCT FROM source.descripcion_calendario_2
# MAGIC   OR target.ciclo_id IS DISTINCT FROM source.ciclo_id
# MAGIC   OR target.section_id IS DISTINCT FROM source.section_id
# MAGIC   OR target.codigo_vertical IS DISTINCT FROM source.codigo_vertical
# MAGIC   OR target.mes_inicio_docencia IS DISTINCT FROM source.mes_inicio_docencia
# MAGIC   OR target.section_title IS DISTINCT FROM source.section_title
# MAGIC   OR target.area_entidad_legal_codigo IS DISTINCT FROM source.area_entidad_legal_codigo
# MAGIC   OR target.group_sede IS DISTINCT FROM source.group_sede
# MAGIC   OR target.fecha_creacion IS DISTINCT FROM source.fecha_creacion
# MAGIC   OR target.tarifa_docencia IS DISTINCT FROM source.tarifa_docencia
# MAGIC   OR target.total_tarifas IS DISTINCT FROM source.total_tarifas
# MAGIC   OR target.group_codigo_vertical IS DISTINCT FROM source.group_codigo_vertical
# MAGIC   OR target.roaster_ind IS DISTINCT FROM source.roaster_ind
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     target.modalidad = source.modalidad,
# MAGIC     target.fecha_inicio_docencia = source.fecha_inicio_docencia,
# MAGIC     target.fecha_inicio = source.fecha_inicio,
# MAGIC     target.meses_cursos_open = source.meses_cursos_open,
# MAGIC     target.admisionsino = source.admisionsino,
# MAGIC     target.degree_title = source.degree_title,
# MAGIC     target.grupo = source.grupo,
# MAGIC     target.degree_id = source.degree_id,
# MAGIC     target.meses_duracion = source.meses_duracion,
# MAGIC     target.horas_acreditadas = source.horas_acreditadas,
# MAGIC     target.plazas = source.plazas,
# MAGIC     target.fecha_fin_pago = source.fecha_fin_pago,
# MAGIC     target.horas_presenciales_2 = source.horas_presenciales_2,
# MAGIC     target.num_plazas_ultimas = source.num_plazas_ultimas,
# MAGIC     target.codigo_programa = source.codigo_programa,
# MAGIC     target.area_title = source.area_title,
# MAGIC     target.fecha_inicio_cuotas = source.fecha_inicio_cuotas,
# MAGIC     target.enroll_group_id = source.enroll_group_id,
# MAGIC     target.tarifa_ampliacion = source.tarifa_ampliacion,
# MAGIC     target.tarifa_euneiz = source.tarifa_euneiz,
# MAGIC     target.term_title = source.term_title,
# MAGIC     target.certificado_euneiz_incluido_2 = source.certificado_euneiz_incluido_2,
# MAGIC     target.especialidad = source.especialidad,
# MAGIC     target.fecha_fin = source.fecha_fin,
# MAGIC     target.creditos = source.creditos,
# MAGIC     target.enroll_group_id_2 = source.enroll_group_id_2,
# MAGIC     target.counters_pre_enrolled = source.counters_pre_enrolled,
# MAGIC     target.fecha_fin_cuotas = source.fecha_fin_cuotas,
# MAGIC     target.ano_inicio_docencia = source.ano_inicio_docencia,
# MAGIC     target.fecha_fin_reconocimiento_ingresos = source.fecha_fin_reconocimiento_ingresos,
# MAGIC     target.term_id = source.term_id,
# MAGIC     target.fecha_inicio_reconocimiento_ingresos = source.fecha_inicio_reconocimiento_ingresos,
# MAGIC     target.group_vertical = source.group_vertical,
# MAGIC     target.tarifa_matricula = source.tarifa_matricula,
# MAGIC     target.area_sede = source.area_sede,
# MAGIC     target.area_id = source.area_id,
# MAGIC     target.admisionsino_2 = source.admisionsino_2,
# MAGIC     target.year = source.year,
# MAGIC     target.codigo_sede = source.codigo_sede,
# MAGIC     target.plan_title = source.plan_title,
# MAGIC     target.no_ultimas_plazas = source.no_ultimas_plazas,
# MAGIC     target.zoho_id = source.zoho_id,
# MAGIC     target.ano_inicio_docencia_2 = source.ano_inicio_docencia_2,
# MAGIC     target.mesesampliacion = source.mesesampliacion,
# MAGIC     target.codigo_antiguo = source.codigo_antiguo,
# MAGIC     target.nombre_del_programa_oficial_completo = source.nombre_del_programa_oficial_completo,
# MAGIC     target.codigo_entidad_legal = source.codigo_entidad_legal,
# MAGIC     target.fecha_fin_docencia = source.fecha_fin_docencia,
# MAGIC     target.nombreweb = source.nombreweb,
# MAGIC     target.area_codigo_vertical = source.area_codigo_vertical,
# MAGIC     target.tiponegocio_2 = source.tiponegocio_2,
# MAGIC     target.enroll_end = source.enroll_end,
# MAGIC     target.modalidad_code = source.modalidad_code,
# MAGIC     target.area_entidad_legal = source.area_entidad_legal,
# MAGIC     target.ciclo_title = source.ciclo_title,
# MAGIC     target.building_title = source.building_title,
# MAGIC     target.school_id = source.school_id,
# MAGIC     target.grupo_2 = source.grupo_2,
# MAGIC     target.building_id = source.building_id,
# MAGIC     target.plan_id = source.plan_id,
# MAGIC     target.counters_availables = source.counters_availables,
# MAGIC     target.ultima_actualizacion = source.ultima_actualizacion,
# MAGIC     target.mes_inicio_docencia_2 = source.mes_inicio_docencia_2,
# MAGIC     target.counters_enrolled = source.counters_enrolled,
# MAGIC     target.horas_acreditadas_2 = source.horas_acreditadas_2,
# MAGIC     target.receipts_count = source.receipts_count,
# MAGIC     target.tiponegocio = source.tiponegocio,
# MAGIC     target.horas_presenciales = source.horas_presenciales,
# MAGIC     target.enroll_group_name = source.enroll_group_name,
# MAGIC     target.fecha_inicio_pago = source.fecha_inicio_pago,
# MAGIC     target.enroll_alias = source.enroll_alias,
# MAGIC     target.building = source.building,
# MAGIC     target.school_name = source.school_name,
# MAGIC     target.cuotas_docencia = source.cuotas_docencia,
# MAGIC     target.enroll_ini = source.enroll_ini,
# MAGIC     target.acreditado = source.acreditado,
# MAGIC     target.descripcion_calendario = source.descripcion_calendario,
# MAGIC     target.destinatarios = source.destinatarios,
# MAGIC     target.enroll_pago_ini_t = source.enroll_pago_ini_t,
# MAGIC     target.nombre_antiguo_de_programa = source.nombre_antiguo_de_programa,
# MAGIC     target.certificado_euneiz_incluido = source.certificado_euneiz_incluido,
# MAGIC     target.group_entidad_legal = source.group_entidad_legal,
# MAGIC     target.area_vertical = source.area_vertical,
# MAGIC     target.group_entidad_legal_codigo = source.group_entidad_legal_codigo,
# MAGIC     target.counters_seats = source.counters_seats,
# MAGIC     target.codigo_especialidad = source.codigo_especialidad,
# MAGIC     target.descripcion_calendario_2 = source.descripcion_calendario_2,
# MAGIC     target.ciclo_id = source.ciclo_id,
# MAGIC     target.section_id = source.section_id,
# MAGIC     target.codigo_vertical = source.codigo_vertical,
# MAGIC     target.mes_inicio_docencia = source.mes_inicio_docencia,
# MAGIC     target.section_title = source.section_title,
# MAGIC     target.area_entidad_legal_codigo = source.area_entidad_legal_codigo,
# MAGIC     target.group_sede = source.group_sede,
# MAGIC     target.fecha_creacion = source.fecha_creacion,
# MAGIC     target.tarifa_docencia = source.tarifa_docencia,
# MAGIC     target.total_tarifas = source.total_tarifas,
# MAGIC     target.group_codigo_vertical = source.group_codigo_vertical,
# MAGIC     target.roaster_ind = source.roaster_ind
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;
# MAGIC
