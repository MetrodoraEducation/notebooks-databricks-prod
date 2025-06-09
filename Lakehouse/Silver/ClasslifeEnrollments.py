# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "enrollments"
table_name = "JsaClassLifeEnrollments"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife/{endpoint_process_name}/{current_date}/{table_name}.json")
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

# DBTITLE 1,Desanida fees
from pyspark.sql.functions import col, explode, first, when, coalesce, lit

# 📌 Desanidar fees si es un ArrayType
if "fees" in classlifetitulaciones_df.columns:
    fees_schema = classlifetitulaciones_df.schema["fees"].dataType

    if hasattr(fees_schema, "elementType"):  # Es un ArrayType
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn("fees", explode(col("fees")))

    # 📌 Extraer los campos de la estructura fees si es un StructType
    fees_schema = classlifetitulaciones_df.schema["fees"].dataType
    if hasattr(fees_schema, "fields"):  # Es un StructType
        fees_cols = [f.name for f in fees_schema.fields]

        classlifetitulaciones_df = classlifetitulaciones_df.select(
            "*", 
            *[col(f"fees.{c}").alias(f"{c}") for c in fees_cols]  # Renombramos con el mismo nombre
        ).drop("fees")

# 📌 Crear fee_title_matricula y fee_title_docencia con valores específicos
classlifetitulaciones_df = classlifetitulaciones_df.withColumn(
    "fee_title_matricula", when(col("fee_title") == "Matrícula", col("fee_amount"))
).withColumn(
    "fee_title_docencia", when(col("fee_title") == "Docencia", col("fee_amount"))
)

# 📌 Obtener todas las columnas originales excepto las eliminadas
columnas_originales = [c for c in classlifetitulaciones_df.columns if c not in ["fee_title", "fee_amount", "enroll_id"]]

# 📌 Consolidar registros por enroll_id y mantener todas las columnas originales
classlifetitulaciones_df = classlifetitulaciones_df.groupBy("enroll_id").agg(
    *[first(col(c), ignorenulls=True).alias(c) for c in columnas_originales if c not in ["fee_title_matricula", "fee_title_docencia"]],
    coalesce(first("fee_title_matricula", ignorenulls=True), lit(0)).alias("fee_title_matricula"),
    coalesce(first("fee_title_docencia", ignorenulls=True), lit(0)).alias("fee_title_docencia")
)

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
            .replace("", "")
            .replace("metas_", "")
            .replace("no__", "no_")
            .replace("fees_", "")
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

# 📌 Verificar DataFrame después de la limpieza
columnas_actuales = set(classlifetitulaciones_df.columns)

# 📌 Seleccionar solo las columnas válidas si existen
columnas_seleccionadas = list(columnas_actuales)

# 📌 Solución para nombres con comillas invertidas
# Aplicamos alias() para normalizar los nombres con comillas invertidas
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).alias(c.strip().replace("", "")) for c in columnas_seleccionadas]
)

#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Asignar Columnas
from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType, IntegerType

# 📌 Lista de columnas con transformaciones
columnas_con_tipo = [
    ("processdate", current_timestamp()), 
    ("sourcesystem", lit("ClasslifeEnrollments")), 
    ("admisiones", col("admisiones").cast(StringType())), 
    ("codigo_promocion_id", col("codigo_promocion_id").cast(StringType())), 
    ("enroll_ini", col("enroll_ini").cast(StringType())), 
    ("modalidad", col("modalidad").cast(StringType())), 
    ("paymentmethod", col("paymentmethod").cast(StringType())), 
    ("lead_admission", col("lead_admission").cast(StringType())), 
    ("lead_segment", col("lead_segment").cast(StringType())), 
    ("lead_asnew", col("lead_asnew").cast(StringType())), 
    ("degree_title", col("degree_title").cast(StringType())), 
    ("lead_date", to_date(col("lead_date"))), 
    ("enroll_id", col("enroll_id").cast(StringType())), 
    ("student_id", col("student_id").cast(StringType())), 
    ("lead_message_read", col("lead_message_read").cast(StringType())), 
    ("lead_phone", col("lead_phone").cast(StringType())), 
    ("lead_lastname", col("lead_lastname").cast(StringType())), 
    ("lead_status", col("lead_status").cast(StringType())), 
    ("lead_name", col("lead_name").cast(StringType())), 
    ("totalenroll", col("totalenroll").cast(StringType())), 
    ("enroll_end", col("enroll_end").cast(StringType())), 
    ("lead_source", col("lead_source").cast(StringType())), 
    ("paymentmethodwannme", col("paymentmethodwannme").cast(StringType())), 
    ("degree_id", col("degree_id").cast(StringType())), 
    ("newsletter", col("newsletter").cast(StringType())), 
    ("school_id_2", col("school_id_2").cast(StringType())), 
    ("codigo_promocion", col("codigo_promocion").cast(StringType())), 
    ("created_on", to_timestamp(col("created_on"))), 
    ("term_id", col("term_id").cast(StringType())), 
    ("enroll_group", col("enroll_group").cast(StringType())), 
    ("ciclo_title", col("ciclo_title").cast(StringType())), 
    ("enroll_stage", col("enroll_stage").cast(StringType())), 
    ("school_id", col("school_id").cast(StringType())), 
    ("lead_id", col("lead_id").cast(StringType())), 
    ("lead_lastnameend", col("lead_lastnameend").cast(StringType())), 
    ("admisiones_acepta_candidato", col("admisiones_acepta_candidato").cast(StringType())), 
    ("tipopagador", col("tipopagador").cast(StringType())), 
    ("ciclo_id", col("ciclo_id").cast(StringType())), 
    ("section_id", col("section_id").cast(StringType())), 
    ("area_id", col("area_id").cast(StringType())), 
    ("lead_area", col("lead_area").cast(StringType())), 
    ("acceso_euneiz", col("acceso_euneiz").cast(StringType())), 
    ("lead_email", col("lead_email").cast(StringType())), 
    ("enroll_alias", col("enroll_alias").cast(StringType())), 
    ("year", col("year").cast(StringType())), 
    ("section_title", col("section_title").cast(StringType())), 
    ("enroll_in", col("enroll_in").cast(StringType())), 
    ("lead_count", col("lead_count").cast(IntegerType())), 
    ("updated_at", to_timestamp(col("updated_at"))), 
    ("lead_alias", col("lead_alias").cast(StringType())), 
    ("suma_descuentos", col("suma_descuentos").cast(StringType())), 
    ("area_title", col("area_title").cast(StringType())), 
    ("incompany", col("incompany").cast(StringType())), 
    ("enroll_step", col("enroll_step").cast(StringType())), 
    ("student_full_name", col("student_full_name").cast(StringType())), 
    ("lead_language", col("lead_language").cast(StringType())), 
    ("enroll_status_id", col("enroll_status_id").cast(StringType())), 
    ("enroll_status", col("enroll_status").cast(StringType())), 
    ("excludesecurityarraymetas", col("excludesecurityarraymetas").cast(StringType())), 
    ("updated_at_2", to_timestamp(col("updated_at_2"))), 
    ("term_title", col("term_title").cast(StringType())),
    ("school_name", col("school_name").cast(StringType())),
    ("zoho_deal_id", col("zoho_deal_id").cast(StringType())),
    ("first_activate_enroll", col("first_activate_enroll").cast(StringType())),
    ("fee_title_docencia", col("fee_title_docencia").cast(StringType())),
    ("fee_title_matricula", col("fee_title_matricula").cast(StringType()))
]

# 📌 Aplicar transformaciones
classlifetitulaciones_df = classlifetitulaciones_df.select(
    [expr.alias(nombre) for nombre, expr in columnas_con_tipo]
)

# 📌 Mostrar solo las columnas que acabas de definir
columnas_a_mostrar = [nombre for nombre, _ in columnas_con_tipo]
#display(classlifetitulaciones_df.select(columnas_a_mostrar))

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
# MAGIC MERGE INTO silver_lakehouse.ClasslifeEnrollments AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.enroll_id = source.enroll_id
# MAGIC WHEN MATCHED AND (
# MAGIC     target.admisiones IS DISTINCT FROM source.admisiones OR
# MAGIC     target.codigo_promocion_id IS DISTINCT FROM source.codigo_promocion_id OR
# MAGIC     target.enroll_ini IS DISTINCT FROM source.enroll_ini OR
# MAGIC     target.modalidad IS DISTINCT FROM source.modalidad OR
# MAGIC     target.paymentmethod IS DISTINCT FROM source.paymentmethod OR
# MAGIC     target.lead_admission IS DISTINCT FROM source.lead_admission OR
# MAGIC     target.lead_segment IS DISTINCT FROM source.lead_segment OR
# MAGIC     target.lead_asnew IS DISTINCT FROM source.lead_asnew OR
# MAGIC     target.degree_title IS DISTINCT FROM source.degree_title OR
# MAGIC     target.lead_date IS DISTINCT FROM source.lead_date OR
# MAGIC     target.student_id IS DISTINCT FROM source.student_id OR
# MAGIC     target.lead_message_read IS DISTINCT FROM source.lead_message_read OR
# MAGIC     target.lead_phone IS DISTINCT FROM source.lead_phone OR
# MAGIC     target.lead_lastname IS DISTINCT FROM source.lead_lastname OR
# MAGIC     target.lead_status IS DISTINCT FROM source.lead_status OR
# MAGIC     target.lead_name IS DISTINCT FROM source.lead_name OR
# MAGIC     target.totalenroll IS DISTINCT FROM source.totalenroll OR
# MAGIC     target.enroll_end IS DISTINCT FROM source.enroll_end OR
# MAGIC     target.lead_source IS DISTINCT FROM source.lead_source OR
# MAGIC     target.paymentmethodwannme IS DISTINCT FROM source.paymentmethodwannme OR
# MAGIC     target.degree_id IS DISTINCT FROM source.degree_id OR
# MAGIC     target.newsletter IS DISTINCT FROM source.newsletter OR
# MAGIC     target.school_id_2 IS DISTINCT FROM source.school_id_2 OR
# MAGIC     target.codigo_promocion IS DISTINCT FROM source.codigo_promocion OR
# MAGIC     target.created_on IS DISTINCT FROM source.created_on OR
# MAGIC     target.term_id IS DISTINCT FROM source.term_id OR
# MAGIC     target.enroll_group IS DISTINCT FROM source.enroll_group OR
# MAGIC     target.ciclo_title IS DISTINCT FROM source.ciclo_title OR
# MAGIC     target.enroll_stage IS DISTINCT FROM source.enroll_stage OR
# MAGIC     target.school_id IS DISTINCT FROM source.school_id OR
# MAGIC     target.lead_id IS DISTINCT FROM source.lead_id OR
# MAGIC     target.lead_lastnameend IS DISTINCT FROM source.lead_lastnameend OR
# MAGIC     target.admisiones_acepta_candidato IS DISTINCT FROM source.admisiones_acepta_candidato OR
# MAGIC     target.tipopagador IS DISTINCT FROM source.tipopagador OR
# MAGIC     target.ciclo_id IS DISTINCT FROM source.ciclo_id OR
# MAGIC     target.section_id IS DISTINCT FROM source.section_id OR
# MAGIC     target.area_id IS DISTINCT FROM source.area_id OR
# MAGIC     target.lead_area IS DISTINCT FROM source.lead_area OR
# MAGIC     target.acceso_euneiz IS DISTINCT FROM source.acceso_euneiz OR
# MAGIC     target.lead_email IS DISTINCT FROM source.lead_email OR
# MAGIC     target.enroll_alias IS DISTINCT FROM source.enroll_alias OR
# MAGIC     target.year IS DISTINCT FROM source.year OR
# MAGIC     target.section_title IS DISTINCT FROM source.section_title OR
# MAGIC     target.enroll_in IS DISTINCT FROM source.enroll_in OR
# MAGIC     target.lead_count IS DISTINCT FROM source.lead_count OR
# MAGIC     target.updated_at IS DISTINCT FROM source.updated_at OR
# MAGIC     target.lead_alias IS DISTINCT FROM source.lead_alias OR
# MAGIC     target.suma_descuentos IS DISTINCT FROM source.suma_descuentos OR
# MAGIC     target.area_title IS DISTINCT FROM source.area_title OR
# MAGIC     target.incompany IS DISTINCT FROM source.incompany OR
# MAGIC     target.enroll_step IS DISTINCT FROM source.enroll_step OR
# MAGIC     target.student_full_name IS DISTINCT FROM source.student_full_name OR
# MAGIC     target.lead_language IS DISTINCT FROM source.lead_language OR
# MAGIC     target.enroll_status_id IS DISTINCT FROM source.enroll_status_id OR
# MAGIC     target.enroll_status IS DISTINCT FROM source.enroll_status OR
# MAGIC     target.excludesecurityarraymetas IS DISTINCT FROM source.excludesecurityarraymetas OR
# MAGIC     target.updated_at_2 IS DISTINCT FROM source.updated_at_2 OR
# MAGIC     target.term_title IS DISTINCT FROM source.term_title OR
# MAGIC     target.school_name IS DISTINCT FROM source.school_name OR
# MAGIC     target.first_activate_enroll IS DISTINCT FROM source.first_activate_enroll OR
# MAGIC     target.fee_title_docencia IS DISTINCT FROM source.fee_title_docencia OR
# MAGIC     target.fee_title_matricula IS DISTINCT FROM source.fee_title_matricula
# MAGIC ) THEN
# MAGIC UPDATE SET
# MAGIC     target.admisiones = source.admisiones,
# MAGIC     target.codigo_promocion_id = source.codigo_promocion_id,
# MAGIC     target.enroll_ini = source.enroll_ini,
# MAGIC     target.modalidad = source.modalidad,
# MAGIC     target.paymentmethod = source.paymentmethod,
# MAGIC     target.lead_admission = source.lead_admission,
# MAGIC     target.lead_segment = source.lead_segment,
# MAGIC     target.lead_asnew = source.lead_asnew,
# MAGIC     target.degree_title = source.degree_title,
# MAGIC     target.lead_date = source.lead_date,
# MAGIC     target.student_id = source.student_id,
# MAGIC     target.lead_message_read = source.lead_message_read,
# MAGIC     target.lead_phone = source.lead_phone,
# MAGIC     target.lead_lastname = source.lead_lastname,
# MAGIC     target.lead_status = source.lead_status,
# MAGIC     target.lead_name = source.lead_name,
# MAGIC     target.totalenroll = source.totalenroll,
# MAGIC     target.enroll_end = source.enroll_end,
# MAGIC     target.lead_source = source.lead_source,
# MAGIC     target.paymentmethodwannme = source.paymentmethodwannme,
# MAGIC     target.degree_id = source.degree_id,
# MAGIC     target.newsletter = source.newsletter,
# MAGIC     target.school_id_2 = source.school_id_2,
# MAGIC     target.codigo_promocion = source.codigo_promocion,
# MAGIC     target.created_on = source.created_on,
# MAGIC     target.term_id = source.term_id,
# MAGIC     target.enroll_group = source.enroll_group,
# MAGIC     target.ciclo_title = source.ciclo_title,
# MAGIC     target.enroll_stage = source.enroll_stage,
# MAGIC     target.school_id = source.school_id,
# MAGIC     target.lead_id = source.lead_id,
# MAGIC     target.lead_lastnameend = source.lead_lastnameend,
# MAGIC     target.admisiones_acepta_candidato = source.admisiones_acepta_candidato,
# MAGIC     target.tipopagador = source.tipopagador,
# MAGIC     target.ciclo_id = source.ciclo_id,
# MAGIC     target.section_id = source.section_id,
# MAGIC     target.area_id = source.area_id,
# MAGIC     target.lead_area = source.lead_area,
# MAGIC     target.acceso_euneiz = source.acceso_euneiz,
# MAGIC     target.lead_email = source.lead_email,
# MAGIC     target.enroll_alias = source.enroll_alias,
# MAGIC     target.year = source.year,
# MAGIC     target.section_title = source.section_title,
# MAGIC     target.enroll_in = source.enroll_in,
# MAGIC     target.lead_count = source.lead_count,
# MAGIC     target.updated_at = source.updated_at,
# MAGIC     target.lead_alias = source.lead_alias,
# MAGIC     target.suma_descuentos = source.suma_descuentos,
# MAGIC     target.area_title = source.area_title,
# MAGIC     target.incompany = source.incompany,
# MAGIC     target.enroll_step = source.enroll_step,
# MAGIC     target.student_full_name = source.student_full_name,
# MAGIC     target.lead_language = source.lead_language,
# MAGIC     target.enroll_status_id = source.enroll_status_id,
# MAGIC     target.enroll_status = source.enroll_status,
# MAGIC     target.excludesecurityarraymetas = source.excludesecurityarraymetas,
# MAGIC     target.updated_at_2 = source.updated_at_2,
# MAGIC     target.term_title = source.term_title,
# MAGIC     target.school_name = source.school_name,
# MAGIC     target.first_activate_enroll = source.first_activate_enroll,
# MAGIC     target.fee_title_docencia = source.fee_title_docencia,
# MAGIC     target.fee_title_matricula = source.fee_title_matricula
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT enroll_id, COUNT(*)
# MAGIC FROM silver_lakehouse.ClasslifeEnrollments
# MAGIC GROUP BY enroll_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
