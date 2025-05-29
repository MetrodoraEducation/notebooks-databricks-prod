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

# 📌 Mostrar los primeros registros
display(classlifetitulaciones_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, to_date
from pyspark.sql.types import StringType, IntegerType

# 📌 Lista de columnas que podrían no existir y deben agregarse como NULL
columnas_a_forzar_null = [
    "admisiones","codigo_promocion_id","paymentmethod","modalidad","lead_admission","lead_segment",
    "lead_asnew","lead_date","lead_message_read","lead_phone","lead_lastname","lead_status",
    "lead_name","lead_source","paymentmethodwannme","newsletter", "school_id_2", "codigo_promocion",
    "created_on", "enroll_stage", "lead_id", "lead_lastnameend", "tipopagador", "lead_area",
    "acceso_euneiz", "lead_email", "lead_count", "lead_alias", "suma_descuentos", "incompany",
    "enroll_step", "lead_language", "updated_at_2", "zoho_deal_id", "first_activate_enroll", "degree_title", "degree_id", "term_id", "updated_at", "centredeprocedencia", "fiscaladmit_codipais", "factura_correo", "cip", "anydepreinscripcio", "foce_create_lead"
]

# 🔧 Crear columnas nulas si no existen
for col_name in columnas_a_forzar_null:
    if col_name not in classlifetitulaciones_df.columns:
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn(col_name, lit(None).cast(StringType()))

# 📌 Transformación de columnas
columnas_con_tipo = [
    ("processdate", current_timestamp()), 
    ("sourcesystem", lit("classlifeStudents_931")),
    ("centredeprocedencia", col("centredeprocedencia").cast(StringType())),
    ("profesion", col("profesion").cast(StringType())),
    ("foce_create_lead", col("foce_create_lead").cast(StringType())),
    ("fiscaladmit_codipais", col("fiscaladmit_codipais").cast(StringType())),
    ("cip", col("cip").cast(StringType())),
    ("anydepreinscripcio", col("anydepreinscripcio").cast(StringType())),
    ("factura_correo", col("factura_correo").cast(StringType())),
    ("student_id", col("student_id").cast(StringType())),
    ("student_phone", col("student_phone").cast(StringType())),
    ("student_email", col("student_email").cast(StringType())),
    ("student_full_name", col("student_full_name").cast(StringType())),
    ("school_id", col("school_id").cast(StringType())),
    ("school_id_2", col("school_id_2").cast(StringType())),
    ("lead_id", col("lead_id").cast(StringType())),
    ("lead_alias", col("lead_alias").cast(StringType())),
    ("lead_name", col("lead_name").cast(StringType())),
    ("lead_lastname", col("lead_lastname").cast(StringType())),
    ("lead_lastnameend", col("lead_lastnameend").cast(StringType())),
    ("lead_email", col("lead_email").cast(StringType())),
    ("lead_phone", col("lead_phone").cast(StringType())),
    ("lead_status", col("lead_status").cast(StringType())),
    ("lead_segment", col("lead_segment").cast(StringType())),
    ("lead_language", col("lead_language").cast(StringType())),
    ("lead_admission", col("lead_admission").cast(StringType())),
    ("lead_asnew", col("lead_asnew").cast(StringType())),
    ("lead_message_read", col("lead_message_read").cast(StringType())),
    ("lead_count", col("lead_count").cast(IntegerType())),
    ("lead_source", col("lead_source").cast(StringType())),
    ("admisiones", col("admisiones").cast(StringType())),
    ("modalidad", col("modalidad").cast(StringType())),
    ("codigo_promocion", col("codigo_promocion").cast(StringType())),
    ("codigo_promocion_id", col("codigo_promocion_id").cast(StringType())),
    ("degree_title", col("degree_title").cast(StringType())),
    ("degree_id", col("degree_id").cast(StringType())),
    ("term_id", col("term_id").cast(StringType())),
    ("enroll_stage", col("enroll_stage").cast(StringType())),
    ("incompany", col("incompany").cast(StringType())),
    ("newsletter", col("newsletter").cast(StringType())),
    ("acceso_euneiz", col("acceso_euneiz").cast(StringType())),
    ("tipopagador", col("tipopagador").cast(StringType())),
    ("lead_area", col("lead_area").cast(StringType())),
    ("suma_descuentos", col("suma_descuentos").cast(StringType())),
    ("enroll_step", col("enroll_step").cast(StringType())),
    ("created_on", to_timestamp(col("created_on"))),
    ("updated_at", to_timestamp(col("updated_at"))),
    ("updated_at_2", to_timestamp(col("updated_at_2"))),
    ("zoho_deal_id", col("zoho_deal_id").cast(StringType())),
    ("first_activate_enroll", col("first_activate_enroll").cast(StringType())),
    ("fee_title_docencia", col("fee_title_docencia").cast(StringType())),
    ("fee_title_matricula", col("fee_title_matricula").cast(StringType())),
]

# ✅ Aplicar las transformaciones
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[expr.alias(nombre) for nombre, expr in columnas_con_tipo]
)

# 👉 Mostrar el resultado
display(classlifetitulaciones_df)

# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.dropDuplicates()

# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

#%sql
#CREATE TABLE silver_lakehouse.classlifeStudents_backup
#AS
#SELECT * FROM silver_lakehouse.classlifeStudents
#WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.classlifeStudents_backup AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.student_id = source.student_id
# MAGIC
# MAGIC WHEN MATCHED AND  
# MAGIC     ( 
# MAGIC         target.centredeprocedencia IS DISTINCT FROM source.centredeprocedencia OR
# MAGIC         target.modalidad IS DISTINCT FROM source.modalidad OR
# MAGIC         target.profesion IS DISTINCT FROM source.profesion OR
# MAGIC         target.cip IS DISTINCT FROM source.cip OR
# MAGIC         target.anydepreinscripcio IS DISTINCT FROM NULL OR
# MAGIC         target.degree_id IS DISTINCT FROM source.degree_id OR
# MAGIC         target.newsletter IS DISTINCT FROM source.newsletter OR
# MAGIC         target.foce_create_lead IS DISTINCT FROM source.foce_create_lead OR
# MAGIC         target.student_language IS DISTINCT FROM NULL OR
# MAGIC         target.fiscaladmit_direccion IS DISTINCT FROM source.fiscaladmit_direccion OR
# MAGIC         target.language IS DISTINCT FROM source.language OR
# MAGIC         target.nacimiento IS DISTINCT FROM source.nacimiento OR
# MAGIC         target.year_id IS DISTINCT FROM source.year_id OR
# MAGIC         target.direccion IS DISTINCT FROM source.direccion OR
# MAGIC         target.databaixaacademica IS DISTINCT FROM source.databaixaacademica OR
# MAGIC         target.fiscaladmit_iban IS DISTINCT FROM source.fiscaladmit_iban OR
# MAGIC         target.codipaisnaixement IS DISTINCT FROM source.codipaisnaixement OR
# MAGIC         target.titulouniversitariofisioterapia IS DISTINCT FROM source.titulouniversitariofisioterapia OR
# MAGIC         target.lead_alias IS DISTINCT FROM source.lead_alias OR
# MAGIC         target.basephone IS DISTINCT FROM source.basephone OR
# MAGIC         target.colegiadoprofesional IS DISTINCT FROM source.colegiadoprofesional OR
# MAGIC         target.ciudad IS DISTINCT FROM source.ciudad OR
# MAGIC         target.seguridadsocial IS DISTINCT FROM source.seguridadsocial OR
# MAGIC         target.factura_pais IS DISTINCT FROM source.factura_pais OR
# MAGIC         target.lastname IS DISTINCT FROM source.lastname OR
# MAGIC         target.datosacceso_ultim_estudi_matriculat IS DISTINCT FROM source.datosacceso_ultim_estudi_matriculat OR
# MAGIC         target.certificadouniversitariosolicitudtitulo IS DISTINCT FROM source.certificadouniversitariosolicitudtitulo OR
# MAGIC         target.dnumero IS DISTINCT FROM source.dnumero OR
# MAGIC         target.codipreinscripcio IS DISTINCT FROM source.codipreinscripcio OR
# MAGIC         target.student_blocked IS DISTINCT FROM source.student_blocked OR
# MAGIC         target.student_phone IS DISTINCT FROM source.student_phone OR
# MAGIC         target.student_email IS DISTINCT FROM source.student_email OR
# MAGIC         target.student_uid IS DISTINCT FROM source.student_uid OR
# MAGIC         target.descala IS DISTINCT FROM source.descala OR
# MAGIC         target.lead_date IS DISTINCT FROM source.lead_date OR
# MAGIC         target.dplanta IS DISTINCT FROM source.dplanta OR
# MAGIC         target.fiscaladmit_movil IS DISTINCT FROM source.fiscaladmit_movil OR
# MAGIC         target.student_active IS DISTINCT FROM source.student_active OR
# MAGIC         target.student_lastname IS DISTINCT FROM source.student_lastname OR
# MAGIC         target.email2 IS DISTINCT FROM source.email2 OR
# MAGIC         target.ncolegiado IS DISTINCT FROM source.ncolegiado OR
# MAGIC         target.term_id IS DISTINCT FROM source.term_id OR
# MAGIC         target.phone IS DISTINCT FROM source.phone OR
# MAGIC         target.admit_dni_front IS DISTINCT FROM source.admit_dni_front OR
# MAGIC         target.classlife_uid IS DISTINCT FROM source.classlife_uid OR
# MAGIC         target.dtipus IS DISTINCT FROM source.dtipus OR
# MAGIC         target.factura_ciudad IS DISTINCT FROM source.factura_ciudad OR
# MAGIC         target.fiscaladmit_cif IS DISTINCT FROM source.fiscaladmit_cif OR
# MAGIC         target.area_id IS DISTINCT FROM source.area_id OR
# MAGIC         target.enroll_ref IS DISTINCT FROM source.enroll_ref OR
# MAGIC         target.zoho_id IS DISTINCT FROM source.zoho_id OR
# MAGIC         target.excludesecurityarraymetas IS DISTINCT FROM source.excludesecurityarraymetas OR
# MAGIC         target.dpuerta IS DISTINCT FROM source.dpuerta OR
# MAGIC         target.politicas IS DISTINCT FROM source.politicas OR
# MAGIC         target.sexo IS DISTINCT FROM source.sexo OR
# MAGIC         target.student_id IS DISTINCT FROM source.student_id OR
# MAGIC         target.datosacceso_curs_ultim_estudi_matriculat IS DISTINCT FROM source.datosacceso_curs_ultim_estudi_matriculat OR
# MAGIC         target.lead_lastname IS DISTINCT FROM source.lead_lastname OR
# MAGIC         target.lead_status IS DISTINCT FROM source.lead_status OR
# MAGIC         target.nommunicipinaixementfora IS DISTINCT FROM source.nommunicipinaixementfora OR
# MAGIC         target.created_on IS DISTINCT FROM source.created_on OR
# MAGIC         target.school_id IS DISTINCT FROM source.school_id OR
# MAGIC         target.fiscaladmit_lastnameend IS DISTINCT FROM source.fiscaladmit_lastnameend OR
# MAGIC         target.student_lastnameend IS DISTINCT FROM source.student_lastnameend OR
# MAGIC         target.fiscaladmit_correo IS DISTINCT FROM source.fiscaladmit_correo OR
# MAGIC         target.dbloc IS DISTINCT FROM source.dbloc OR
# MAGIC         target.factura_numfiscal IS DISTINCT FROM source.factura_numfiscal OR
# MAGIC         target.lead_count IS DISTINCT FROM source.lead_count OR
# MAGIC         target.nia IS DISTINCT FROM source.nia OR
# MAGIC         target.factura_nomfactura IS DISTINCT FROM source.factura_nomfactura OR
# MAGIC         target.updated_at IS DISTINCT FROM source.updated_at OR
# MAGIC         target.dataingres IS DISTINCT FROM source.dataingres OR
# MAGIC         target.edad IS DISTINCT FROM source.edad OR
# MAGIC         target.incompany IS DISTINCT FROM source.incompany OR
# MAGIC         target.telefono IS DISTINCT FROM source.telefono OR
# MAGIC         target.fiscaladmit_codigo IS DISTINCT FROM source.fiscaladmit_codigo OR
# MAGIC         target.comunidadautonoma IS DISTINCT FROM source.comunidadautonoma OR
# MAGIC         target.admit_dni_back IS DISTINCT FROM source.admit_dni_back OR
# MAGIC         target.codigo IS DISTINCT FROM source.codigo OR
# MAGIC         target.lastnameend IS DISTINCT FROM source.lastnameend OR
# MAGIC         target.lead_admission IS DISTINCT FROM source.lead_admission OR
# MAGIC         target.lead_asnew IS DISTINCT FROM source.lead_asnew OR
# MAGIC         target.email IS DISTINCT FROM source.email OR
# MAGIC         target.student_registration_date IS DISTINCT FROM source.student_registration_date OR
# MAGIC         target.student_name IS DISTINCT FROM source.student_name OR
# MAGIC         target.name IS DISTINCT FROM source.name OR
# MAGIC         target.pais IS DISTINCT FROM source.pais OR
# MAGIC         target.school_id_2 IS DISTINCT FROM source.school_id_2 OR
# MAGIC         target.datosacceso_pais_ultim_curs_matriculat IS DISTINCT FROM source.datosacceso_pais_ultim_curs_matriculat OR
# MAGIC         target.medicas IS DISTINCT FROM source.medicas OR
# MAGIC         target.lead_id IS DISTINCT FROM source.lead_id OR
# MAGIC         target.ciclo_id IS DISTINCT FROM source.ciclo_id OR
# MAGIC         target.section_id IS DISTINCT FROM source.section_id OR
# MAGIC         target.factura_movil IS DISTINCT FROM source.factura_movil OR
# MAGIC         target.codinacionalitat IS DISTINCT FROM source.codinacionalitat OR
# MAGIC         target.factura_direccion IS DISTINCT FROM source.factura_direccion OR
# MAGIC         target.provincia IS DISTINCT FROM source.provincia OR
# MAGIC         target.codiprovincianaixement IS DISTINCT FROM source.codiprovincianaixement OR
# MAGIC         target.telefono2 IS DISTINCT FROM source.telefono2 OR
# MAGIC         target.codimunicipinaixement IS DISTINCT FROM source.codimunicipinaixement OR
# MAGIC         target.matricula4gradofisioterapia IS DISTINCT FROM source.matricula4gradofisioterapia OR
# MAGIC         target.fiscaladmit_ciudad IS DISTINCT FROM source.fiscaladmit_ciudad OR
# MAGIC         target.student_key IS DISTINCT FROM source.student_key OR
# MAGIC         target.fiscaladmit_titular IS DISTINCT FROM source.fiscaladmit_titular OR
# MAGIC         target.codipais IS DISTINCT FROM source.codipais OR
# MAGIC         target.student_full_name IS DISTINCT FROM source.student_full_name OR
# MAGIC         target.tipusdocument IS DISTINCT FROM source.tipusdocument OR
# MAGIC         target.factura_codigo IS DISTINCT FROM source.factura_codigo
# MAGIC     ) 
# MAGIC THEN 
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT *;
