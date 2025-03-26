# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "students"
table_name = "JsaClassLifeStudents"

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

from pyspark.sql.functions import col, to_date, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType, IntegerType, DoubleType

# 📌 Aplicar transformaciones a las columnas con nombres corregidos y tipos de datos adecuados
classlifetitulaciones_df = classlifetitulaciones_df \
    .select(
        "centredeprocedencia", "modalidad", "profesion", "fiscaladmit_codipais", "lead_phone", "zenddesk_id",
        "lead_name", "factura_correo", "cip", "lead_source", "anydepreinscripcio", "degree_id", "newsletter",
        "foce_create_lead", "student_language", "fiscaladmit_direccion", "language", "nacimiento",
        "year_id", "direccion", "databaixaacademica", "fiscaladmit_iban", "codipaisnaixement",
        "titulouniversitariofisioterapia", "lead_alias", "basephone", "colegiadoprofesional", "ciudad",
        "seguridadsocial", "factura_pais", "lastname", "datosacceso_ultim_estudi_matriculat",
        "certificadouniversitariosolicitudtitulo", "dnumero", "codipreinscripcio", "student_blocked",
        "secondguardian_movil", "student_phone", "student_email", "student_uid", "descala", "lead_date", "dplanta",
        "fiscaladmit_movil", "student_active", "student_lastname", "email2", "ncolegiado", "term_id", "phone",
        "admit_dni_front", "classlife_uid", "dtipus", "factura_ciudad", "lead_lastnameend", "fiscaladmit_cif",
        "area_id", "lead_area", "secondguardian_name", "enroll_ref", "lead_email", "secondguardian_tipusdocument",
        "zoho_id", "excludesecurityarraymetas", "dpuerta", "politicas", "sexo", "lead_segment", "student_id",
        "secondguardian_numerodocument", "datosacceso_curs_ultim_estudi_matriculat", "lead_lastname", "lead_status",
        "nommunicipinaixementfora", "secondguardian_lastname", "created_on", "school_id", "fiscaladmit_lastnameend",
        "student_lastnameend", "fiscaladmit_correo", "dbloc", "factura_numfiscal", "lead_count", "nia",
        "factura_nomfactura", "updated_at", "dataingres", "edad", "incompany", "telefono", "fiscaladmit_codigo",
        "comunidadautonoma", "admit_dni_back", "codigo", "lastnameend", "secondguardian_telefono", "lead_admission",
        "lead_asnew", "email", "student_registration_date", "student_name", "name", "secondguardian_email", "pais",
        "school_id_2", "datosacceso_pais_ultim_curs_matriculat", "medicas", "lead_id", "ciclo_id", "section_id",
        "factura_movil", "codinacionalitat", "factura_direccion", "provincia", "codiprovincianaixement", "telefono2",
        "codimunicipinaixement", "matricula4gradofisioterapia", "fiscaladmit_ciudad", "student_key",
        "fiscaladmit_titular", "codipais", "lead_language", "student_full_name", "tipusdocument", "factura_codigo",
        "lead_message_read", "group"
    ) \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("ClasslifeStudents")) \
    .withColumn("nacimiento", col("nacimiento").cast(StringType())) \
    .withColumn("lead_date",  col("lead_date").cast(StringType())) \
    .withColumn("created_on", col("created_on").cast(StringType())) \
    .withColumn("updated_at", col("updated_at").cast(StringType())) \
    .withColumn("dataingres", col("dataingres").cast(StringType())) \
    .withColumn("student_registration_date", col("student_registration_date").cast(StringType())) \
    .withColumn("edad", col("edad").cast(IntegerType())) \
    .withColumn("lead_count", col("lead_count").cast(IntegerType())) \
    .withColumn("term_id", col("term_id").cast(IntegerType())) \
    .withColumn("nia", col("nia").cast(IntegerType())) \
    .withColumn("ciclo_id", col("ciclo_id").cast(IntegerType())) \
    .withColumn("section_id", col("section_id").cast(IntegerType())) \
    .withColumn("year_id", col("year_id").cast(IntegerType())) \
    .withColumn("seguridadsocial", col("seguridadsocial").cast(DoubleType())) \
    .withColumn("centredeprocedencia", col("centredeprocedencia").cast(StringType())) \
    .withColumn("modalidad", col("modalidad").cast(StringType())) \
    .withColumn("profesion", col("profesion").cast(StringType())) \
    .withColumn("fiscaladmit_codipais", col("fiscaladmit_codipais").cast(StringType())) \
    .withColumn("lead_phone", col("lead_phone").cast(StringType())) \
    .withColumn("zenddesk_id", col("zenddesk_id").cast(StringType())) \
    .withColumn("lead_name", col("lead_name").cast(StringType())) \
    .withColumn("factura_correo", col("factura_correo").cast(StringType())) \
    .withColumn("cip", col("cip").cast(StringType())) \
    .withColumn("lead_source", col("lead_source").cast(StringType())) \
    .withColumn("anydepreinscripcio", col("anydepreinscripcio").cast(StringType())) \
    .withColumn("degree_id", col("degree_id").cast(StringType())) \
    .withColumn("newsletter", col("newsletter").cast(StringType())) \
    .withColumn("foce_create_lead", col("foce_create_lead").cast(StringType())) \
    .withColumn("student_language", col("student_language").cast(StringType())) \
    .withColumn("fiscaladmit_direccion", col("fiscaladmit_direccion").cast(StringType())) \
    .withColumn("language", col("language").cast(StringType())) \
    .withColumn("direccion", col("direccion").cast(StringType())) \
    .withColumn("fiscaladmit_iban", col("fiscaladmit_iban").cast(StringType())) \
    .withColumn("codipaisnaixement", col("codipaisnaixement").cast(StringType())) \
    .withColumn("lead_alias", col("lead_alias").cast(StringType())) \
    .withColumn("basephone", col("basephone").cast(StringType())) \
    .withColumn("colegiadoprofesional", col("colegiadoprofesional").cast(StringType())) \
    .withColumn("ciudad", col("ciudad").cast(StringType())) \
    .withColumn("factura_pais", col("factura_pais").cast(StringType())) \
    .withColumn("lastname", col("lastname").cast(StringType())) \
    .withColumn("lead_message_read", col("lead_message_read").cast(StringType())) \
    .withColumn("group", col("group").cast(StringType())) \

# 📌 Mostrar los primeros registros con todas las columnas
display(classlifetitulaciones_df)


# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.dropDuplicates()

# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.classlifeStudents AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.student_id = source.student_id
# MAGIC
# MAGIC WHEN MATCHED AND 
# MAGIC     ( 
# MAGIC         target.centredeprocedencia <> source.centredeprocedencia OR
# MAGIC         target.modalidad <> source.modalidad OR
# MAGIC         target.profesion <> source.profesion OR
# MAGIC         target.fiscaladmit_codipais <> source.fiscaladmit_codipais OR
# MAGIC         target.lead_phone <> source.lead_phone OR
# MAGIC         target.lead_name <> source.lead_name OR
# MAGIC         target.factura_correo <> source.factura_correo OR
# MAGIC         target.cip <> source.cip OR
# MAGIC         target.lead_source <> source.lead_source OR
# MAGIC         target.anydepreinscripcio <> source.anydepreinscripcio OR
# MAGIC         target.degree_id <> source.degree_id OR
# MAGIC         target.newsletter <> source.newsletter OR
# MAGIC         target.foce_create_lead <> source.foce_create_lead OR
# MAGIC         target.student_language <> source.student_language OR
# MAGIC         target.fiscaladmit_direccion <> source.fiscaladmit_direccion OR
# MAGIC         target.language <> source.language OR
# MAGIC         target.nacimiento <> source.nacimiento OR
# MAGIC         target.year_id <> source.year_id OR
# MAGIC         target.direccion <> source.direccion OR
# MAGIC         target.databaixaacademica <> source.databaixaacademica OR
# MAGIC         target.fiscaladmit_iban <> source.fiscaladmit_iban OR
# MAGIC         target.codipaisnaixement <> source.codipaisnaixement OR
# MAGIC         target.titulouniversitariofisioterapia <> source.titulouniversitariofisioterapia OR
# MAGIC         target.lead_alias <> source.lead_alias OR
# MAGIC         target.basephone <> source.basephone OR
# MAGIC         target.colegiadoprofesional <> source.colegiadoprofesional OR
# MAGIC         target.ciudad <> source.ciudad OR
# MAGIC         target.seguridadsocial <> source.seguridadsocial OR
# MAGIC         target.factura_pais <> source.factura_pais OR
# MAGIC         target.lastname <> source.lastname OR
# MAGIC         target.datosacceso_ultim_estudi_matriculat <> source.datosacceso_ultim_estudi_matriculat OR
# MAGIC         target.certificadouniversitariosolicitudtitulo <> source.certificadouniversitariosolicitudtitulo OR
# MAGIC         target.dnumero <> source.dnumero OR
# MAGIC         target.codipreinscripcio <> source.codipreinscripcio OR
# MAGIC         target.student_blocked <> source.student_blocked OR
# MAGIC         target.secondguardian_movil <> source.secondguardian_movil OR
# MAGIC         target.student_phone <> source.student_phone OR
# MAGIC         target.student_email <> source.student_email OR
# MAGIC         target.student_uid <> source.student_uid OR
# MAGIC         target.descala <> source.descala OR
# MAGIC         target.lead_date <> source.lead_date OR
# MAGIC         target.dplanta <> source.dplanta OR
# MAGIC         target.fiscaladmit_movil <> source.fiscaladmit_movil OR
# MAGIC         target.student_active <> source.student_active OR
# MAGIC         target.student_lastname <> source.student_lastname OR
# MAGIC         target.email2 <> source.email2 OR
# MAGIC         target.ncolegiado <> source.ncolegiado OR
# MAGIC         target.term_id <> source.term_id OR
# MAGIC         target.phone <> source.phone OR
# MAGIC         target.admit_dni_front <> source.admit_dni_front OR
# MAGIC         target.classlife_uid <> source.classlife_uid OR
# MAGIC         target.dtipus <> source.dtipus OR
# MAGIC         target.factura_ciudad <> source.factura_ciudad OR
# MAGIC         target.lead_lastnameend <> source.lead_lastnameend OR
# MAGIC         target.fiscaladmit_cif <> source.fiscaladmit_cif OR
# MAGIC         target.area_id <> source.area_id OR
# MAGIC         target.lead_area <> source.lead_area OR
# MAGIC         target.secondguardian_name <> source.secondguardian_name OR
# MAGIC         target.enroll_ref <> source.enroll_ref OR
# MAGIC         target.lead_email <> source.lead_email OR
# MAGIC         target.secondguardian_tipusdocument <> source.secondguardian_tipusdocument OR
# MAGIC         target.zoho_id <> source.zoho_id OR
# MAGIC         target.excludesecurityarraymetas <> source.excludesecurityarraymetas OR
# MAGIC         target.dpuerta <> source.dpuerta OR
# MAGIC         target.politicas <> source.politicas OR
# MAGIC         target.sexo <> source.sexo OR
# MAGIC         target.lead_segment <> source.lead_segment OR
# MAGIC         target.student_id <> source.student_id OR
# MAGIC         target.secondguardian_numerodocument <> source.secondguardian_numerodocument OR
# MAGIC         target.datosacceso_curs_ultim_estudi_matriculat <> source.datosacceso_curs_ultim_estudi_matriculat OR
# MAGIC         target.lead_lastname <> source.lead_lastname OR
# MAGIC         target.lead_status <> source.lead_status OR
# MAGIC         target.nommunicipinaixementfora <> source.nommunicipinaixementfora OR
# MAGIC         target.secondguardian_lastname <> source.secondguardian_lastname OR
# MAGIC         target.created_on <> source.created_on OR
# MAGIC         target.school_id <> source.school_id OR
# MAGIC         target.fiscaladmit_lastnameend <> source.fiscaladmit_lastnameend OR
# MAGIC         target.student_lastnameend <> source.student_lastnameend OR
# MAGIC         target.fiscaladmit_correo <> source.fiscaladmit_correo OR
# MAGIC         target.dbloc <> source.dbloc OR
# MAGIC         target.factura_numfiscal <> source.factura_numfiscal OR
# MAGIC         target.lead_count <> source.lead_count OR
# MAGIC         target.nia <> source.nia OR
# MAGIC         target.factura_nomfactura <> source.factura_nomfactura OR
# MAGIC         target.updated_at <> source.updated_at OR
# MAGIC         target.dataingres <> source.dataingres OR
# MAGIC         target.edad <> source.edad OR
# MAGIC         target.incompany <> source.incompany OR
# MAGIC         target.telefono <> source.telefono OR
# MAGIC         target.fiscaladmit_codigo <> source.fiscaladmit_codigo OR
# MAGIC         target.comunidadautonoma <> source.comunidadautonoma OR
# MAGIC         target.admit_dni_back <> source.admit_dni_back OR
# MAGIC         target.codigo <> source.codigo OR
# MAGIC         target.lastnameend <> source.lastnameend OR
# MAGIC         target.secondguardian_telefono <> source.secondguardian_telefono OR
# MAGIC         target.lead_admission <> source.lead_admission OR
# MAGIC         target.lead_asnew <> source.lead_asnew OR
# MAGIC         target.email <> source.email OR
# MAGIC         target.student_registration_date <> source.student_registration_date OR
# MAGIC         target.student_name <> source.student_name OR
# MAGIC         target.name <> source.name OR
# MAGIC         target.secondguardian_email <> source.secondguardian_email OR
# MAGIC         target.pais <> source.pais OR
# MAGIC         target.school_id_2 <> source.school_id_2 OR
# MAGIC         target.datosacceso_pais_ultim_curs_matriculat <> source.datosacceso_pais_ultim_curs_matriculat OR
# MAGIC         target.medicas <> source.medicas OR
# MAGIC         target.lead_id <> source.lead_id OR
# MAGIC         target.ciclo_id <> source.ciclo_id OR
# MAGIC         target.section_id <> source.section_id OR
# MAGIC         target.factura_movil <> source.factura_movil OR
# MAGIC         target.codinacionalitat <> source.codinacionalitat OR
# MAGIC         target.factura_direccion <> source.factura_direccion OR
# MAGIC         target.provincia <> source.provincia OR
# MAGIC         target.codiprovincianaixement <> source.codiprovincianaixement OR
# MAGIC         target.telefono2 <> source.telefono2 OR
# MAGIC         target.codimunicipinaixement <> source.codimunicipinaixement OR
# MAGIC         target.matricula4gradofisioterapia <> source.matricula4gradofisioterapia OR
# MAGIC         target.fiscaladmit_ciudad <> source.fiscaladmit_ciudad OR
# MAGIC         target.student_key <> source.student_key OR
# MAGIC         target.fiscaladmit_titular <> source.fiscaladmit_titular OR
# MAGIC         target.codipais <> source.codipais OR
# MAGIC         target.lead_language <> source.lead_language OR
# MAGIC         target.student_full_name <> source.student_full_name OR
# MAGIC         target.tipusdocument <> source.tipusdocument OR
# MAGIC         target.factura_codigo <> source.factura_codigo OR
# MAGIC         target.lead_message_read <> source.lead_message_read
# MAGIC     ) 
# MAGIC THEN 
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT *;
