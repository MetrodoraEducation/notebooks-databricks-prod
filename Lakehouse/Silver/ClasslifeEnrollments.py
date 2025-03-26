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

    display(classlifetitulaciones_df)

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

display(classlifetitulaciones_df)


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

# 📌 Mostrar el resultado final
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

# 📌 Mostrar los primeros registros
display(classlifetitulaciones_df)

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
    ("totalenroll", col("totalenroll").cast(IntegerType())), 
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
display(classlifetitulaciones_df.select(columnas_a_mostrar))

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
# MAGIC WHEN MATCHED THEN 
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT enroll_id, COUNT(*)
# MAGIC FROM silver_lakehouse.ClasslifeEnrollments
# MAGIC GROUP BY enroll_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
