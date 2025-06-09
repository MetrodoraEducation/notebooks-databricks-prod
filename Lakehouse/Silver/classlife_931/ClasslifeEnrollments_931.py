# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../configuration"

# COMMAND ----------

endpoint_process_name = "enrollments"
table_name = "JsaClassLifeEnrollments"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife_931/{endpoint_process_name}/{current_date}/{table_name}.json")
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

#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Desanida Fees y elimina enrroll_id duplicado
from pyspark.sql.functions import col, explode
from collections import OrderedDict

# 1️⃣ Desanidar 'fees' si es un array de structs
if "fees" in classlifetitulaciones_df.columns:
    if str(classlifetitulaciones_df.schema["fees"].dataType).startswith("ArrayType(StructType"):
        classlifetitulaciones_df = classlifetitulaciones_df.withColumn("fee", explode(col("fees")))

        # Extraer 'fee_title' y 'fee_amount'
        classlifetitulaciones_df = classlifetitulaciones_df \
            .withColumn("fee_title", col("fee.fee_title")) \
            .withColumn("fee_amount", col("fee.fee_amount")) \
            .drop("fees", "fee")

# 2️⃣ Eliminar columnas duplicadas, dejando solo una 'enroll_id'
columnas_unicas = list(OrderedDict.fromkeys(classlifetitulaciones_df.columns))

# Asegurarse de mantener solo la primera ocurrencia de 'enroll_id'
seen = set()
columnas_finales = []
for c in columnas_unicas:
    if c == "enroll_id":
        if "enroll_id" in seen:
            continue
    columnas_finales.append(c)
    seen.add(c)

# 3️⃣ Seleccionar columnas sin ambigüedad
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).alias(c) for c in columnas_finales]
)

# 4️⃣ Mostrar resultado final
#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Valida columnas duplicadas
from pyspark.sql.functions import col
from collections import OrderedDict

# 👁️ Mostrar columnas después de la limpieza
for col_name in classlifetitulaciones_df.columns:
    print(f"- '{col_name}'")

# 🛡️ Evitamos duplicados y nos quedamos solo con una 'enroll_id'
columnas_unicas = list(OrderedDict.fromkeys(classlifetitulaciones_df.columns))

# 👉 Si hay más de una columna 'enroll_id', deja solo la primera ocurrencia
seen = set()
columnas_finales = []
for c in columnas_unicas:
    if c == "enroll_id":
        if "enroll_id" in seen:
            continue  # saltamos duplicados
    columnas_finales.append(c)
    seen.add(c)

# 🎯 Verifica duplicados
duplicados = [c for c in columnas_finales if columnas_finales.count(c) > 1]
if duplicados:
    print(f"🚨 Columnas duplicadas: {duplicados}")
    raise ValueError("❌ Aún existen columnas duplicadas.")

# ✅ Selección segura sin ambigüedades
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).alias(c) for c in columnas_finales]
)

#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Desanida metas_API-Data
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# 1️⃣ Definimos el esquema del JSON dentro de `api-data`
api_data_schema = StructType() \
    .add("enroll_stage_id", StringType()) \
    .add("lastnameend", StringType()) \
    .add("address", StringType()) \
    .add("birthdate", StringType()) \
    .add("id_contacto", StringType()) \
    .add("apiKey", StringType()) \
    .add("city", StringType()) \
    .add("perform", StringType()) \
    .add("asesor", StringType()) \
    .add("id_oportunidad", StringType()) \
    .add("zip_code", StringType()) \
    .add("lastname", StringType()) \
    .add("password", StringType()) \
    .add("enroll_group_id", StringType()) \
    .add("phone", StringType()) \
    .add("name", StringType()) \
    .add("TipusDocument", StringType()) \
    .add("state_id", StringType()) \
    .add("email", StringType()) \
    .add("dni", StringType()) \
    .add("country_id", StringType()) \
    .add("last", StringType()) \
    .add("school_id", StringType())

# 2️⃣ Renombramos temporalmente si contiene guiones
df = classlifetitulaciones_df.withColumnRenamed("metas_API-Data", "api_data")

# 3️⃣ Parseamos el campo JSON
df = df.withColumn("api_struct", from_json(col("api_data"), api_data_schema))

# 4️⃣ Desanidamos todos los campos de `api_struct` con prefijo `api_`
for field in api_data_schema.fieldNames():
    df = df.withColumn(f"api_{field}", col(f"api_struct.{field}"))

# 5️⃣ Eliminamos las columnas temporales
df = df.drop("api_data", "api_struct")

# 6️⃣ Reemplazamos el DataFrame original
classlifetitulaciones_df = df

# 7️⃣ Mostramos resultado
#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Limpia nombres columnas
from pyspark.sql.functions import col
from collections import OrderedDict

def clean_column_names(df):
    """
    Limpia los nombres de columnas eliminando tildes, espacios, guiones y caracteres especiales.
    Evita nombres duplicados aplicando sufijos cuando es necesario.
    """
    cleaned_columns = {}
    col_count = {}

    for old_col in df.columns:
        new_col = (
            old_col.lower()
            .strip()
            .replace(" ", "_")
            .replace(".", "_")
            .replace("-", "_")
            .replace("ñ", "n")
            .replace("ó", "o")
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ú", "u")
            .replace("metas_", "")
            .replace("no__", "no_")
            .replace("fees_", "")
        )

        # Evitar nombres duplicados
        if new_col in cleaned_columns.values():
            counter = col_count.get(new_col, 2)
            while f"{new_col}_{counter}" in cleaned_columns.values():
                counter += 1
            new_col = f"{new_col}_{counter}"
            col_count[new_col] = counter + 1
            print(f"⚠️ Nombre duplicado detectado, renombrando como: {new_col}")

        cleaned_columns[old_col] = new_col

    # Aplicar renombramientos
    for old_col, new_col in cleaned_columns.items():
        df = df.withColumnRenamed(old_col, new_col)

    return df

# ✅ Aplicamos limpieza
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# ✅ Confirmamos columnas finales
print("\n📌 Columnas finales normalizadas:")
for col_name in classlifetitulaciones_df.columns:
    print(f"- {col_name}")

# ✅ Visualizamos
#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Selecciona columnas
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StringType

# 📌 Lista de columnas requeridas
columnas_requeridas = [
    "enroll_id", "area_id", "area_title", "ciclo_id", "ciclo_title", "created_on",
    "degree_id", "degree_title", "enroll_alias", "enroll_end", "enroll_group",
    "enroll_in", "enroll_ini", "enroll_status", "enroll_status_id",
    "first_activate_enroll", "school_id", "school_name", "section_id", "section_title",
    "student_full_name", "student_id", "term_title", "updated_at", "year",
    "zoho_deal_id", "ano_inicio_docencia", "cuota_matricula", "fecha_fin_cuotas",
    "fecha_fin_docencia", "fecha_fin_reconocimiento_ingresos", "fecha_inicio_cuotas",
    "fecha_inicio_docencia", "fecha_inicio_reconocimiento_ingresos", "fee_title", "fee_amount"
]

# 📌 Lista de columnas con transformaciones a string + metadatos
columnas_con_tipo = [
    *[(col(nombre).cast(StringType()), nombre) for nombre in columnas_requeridas],
    (current_timestamp(), "processdate"),
    (lit("ClasslifeEnrollments_931"), "sourcesystem")
]

# 📌 Aplicar transformación
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[expr.alias(nombre) for expr, nombre in columnas_con_tipo]
)

# 📌 Mostrar resultado final
#display(classlifetitulaciones_df)

# COMMAND ----------

# DBTITLE 1,Agrupa por fee_title y fee_amount
from pyspark.sql.functions import sum, when, col, first, expr

df_agrupado = classlifetitulaciones_df.groupBy("enroll_id").agg(
    
    # ❌ Excluimos "enroll_id" porque ya está en el groupBy
    *[
        first(col(c), ignorenulls=True).alias(c)
        for c in classlifetitulaciones_df.columns
        if c not in ["fee_title", "fee_amount", "enroll_id"]
    ],
    
    # ✅ MATRÍCULA → Solo cuando es exactamente 'Matrícula' (y positivo)
    sum(
        when(
            (col("fee_title") == "Matrícula") & (col("fee_amount") > 0),
            col("fee_amount").cast("decimal(10,2)")
        )
    ).alias("importe_matricula"),
    
    # ✅ DOCENCIA → Coincidencia con "docencia" (y positivo)
    sum(
        when(
            col("fee_title").rlike("(?i)docencia") & (col("fee_amount") > 0),
            col("fee_amount").cast("decimal(10,2)")
        )
    ).alias("importe_docencia"),
    
    # ✅ DESCUENTOS → Todo valor negativo
    sum(
        when(col("fee_amount") < 0, col("fee_amount").cast("decimal(10,2)"))
    ).alias("suma_descuentos"),

    # ✅ TOTAL FEES sin ningún filtro
    sum(
        col("fee_amount").cast("decimal(10,2)")
    ).alias("total_fees")
)

# ✅ Calculamos columna adicional: otros positivos = total_fees - (matrícula + docencia + descuentos)
df_agrupado = df_agrupado.withColumn(
    "total_restantes",
    expr("total_fees - (importe_matricula + importe_docencia + suma_descuentos)")
)

# display(df_agrupado)


# COMMAND ----------

classlifetitulaciones_df = df_agrupado.dropDuplicates()

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
# MAGIC MERGE INTO silver_lakehouse.ClasslifeEnrollments_931 AS target 
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.enroll_id = source.enroll_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.area_id IS DISTINCT FROM source.area_id OR
# MAGIC     target.area_title IS DISTINCT FROM source.area_title OR
# MAGIC     target.ciclo_id IS DISTINCT FROM source.ciclo_id OR
# MAGIC     target.ciclo_title IS DISTINCT FROM source.ciclo_title OR
# MAGIC     target.created_on IS DISTINCT FROM source.created_on OR
# MAGIC     target.degree_id IS DISTINCT FROM source.degree_id OR
# MAGIC     target.degree_title IS DISTINCT FROM source.degree_title OR
# MAGIC     target.enroll_alias IS DISTINCT FROM source.enroll_alias OR
# MAGIC     target.enroll_end IS DISTINCT FROM source.enroll_end OR
# MAGIC     target.enroll_group IS DISTINCT FROM source.enroll_group OR
# MAGIC     target.enroll_in IS DISTINCT FROM source.enroll_in OR
# MAGIC     target.enroll_ini IS DISTINCT FROM source.enroll_ini OR
# MAGIC     target.enroll_status IS DISTINCT FROM source.enroll_status OR
# MAGIC     target.enroll_status_id IS DISTINCT FROM source.enroll_status_id OR
# MAGIC     target.first_activate_enroll IS DISTINCT FROM source.first_activate_enroll OR
# MAGIC     target.school_id IS DISTINCT FROM source.school_id OR
# MAGIC     target.school_name IS DISTINCT FROM source.school_name OR
# MAGIC     target.section_id IS DISTINCT FROM source.section_id OR
# MAGIC     target.section_title IS DISTINCT FROM source.section_title OR
# MAGIC     target.student_full_name IS DISTINCT FROM source.student_full_name OR
# MAGIC     target.student_id IS DISTINCT FROM source.student_id OR
# MAGIC     target.term_title IS DISTINCT FROM source.term_title OR
# MAGIC     target.updated_at IS DISTINCT FROM source.updated_at OR
# MAGIC     target.year IS DISTINCT FROM source.year OR
# MAGIC     target.zoho_deal_id IS DISTINCT FROM source.zoho_deal_id OR
# MAGIC     target.ano_inicio_docencia IS DISTINCT FROM source.ano_inicio_docencia OR
# MAGIC     target.cuota_matricula IS DISTINCT FROM source.cuota_matricula OR
# MAGIC     target.fecha_fin_cuotas IS DISTINCT FROM source.fecha_fin_cuotas OR
# MAGIC     target.fecha_fin_docencia IS DISTINCT FROM source.fecha_fin_docencia OR
# MAGIC     target.fecha_fin_reconocimiento_ingresos IS DISTINCT FROM source.fecha_fin_reconocimiento_ingresos OR
# MAGIC     target.fecha_inicio_cuotas IS DISTINCT FROM source.fecha_inicio_cuotas OR
# MAGIC     target.fecha_inicio_docencia IS DISTINCT FROM source.fecha_inicio_docencia OR
# MAGIC     target.fecha_inicio_reconocimiento_ingresos IS DISTINCT FROM source.fecha_inicio_reconocimiento_ingresos OR
# MAGIC     target.processdate IS DISTINCT FROM source.processdate OR
# MAGIC     target.sourcesystem IS DISTINCT FROM source.sourcesystem OR
# MAGIC     target.importe_matricula IS DISTINCT FROM source.importe_matricula OR
# MAGIC     target.importe_docencia IS DISTINCT FROM source.importe_docencia OR
# MAGIC     target.suma_descuentos IS DISTINCT FROM source.suma_descuentos OR
# MAGIC     target.total_fees IS DISTINCT FROM source.total_fees OR
# MAGIC     target.total_restantes IS DISTINCT FROM source.total_restantes
# MAGIC )
# MAGIC
# MAGIC THEN UPDATE SET
# MAGIC     target.area_id = source.area_id,
# MAGIC     target.area_title = source.area_title,
# MAGIC     target.ciclo_id = source.ciclo_id,
# MAGIC     target.ciclo_title = source.ciclo_title,
# MAGIC     target.created_on = source.created_on,
# MAGIC     target.degree_id = source.degree_id,
# MAGIC     target.degree_title = source.degree_title,
# MAGIC     target.enroll_alias = source.enroll_alias,
# MAGIC     target.enroll_end = source.enroll_end,
# MAGIC     target.enroll_group = source.enroll_group,
# MAGIC     target.enroll_in = source.enroll_in,
# MAGIC     target.enroll_ini = source.enroll_ini,
# MAGIC     target.enroll_status = source.enroll_status,
# MAGIC     target.enroll_status_id = source.enroll_status_id,
# MAGIC     target.first_activate_enroll = source.first_activate_enroll,
# MAGIC     target.school_id = source.school_id,
# MAGIC     target.school_name = source.school_name,
# MAGIC     target.section_id = source.section_id,
# MAGIC     target.section_title = source.section_title,
# MAGIC     target.student_full_name = source.student_full_name,
# MAGIC     target.student_id = source.student_id,
# MAGIC     target.term_title = source.term_title,
# MAGIC     target.updated_at = source.updated_at,
# MAGIC     target.year = source.year,
# MAGIC     target.zoho_deal_id = source.zoho_deal_id,
# MAGIC     target.ano_inicio_docencia = source.ano_inicio_docencia,
# MAGIC     target.cuota_matricula = source.cuota_matricula,
# MAGIC     target.fecha_fin_cuotas = source.fecha_fin_cuotas,
# MAGIC     target.fecha_fin_docencia = source.fecha_fin_docencia,
# MAGIC     target.fecha_fin_reconocimiento_ingresos = source.fecha_fin_reconocimiento_ingresos,
# MAGIC     target.fecha_inicio_cuotas = source.fecha_inicio_cuotas,
# MAGIC     target.fecha_inicio_docencia = source.fecha_inicio_docencia,
# MAGIC     target.fecha_inicio_reconocimiento_ingresos = source.fecha_inicio_reconocimiento_ingresos,
# MAGIC     target.processdate = source.processdate,
# MAGIC     target.sourcesystem = source.sourcesystem,
# MAGIC     target.importe_matricula = source.importe_matricula,
# MAGIC     target.importe_docencia = source.importe_docencia,
# MAGIC     target.suma_descuentos = source.suma_descuentos,
# MAGIC     target.total_fees = source.total_fees,
# MAGIC     target.total_restantes = source.total_restantes
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT enroll_id, COUNT(*)
# MAGIC FROM silver_lakehouse.ClasslifeEnrollments_931
# MAGIC GROUP BY enroll_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC

# COMMAND ----------

#%sql select * from silver_lakehouse.ClasslifeEnrollments_931
