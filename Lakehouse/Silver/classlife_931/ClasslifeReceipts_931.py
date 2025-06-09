# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../configuration"

# COMMAND ----------

endpoint_process_name = "receipts"
table_name = "JsaClassLifeReceipts"

classlifetitulaciones_df = spark.read.json(f"{bronze_folder_path}/lakehouse/classlife_931/{endpoint_process_name}/{current_date}/{table_name}.json")
#print(current_date)

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

# 📌 Aplicar limpieza de nombres de columnas
classlifetitulaciones_df = clean_column_names(classlifetitulaciones_df)

# 📌 Verificar si `tarifa_matricula` existe en el DataFrame después de la limpieza
columnas_actuales = set(classlifetitulaciones_df.columns)

# 📌 Seleccionar solo las columnas válidas si existen
columnas_seleccionadas = list(columnas_actuales)

# 📌 Solución para nombres con comillas invertidas
# Aplicamos alias() para normalizar los nombres con comillas invertidas
classlifetitulaciones_df = classlifetitulaciones_df.select(
    *[col(c).alias(c.strip().replace("`", "")) for c in columnas_seleccionadas]
)

display(classlifetitulaciones_df)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType, DoubleType

# 🧼 Normalización y casting de columnas
classlifetitulaciones_df = classlifetitulaciones_df \
    .withColumn("receipt_id", col("receipt_id").cast(StringType())) \
    .withColumn("receipt_tax_per", col("receipt_tax_per").cast(DoubleType())) \
    .withColumn("payment_method", col("payment_method").cast(StringType())) \
    .withColumn("receipt_tax", col("receipt_tax").cast(DoubleType())) \
    .withColumn("student_id", col("student_id").cast(StringType())) \
    .withColumn("enroll_id", col("enroll_id").cast(StringType())) \
    .withColumn("remittance_id", col("remittance_id").cast(StringType())) \
    .withColumn("receipt_total", col("receipt_total").cast(DoubleType())) \
    .withColumn("invoice_id", col("invoice_id").cast(StringType())) \
    .withColumn("emission_date", col("emission_date").cast(StringType())) \
    .withColumn("expiry_date", col("expiry_date").cast(StringType())) \
    .withColumn("receipt_status", col("receipt_status").cast(StringType())) \
    .withColumn("payment_method_id", col("payment_method_id").cast(StringType())) \
    .withColumn("receipt_advanced", col("receipt_advanced").cast(DoubleType())) \
    .withColumn("collection_date", to_timestamp(col("collection_date"), "yyyy-MM-dd")) \
    .withColumn("receipt_concept", col("receipt_concept").cast(StringType())) \
    .withColumn("receipt_status_id", col("receipt_status_id").cast(StringType())) \
    .withColumn("student_full_name", col("student_full_name").cast(StringType())) \
    .withColumn("receipt_price", col("receipt_price").cast(DoubleType())) \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("ClasslifeReceipts_931"))

# 👁️ Mostrar resultado
display(classlifetitulaciones_df)

# COMMAND ----------

classlifetitulaciones_df = classlifetitulaciones_df.dropDuplicates()

# COMMAND ----------

classlifetitulaciones_df.createOrReplaceTempView("classlifetitulaciones_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT receipt_id, COUNT(*)
# MAGIC FROM classlifetitulaciones_view
# MAGIC GROUP BY receipt_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_lakehouse.ClasslifeReceipts_931 AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.receipt_id = source.receipt_id
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC        target.receipt_tax_per IS DISTINCT FROM source.receipt_tax_per OR
# MAGIC        target.payment_method IS DISTINCT FROM source.payment_method OR
# MAGIC        target.receipt_tax IS DISTINCT FROM source.receipt_tax OR
# MAGIC        target.student_id IS DISTINCT FROM source.student_id OR
# MAGIC        target.enroll_id IS DISTINCT FROM source.enroll_id OR
# MAGIC        target.remittance_id IS DISTINCT FROM source.remittance_id OR
# MAGIC        target.receipt_total IS DISTINCT FROM source.receipt_total OR
# MAGIC        target.invoice_id IS DISTINCT FROM source.invoice_id OR
# MAGIC        target.emission_date IS DISTINCT FROM source.emission_date OR
# MAGIC        target.expiry_date IS DISTINCT FROM source.expiry_date OR
# MAGIC        target.receipt_status IS DISTINCT FROM source.receipt_status OR
# MAGIC        target.payment_method_id IS DISTINCT FROM source.payment_method_id OR
# MAGIC        target.receipt_advanced IS DISTINCT FROM source.receipt_advanced OR
# MAGIC        target.collection_date IS DISTINCT FROM source.collection_date OR
# MAGIC        target.receipt_concept IS DISTINCT FROM source.receipt_concept OR
# MAGIC        target.receipt_status_id IS DISTINCT FROM source.receipt_status_id OR
# MAGIC        target.student_full_name IS DISTINCT FROM source.student_full_name OR
# MAGIC        target.receipt_price IS DISTINCT FROM source.receipt_price
# MAGIC ) THEN 
# MAGIC UPDATE SET
# MAGIC     receipt_tax_per = source.receipt_tax_per,
# MAGIC     payment_method = source.payment_method,
# MAGIC     receipt_tax = source.receipt_tax,
# MAGIC     student_id = source.student_id,
# MAGIC     enroll_id = source.enroll_id,
# MAGIC     remittance_id = source.remittance_id,
# MAGIC     receipt_total = source.receipt_total,
# MAGIC     invoice_id = source.invoice_id,
# MAGIC     emission_date = source.emission_date,
# MAGIC     expiry_date = source.expiry_date,
# MAGIC     receipt_status = source.receipt_status,
# MAGIC     payment_method_id = source.payment_method_id,
# MAGIC     receipt_advanced = source.receipt_advanced,
# MAGIC     collection_date = source.collection_date,
# MAGIC     receipt_concept = source.receipt_concept,
# MAGIC     receipt_status_id = source.receipt_status_id,
# MAGIC     student_full_name = source.student_full_name,
# MAGIC     receipt_price = source.receipt_price,
# MAGIC     processdate = source.processdate,
# MAGIC     sourcesystem = source.sourcesystem
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT (
# MAGIC     receipt_id, receipt_tax_per, payment_method, receipt_tax, student_id,
# MAGIC     enroll_id, remittance_id, receipt_total, invoice_id, emission_date,
# MAGIC     expiry_date, receipt_status, payment_method_id, receipt_advanced,
# MAGIC     collection_date, receipt_concept, receipt_status_id, student_full_name,
# MAGIC     receipt_price, processdate, sourcesystem
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.receipt_id, source.receipt_tax_per, source.payment_method, source.receipt_tax, source.student_id,
# MAGIC     source.enroll_id, source.remittance_id, source.receipt_total, source.invoice_id, source.emission_date,
# MAGIC     source.expiry_date, source.receipt_status, source.payment_method_id, source.receipt_advanced,
# MAGIC     source.collection_date, source.receipt_concept, source.receipt_status_id, source.student_full_name,
# MAGIC     source.receipt_price, source.processdate, source.sourcesystem
# MAGIC );
