# Databricks notebook source
# DBTITLE 1,ulac
# MAGIC %run "../Silver/configuration"

# COMMAND ----------

endpoint_process_name = "receipts"
table_name = "JsaClassLifeReceipts"

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

# 📌 Mostrar los primeros registros
display(classlifetitulaciones_df)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp
from pyspark.sql.types import StringType, IntegerType, DoubleType

# 📌 Aplicar transformaciones a las columnas con nombres corregidos y tipos de datos adecuados
classlifetitulaciones_df = classlifetitulaciones_df \
    .withColumn("processdate", current_timestamp()) \
    .withColumn("sourcesystem", lit("ReceiptSystem")) \
    .withColumn("receipt_id", col("receipt_id").cast(StringType())) \
    .withColumn("receipt_tax_per", col("receipt_tax_per").cast(DoubleType())) \
    .withColumn("payment_method", col("payment_method").cast(StringType())) \
    .withColumn("receipt_tax", col("receipt_tax").cast(DoubleType())) \
    .withColumn("student_id", col("student_id").cast(StringType())) \
    .withColumn("enroll_id", col("enroll_id").cast(StringType())) \
    .withColumn("remittance_id", col("remittance_id").cast(StringType())) \
    .withColumn("receipt_total", col("receipt_total").cast(DoubleType())) \
    .withColumn("invoice_id", col("invoice_id").cast(StringType())) \
    .withColumn("receipt_concept", col("receipt_concept").cast(StringType())) \
    .withColumn("receipt_status_id", col("receipt_status_id").cast(StringType())) \
    .withColumn("student_full_name", col("student_full_name").cast(StringType())) \
    .withColumn("receipt_price", col("receipt_price").cast(DoubleType())) \
    .withColumn("receipt_status", col("receipt_status").cast(StringType())) \
    .withColumn("payment_method_id", col("payment_method_id").cast(StringType())) \
    .withColumn("receipt_advanced", col("receipt_advanced").cast(DoubleType())) \
    .withColumn("emission_date", to_timestamp(col("emission_date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("expiry_date", to_timestamp(col("expiry_date"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("collection_date", to_timestamp(col("collection_date"), "yyyy-MM-dd HH:mm:ss"))

# 📌 Mostrar los primeros registros con todas las columnas
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
# MAGIC MERGE INTO silver_lakehouse.ClasslifeReceipts AS target
# MAGIC USING classlifetitulaciones_view AS source
# MAGIC ON target.receipt_id = source.receipt_id
# MAGIC
# MAGIC WHEN MATCHED AND 
# MAGIC     (
# MAGIC         target.receipt_tax_per <> source.receipt_tax_per OR
# MAGIC         target.payment_method <> source.payment_method OR
# MAGIC         target.receipt_tax <> source.receipt_tax OR
# MAGIC         target.student_id <> source.student_id OR
# MAGIC         target.enroll_id <> source.enroll_id OR
# MAGIC         target.remittance_id <> source.remittance_id OR
# MAGIC         target.receipt_total <> source.receipt_total OR
# MAGIC         target.invoice_id <> source.invoice_id OR
# MAGIC         target.receipt_concept <> source.receipt_concept OR
# MAGIC         target.receipt_status_id <> source.receipt_status_id OR
# MAGIC         target.student_full_name <> source.student_full_name OR
# MAGIC         target.receipt_price <> source.receipt_price OR
# MAGIC         target.receipt_status <> source.receipt_status OR
# MAGIC         target.payment_method_id <> source.payment_method_id OR
# MAGIC         target.receipt_advanced <> source.receipt_advanced OR
# MAGIC         target.emission_date <> source.emission_date OR
# MAGIC         target.expiry_date <> source.expiry_date OR
# MAGIC         target.collection_date <> source.collection_date
# MAGIC     ) 
# MAGIC THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.receipt_tax_per = source.receipt_tax_per,
# MAGIC         target.payment_method = source.payment_method,
# MAGIC         target.receipt_tax = source.receipt_tax,
# MAGIC         target.student_id = source.student_id,
# MAGIC         target.enroll_id = source.enroll_id,
# MAGIC         target.remittance_id = source.remittance_id,
# MAGIC         target.receipt_total = source.receipt_total,
# MAGIC         target.invoice_id = source.invoice_id,
# MAGIC         target.receipt_concept = source.receipt_concept,
# MAGIC         target.receipt_status_id = source.receipt_status_id,
# MAGIC         target.student_full_name = source.student_full_name,
# MAGIC         target.receipt_price = source.receipt_price,
# MAGIC         target.receipt_status = source.receipt_status,
# MAGIC         target.payment_method_id = source.payment_method_id,
# MAGIC         target.receipt_advanced = source.receipt_advanced,
# MAGIC         target.emission_date = source.emission_date,
# MAGIC         target.expiry_date = source.expiry_date,
# MAGIC         target.collection_date = source.collection_date,
# MAGIC         target.processdate = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC     INSERT (
# MAGIC         processdate,
# MAGIC         sourcesystem,
# MAGIC         receipt_id,
# MAGIC         receipt_tax_per,
# MAGIC         payment_method,
# MAGIC         receipt_tax,
# MAGIC         student_id,
# MAGIC         enroll_id,
# MAGIC         remittance_id,
# MAGIC         receipt_total,
# MAGIC         invoice_id,
# MAGIC         receipt_concept,
# MAGIC         receipt_status_id,
# MAGIC         student_full_name,
# MAGIC         receipt_price,
# MAGIC         receipt_status,
# MAGIC         payment_method_id,
# MAGIC         receipt_advanced,
# MAGIC         emission_date,
# MAGIC         expiry_date,
# MAGIC         collection_date
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         current_timestamp(), 
# MAGIC         'ReceiptSystem', 
# MAGIC         source.receipt_id,
# MAGIC         source.receipt_tax_per,
# MAGIC         source.payment_method,
# MAGIC         source.receipt_tax,
# MAGIC         source.student_id,
# MAGIC         source.enroll_id,
# MAGIC         source.remittance_id,
# MAGIC         source.receipt_total,
# MAGIC         source.invoice_id,
# MAGIC         source.receipt_concept,
# MAGIC         source.receipt_status_id,
# MAGIC         source.student_full_name,
# MAGIC         source.receipt_price,
# MAGIC         source.receipt_status,
# MAGIC         source.payment_method_id,
# MAGIC         source.receipt_advanced,
# MAGIC         source.emission_date,
# MAGIC         source.expiry_date,
# MAGIC         source.collection_date
# MAGIC     );
# MAGIC
