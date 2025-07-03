# Databricks notebook source
# MAGIC %run "../configuration" 

# COMMAND ----------

from pyspark.sql.functions import explode
from datetime import datetime
import os

# Parámetros
bronze_folder_path = "/mnt/stmetrodoralakehousepro/bronze"
current_date = datetime.today().strftime('%Y/%m/%d')
table_prefix = "JsaZohoDeals_"
relative_path = f"lakehouse/zoho_38b/{current_date}"
folder_path = f"{bronze_folder_path}/{relative_path}"

# Internos
page = 1
next_page_token = "start"
all_data = []

# 💡 Nuevo: función para validar si el archivo existe
def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except:
        return False

while next_page_token:
    file_name = f"{table_prefix}{page}.json"
    full_path = f"{folder_path}/{file_name}"
    print(f"📥 Buscando archivo: {full_path}")

    if not file_exists(full_path):
        print(f"🕒 Esperando a que ADF genere el archivo: {file_name}")
        break  # O puedes hacer un sleep y reintentar si quieres polling

    try:
        df = spark.read.option("multiline", True).json(full_path)

        if "data" not in df.columns or "info" not in df.columns:
            print(f"⚠️ Estructura inesperada en: {file_name}")
            break

        deals_df = df.select(explode("data").alias("registro")).select("registro.*")
        all_data.append(deals_df)

        next_token_row = df.select("info.next_page_token").limit(1).collect()
        next_page_token = next_token_row[0]["next_page_token"] if next_token_row else None

        print(f"➡️ Token siguiente: {next_page_token}")
        page += 1

    except Exception as e:
        print(f"❌ Error leyendo {file_name}: {e}")
        break

# Unión final
if all_data:
    final_df = all_data[0]
    for part_df in all_data[1:]:
        final_df = final_df.unionByName(part_df)

    print("✅ Datos combinados correctamente.")
else:
    print("❗No se encontraron datos válidos.")

# ✨ Devolver el token final a ADF
dbutils.notebook.exit(next_page_token or "")
