# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_tipo_conversion lakehouse postgresql
from pyspark.sql.functions import col
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_tipo_conversion`
def upsert_dim_tipo_conversion(partition):
    if not partition:
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        # Query para hacer UPSERT en dim_tipo_conversion
        query = """
        INSERT INTO dim_tipo_conversion (
            id_dim_tipo_conversion,
            tipo_conversion,
            ETLcreatedDate,
            ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_dim_tipo_conversion) DO UPDATE SET
            tipo_conversion = EXCLUDED.tipo_conversion,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        values = [(
            row["id_dim_tipo_conversion"],
            row["tipo_conversion"],
            row["ETLcreatedDate"],
            row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            execute_values(cursor, query, values)
            conn.commit()
            logger.info(f"Procesados {len(values)} registros en PostgreSQL (insertados o actualizados).")
        else:
            logger.info("No se encontraron datos válidos en esta partición.")

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error al procesar registros: {e}")
        raise

# Leer datos desde la tabla gold_lakehouse.dim_tipo_conversion en Databricks
source_table = (spark.table("gold_lakehouse.dim_tipo_conversion")
                .select("id_dim_tipo_conversion", "tipo_conversion", "ETLcreatedDate", "ETLupdatedDate"))

# Ejecutar foreachPartition para hacer upsert en PostgreSQL
try:
    source_table.foreachPartition(upsert_dim_tipo_conversion)
    logger.info("Proceso completado con éxito (Upsert en dim_tipo_conversion de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
