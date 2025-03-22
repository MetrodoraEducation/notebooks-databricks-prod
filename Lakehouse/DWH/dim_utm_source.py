# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_escenario_budget lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_source`
def upsert_dim_utm_source(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_source (
            id_dim_utm_source, utm_source, utm_type, utm_medium, utm_profile, 
            ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_dim_utm_source) DO UPDATE SET
            utm_source = EXCLUDED.utm_source,
            utm_type = EXCLUDED.utm_type,
            utm_medium = EXCLUDED.utm_medium,
            utm_profile = EXCLUDED.utm_profile,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_utm_source"], row["utm_source"], row["utm_type"], row["utm_medium"],
            row["utm_profile"], row["ETLcreatedDate"], row["ETLupdatedDate"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización en lotes
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_source` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_source")
                .select("id_dim_utm_source", "utm_source", "utm_type", "utm_medium", 
                        "utm_profile", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_source)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_source de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

