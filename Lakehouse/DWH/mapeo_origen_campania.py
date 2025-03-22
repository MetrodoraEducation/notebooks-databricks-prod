# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration" 

# COMMAND ----------

# DBTITLE 1,Insert mapeo_origen_campania lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `mapeo_origen_campania`
def upsert_mapeo_origen_campania(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO mapeo_origen_campania (
            utm_source,
            utm_type,
            utm_channel,
            utm_medium
        )
        VALUES %s
        ON CONFLICT (utm_source) DO UPDATE SET
            utm_type = EXCLUDED.utm_type,
            utm_channel = EXCLUDED.utm_channel,
            utm_medium = EXCLUDED.utm_medium;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["utm_source"],
            row["utm_type"],
            row["utm_channel"],
            row["utm_medium"]
        ) for row in partition]

        if values:
            # Ejecutar la inserción o actualización
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

# Leer datos desde la tabla `gold_lakehouse.mapeo_origen_campania` en Databricks
source_table = (spark.table("gold_lakehouse.mapeo_origen_campania")
                .select("utm_source", "utm_type", "utm_channel", "utm_medium"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_mapeo_origen_campania)
    logger.info("Proceso completado con éxito (Upsert en mapeo_origen_campania de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
