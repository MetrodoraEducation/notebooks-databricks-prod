# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_escenario_budget lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_utm_campaign`
def upsert_dim_utm_campaign(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión a PostgreSQL
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_utm_campaign (
            id_dim_utm_campaign, utm_campaign_id, utm_campaign_name, utm_strategy, 
            utm_channel, ETLcreatedDate, ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_dim_utm_campaign) DO UPDATE SET
            utm_campaign_id = EXCLUDED.utm_campaign_id,
            utm_campaign_name = EXCLUDED.utm_campaign_name,
            utm_strategy = EXCLUDED.utm_strategy,
            utm_channel = EXCLUDED.utm_channel,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_utm_campaign"], row["utm_campaign_id"], row["utm_campaign_name"],
            row["utm_strategy"], row["utm_channel"], row["ETLcreatedDate"], row["ETLupdatedDate"]
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

# Leer datos desde la tabla `gold_lakehouse.dim_utm_campaign` en Databricks
source_table = (spark.table("gold_lakehouse.dim_utm_campaign")
                .select("id_dim_utm_campaign", "utm_campaign_id", "utm_campaign_name",
                        "utm_strategy", "utm_channel", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_utm_campaign)
    logger.info("Proceso completado con éxito (Upsert en dim_utm_campaign de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

