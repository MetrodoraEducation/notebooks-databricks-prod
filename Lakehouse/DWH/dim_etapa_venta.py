# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_etapa_venta lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_etapa_venta`
def upsert_dim_etapa_venta(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_etapa_venta (
            id_dim_etapa_venta,
            orden_etapa,
            nombre_etapa_venta,
            nombreEtapaVentaAgrupado,
            esNE,
            ETLcreatedDate,
            ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_dim_etapa_venta) DO UPDATE SET
            orden_etapa = EXCLUDED.orden_etapa,
            nombre_etapa_venta = EXCLUDED.nombre_etapa_venta,
            nombreEtapaVentaAgrupado = EXCLUDED.nombreEtapaVentaAgrupado,
            esNE = EXCLUDED.esNE,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_etapa_venta"],
            row["orden_etapa"],
            row["nombre_etapa_venta"],
            row["nombreEtapaVentaAgrupado"],
            row["esNE"],
            row["ETLcreatedDate"],
            row["ETLupdatedDate"]
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

# Leer datos desde la tabla `gold_lakehouse.dim_etapa_venta` en Databricks
source_table = (spark.table("gold_lakehouse.dim_etapa_venta")
                .select("id_dim_etapa_venta", "orden_etapa", "nombre_etapa_venta", "nombreEtapaVentaAgrupado", "esNE", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_etapa_venta)
    logger.info("Proceso completado con éxito (Upsert en dim_etapa_venta de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
