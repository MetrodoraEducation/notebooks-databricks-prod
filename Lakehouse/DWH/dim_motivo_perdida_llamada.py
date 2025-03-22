# Databricks notebook source
# MAGIC %run "../DWH/Configuration" 

# COMMAND ----------

# DBTITLE 1,Insert dim_motivo_perdida_llamada lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_motivo_perdida_llamada`
def upsert_dim_motivo_perdida_llamada(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_motivo_perdida_llamada (
            id_dim_motivo_perdida_llamada,
            motivo_perdida_llamada,
            tipo_perdida
        )
        VALUES %s
        ON CONFLICT (id_dim_motivo_perdida_llamada) DO UPDATE SET
            motivo_perdida_llamada = EXCLUDED.motivo_perdida_llamada,
            tipo_perdida = EXCLUDED.tipo_perdida;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_motivo_perdida_llamada"],
            row["motivo_perdida_llamada"],
            row["tipo_perdida"]
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

# Leer datos desde la tabla `gold_lakehouse.dim_motivo_perdida_llamada` en Databricks
source_table = (spark.table("gold_lakehouse.dim_motivo_perdida_llamada")
                .select("id_dim_motivo_perdida_llamada", "motivo_perdida_llamada", "tipo_perdida"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_motivo_perdida_llamada)
    logger.info("Proceso completado con éxito (Upsert en dim_motivo_perdida_llamada de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
