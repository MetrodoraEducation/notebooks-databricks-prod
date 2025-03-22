# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration" 

# COMMAND ----------

# DBTITLE 1,Insert fct_llamada lakehouse postgresql
from pyspark.sql.functions import col
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros
def upsert_records(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO fct_llamada (
            cod_llamada,
            id_dim_tiempo,
            id_dim_hora,
            direccion_llamada,
            duration,
            numero_telefono,
            id_dim_pais,
            id_dim_comercial,
            id_dim_motivo_perdida_llamada,
            processdate
        )
        VALUES %s
        ON CONFLICT (cod_llamada) DO UPDATE SET
            id_dim_tiempo = EXCLUDED.id_dim_tiempo,
            id_dim_hora = EXCLUDED.id_dim_hora,
            direccion_llamada = EXCLUDED.direccion_llamada,
            duration = EXCLUDED.duration,
            numero_telefono = EXCLUDED.numero_telefono,
            id_dim_pais = EXCLUDED.id_dim_pais,
            id_dim_comercial = EXCLUDED.id_dim_comercial,
            id_dim_motivo_perdida_llamada = EXCLUDED.id_dim_motivo_perdida_llamada,
            processdate = EXCLUDED.processdate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["cod_llamada"],
            row["id_dim_tiempo"],
            row["id_dim_hora"],
            row["direccion_llamada"],
            row["duration"],
            row["numero_telefono"],
            row["id_dim_pais"],
            row["id_dim_comercial"],
            row["id_dim_motivo_perdida_llamada"],
            row["processdate"]
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

# Leer datos desde la tabla `gold_lakehouse.fct_llamada` en Databricks
source_table = (spark.table("gold_lakehouse.fct_llamada")
                .filter(col("cod_llamada") != -1)
                .select("cod_llamada", "id_dim_tiempo", "id_dim_hora", "direccion_llamada",
                        "duration", "numero_telefono", "id_dim_pais", "id_dim_comercial",
                        "id_dim_motivo_perdida_llamada", "processdate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_records)
    logger.info("Proceso completado con éxito (Upsert en PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
