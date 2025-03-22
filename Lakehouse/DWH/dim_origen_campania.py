# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration" 

# COMMAND ----------

# DBTITLE 1,Insert dim_origen_campania lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_origen_campania`
def upsert_dim_origen_campania(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_origen_campania (
            id_dim_origen_campania,
            nombre_origen_campania,
            tipo_campania,
            canal_campania,
            medio_campania,
            fec_procesamiento,
            ETLcreatedDate,
            ETLupdatedDate
        )
        VALUES %s
        ON CONFLICT (id_dim_origen_campania) DO UPDATE SET
            nombre_origen_campania = EXCLUDED.nombre_origen_campania,
            tipo_campania = EXCLUDED.tipo_campania,
            canal_campania = EXCLUDED.canal_campania,
            medio_campania = EXCLUDED.medio_campania,
            fec_procesamiento = EXCLUDED.fec_procesamiento,
            ETLcreatedDate = EXCLUDED.ETLcreatedDate,
            ETLupdatedDate = EXCLUDED.ETLupdatedDate;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_origen_campania"],
            row["nombre_origen_campania"],
            row["tipo_campania"],
            row["canal_campania"],
            row["medio_campania"],
            row["fec_procesamiento"],
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

# Leer datos desde la tabla `gold_lakehouse.dim_origen_campania` en Databricks
source_table = (spark.table("gold_lakehouse.dim_origen_campania")
                .select("id_dim_origen_campania", "nombre_origen_campania", "tipo_campania",
                        "canal_campania", "medio_campania", "fec_procesamiento", "ETLcreatedDate", "ETLupdatedDate"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_origen_campania)
    logger.info("Proceso completado con éxito (Upsert en dim_origen_campania de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")

