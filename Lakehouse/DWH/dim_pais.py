# Databricks notebook source
# DBTITLE 1,Configuración y Conexión a PostgreSQL
# MAGIC %run "../DWH/Configuration" 

# COMMAND ----------

# DBTITLE 1,Insert dim_pais lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_pais`
def upsert_dim_pais(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_pais (
            id,
            nombre,
            name,
            nombre_nacionalidad,
            iso2,
            iso3
        )
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            nombre = EXCLUDED.nombre,
            name = EXCLUDED.name,
            nombre_nacionalidad = EXCLUDED.nombre_nacionalidad,
            iso2 = EXCLUDED.iso2,
            iso3 = EXCLUDED.iso3;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id"],
            row["nombre"],
            row["name"],
            row["nombre_nacionalidad"],
            row["iso2"],
            row["iso3"]
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

# Leer datos desde la tabla `gold_lakehouse.dim_pais` en Databricks
source_table = (spark.table("gold_lakehouse.dim_pais")
                .select("id", "nombre", "name", "nombre_nacionalidad", "iso2", "iso3"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_pais)
    logger.info("Proceso completado con éxito (Upsert en dim_pais de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
