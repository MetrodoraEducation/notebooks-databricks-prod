# Databricks notebook source
# DBTITLE 1,Configuración y Conexión a PostgreSQL
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_comercial lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_comercial`
def upsert_dim_comercial(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_comercial (
            id_dim_comercial,
            nombre_comercial,
            equipo_comercial,
            cod_comercial,
            activo,
            fecha_desde,
            fecha_hasta
        )
        VALUES %s
        ON CONFLICT (id_dim_comercial) DO UPDATE SET
            nombre_comercial = EXCLUDED.nombre_comercial,
            equipo_comercial = EXCLUDED.equipo_comercial,
            cod_comercial = EXCLUDED.cod_comercial,
            activo = EXCLUDED.activo,
            fecha_desde = EXCLUDED.fecha_desde,
            fecha_hasta = EXCLUDED.fecha_hasta;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_comercial"],
            row["nombre_comercial"],
            row["equipo_comercial"],
            row["cod_comercial"],
            row["activo"],
            row["fecha_desde"],
            row["fecha_hasta"]
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

# Leer datos desde la tabla `gold_lakehouse.dim_comercial` en Databricks
source_table = (spark.table("gold_lakehouse.dim_comercial")
                .select("id_dim_comercial", "nombre_comercial", "equipo_comercial", "cod_comercial",
                        "activo", "fecha_desde", "fecha_hasta"))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_comercial)
    logger.info("Proceso completado con éxito (Upsert en dim_comercial de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
