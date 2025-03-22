# Databricks notebook source
# DBTITLE 1,Establecer la conexión a PostgreSQL.
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert dim_estudio lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar o actualizar registros en `dim_estudio`
def upsert_dim_estudio(partition):
    if not partition:  # Si la partición está vacía, no hacer nada
        logger.info("La partición está vacía, no se procesarán registros.")
        return

    try:
        conn = get_pg_connection()  # Obtener conexión desde la función del Bloque 1
        cursor = conn.cursor()

        # Query para insertar o actualizar registros
        query = """
        INSERT INTO dim_estudio (
            id_dim_estudio,
            cod_estudio,
            nombre_de_programa,
            cod_vertical,
            vertical_desc,
            cod_entidad_legal,
            entidad_legal_desc,
            cod_especialidad,
            especialidad_desc,
            cod_tipo_formacion,
            tipo_formacion_desc,
            cod_tipo_negocio,
            tipo_negocio_desc
        )
        VALUES %s
        ON CONFLICT (id_dim_estudio) DO UPDATE SET
            cod_estudio = EXCLUDED.cod_estudio,
            nombre_de_programa = EXCLUDED.nombre_de_programa,
            cod_vertical = EXCLUDED.cod_vertical,
            vertical_desc = EXCLUDED.vertical_desc,
            cod_entidad_legal = EXCLUDED.cod_entidad_legal,
            entidad_legal_desc = EXCLUDED.entidad_legal_desc,
            cod_especialidad = EXCLUDED.cod_especialidad,
            especialidad_desc = EXCLUDED.especialidad_desc,
            cod_tipo_formacion = EXCLUDED.cod_tipo_formacion,
            tipo_formacion_desc = EXCLUDED.tipo_formacion_desc,
            cod_tipo_negocio = EXCLUDED.cod_tipo_negocio,
            tipo_negocio_desc = EXCLUDED.tipo_negocio_desc;
        """

        # Transformar la partición de Spark en una lista de tuplas para insertar
        values = [(
            row["id_dim_estudio"],
            row["cod_estudio"],
            row["nombre_de_programa"],
            row["cod_vertical"],
            row["vertical_desc"],
            row["cod_entidad_legal"],
            row["entidad_legal_desc"],
            row["cod_especialidad"],
            row["especialidad_desc"],
            row["cod_tipo_formacion"],
            row["tipo_formacion_desc"],
            row["cod_tipo_negocio"],
            row["tipo_negocio_desc"]
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

# Leer datos desde la tabla `gold_lakehouse.dim_estudio` en Databricks
source_table = (spark.table("gold_lakehouse.dim_estudio")
                .select(
                    "id_dim_estudio", "cod_estudio", "nombre_de_programa", "cod_vertical",
                    "vertical_desc", "cod_entidad_legal", "entidad_legal_desc",
                    "cod_especialidad", "especialidad_desc", "cod_tipo_formacion",
                    "tipo_formacion_desc", "cod_tipo_negocio", "tipo_negocio_desc"
                ))

# Aplicar la función a las particiones de datos
try:
    source_table.foreachPartition(upsert_dim_estudio)
    logger.info("Proceso completado con éxito (Upsert en dim_estudio de PostgreSQL).")
except Exception as e:
    logger.error(f"Error general en el proceso: {e}")

print("¡Proceso completado!")
