# Databricks notebook source
# DBTITLE 1,Configuración y Conexión a PostgreSQL
# MAGIC %run "../DWH/Configuration"

# COMMAND ----------

# DBTITLE 1,Insert auxiliar_periodificacion lakehouse postgresql
from psycopg2.extras import execute_values

# Función para insertar registros en `auxiliar_periodificacion` sin ON CONFLICT
def insert_auxiliar_periodificacion(partition):
    if not partition:
        logger.info("La partición está vacía, no se insertan registros.")
        return

    try:
        conn = get_pg_connection()  # Debes tener esta función definida
        cursor = conn.cursor()

        query = """
        INSERT INTO auxiliar_periodificacion (
            fecha_devengo,
            tipo_cargo,
            id_fct_matricula,
            id_dim_estudiante,
            id_dim_producto,
            id_dim_programa,
            id_dim_modalidad,
            id_dim_institucion,
            id_dim_sede,
            id_dim_tipo_formacion,
            id_dim_tipo_negocio,
            fecha_inicio,
            fecha_fin,
            dias_duracion,
            modalidad,
            importe,
            importe_diario
        )
        VALUES %s;
        """

        # Convertimos cada fila del Spark DataFrame en tuplas
        values = [
            (
                row["fecha_devengo"],
                row["tipo_cargo"],
                row["id_fct_matricula"],
                row["id_dim_estudiante"],
                row["id_dim_producto"],
                row["id_dim_programa"],
                row["id_dim_modalidad"],
                row["id_dim_institucion"],
                row["id_dim_sede"],
                row["id_dim_tipo_formacion"],
                row["id_dim_tipo_negocio"],
                row["fecha_inicio"],
                row["fecha_fin"],
                row["dias_duracion"],
                row["modalidad"],
                row["importe"],
                row["importe_diario"]
            )
            for row in partition
        ]

        if values:
            execute_values(cursor, query, values)
            conn.commit()
            logger.info(f"{len(values)} registros insertados en auxiliar_periodificacion.")
        else:
            logger.info("No se encontraron datos válidos en esta partición.")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error al insertar registros en auxiliar_periodificacion: {e}")
        raise


# Leer la tabla desde Databricks
source_table = (
    spark.table("gold_lakehouse.auxiliar_periodificacion")
    .select(
        "fecha_devengo", "tipo_cargo", "id_fct_matricula",
        "id_dim_estudiante", "id_dim_producto", "id_dim_programa",
        "id_dim_modalidad", "id_dim_institucion", "id_dim_sede",
        "id_dim_tipo_formacion", "id_dim_tipo_negocio", "fecha_inicio",
        "fecha_fin", "dias_duracion", "modalidad", "importe", "importe_diario"
    )
)

# Ejecutar la inserción por particiones
try:
    source_table.foreachPartition(insert_auxiliar_periodificacion)
    logger.info("Inserción completa en auxiliar_periodificacion.")
except Exception as e:
    logger.error(f"Error general durante la carga: {e}")
