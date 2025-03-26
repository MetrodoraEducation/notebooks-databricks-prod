# Databricks notebook source
import psycopg2
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Obtener la contraseña de la base de datos desde Databricks
db_password = dbutils.secrets.get('KeyVaultSecretScope', 'psqlpwd')

# Función para establecer la conexión a PostgreSQL
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            dbname="lakehouse",
            user="sqladminuser",
            password=db_password,
            host="psql-metrodoralakehouse-pro.postgres.database.azure.com",
            port="5432",
            keepalives=1,        # Mantener conexión viva
            keepalives_idle=30,  # Enviar ping cada 30 segundos
            keepalives_interval=10,
            keepalives_count=5
        )
        logger.info("Conexión a PostgreSQL establecida correctamente.")
        return conn
    except Exception as e:
        logger.error(f"Error al conectar a PostgreSQL: {e}")
        raise

# COMMAND ----------

import requests
print(requests.get('https://ifconfig.me').text)

