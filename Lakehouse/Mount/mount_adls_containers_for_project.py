# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
        # Get secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksApplicationID')
    tenant_id = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksTenantID')
    client_secret = dbutils.secrets.get(scope = 'KeyVaultSecretScope', key = 'spDatabricksSecret')
    
    # Set spark configurations
    
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }
    mount_point = f"/mnt/{storage_account_name}/{container_name}"
    try:
        # Try to mount the storage account container
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
    except Exception as e:
        if "already mounted" in str(e):
            print(f"{mount_point} already mounted. Attempting to remount.")
            # Optionally, you can unmount and remount here
            # dbutils.fs.unmount(mount_point)
            # dbutils.fs.mount(...)
        else:
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Bronze Container

# COMMAND ----------

mount_adls('stmetrodoralakehousedev', 'bronze')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Silver Container

# COMMAND ----------

mount_adls('stmetrodoralakehousedev', 'silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount Gold Container

# COMMAND ----------

mount_adls('stmetrodoralakehousedev', 'gold')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

#dbutils.fs.unmount("/mnt/bronze")
