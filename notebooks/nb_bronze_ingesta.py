# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

CATALOG = "bank"
dbutils.widgets.text("fecha_proceso", "2026-11-24")
fecha_proceso = dbutils.widgets.get("fecha_proceso")
fecha_archivo = fecha_proceso.replace("-", "_")

LANDING = f"/Volumes/{CATALOG}/bronze/landing_zone"

# COMMAND ----------

#Ingesta Bronze de Créditos
df_creditos_raw = (
    spark.read.json(f"{LANDING}/creditos/creditos_{fecha_archivo}.json")
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_batch_date", lit(fecha_proceso))
)

(
    df_creditos_raw.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.creditos_raw")
)

# COMMAND ----------

# Ingesta Bronze de Pagos
df_pagos_raw = (
    spark.read.json(f"{LANDING}/pagos/pagos_*.json")
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_batch_date", lit(fecha_proceso))
)

(
    df_pagos_raw.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.pagos_raw")
)

print(f"Bronze actualizado exitosamente. Créditos: {df_creditos_raw.count()} | Pagos leídos: {df_pagos_raw.count()}")