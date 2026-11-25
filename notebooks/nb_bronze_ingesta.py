# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, lit, to_date

# COMMAND ----------

CATALOG = "bank"
dbutils.widgets.text("fecha_proceso", "2026-11-24")
fecha_proceso = dbutils.widgets.get("fecha_proceso")
fecha_archivo = fecha_proceso.replace("-", "_")

LANDING = f"/Volumes/{CATALOG}/bronze/landing_zone"
archivo_creditos = f"{LANDING}/creditos/creditos_{fecha_archivo}.json"
archivo_pagos = f"{LANDING}/pagos/pagos_{fecha_archivo}.json"

# COMMAND ----------

def escribir_lote_idempotente(df, table_name, batch_date):
    """
    Reemplaza atómicamente solo los registros del lote actual (_batch_date) sin tocar días previos.
    Si el Job del día se reejecuta, garantiza cero duplicados en Bronze.
    """
    if spark.catalog.tableExists(table_name):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_batch_date = DATE '{batch_date}'")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

# COMMAND ----------

# 1. Ingesta Bronze de Créditos
df_creditos_raw = (
    spark.read.json(archivo_creditos)
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_batch_date", to_date(lit(fecha_proceso), "yyyy-MM-dd"))
)
escribir_lote_idempotente(df_creditos_raw, f"{CATALOG}.bronze.creditos_raw", fecha_proceso)

# COMMAND ----------

# 2. Ingesta Bronze de Pagos 
df_pagos_raw = (
    spark.read.json(archivo_pagos)
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_batch_date", to_date(lit(fecha_proceso), "yyyy-MM-dd"))
)
escribir_lote_idempotente(df_pagos_raw, f"{CATALOG}.bronze.pagos_raw", fecha_proceso)

# COMMAND ----------

# 3. Registro en tabla de auditoría de lotes procesados
cnt_creditos = df_creditos_raw.count()
cnt_pagos = df_pagos_raw.count()

df_auditoria = spark.createDataFrame([(
    fecha_proceso, archivo_creditos, cnt_creditos, archivo_pagos, cnt_pagos, "EXITOSO"
)], ["batch_date_str", "archivo_creditos", "reg_creditos", "archivo_pagos", "reg_pagos", "estado"]).withColumn(
    "_batch_date", to_date(col("batch_date_str"), "yyyy-MM-dd")
).withColumn("_processed_ts", current_timestamp()).drop("batch_date_str")

df_auditoria.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.bronze.control_lotes")

print(f"Bronze lote {fecha_proceso} completado -> Créditos: {cnt_creditos} | Pagos: {cnt_pagos}")