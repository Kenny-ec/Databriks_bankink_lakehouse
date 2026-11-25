# Databricks notebook source
from pyspark.sql.functions import (
    col, concat, current_timestamp, length, lit, row_number,
    sha2, substring, to_date, to_timestamp, trim, when
)
from pyspark.sql.window import Window

# COMMAND ----------

CATALOG = "bank"
SALT_LOPDP = "GYE_BANK_LOPDP_2026"

# COMMAND ----------

# Procesamiento silver de créditos con protección LOPDP
df_creditos = spark.table(f"{CATALOG}.bronze.creditos_raw")

df_creditos_silver = (
    df_creditos
    .filter(col("credito_id").isNotNull() & (length(trim(col("cedula_cliente"))) == 10))
    .withColumn("cliente_hash_id", sha2(concat(trim(col("cedula_cliente")), lit(SALT_LOPDP)), 256))
    .withColumn("cedula_enmascarada", concat(lit("******"), substring(trim(col("cedula_cliente")), 7, 4)))
    .withColumn("fecha_desembolso", to_date(col("fecha_desembolso"), "yyyy-MM-dd"))
    .withColumn("en_riesgo_par30", when(col("dias_mora") >= 30, lit(1)).otherwise(lit(0)))
    .drop("cedula_cliente") # Eliminamos cedula en texto plano (Cumplimiento LOPDP)
    .dropDuplicates(["credito_id"])
)

(
    df_creditos_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.creditos_limpios")
)

# COMMAND ----------

#Procesamiento silver de pagos (calidad, merge, cuarentena)

df_pagos = spark.table(f"{CATALOG}.bronze.pagos_raw")

# Regla estricta de calidad bancaria
condicion_valida = (
    col("tx_id").isNotNull() &
    col("credito_id").isNotNull() &
    col("cedula_cliente").isNotNull() &
    (length(trim(col("cedula_cliente"))) == 10) &
    (col("monto_usd") > 0)
)

# 2A. Aislar registros defectuosos en tabla de Cuarentena
df_cuarentena = (
    df_pagos.filter(~condicion_valida)
    .withColumn(
        "motivo_rechazo",
        when(col("monto_usd") <= 0, "Monto invalido (menor o igual a 0)")
        .when(col("cedula_cliente").isNull() | (length(trim(col("cedula_cliente"))) != 10), "Cedula nula o formato invalido")
        .otherwise("ID de transaccion o credito faltante")
    )
    .withColumn("_quarantine_ts", current_timestamp())
)

(
    df_cuarentena.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.silver.quarantine_pagos")
)

# COMMAND ----------

# Limpieza, deduplicación y aplicar LOPDP a registros válidos
ventana_dedup = Window.partitionBy("tx_id").orderBy(col("fecha_evento").desc(), col("_ingestion_ts").desc())

df_pagos_limpios = (
    df_pagos.filter(condicion_valida)
    .withColumn("rn", row_number().over(ventana_dedup))
    .filter(col("rn") == 1)
    .withColumn("cliente_hash_id", sha2(concat(trim(col("cedula_cliente")), lit(SALT_LOPDP)), 256))
    .withColumn("fecha_evento_ts", to_timestamp(col("fecha_evento"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("fecha_pago", to_date(col("fecha_evento_ts")))
    .select(
        "tx_id", "credito_id", "cliente_hash_id", "canal_pago",
        "monto_usd", "estado_tx", "fecha_evento_ts", "fecha_pago", "_ingestion_ts"
    )
)

df_pagos_limpios.createOrReplaceTempView("stg_pagos_limpios")

# COMMAND ----------

# Creamos tabla Silver si no existe y ejecutamos MERGE INTO (Idempotencia)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.pagos_limpios (
    tx_id STRING,
    credito_id STRING,
    cliente_hash_id STRING,
    canal_pago STRING,
    monto_usd DOUBLE,
    estado_tx STRING,
    fecha_evento_ts TIMESTAMP,
    fecha_pago DATE,
    _ingestion_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
MERGE INTO {CATALOG}.silver.pagos_limpios AS target
USING stg_pagos_limpios AS source
ON target.tx_id = source.tx_id
WHEN MATCHED AND source.fecha_evento_ts >= target.fecha_evento_ts THEN
  UPDATE SET
    target.estado_tx = source.estado_tx,
    target.monto_usd = source.monto_usd,
    target.fecha_evento_ts = source.fecha_evento_ts,
    target._ingestion_ts = source._ingestion_ts
WHEN NOT MATCHED THEN
  INSERT *
""")

print(f"Silver completado | Cuarentena detectada: {df_cuarentena.count()} registros aislados.")