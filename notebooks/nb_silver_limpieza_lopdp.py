# Databricks notebook source
import hashlib
import hmac
import pandas as pd
from pyspark.sql.functions import (
    col, concat, current_timestamp, length, lit, pandas_udf,
    row_number, substring, to_date, to_timestamp, trim, when
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# COMMAND ----------

CATALOG = "bank"
dbutils.widgets.text("fecha_proceso", "2026-11-24")
dbutils.widgets.text("hmac_key_dev", "DEV_DEMO_KEY_USE_DATABRICKS_SECRETS_IN_PROD")

fecha_proceso = dbutils.widgets.get("fecha_proceso")

# COMMAND ----------

# 0. GESTIÓN SEGURA DE CLAVE PARA SEUDONIMIZACIÓN (HMAC-SHA-256)

try:
    #Databricks Secrets
    HMAC_SECRET_KEY = dbutils.secrets.get(scope="lopdp_security", key="cedula_hmac_key")
    origen_clave = "Databricks Secrets"
except Exception:
    # Fallback explícito solo para entorno de demostración en Free Edition
    HMAC_SECRET_KEY = dbutils.widgets.get("hmac_key_dev")
    origen_clave = "Widget de desarrollo (Free Edition)"

def crear_hmac_sha256_udf(secret_key: str):
    key_bytes = secret_key.encode("utf-8")

    @pandas_udf(StringType())
    def hmac_sha256_vec(series: pd.Series) -> pd.Series:
        return series.apply(
            lambda val: hmac.new(key_bytes, str(val).strip().encode("utf-8"), hashlib.sha256).hexdigest()
            if pd.notna(val) else None
        )
    return hmac_sha256_vec

hmac_sha256 = crear_hmac_sha256_udf(HMAC_SECRET_KEY)

# COMMAND ----------

# 1. SILVER CRÉDITOS: Snapshot histórico por (credito_id, fecha_corte)

df_creditos_lote = (
    spark.table(f"{CATALOG}.bronze.creditos_raw")
    .filter(col("_batch_date") == to_date(lit(fecha_proceso), "yyyy-MM-dd"))
)

df_creditos_silver = (
    df_creditos_lote
    .filter(col("credito_id").isNotNull() & (length(trim(col("cedula_cliente"))) == 10))
    .withColumn("fecha_corte", to_date(col("fecha_corte"), "yyyy-MM-dd"))
    .withColumn("cliente_hash_id", hmac_sha256(col("cedula_cliente")))
    .withColumn("cedula_enmascarada", concat(lit("******"), substring(trim(col("cedula_cliente")), 7, 4)))
    .withColumn("fecha_desembolso", to_date(col("fecha_desembolso"), "yyyy-MM-dd"))
    .withColumn("en_riesgo_par30", when(col("dias_mora") >= 30, lit(1)).otherwise(lit(0)))
    .drop("cedula_cliente")  # Seudonimización LOPDP: sin PII plano en Silver/Gold
    .dropDuplicates(["credito_id", "fecha_corte"])
)

df_creditos_silver.createOrReplaceTempView("stg_creditos_snapshot")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.creditos_snapshot (
    fecha_corte DATE,
    credito_id STRING,
    cliente_hash_id STRING,
    cedula_enmascarada STRING,
    zona_guayaquil STRING,
    segmento_credito STRING,
    monto_aprobado_usd DOUBLE,
    saldo_pendiente_usd DOUBLE,
    cuota_mensual_usd DOUBLE,
    dias_mora BIGINT,
    en_riesgo_par30 INT,
    fecha_desembolso DATE,
    _ingestion_ts TIMESTAMP,
    _source_file STRING,
    _batch_date DATE
) USING DELTA
CLUSTER BY (fecha_corte, zona_guayaquil)
""")

spark.sql(f"""
MERGE INTO {CATALOG}.silver.creditos_snapshot AS target
USING stg_creditos_snapshot AS source
ON target.credito_id = source.credito_id AND target.fecha_corte = source.fecha_corte
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# 2. SILVER PAGOS: Calidad Sintáctica + Integridad Referencial + Cuarentena Histórica

df_pagos_lote = (
    spark.table(f"{CATALOG}.bronze.pagos_raw")
    .filter(col("_batch_date") == to_date(lit(fecha_proceso), "yyyy-MM-dd"))
)

condicion_sintactica = (
    col("tx_id").isNotNull() &
    col("credito_id").isNotNull() &
    col("cedula_cliente").isNotNull() &
    (length(trim(col("cedula_cliente"))) == 10) &
    (col("monto_usd") > 0) &
    col("event_version").isNotNull()
)

# COMMAND ----------

# 2A. Rechazos sintácticos
df_rechazo_sintactico = (
    df_pagos_lote.filter(~condicion_sintactica)
    .withColumn(
        "motivo_rechazo",
        when(col("monto_usd") <= 0, "Regla calidad: Monto invalido (menor o igual a 0)")
        .when(col("cedula_cliente").isNull() | (length(trim(col("cedula_cliente"))) != 10), "Regla calidad: Cedula nula o formato invalido")
        .when(col("event_version").isNull(), "Regla calidad: Version de evento faltante")
        .otherwise("Regla calidad: ID de transaccion o credito faltante")
    )
    .select(
        "tx_id", "credito_id", "canal_pago", "monto_usd", "estado_tx",
        "_source_file", "_ingestion_ts", "_batch_date", "motivo_rechazo"
    )
)

# 2B. Validación de integridad referencial
df_pagos_sintacticos_ok = (
    df_pagos_lote.filter(condicion_sintactica)
    .withColumn("cliente_hash_id", hmac_sha256(col("cedula_cliente")))
    .drop("cedula_cliente")
)

df_maestro_vigente = (
    spark.table(f"{CATALOG}.silver.creditos_snapshot")
    .filter(col("fecha_corte") == to_date(lit(fecha_proceso), "yyyy-MM-dd"))
    .select(
        col("credito_id").alias("m_credito_id"),
        col("cliente_hash_id").alias("m_cliente_hash_id")
    )
)

df_pagos_validados = df_pagos_sintacticos_ok.join(
    df_maestro_vigente,
    df_pagos_sintacticos_ok["credito_id"] == df_maestro_vigente["m_credito_id"],
    how="left"
)

condicion_referencial_ok = (
    col("m_credito_id").isNotNull() &
    (col("cliente_hash_id") == col("m_cliente_hash_id"))
)

df_rechazo_referencial = (
    df_pagos_validados.filter(~condicion_referencial_ok)
    .withColumn(
        "motivo_rechazo",
        when(col("m_credito_id").isNull(), "Integridad referencial: credito_id inexistente en maestro")
        .otherwise("Integridad referencial: cedula no corresponde al titular del credito")
    )
    .select(
        "tx_id", "credito_id", "canal_pago", "monto_usd", "estado_tx",
        "_source_file", "_ingestion_ts", "_batch_date", "motivo_rechazo"
    )
)

# 2C. Cuarentena histórica acumulativa e idempotente por lote
df_cuarentena_lote = (
    df_rechazo_sintactico.unionByName(df_rechazo_referencial)
    .withColumn("_quarantine_ts", current_timestamp())
)

tabla_cuarentena = f"{CATALOG}.silver.quarantine_pagos"
if spark.catalog.tableExists(tabla_cuarentena):
    (
        df_cuarentena_lote.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_date = DATE '{fecha_proceso}'")
        .saveAsTable(tabla_cuarentena)
    )
else:
    df_cuarentena_lote.write.format("delta").mode("overwrite").saveAsTable(tabla_cuarentena)

# COMMAND ----------


# 3. DEDUPLICACIÓN + MERGE COMPLETO EN SILVER

df_pagos_candidatos = (
    df_pagos_validados.filter(condicion_referencial_ok)
    .withColumn("fecha_evento_ts", to_timestamp(col("fecha_evento"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("updated_at_ts", to_timestamp(col("updated_at"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("fecha_pago", to_date(col("fecha_evento_ts")))
    .withColumn(
        "estado_prioridad",
        when(col("estado_tx") == "CONFIRMADO", lit(3))
        .when(col("estado_tx") == "RECHAZADO", lit(2))
        .when(col("estado_tx") == "PENDIENTE", lit(1))
        .otherwise(lit(0))
    )
)

# Regla explícita de precedencia ante empates absolutos
ventana_deterministica = Window.partitionBy("tx_id").orderBy(
    col("event_version").desc(),
    col("updated_at_ts").desc(),
    col("estado_prioridad").desc(),
    col("_ingestion_ts").desc(),
    col("monto_usd").desc(),
    col("canal_pago").asc()
)

df_pagos_limpios = (
    df_pagos_candidatos
    .withColumn("rn", row_number().over(ventana_deterministica))
    .filter(col("rn") == 1)
    .select(
        "tx_id", "credito_id", "cliente_hash_id", "canal_pago",
        "monto_usd", "estado_tx", "event_version", "fecha_evento_ts",
        "fecha_pago", "updated_at_ts", "_ingestion_ts", "_batch_date"
    )
)

df_pagos_limpios.createOrReplaceTempView("stg_pagos_limpios")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.pagos_limpios (
    tx_id STRING,
    credito_id STRING,
    cliente_hash_id STRING,
    canal_pago STRING,
    monto_usd DOUBLE,
    estado_tx STRING,
    event_version BIGINT,
    fecha_evento_ts TIMESTAMP,
    fecha_pago DATE,
    updated_at_ts TIMESTAMP,
    _ingestion_ts TIMESTAMP,
    _batch_date DATE
) USING DELTA
CLUSTER BY (fecha_pago, credito_id)
""")

# MERGE completo: actualiza TODOS los campos cuando llega una versión mayor o actualización más reciente
spark.sql(f"""
MERGE INTO {CATALOG}.silver.pagos_limpios AS target
USING stg_pagos_limpios AS source
ON target.tx_id = source.tx_id
WHEN MATCHED AND (
    source.event_version > target.event_version
    OR (source.event_version = target.event_version AND source.updated_at_ts >= target.updated_at_ts)
) THEN
  UPDATE SET
    target.credito_id = source.credito_id,
    target.cliente_hash_id = source.cliente_hash_id,
    target.canal_pago = source.canal_pago,
    target.monto_usd = source.monto_usd,
    target.estado_tx = source.estado_tx,
    target.event_version = source.event_version,
    target.fecha_evento_ts = source.fecha_evento_ts,
    target.fecha_pago = source.fecha_pago,
    target.updated_at_ts = source.updated_at_ts,
    target._ingestion_ts = source._ingestion_ts,
    target._batch_date = source._batch_date
WHEN NOT MATCHED THEN
  INSERT *
""")

print(f"Silver lote {fecha_proceso} OK (Clave HMAC: {origen_clave}) | Aislados en cuarentena hoy: {df_cuarentena_lote.count()}")