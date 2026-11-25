# Databricks notebook source
CATALOG = "bank"
dbutils.widgets.text("fecha_proceso", "2026-11-24")
dbutils.widgets.text("ventana_dias", "7")

fecha_proceso = dbutils.widgets.get("fecha_proceso")
ventana_dias = int(dbutils.widgets.get("ventana_dias"))

# COMMAND ----------

# 1. KPI RIESGO DE CARTERA (PAR 30) CON HISTÓRICO POR FECHA DE CORTE Y RECAUDACIÓN CON VENTANAS TEMPORALES

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.kpi_riesgo_cartera_guayaquil (
    fecha_corte DATE,
    zona_guayaquil STRING,
    segmento_credito STRING,
    cantidad_creditos BIGINT,
    cartera_total_usd DOUBLE,
    cartera_par30_usd DOUBLE,
    tasa_morosidad_par30_pct DOUBLE,
    ventana_dias_aplicada INT,
    recaudacion_dia_corte_usd DOUBLE,
    recaudacion_ventana_dias_usd DOUBLE,
    recaudacion_acumulada_usd DOUBLE
) USING DELTA
CLUSTER BY (fecha_corte, zona_guayaquil, segmento_credito)
""")

df_kpi_cartera = spark.sql(f"""
WITH pagos_confirmados_corte AS (
    SELECT
        credito_id,
        ROUND(SUM(CASE WHEN fecha_pago = DATE '{fecha_proceso}' THEN monto_usd ELSE 0 END), 2) AS rec_dia_corte,
        ROUND(SUM(CASE WHEN fecha_pago BETWEEN DATE_SUB(DATE '{fecha_proceso}', {ventana_dias - 1}) AND DATE '{fecha_proceso}' THEN monto_usd ELSE 0 END), 2) AS rec_ventana,
        ROUND(SUM(monto_usd), 2) AS rec_acumulada
    FROM {CATALOG}.silver.pagos_limpios
    WHERE estado_tx = 'CONFIRMADO'
      AND fecha_pago <= DATE '{fecha_proceso}'
    GROUP BY credito_id
)
SELECT
    c.fecha_corte,
    c.zona_guayaquil,
    c.segmento_credito,
    COUNT(c.credito_id) AS cantidad_creditos,
    ROUND(SUM(c.saldo_pendiente_usd), 2) AS cartera_total_usd,
    ROUND(SUM(CASE WHEN c.en_riesgo_par30 = 1 THEN c.saldo_pendiente_usd ELSE 0 END), 2) AS cartera_par30_usd,
    ROUND(
        100.0 * SUM(CASE WHEN c.en_riesgo_par30 = 1 THEN c.saldo_pendiente_usd ELSE 0 END)
        / NULLIF(SUM(c.saldo_pendiente_usd), 0),
    2) AS tasa_morosidad_par30_pct,
    CAST({ventana_dias} AS INT) AS ventana_dias_aplicada,
    ROUND(COALESCE(SUM(p.rec_dia_corte), 0), 2) AS recaudacion_dia_corte_usd,
    ROUND(COALESCE(SUM(p.rec_ventana), 0), 2) AS recaudacion_ventana_dias_usd,
    ROUND(COALESCE(SUM(p.rec_acumulada), 0), 2) AS recaudacion_acumulada_usd
FROM {CATALOG}.silver.creditos_snapshot c
LEFT JOIN pagos_confirmados_corte p ON c.credito_id = p.credito_id
WHERE c.fecha_corte = DATE '{fecha_proceso}'
GROUP BY c.fecha_corte, c.zona_guayaquil, c.segmento_credito
""")

# COMMAND ----------

# Carga incremental idempotente por fecha_corte
(
    df_kpi_cartera.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"fecha_corte = DATE '{fecha_proceso}'")
    .saveAsTable(f"{CATALOG}.gold.kpi_riesgo_cartera_guayaquil")
)

# COMMAND ----------

# 2. KPI CANALES DE PAGO

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.kpi_canales_pago (
    fecha_pago DATE,
    canal_pago STRING,
    volumen_tx BIGINT,
    monto_confirmado_usd DOUBLE,
    monto_pendiente_usd DOUBLE,
    tasa_confirmacion_pct DOUBLE
) USING DELTA
CLUSTER BY (fecha_pago, canal_pago)
""")

spark.sql(f"""
MERGE INTO {CATALOG}.gold.kpi_canales_pago AS target
USING (
    WITH fechas_afectadas AS (
        SELECT DISTINCT fecha_pago
        FROM {CATALOG}.silver.pagos_limpios
        WHERE _batch_date = DATE '{fecha_proceso}'
    )
    SELECT
        p.fecha_pago,
        p.canal_pago,
        COUNT(p.tx_id) AS volumen_tx,
        ROUND(SUM(CASE WHEN p.estado_tx = 'CONFIRMADO' THEN p.monto_usd ELSE 0 END), 2) AS monto_confirmado_usd,
        ROUND(SUM(CASE WHEN p.estado_tx = 'PENDIENTE' THEN p.monto_usd ELSE 0 END), 2) AS monto_pendiente_usd,
        ROUND(100.0 * SUM(CASE WHEN p.estado_tx = 'CONFIRMADO' THEN 1 ELSE 0 END) / COUNT(*), 2) AS tasa_confirmacion_pct
    FROM {CATALOG}.silver.pagos_limpios p
    INNER JOIN fechas_afectadas f ON p.fecha_pago = f.fecha_pago
    GROUP BY p.fecha_pago, p.canal_pago
) AS source
ON target.fecha_pago = source.fecha_pago AND target.canal_pago = source.canal_pago
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# 3. ALERTAS DE TRANSACCIONES SOSPECHOSAS BASADAS EN REGLAS DE NEGOCIO

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.alertas_reglas_negocio (
    tx_id STRING,
    credito_id STRING,
    cedula_enmascarada STRING,
    zona_guayaquil STRING,
    canal_pago STRING,
    estado_tx STRING,
    monto_usd DOUBLE,
    cuota_mensual_usd DOUBLE,
    fecha_evento_ts TIMESTAMP,
    regla_activada STRING,
    prioridad_revision STRING,
    fecha_evaluacion DATE
) USING DELTA
CLUSTER BY (fecha_evaluacion, prioridad_revision)
""")

spark.sql(f"""
MERGE INTO {CATALOG}.gold.alertas_reglas_negocio AS target
USING (
    SELECT
        p.tx_id,
        p.credito_id,
        c.cedula_enmascarada,
        c.zona_guayaquil,
        p.canal_pago,
        p.estado_tx,
        p.monto_usd,
        c.cuota_mensual_usd,
        p.fecha_evento_ts,
        CASE
            WHEN p.monto_usd > (c.cuota_mensual_usd * 3) AND HOUR(p.fecha_evento_ts) BETWEEN 0 AND 5
                THEN 'Monto > 3x cuota en horario de madrugada (00h-05h)'
            WHEN p.monto_usd > (c.cuota_mensual_usd * 3)
                THEN 'Monto > 3x cuota mensual (posible prepago o error operativo)'
            ELSE 'Operacion en horario de madrugada en Corresponsal de Barrio'
        END AS regla_activada,
        CASE
            WHEN p.estado_tx = 'CONFIRMADO' AND p.monto_usd > (c.cuota_mensual_usd * 3) AND HOUR(p.fecha_evento_ts) BETWEEN 0 AND 5
                THEN 'CRITICA (Fondos confirmados - revisar inmediato)'
            WHEN p.estado_tx = 'CONFIRMADO' AND p.monto_usd > (c.cuota_mensual_usd * 3)
                THEN 'MEDIA (Fondos confirmados - validar si es prepago)'
            ELSE 'PREVENTIVA (Transaccion PENDIENTE - monitorear antes de liquidar)'
        END AS prioridad_revision,
        DATE '{fecha_proceso}' AS fecha_evaluacion
    FROM {CATALOG}.silver.pagos_limpios p
    INNER JOIN {CATALOG}.silver.creditos_snapshot c
        ON p.credito_id = c.credito_id AND c.fecha_corte = DATE '{fecha_proceso}'
    WHERE p._batch_date = DATE '{fecha_proceso}'
      AND (
          p.monto_usd > (c.cuota_mensual_usd * 3)
          OR (p.canal_pago = 'Corresponsal de Barrio' AND HOUR(p.fecha_evento_ts) BETWEEN 0 AND 5)
      )
) AS source
ON target.tx_id = source.tx_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# 4. Compactación Delta Lake
spark.sql(f"OPTIMIZE {CATALOG}.gold.kpi_riesgo_cartera_guayaquil")
spark.sql(f"OPTIMIZE {CATALOG}.gold.kpi_canales_pago")
spark.sql(f"OPTIMIZE {CATALOG}.gold.alertas_reglas_negocio")

print(f"Capa Gold actualizada incrementalmente para fecha_corte = {fecha_proceso} (ventana recaudacion: {ventana_dias} dias).")