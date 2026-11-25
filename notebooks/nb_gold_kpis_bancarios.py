# Databricks notebook source
CATALOG = "bank"

# COMMAND ----------

# KPI Riesgo de Cartera y Recaudación por Zona de Guayaquil y Segmento
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.kpi_riesgo_cartera_guayaquil
CLUSTER BY (zona_guayaquil, segmento_credito)
AS
WITH pagos_confirmados AS (
    SELECT
        credito_id,
        COUNT(tx_id) AS total_pagos_confirmados,
        ROUND(SUM(monto_usd), 2) AS total_recaudado_usd
    FROM {CATALOG}.silver.pagos_limpios
    WHERE estado_tx = 'CONFIRMADO'
    GROUP BY credito_id
)
SELECT
    c.zona_guayaquil,
    c.segmento_credito,
    COUNT(c.credito_id) AS cantidad_creditos,
    ROUND(SUM(c.saldo_pendiente_usd), 2) AS cartera_total_usd,
    ROUND(SUM(CASE WHEN c.en_riesgo_par30 = 1 THEN c.saldo_pendiente_usd ELSE 0 END), 2) AS cartera_par30_usd,
    ROUND(
        100.0 * SUM(CASE WHEN c.en_riesgo_par30 = 1 THEN c.saldo_pendiente_usd ELSE 0 END)
        / NULLIF(SUM(c.saldo_pendiente_usd), 0),
    2) AS tasa_morosidad_par30_pct,
    ROUND(COALESCE(SUM(p.total_recaudado_usd), 0), 2) AS recaudacion_reciente_usd
FROM {CATALOG}.silver.creditos_limpios c
LEFT JOIN pagos_confirmados p ON c.credito_id = p.credito_id
GROUP BY c.zona_guayaquil, c.segmento_credito
ORDER BY tasa_morosidad_par30_pct DESC
""")

# COMMAND ----------

# KPI Rendimiento por Canal Bancario
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.kpi_canales_pago
CLUSTER BY (canal_pago, fecha_pago)
AS
SELECT
    fecha_pago,
    canal_pago,
    COUNT(tx_id) AS volumen_tx,
    ROUND(SUM(CASE WHEN estado_tx = 'CONFIRMADO' THEN monto_usd ELSE 0 END), 2) AS monto_confirmado_usd,
    ROUND(100.0 * SUM(CASE WHEN estado_tx = 'CONFIRMADO' THEN 1 ELSE 0 END) / COUNT(*), 2) AS tasa_exito_pct
FROM {CATALOG}.silver.pagos_limpios
GROUP BY fecha_pago, canal_pago
""")

# COMMAND ----------

# Tabla de Alertas
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.alertas_transacciones_atipicas
AS
SELECT
    p.tx_id,
    p.credito_id,
    c.cedula_enmascarada,
    c.zona_guayaquil,
    p.canal_pago,
    p.monto_usd,
    c.cuota_mensual_usd,
    p.fecha_evento_ts,
    CASE
        WHEN p.monto_usd > (c.cuota_mensual_usd * 3) AND HOUR(p.fecha_evento_ts) BETWEEN 0 AND 5
            THEN 'CRITICA: Monto alto en horario de madrugada'
        WHEN p.monto_usd > (c.cuota_mensual_usd * 3)
            THEN 'ALTA: Pago supera 3x la cuota mensual'
        ELSE 'MEDIA: Horario atipico en canal fisico'
    END AS nivel_alerta
FROM {CATALOG}.silver.pagos_limpios p
INNER JOIN {CATALOG}.silver.creditos_limpios c ON p.credito_id = c.credito_id
WHERE p.monto_usd > (c.cuota_mensual_usd * 3)
   OR (p.canal_pago = 'Corresponsal de Barrio' AND HOUR(p.fecha_evento_ts) BETWEEN 0 AND 5)
""")

# COMMAND ----------

#Mantenimiento Delta Lake
spark.sql(f"OPTIMIZE {CATALOG}.gold.kpi_riesgo_cartera_guayaquil")
spark.sql(f"OPTIMIZE {CATALOG}.gold.kpi_canales_pago")

print("Capa Gold generada y optimizada correctamente.")
display(spark.table(f"{CATALOG}.gold.alertas_transacciones_atipicas"))