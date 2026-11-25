# Databricks notebook source
CATALOG = "bank"

# 1. Verificar Bronze incremental: exactamente 302 registros en Día 1 y 358 en Día 2 (cero multiplicación)
bronze_por_lote = spark.sql(f"""
    SELECT _batch_date, COUNT(*) AS filas
    FROM {CATALOG}.bronze.pagos_raw
    GROUP BY _batch_date ORDER BY _batch_date
""").collect()
print("1. Bronze por lote:", [(str(r["_batch_date"]), r["filas"]) for r in bronze_por_lote])
assert len(bronze_por_lote) == 2, "Deben existir exactamente 2 lotes en Bronze"
assert bronze_por_lote[0]["filas"] == 302 and bronze_por_lote[1]["filas"] == 358, "Conteo inesperado en Bronze"

# 2. Verificar actualización tardía TX-000500 (v1 PENDIENTE/Banca Web -> v2 CONFIRMADO/App Movil, sin duplicados)
tx_late = spark.sql(f"""
    SELECT tx_id, estado_tx, canal_pago, event_version, fecha_pago
    FROM {CATALOG}.silver.pagos_limpios
    WHERE tx_id = 'TX-000500'
""").collect()
print("2. Actualizacion tardia TX-000500:", tx_late)
assert len(tx_late) == 1, "TX-000500 no debe estar duplicada"
assert tx_late[0]["estado_tx"] == "CONFIRMADO" and tx_late[0]["canal_pago"] == "App Movil" and tx_late[0]["event_version"] == 2

# 3. Verificar desempate determinístico en TX-999010 (debe ganar CONFIRMADO sobre PENDIENTE)
tx_tie = spark.sql(f"SELECT tx_id, estado_tx FROM {CATALOG}.silver.pagos_limpios WHERE tx_id = 'TX-999010'").collect()
print("3. Desempate deterministico TX-999010:", tx_tie)
assert len(tx_tie) == 1 and tx_tie[0]["estado_tx"] == "CONFIRMADO"

# 4. Verificar Cuarentena histórica + Integridad referencial (debe conservar Día 1 y Día 2)
cuarentena = spark.sql(f"""
    SELECT _batch_date, tx_id, motivo_rechazo
    FROM {CATALOG}.silver.quarantine_pagos
    ORDER BY _batch_date, tx_id
""").collect()
print("4. Cuarentena historica:")
for r in cuarentena:
    print("  ", r["_batch_date"], "|", r["tx_id"], "|", r["motivo_rechazo"])
tx_cuarentena = {r["tx_id"] for r in cuarentena}
assert {"TX-998001", "TX-999001", "TX-999002", "TX-999004"}.issubset(tx_cuarentena), "Faltan registros historicos o referenciales en cuarentena"

# 5. Verificar evolución temporal del PAR 30 en Gold (2 fechas de corte distintas)
par30_hist = spark.sql(f"""
    SELECT fecha_corte,
           ROUND(100.0 * SUM(cartera_par30_usd) / SUM(cartera_total_usd), 2) AS par30_global_pct,
           ROUND(SUM(recaudacion_dia_corte_usd), 2) AS rec_dia,
           ROUND(SUM(recaudacion_ventana_dias_usd), 2) AS rec_7d
    FROM {CATALOG}.gold.kpi_riesgo_cartera_guayaquil
    GROUP BY fecha_corte ORDER BY fecha_corte
""").collect()
print("5. Evolucion historica PAR 30 y Recaudacion:", par30_hist)
assert len(par30_hist) == 2, "Gold debe conservar el historico de ambos dias de corte"

# 6. Verificar Alertas basadas en reglas de negocio (diferenciando CONFIRMADO vs PENDIENTE)
print("6. Alertas por reglas de negocio:")
display(spark.table(f"{CATALOG}.gold.alertas_reglas_negocio"))

print("TODAS LAS VALIDACIONES END-TO-END PASARON EXITOSAMENTE.")