# Databricks notebook source
import json
import os
import random
from datetime import datetime, timedelta

# COMMAND ----------

random.seed(42)

CATALOG = "bank"
BASE_PATH = f"/Volumes/{CATALOG}/bronze/landing_zone"
os.makedirs(f"{BASE_PATH}/creditos", exist_ok=True)
os.makedirs(f"{BASE_PATH}/pagos", exist_ok=True)

# COMMAND ----------

zonas_gye = [
    "Norte - Alborada", "Urdesa / Kennedy", "Centro",
    "Sur - Guasmo", "Via a la Costa", "Samborondon", "Duran"
]
segmentos = ["Consumo", "Microcredito", "Productivo", "Vivienda"]
canales = ["App Movil", "Banca Web", "Corresponsal de Barrio", "Agencia", "ATM"]

# COMMAND ----------

#Genera un maestro de 500 créditos activos en Guayaquil, Snaphot dia 2026-11-23b
creditos_d1 = []
for i in range(1, 501):
    segmento = random.choices(segmentos, weights=[45, 30, 15, 10])[0]
    monto = round(
        random.uniform(800, 15000) if segmento in ["Consumo", "Microcredito"]
        else random.uniform(15000, 90000), 2
    )
    dias_mora = random.choices([0, 15, 29, 35, 65, 95], weights=[64, 12, 5, 9, 6, 4])[0]
    creditos_d1.append({
        "fecha_corte": "2026-11-23",
        "credito_id": f"CR-GYE-{i:05d}",
        "cedula_cliente": f"09{random.randint(10000000, 99999999)}",
        "zona_guayaquil": random.choice(zonas_gye),
        "segmento_credito": segmento,
        "monto_aprobado_usd": monto,
        "saldo_pendiente_usd": round(monto * random.uniform(0.25, 0.95), 2),
        "cuota_mensual_usd": round(monto / random.choice([12, 24, 36, 48]), 2),
        "dias_mora": dias_mora,
        "fecha_desembolso": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d")
    })

with open(f"{BASE_PATH}/creditos/creditos_2026_11_23.json", "w") as f:
    for row in creditos_d1:
        f.write(json.dumps(row) + "\n")


# COMMAND ----------

# 2. PAGOS DÍA 1 (2026-11-23) + Escenario Late Update v1 + Error histórico
pagos_d1 = []
pagos_confirmados_d1_por_credito = {}

for idx in range(300):
    cr = random.choice(creditos_d1)
    hora = f"{random.randint(7, 21):02d}:{random.randint(0, 59):02d}:00"
    estado = random.choices(["CONFIRMADO", "PENDIENTE"], weights=[92, 8])[0]
    monto_pago = round(min(cr["saldo_pendiente_usd"], cr["cuota_mensual_usd"] * random.uniform(0.9, 1.05)), 2)

    tx = {
        "tx_id": f"TX-{idx + 1:06d}",
        "credito_id": cr["credito_id"],
        "cedula_cliente": cr["cedula_cliente"],
        "canal_pago": random.choices(canales, weights=[40, 20, 22, 10, 8])[0],
        "monto_usd": monto_pago,
        "estado_tx": estado,
        "event_version": 1,
        "fecha_evento": f"2026-11-23 {hora}",
        "updated_at": f"2026-11-23 {hora}"
    }
    pagos_d1.append(tx)
    if estado == "CONFIRMADO":
        pagos_confirmados_d1_por_credito[cr["credito_id"]] = (
            pagos_confirmados_d1_por_credito.get(cr["credito_id"], 0.0) + monto_pago
        )

# COMMAND ----------

# Caso T3 (v1): Pago PENDIENTE en Día 1 que se confirmará tardíamente en Día 2
cr_late = creditos_d1[2]
pagos_d1.append({
    "tx_id": "TX-000500",
    "credito_id": cr_late["credito_id"],
    "cedula_cliente": cr_late["cedula_cliente"],
    "canal_pago": "Banca Web",
    "monto_usd": cr_late["cuota_mensual_usd"],
    "estado_tx": "PENDIENTE",
    "event_version": 1,
    "fecha_evento": "2026-11-23 18:05:00",
    "updated_at": "2026-11-23 18:10:00"
})

# Error histórico en Día 1 (para verificar que Cuarentena conserva días previos)
pagos_d1.append({
    "tx_id": "TX-998001",
    "credito_id": creditos_d1[4]["credito_id"],
    "cedula_cliente": creditos_d1[4]["cedula_cliente"],
    "canal_pago": "ATM",
    "monto_usd": -90.00,
    "estado_tx": "CONFIRMADO",
    "event_version": 1,
    "fecha_evento": "2026-11-23 14:20:00",
    "updated_at": "2026-11-23 14:20:00"
})

with open(f"{BASE_PATH}/pagos/pagos_2026_11_23.json", "w") as f:
    for row in pagos_d1:
        f.write(json.dumps(row) + "\n")

# COMMAND ----------

# 3. SNAPSHOT DÍA 2 (2026-11-24): Evolución coherente de saldo y mora
creditos_d2 = []
for cr in creditos_d1:
    cr_new = cr.copy()
    cr_new["fecha_corte"] = "2026-11-24"
    pagado_d1 = pagos_confirmados_d1_por_credito.get(cr["credito_id"], 0.0)

    if pagado_d1 > 0:
        cr_new["saldo_pendiente_usd"] = round(max(0.0, cr["saldo_pendiente_usd"] - pagado_d1), 2)
        # Si pagó al menos una cuota, regulariza parcialmente o totalmente la mora
        cr_new["dias_mora"] = max(0, cr["dias_mora"] - 30) if pagado_d1 >= cr["cuota_mensual_usd"] * 0.9 else cr["dias_mora"]
    else:
        # Si ya tenía atraso y no pagó ayer, avanza 1 día de mora (ej. 29 -> 30 entra a PAR 30)
        if cr["dias_mora"] > 0:
            cr_new["dias_mora"] = cr["dias_mora"] + 1

    creditos_d2.append(cr_new)

with open(f"{BASE_PATH}/creditos/creditos_2026_11_24.json", "w") as f:
    for row in creditos_d2:
        f.write(json.dumps(row) + "\n")

# COMMAND ----------

# 4. PAGOS DÍA 2 (2026-11-24) + Casos de prueba controlados
pagos_d2 = []
for idx in range(350):
    cr = random.choice(creditos_d2)
    hora = f"{random.randint(7, 21):02d}:{random.randint(0, 59):02d}:00"
    estado = random.choices(["CONFIRMADO", "PENDIENTE"], weights=[92, 8])[0]
    monto_pago = round(min(max(cr["saldo_pendiente_usd"], 50.0), cr["cuota_mensual_usd"] * random.uniform(0.9, 1.05)), 2)

    pagos_d2.append({
        "tx_id": f"TX-{501 + idx:06d}",
        "credito_id": cr["credito_id"],
        "cedula_cliente": cr["cedula_cliente"],
        "canal_pago": random.choices(canales, weights=[40, 20, 22, 10, 8])[0],
        "monto_usd": monto_pago,
        "estado_tx": estado,
        "event_version": 1,
        "fecha_evento": f"2026-11-24 {hora}",
        "updated_at": f"2026-11-24 {hora}"
    })

# COMMAND ----------

# Caso T3 (v2): Actualización tardía de TX-000500 (mismo fecha_evento de ayer, nueva versión y corrección de canal)
pagos_d2.append({
    "tx_id": "TX-000500",
    "credito_id": cr_late["credito_id"],
    "cedula_cliente": cr_late["cedula_cliente"],
    "canal_pago": "App Movil",  # Corrección legítima de canal
    "monto_usd": cr_late["cuota_mensual_usd"],
    "estado_tx": "CONFIRMADO",
    "event_version": 2,
    "fecha_evento": "2026-11-23 18:05:00",  # Fecha del evento original (ayer)
    "updated_at": "2026-11-24 08:30:00"     # Timestamp de actualización hoy
})

# Caso T4A: Empate exacto de timestamps y versión (prueba desempate determinístico por estado)
cr_tie = creditos_d2[7]
for est in ["PENDIENTE", "CONFIRMADO"]:
    pagos_d2.append({
        "tx_id": "TX-999010",
        "credito_id": cr_tie["credito_id"],
        "cedula_cliente": cr_tie["cedula_cliente"],
        "canal_pago": "App Movil",
        "monto_usd": cr_tie["cuota_mensual_usd"],
        "estado_tx": est,
        "event_version": 1,
        "fecha_evento": "2026-11-24 10:00:00",
        "updated_at": "2026-11-24 10:00:00"
    })

# Error 1: Monto negativo -> Cuarentena Día 2
pagos_d2.append({
    "tx_id": "TX-999001", "credito_id": creditos_d2[9]["credito_id"],
    "cedula_cliente": creditos_d2[9]["cedula_cliente"], "canal_pago": "Agencia",
    "monto_usd": -250.00, "estado_tx": "CONFIRMADO", "event_version": 1,
    "fecha_evento": "2026-11-24 11:00:00", "updated_at": "2026-11-24 11:00:00"
})

# Error 2: Cédula nula -> Cuarentena Día 2
pagos_d2.append({
    "tx_id": "TX-999002", "credito_id": creditos_d2[14]["credito_id"],
    "cedula_cliente": None, "canal_pago": "ATM",
    "monto_usd": 180.00, "estado_tx": "CONFIRMADO", "event_version": 1,
    "fecha_evento": "2026-11-24 12:30:00", "updated_at": "2026-11-24 12:30:00"
})

# Error 3 (T11): Inconsistencia referencial (crédito válido + cédula válida de OTRA persona) -> Cuarentena Día 2
cedula_ajena = "0911111111" if creditos_d2[9]["cedula_cliente"] != "0911111111" else "0922222222"
pagos_d2.append({
    "tx_id": "TX-999004", "credito_id": creditos_d2[9]["credito_id"],
    "cedula_cliente": cedula_ajena, "canal_pago": "Agencia",
    "monto_usd": 310.00, "estado_tx": "CONFIRMADO", "event_version": 1,
    "fecha_evento": "2026-11-24 13:45:00", "updated_at": "2026-11-24 13:45:00"
})

# COMMAND ----------

# Regla de negocio 1: Pago CONFIRMADO inusual de madrugada en Corresponsal -> Alerta CRÍTICA
pagos_d2.append({
    "tx_id": "TX-999003", "credito_id": creditos_d2[5]["credito_id"],
    "cedula_cliente": creditos_d2[5]["cedula_cliente"], "canal_pago": "Corresponsal de Barrio",
    "monto_usd": 6500.00, "estado_tx": "CONFIRMADO", "event_version": 1,
    "fecha_evento": "2026-11-24 03:12:00", "updated_at": "2026-11-24 03:12:00"
})

# Regla de negocio 2: Pago PENDIENTE alto -> Alerta preventiva (no acreditado aún)
pagos_d2.append({
    "tx_id": "TX-999005", "credito_id": creditos_d2[6]["credito_id"],
    "cedula_cliente": creditos_d2[6]["cedula_cliente"], "canal_pago": "Banca Web",
    "monto_usd": 4800.00, "estado_tx": "PENDIENTE", "event_version": 1,
    "fecha_evento": "2026-11-24 15:10:00", "updated_at": "2026-11-24 15:10:00"
})

with open(f"{BASE_PATH}/pagos/pagos_2026_11_24.json", "w") as f:
    for row in pagos_d2:
        f.write(json.dumps(row) + "\n")

print(f"Lotes generados -> Día 1: {len(creditos_d1)} créditos / {len(pagos_d1)} pagos | Día 2: {len(creditos_d2)} créditos / {len(pagos_d2)} pagos")