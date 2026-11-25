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

#Genera un maestro de 500 créditos activos en Guayaquil
creditos = []
for i in range(1, 501):
    segmento = random.choices(segmentos, weights=[45, 30, 15, 10])[0]
    monto = round(random.uniform(800, 15000) if segmento in ["Consumo", "Microcredito"] else random.uniform(15000, 90000), 2)
    dias_mora = random.choices([0, 15, 35, 65, 95], weights=[68, 12, 10, 6, 4])[0]
    creditos.append({
        "credito_id": f"CR-GYE-{i:05d}",
        "cedula_cliente": f"09{random.randint(10000000, 99999999)}", # Cédula formato Guayas (09...)
        "zona_guayaquil": random.choice(zonas_gye),
        "segmento_credito": segmento,
        "monto_aprobado_usd": monto,
        "saldo_pendiente_usd": round(monto * random.uniform(0.2, 0.95), 2),
        "cuota_mensual_usd": round(monto / random.choice([12, 24, 36, 48]), 2),
        "dias_mora": dias_mora,
        "fecha_desembolso": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d")
    })

with open(f"{BASE_PATH}/creditos/creditos_2026_11_24.json", "w") as f:
    for row in creditos:
        f.write(json.dumps(row) + "\n")


# COMMAND ----------

# Función para generar lotes diarios de pagos
def generar_lote_pagos(fecha_str, num_tx, start_id, inyectar_errores=False):
    pagos = []
    for idx in range(num_tx):
        cr = random.choice(creditos)
        tx_num = start_id + idx
        hora = f"{random.randint(7, 21):02d}:{random.randint(0, 59):02d}:00"
        pagos.append({
            "tx_id": f"TX-{tx_num:06d}",
            "credito_id": cr["credito_id"],
            "cedula_cliente": cr["cedula_cliente"],
            "canal_pago": random.choices(canales, weights=[40, 20, 22, 10, 8])[0],
            "monto_usd": round(cr["cuota_mensual_usd"] * random.uniform(0.85, 1.15), 2),
            "estado_tx": random.choices(["CONFIRMADO", "PENDIENTE"], weights=[92, 8])[0],
            "fecha_evento": f"{fecha_str} {hora}"
        })

    if inyectar_errores:
        #Registro duplicado exacto del lote anterior
        pagos.append({
            "tx_id": "TX-000001", "credito_id": creditos[0]["credito_id"],
            "cedula_cliente": creditos[0]["cedula_cliente"], "canal_pago": "App Movil",
            "monto_usd": creditos[0]["cuota_mensual_usd"], "estado_tx": "CONFIRMADO",
            "fecha_evento": f"{fecha_str} 09:15:00"
        })
        #Monto negativo
        pagos.append({
            "tx_id": "TX-999001", "credito_id": "CR-GYE-00010",
            "cedula_cliente": "0912345678", "canal_pago": "Agencia",
            "monto_usd": -250.00, "estado_tx": "CONFIRMADO",
            "fecha_evento": f"{fecha_str} 11:00:00"
        })
        #Cédula nula o corrupta
        pagos.append({
            "tx_id": "TX-999002", "credito_id": "CR-GYE-00015",
            "cedula_cliente": None, "canal_pago": "ATM",
            "monto_usd": 180.00, "estado_tx": "CONFIRMADO",
            "fecha_evento": f"{fecha_str} 12:30:00"
        })
        #Pago inusual de $6,500 a las 03:12 AM en Corresponsal)
        pagos.append({
            "tx_id": "TX-999003", "credito_id": creditos[5]["credito_id"],
            "cedula_cliente": creditos[5]["cedula_cliente"], "canal_pago": "Corresponsal de Barrio",
            "monto_usd": 6500.00, "estado_tx": "CONFIRMADO",
            "fecha_evento": f"{fecha_str} 03:12:00"
        })

    file_path = f"{BASE_PATH}/pagos/pagos_{fecha_str.replace('-', '_')}.json"
    with open(file_path, "w") as f:
        for row in pagos:
            f.write(json.dumps(row) + "\n")
    print(f"Lote generado: {file_path} ({len(pagos)} registros)")


# COMMAND ----------

generar_lote_pagos("2026-11-23", num_tx=300, start_id=1, inyectar_errores=False)
generar_lote_pagos("2026-11-24", num_tx=350, start_id=301, inyectar_errores=True)