# Guayaquil Banking Lakehouse: Riesgo de Cartera

Pipeline **End-to-End en Databricks (Arquitectura Medallion)** diseñado para resolver desafíos reales de ingeniería de datos en la banca ecuatoriana: consolidación incremental de pagos multicanal, seguimiento histórico de cartera en riesgo (PAR 30) por zonas de Guayaquil, alertas operativas basadas en reglas de negocio y seudonimización de datos personales conforme a la **LOPDP**.

---

## Problema de Negocio

Ante el crecimiento sostenido del crédito de consumo y microcrédito en Guayaquil, las áreas de Riesgo Crediticio, Cobranzas y Operaciones enfrentan tres retos diarios:

1. **Integridad contable en ingesta multicanal:** Los pagos provenientes de App Móvil, Banca Web, Agencias, ATMs y Corresponsales de Barrio pueden llegar duplicados, con montos inválidos, con inconsistencias entre el crédito y el titular, o con actualizaciones tardías de estado (`PENDIENTE` a `CONFIRMADO`).
2. **Visibilidad oportuna del riesgo y la recaudación:** Necesidad de monitorear la evolución diaria de la tasa de morosidad mayor a 30 días (**PAR 30**) por sector de Guayaquil y segmento de crédito, contrastarla contra ventanas claras de recaudación confirmada y alertar operaciones sospechosas que requieran revisión operativa.
3. **Cumplimiento regulatorio y privacidad (LOPDP):** Evitar la exposición de cédulas de identidad en texto plano en las capas analíticas y mantener trazabilidad completa de lotes, rechazos y estados históricos para auditorías de la Superintendencia de Bancos.

---

## Arquitectura y Flujo de Datos

```mermaid
flowchart LR
    subgraph SRC["1. Fuentes / Landing (Unity Catalog Volume)"]
        A1[("Snapshots Créditos<br/>JSON por Fecha de Corte")]
        A2[("Lotes Pagos Multicanal<br/>JSON por Lote Diario")]
    end

    subgraph BRZ["2. Capa Bronze (Delta Lake)"]
        B1["bronze.creditos_raw<br/>Histórico por _batch_date"]
        B2["bronze.pagos_raw<br/>Ingesta incremental (replaceWhere)"]
        B3["bronze.control_lotes<br/>Auditoría de ejecución"]
    end

    subgraph SLV["3. Capa Silver (Calidad + LOPDP + ACID)"]
        S1["silver.creditos_snapshot<br/>Histórico por fecha_corte + HMAC-SHA-256"]
        S2["silver.pagos_limpios<br/>Integridad Referencial + Dedup + MERGE"]
        SQ["silver.quarantine_pagos<br/>Histórico acumulativo de rechazos"]
    end

    subgraph GLD["4. Capa Gold (Liquid Clustering + BI)"]
        G1["gold.kpi_riesgo_cartera_guayaquil<br/>PAR 30 por fecha_corte y Ventanas de Recaudo"]
        G2["gold.kpi_canales_pago<br/>Confirmado vs. Pendiente (Incremental)"]
        G3["gold.alertas_reglas_negocio<br/>Señales Operativas por Estado y Prioridad"]
    end

    A1 --> B1
    A2 --> B2
    B1 --> B3
    B2 --> B3
    B1 --> S1
    B2 -->|Validación sintáctica y referencial| S2
    S1 -.->|Cruce titular-crédito| S2
    B2 -->|Rechazos de calidad o titularidad| SQ
    S1 --> G1
    S2 --> G1
    S2 --> G2
    S1 --> G3
    S2 --> G3
---

## Desarrollo Técnico de Ingenieria de Datos

| Desafío Bancario | Solución Implementada en Databricks | Impacto |
| :--- | :--- | :--- |
| **Idempotencia en reprocesos** | Lectura estricta del archivo correspondiente a fecha_proceso, escritura atómica por lote con replaceWhere sobre _batch_date y registro en bronze.control_lotes | Evita reprocesar archivos históricos en cada corrida y mantiene trazabilidad auditable de los lotes ingeridos. |
| **Idempotencia y actualizaciones tardías** | Deduplicación determinística con ROW_NUMBER() (ordenada por event_version, updated_at, prioridad de estado y reglas explícitas de desempate) seguida de MERGE INTO completo sobre tx_id. | Absorbe cambios tardíos de estado o correcciones de atributos y garantiza cero duplicados ante reintentos del Job. |
| **Calidad e integridad** | Validaciones sintácticas y cruce contra silver.creditos_snapshot para verificar que el titular corresponda al credito_id. Los registros inválidos se almacenan de forma acumulativa en silver.quarantine_pagos con archivo de origen, lote y motivo_rechazo. | Impide atribuir pagos a clientes equivocados y conserva el historial de rechazos para investigación sin detener el cierre diario. |
| **Privacidad y seudonimización (LOPDP)** | Seudonimización de cédulas con HMAC-SHA-256 utilizando clave gestionada en Databricks Secrets (con parámetro de respaldo para desarrollo), enmascaramiento parcial (******5678) y eliminación de la cédula plana desde Silver. | Permite cruces determinísticos y consumo analítico sin exponer identificaciones en texto plano, complementable con RBAC en Unity Catalog. |
| **Rigor temporal en KPIs y alertas** | Snapshots históricos de PAR 30 por fecha_corte (carga incremental por fecha), métricas separadas de recaudación (día de corte, ventana parametrizable de 7 días y acumulada) y alertas basadas en reglas de negocio diferenciando transacciones confirmadas y pendientes. | Habilita el análisis de evolución diaria de la morosidad y entrega señales claras y auditables para revisión operativa. |
| **Optimización y gobernanza** | Liquid Clustering (CLUSTER BY), compactación con OPTIMIZE, Unity Catalog (modelo de 3 niveles y Volumes) y Delta Time Travel. | Evita el sobreparticionamiento y el problema de small files, asegurando linaje automático y auditoría histórica. |

---

## Estrategia de Datos

Debido al sigilo bancario y la LOPDP, el pipeline utiliza un generador determinístico (nb_generador_datos_guayaquil.py) calibrado con los segmentos oficiales del sistema financiero ecuatoriano (Consumo, Microcrédito, Productivo y Vivienda, en USD) y sectores reales de Guayaquil en dos fechas de corte consecutivas (2026-11-23 y 2026-11-24), donde los pagos confirmados del primer día inciden directamente en la evolución del saldo y los días de mora del día siguiente.

Para comprobar la resiliencia del flujo, los lotes incluyen escenarios controlados:

 - Actualización tardía de estado y corrección de canal en una transacción entre dos días (PENDIENTE versión 1 a CONFIRMADO versión 2).

 - Registros duplicados con empate exacto de marcas de tiempo para validar el desempate determinístico.

 - Errores de calidad (montos negativos, cédulas nulas) y una inconsistencia referencial (pago con crédito válido y cédula válida perteneciente a otro cliente) distribuidos en ambos días para comprobar la cuarentena histórica.

 - Transacciones de alto monto en horario de madrugada y operaciones pendientes de liquidación para validar la priorización de alertas operativas.

El volumen del conjunto sintético (500 créditos y 660 pagos en dos cortes diarios) está dimensionado para validar corrección funcional, integridad referencial, eventos tardíos e idempotencia de extremo a extremo. No pretende simular un benchmark de gran volumen, aunque los patrones implementados (carga incremental con replaceWhere/MERGE, Liquid Clustering y OPTIMIZE) responden al diseño requerido para escalar en entornos productivos.

---

## Verificación de Ejecución en Databricks

### 1. Orquestación End-to-End (Databricks Jobs DAG)
![Databricks Job DAG](docs/job_dag.png)

### 2. Linaje Automático en Unity Catalog
![Unity Catalog Lineage](docs/unity_catalog_lineage.png)

### 3. Dashboard de Riesgo de Cartera
![Dashboard Gold](docs/Dashboard_gold.png)

---

## Reproduce el Proyecto en tu Workspace de Databricks

1. Importar los notebooks de la carpeta `notebooks/` en un workspace de **Databricks Free Edition**.
2. Ejecutar la creación de schemas (`bronze`, `silver`, `gold`) y el Volume `landing_zone` dentro del catálogo `bank`.
3. Ejecutar nb_generador_datos_guayaquil.py para generar los lotes diarios de créditos y pagos (2026-11-23 y 2026-11-24) en el Volume de aterrizaje.
4. Configurar un Databricks Job encadenando los notebooks de Bronze, Silver y Gold con el parámetro global fecha_proceso (y opcionalmente ventana_dias = 7). Ejecutar el Job primero con fecha_proceso = 2026-11-23 y luego con fecha_proceso = 2026-11-24
5. Opcionalmente, re-ejecutar cualquiera de las fechas
