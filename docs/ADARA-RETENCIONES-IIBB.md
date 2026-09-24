# ADARA — Retenciones IIBB y Devoluciones de Envío (Settlement MP)

Última actualización: 10 Agosto 2026 (**BUG CORREGIDO: `iibb_tucuman` en el campo jurisdicción** — Tucumán venía partido en dos filas; backfill + fix en la vista · **brecha de retenciones contra el CM03 202606** (falta SIRCREB) · **$2,87M de saldos a favor confirmados por el papel** · acumulado retenido por jurisdicción. Ver "Actualización — 10 Agosto 2026" al final) · 9 Junio 2026 (**cobertura temporal + por qué faltan ene/feb/mar + regla de las dos fuentes** monto vs jurisdicción — sección nueva al final, duda recurrente resuelta) · 4 Junio 2026 (fix colecta `SETTLEMENT_SHIPPING` implementado en el front; prerequisito RLS off + grant a `anon` documentado)
Estado: **IMPLEMENTADO** (captura + acumulado por provincia). Conciliación RESUELTA (jun 2026): `SETTLEMENT` ya está dentro de `por_cobrar`; `SETTLEMENT_SHIPPING` (envío colecta) se contempla en el front. Ver "Hallazgo clave".

Captura, desde el **settlement de Mercado Pago bajado por API** (sin subir archivos), de las **retenciones de IIBB por jurisdicción** y la base para **devoluciones de envío**, su acumulado fiscal por provincia, y —si hiciera falta— su uso en la conciliación.

---

## Estado por punto del plan

| # | Punto | Estado |
|---|-------|--------|
| 1 | Ingesta del settlement por API, en segundo plano, integrada al flujo (sin botón), idempotente | ✅ hecho (2a manual + 2b background) |
| 2 | Tabla `retenciones` | ✅ hecha (ver `ADARA-SCHEMA.md`) |
| 3 | Retención en la conciliación | ✅ resuelto (jun 2026) — `SETTLEMENT` ya en `por_cobrar` (no se suma); `SETTLEMENT_SHIPPING` (envío colecta) se contempla en el front. Ver "Hallazgo clave" |
| 4 | Acumulado de IIBB por jurisdicción | ✅ hecho (vista `v_retenciones_iibb`) |

## Arquitectura elegida
**Tabla derivada `retenciones`** (no libro espejo del extracto). Captura solo las retenciones impositivas por fila del desagregado, atadas a la venta. Cierra el acumulado por provincia sin duplicar el extracto (que ya vive en `movimientos`).

---

## Fuente del dato: settlement por API (CSV, NO XLSX, NO subir archivos)

> ⚠️ El reporte que entrega la **API** es **CSV separado por `;`** con headers **en inglés** (`SOURCE_ID`, `TAXES_DISAGGREGATED`, `MONEY_RELEASE_DATE`…). NO son las columnas en español del XLSX que se baja a mano desde la web de MP. Son el mismo dato con otros nombres. Los campos JSON (`TAXES_DISAGGREGATED`, `METADATA`) traen `,` adentro → por eso el separador es `;`.

- MP descarga el settlement **por `file_name`** (que viene en el listado), **no por `id`** numérico (el GET por id da **403**). No existe `download_url` ni `status:'ready'`; el estado terminal es **`processed`** y el archivo se baja con `GET /v1/account/settlement_report/{file_name}`.
- Generación lenta del lado de MP (minutos) → ingesta **en segundo plano**, integrada al flujo, **sin botón**, idempotente.

### Columnas clave del CSV
| Columna | Uso |
|---|---|
| `SOURCE_ID` | = `mp_payment_id` → match con `ventas_ml.mp_payment_id` / `mp_payment_ids` |
| `TRANSACTION_TYPE` | tipo de fila (ver abajo) |
| `TAXES_DISAGGREGATED` | **pseudo-JSON sin comillas**: `[{financial_entity:caba,amount:-107.73,detail:tax_withholding_sirtac}, …]` |
| `TAXES_AMOUNT` | suma de las entradas del desagregado de la fila |
| `MONEY_RELEASE_DATE` | fecha del **release** → devengado de la retención (fallback `SETTLEMENT_DATE`) |
| `ORDER_ID`, `PACK_ID`, `SHIPPING_ID` | atan la liquidación de envío a la venta |
| `SALE_DETAIL` | título del producto |

### Transaction types (abril real, 5117 filas)
| type | qué es | retención |
|---|---|---|
| `SETTLEMENT` | venta normal | IIBB **negativa** (original) |
| `REFUND` / `DISPUTE` | devolución / contracargo | IIBB **positiva** = **reverso** (mismo `SOURCE_ID`) |
| `SETTLEMENT_SHIPPING` | **liquidación del envío** (normal/full/colecta) | IIBB por provincia, con `SOURCE_ID` propio, atado por `PACK_ID`/`ORDER_ID` |
| `DISPUTE_SHIPPING` | reverso de envío | a veces IIBB |
| `PAYOUTS` | retiro de plata | `tax_withholding_payout` |
| `CASHBACK` / `CASHBACK_CANCEL` | **bonificaciones Flex** | **solo imp. al cheque, SIN IIBB provincial** → se excluyen |

---

## Clasificación (verificada con el archivo entero — `?taxmap=1`)

Por **`detail` + `financial_entity`** (no alcanza solo `detail`):

| `detail` | `financial_entity` | `tipo` | `jurisdiccion` | acción |
|---|---|---|---|---|
| `tax_withholding_sirtac` | provincia | `iibb_provincial` | provincia | capturar (SIRTAC) |
| `tax_withholding` | `santa_fe` / `corrientes` | `iibb_provincial` | provincia | capturar (**régimen propio**, montos grandes) |
| `tax_withholding_collector` | `iibb_tucuman` | `iibb_tucuman` | — | capturar |
| `tax_withholding_collector` | `debitos_creditos` | `impuesto_cheque` | — | capturar (costo, no IIBB) |
| `tax_withholding_payout` | `tax_withholding_payout` | `payout` | — | capturar |
| `tax_withholding_payer*` | — | — | — | **IGNORAR** (es del comprador) |

> ⚠️ **Corrección 10/8/2026 sobre la fila `iibb_tucuman`.** El `—` de la columna `jurisdiccion` era el bug: la ingesta deja `jurisdiccion` en NULL y la vista terminaba mostrando la jurisdicción literal `'iibb_tucuman'`, partiendo Tucumán en dos. Hoy la jurisdicción correcta de esas filas es **`tucuman`** (backfill + derivación en la vista). Ver "Actualización — 10 Agosto 2026".

- Aparecen las **24 jurisdicciones**. Santa Fe y Corrientes tienen **dos regímenes simultáneos** (SIRTAC + propio); se mantienen separables por `detail`.
- `iibb_tucuman` (collector, casi en todas las filas) ≠ `tucuman` (SIRTAC, por destino) → tipos distintos.
- Bonificaciones Flex (`bonificaciones_flex_fc`, filas `CASHBACK`) traen solo `debitos_creditos`. **Importante:** el sync mete ese cheque dentro del `impuestos` de la venta → por eso aparecen diferencias chicas (2,69 / 5,09) entre `ventas_ml.impuestos` y la suma de `retenciones` de la venta.

---

## Esquema y endpoints (resumen; detalle en SCHEMA y CHANGELOG)

- **Tabla `retenciones`** + **vista `v_retenciones_iibb`** → `ADARA-SCHEMA.md`.
- **`GET /debug/settlement`** (read-only): `?file=` / `?reportId=` / `?list=1` / `?types=1` / `?taxmap=1`. Descarga por `file_name`, separador `;`.
- **`GET /mp/retenciones-sync`** (2a): ingesta de un reporte ya generado. Dry-run por defecto; escribe con `&write=1`. Idempotente. **No toca `por_cobrar`.**
- **Background 2b**: `kickRetenciones()` al final de `/ml/sync` (sin botón, throttle 6 h, ventana mes anterior→hoy, reporte pendiente resumible). Estado: `GET /mp/retenciones-estado`. Forzar/reanudar: `GET /mp/retenciones-refresh`.

## Match settlement → venta
Por `SOURCE_ID` = `mp_payment_id` (o en `mp_payment_ids` para split). Para filas de envío (`SETTLEMENT_SHIPPING`/`DISPUTE_SHIPPING`), por `PACK_ID` (su `SOURCE_ID` es el del pago del envío, no el de la venta). Sin match → la fila igual cuenta para el acumulado provincial (ej. `PAYOUTS`, que nunca se ata a una venta).

> 🔗 **`retenciones` es además el puente de las devoluciones (capa 2 del matcher por `op_id`).** Cuando un bundle de devolución del AS tiene un `op_id` que **no** es el `mp_payment_id` de ninguna venta (porque es el *source_id* de la liquidación, distinto del payment), se resuelve buscando `retenciones.mp_source_id == op_id` y de ahí `COALESCE(venta_id, ventas_ml por ml_order_id = retenciones.order_id)`. Por eso esta tabla es clave para cerrar devoluciones. Ver `ADARA-CANCELACIONES-DEVOLUCIONES.md` › Match en capas.
>
> ⏳ **Backfill pendiente:** hay filas de `retenciones` con `venta_id` NULL que **sí** resuelven por `order_id` (7 detectadas en el set de devoluciones de abril). Conviene rellenar el `venta_id` para que el puente sea directo:
> `UPDATE retenciones r SET venta_id = v.id FROM ventas_ml v WHERE r.order_id = v.ml_order_id AND r.venta_id IS NULL;`

---

## Hallazgo clave — la retención `SETTLEMENT` ya está dentro de `por_cobrar`

> ⚠️ **Corrección (jun 2026):** la versión anterior de esta sección razonaba sobre la fórmula
> `esperado = por_cobrar + sumDev` del motor de conciliación del **server** (`autoConciliarMP` /
> tabla `movimientos_mp`). **Ese motor está MUERTO**: `movimientos_mp` se borró en el reset del
> 27/05/2026 y no se recreó. La conciliación VIVA es el **front** (`ventas-ml.js` + `vinculos`):
> una venta concilia cuando `|Σ cobros − por_cobrar| < 0,02`. Ver `ADARA-VENTAS-ML-V22.md` y
> `ADARA-DECISIONES.md` O7.

Verificado con 30 ventas reales (`paid`):
- `ventas_ml.impuestos` (lo que **ya está dentro de `por_cobrar`**) ≈ la suma de las `retenciones`
  **`SETTLEMENT`** (producto) de la venta. En la mayoría **idéntico**; en algunas el sync tiene un
  poco **de más** (el cheque de Flex); **en ninguna de menos**.
- O sea: **`por_cobrar` ya descuenta la retención `SETTLEMENT` (IIBB producto + cheque).** Sumarla
  a la conciliación **doble-contaría** y rompería las ventas que cierran.

Conclusión: **la retención `SETTLEMENT` (producto) NO se suma a la conciliación** — ya está en
`por_cobrar`. El valor de `retenciones` es el **acumulado fiscal por provincia** (punto 4) y fuente
de verdad/auditoría del desagregado.

### Excepción: `SETTLEMENT_SHIPPING` (envío colecta) — RESUELTO (jun 2026)
La retención de IIBB sobre la **liquidación del envío** (`transaction_type='SETTLEMENT_SHIPPING'`)
**NO** está dentro de `por_cobrar` (el sync arma `por_cobrar` desde el pago del producto, no desde la
liquidación del envío, que tiene `SOURCE_ID` propio). Por eso, en **colecta con envío a cargo del
comprador**, la venta quedaba en "revisar (monto ≠)" por exactamente esa retención (caso testigo $10,77
= $10,46 SIRTAC + $0,31 Tucumán).

Identidad de cierre: **`por_cobrar = cash(producto + envío) + |IIBB envío|`**. El fix vive en el **front**:
se ata la liquidación del envío a la venta vía `retenciones SETTLEMENT_SHIPPING` (puente por `venta_id`
y, si null, por `ml_order_id`/`pack_id`) y el test de conciliable de esas ventas pasa a
`|Σ cobros − (por_cobrar + retEnvio)| < 0,02` (`retEnvio = Σ monto SETTLEMENT_SHIPPING ≤ 0`).
Validado a escala: 58 ventas con retención de envío, **46 cierran exacto**. **Implementado en el front
y desplegado el 4/6/2026.** Detalle:
`ADARA-VENTAS-ML-V22.md` (Pendiente 2 RESUELTO) y `ADARA-DECISIONES.md` O8.

**⚠ Prerequisito de lectura (costó un debug el 4/6):** para que el front arme `SHIP_BY_VENTA`, la tabla
`retenciones` DEBE tener **RLS off** + `grant select` a `anon`. Con RLS on, la REST API devuelve `200`
con `[]` (sin error en consola), `SHIP_BY_VENTA` queda vacío y NINGUNA colecta-envío engancha — el
síntoma es **silencioso**. Pista en Network: el request `retenciones?...` pesa ~0,8 kB en vez de varios kB.

**Resumen de la regla:** `SETTLEMENT` (producto) → dentro de `por_cobrar`, no se suma.
`SETTLEMENT_SHIPPING` (envío) → fuera de `por_cobrar`, se contempla solo en colecta, en el front.
`por_cobrar` **NO** se toca en ningún caso.

---

## Acumulado por jurisdicción (punto 4)

Vista **`v_retenciones_iibb`**: `periodo` × `jurisdiccion` × `detail` × `tipo`, `total_neto`, `movimientos`. Devengado al **release** → las ventas de un mes caen en el `periodo` siguiente (puede haber `periodo` futuro por releases programados). El cálculo de la DDJJ y las alícuotas siguen siendo del contador (LN8).

Carga real al 3/6/2026: abril (10.857 filas) + mayo/junio (11.541). Acumulado neto IIBB ≈ −2,44 M repartido 2026-03 (reversos, +) a 2026-07 (releases futuros).

---

## Cobertura temporal: por qué faltan ene/feb/mar y por qué NO es trabajo manual (9/6/2026)

> Duda recurrente, resuelta acá para no repetirla. La tabla `retenciones` arranca el **10/3/2026** (enero y febrero vacíos; marzo parcial, desde el 10). **No es un bug ni requiere carga manual recurrente.**

**Cómo se puebla hoy (automático, sin subir archivos).** `correrRetenciones()` (disparada por `kickRetenciones()` al final de `/ml/sync`, throttle 6 h) le pide a la API de MP un settlement report del **1° del mes anterior hasta hoy** (`begin_date`/`end_date`), espera a que MP lo genere (estado `processed`), lo descarga por `file_name` y lo ingesta idempotente. De junio en adelante la tabla se mantiene sola; no hay que tocar nada.

**Por qué faltan ene/feb/mar — dos causas que se suman:**
1. **Ventana móvil**: el auto-sync solo pide *mes anterior + mes actual*. Cuando se activó (mayo/junio) nunca llegó a pedir enero/febrero.
2. **Límite de MP**: la API de settlement solo expone **~3 meses hacia atrás**. A junio, ene/feb ya quedaron fuera de ventana → **irrecuperables por API** (tampoco "a mano" desde la web).

**Origen de la carga inicial.** Las filas actuales vinieron de **2 CSV de settlement cargados puntualmente** (arrancan el 10/3); por eso `settlement_file` dice `…manual…`. Esos archivos se subieron al **proyecto Claude** (no a la app) y se ingirieron a la DB en la sesión del 3/6 vía la misma `ingestRetenciones`. Fue carga de **arranque**, no el mecanismo de mantenimiento.

**Consecuencia para Posición Fiscal / P&L — dos fuentes, dos niveles (regla canónica):**
- **Monto total de IIBB retenido por venta** → **`ventas_ml.impuestos`** (la columna IMPUESTOS de Ventas ML). **Completo desde enero**, venta por venta, al centavo, y se mantiene en cada sync. Es la **fuente canónica del monto**.
- **Apertura por jurisdicción** (en qué provincia cayó) → **`retenciones`**. Automática de acá en más; **gap histórico ene/feb** que afecta **solo el desglose, no el monto**.

> **Regla (evita doble conteo):** el **monto** de IIBB retenido sale de `ventas_ml.impuestos`; `retenciones` se usa **solo para abrir ese monto por jurisdicción**, nunca para sumar aparte. El gap de jurisdicción de ene/feb no afecta los totales del período.

> **Si hiciera falta recuperar un rango viejo** (ej. marzo 1–9, o cualquier mes que MP aún exponga): hoy `correrRetenciones` está fijada a "mes anterior → hoy". Para pedir un rango arbitrario habría que extenderla para aceptar `desde`/`hasta` por querystring (mejora menor, no implementada).

---

## ⚠ Guardrails técnicos
- La retención existe **post-release** → la ingesta corre sobre operaciones ya liquidadas.
- **Clasificar por `detail` + `financial_entity`**, no solo por `detail`.
- **No mezclar tipos:** IIBB provincial (pago a cuenta) ≠ imp. al cheque (costo) ≠ payout.
- Ingesta **idempotente** por `UNIQUE (mp_source_id, transaction_type, detail, financial_entity)`; **sin** `report_id` en la clave.
- **`por_cobrar` NO se toca.** La retención `SETTLEMENT` (producto) ya está dentro de `por_cobrar` → no se suma a la conciliación. La `SETTLEMENT_SHIPPING` (envío) NO está en `por_cobrar` → se contempla solo en colecta, en el front. El motor `esperado`/`autoConciliarMP` del server está muerto (`movimientos_mp` no existe) — no aplica.
- **`retenciones` requiere RLS off + `grant select` a `anon` (A6).** Si no, el front recibe `200` con `[]` sin error y la conciliación de colecta-envío queda muda. Verificar con el tamaño del request `retenciones?...` en Network (varios kB, no 0,8 kB). Incidente 4/6/2026.
- Ignorar `tax_withholding_payer*` (comprador) y excluir `CASHBACK`/`CASHBACK_CANCEL`.
- El CSV de la API es `;` y en inglés (no el XLSX en español).
- **La jurisdicción de `tipo='iibb_tucuman'` es `tucuman`, nunca el literal `iibb_tucuman`** (10/8/2026). La vista lo deriva sola, pero la ingesta sigue escribiendo NULL: si se toca `v_retenciones_iibb`, no perder el `CASE`. Si Tucumán vuelve a aparecer en dos filas, es esto.

---

## Actualización — 10 Agosto 2026 (fix de jurisdicción Tucumán + brecha contra el CM03 + saldos a favor)

### 1. BUG CORREGIDO — `iibb_tucuman` aparecía como jurisdicción

Las retenciones del **régimen de recaudación de Tucumán** entran con `tipo='iibb_tucuman'`, `detail='tax_withholding_collector'` y **`jurisdiccion` en NULL**. `v_retenciones_iibb` caía al `COALESCE(jurisdiccion, financial_entity)` y devolvía la jurisdicción literal **`'iibb_tucuman'`**, partiendo Tucumán en **dos filas**:

| fila | monto |
|---|---|
| `tucuman` (SIRTAC) | −$77.495,49 |
| `iibb_tucuman` | −$79.947,96 |

Eso rompía el **cruce contra las percepciones de compra**, que sí se guardan con el código `tucuman` correcto (regla dura 9 de `ADARA-COMPRAS-IMPORTACIONES.md`).

**Fix en dos partes:**
1. **Backfill:** `update retenciones set jurisdiccion='tucuman' where tipo='iibb_tucuman'` → **9.980 filas**.
2. **Vista defensiva:** `v_retenciones_iibb` deriva ahora la jurisdicción con
   `case when tipo='iibb_tucuman' then 'tucuman' else coalesce(jurisdiccion, financial_entity) end`,
   para no depender de que la ingesta lo escriba bien.

Migración: **`fix_jurisdiccion_iibb_tucuman`**. Tucumán quedó unificado en **−$157.443,45**.

> ⏳ **Pendiente: arreglarlo también en la ingesta (`server.js`).** Las filas nuevas van a seguir entrando con `jurisdiccion` en NULL. La vista las tapa, pero la tabla queda sucia.

### 2. BRECHA DE RETENCIONES (hallazgo importante) — falta SIRCREB

Contrastado contra la **DJ Mensual CM03 del anticipo 202606** (junio 2026):

| Fuente | Monto |
|---|---|
| CM03 — "Valores Restan" | **$15.471.267,94** |
| ADARA — ingesta del settlement de MP (`retenciones`) | $1.531.430,08 |
| ADARA — `ventas_ml.impuestos` (API de ML) | $4.346.103,85 |

Falta la mayor parte. Casi seguro es **SIRCREB (recaudaciones bancarias del Supervielle)**, que **no se ingesta en absoluto**.

**Impacto:** no afecta el **margen** (la retención no es gasto, es anticipo), pero **sí el saldo a pagar** de la nueva vista `v_iibb_jurisdiccion_mensual`.

### 3. DOS FUENTES QUE NO COINCIDEN

`ventas_ml.impuestos` (API de ML — lo que usan el **Resultado** y la **Posición Fiscal**) y la tabla `retenciones` (settlement de MP) **miden cosas distintas**. En junio: **$4.346.103,85** contra **$1.531.430,08**. Hay que documentar cuál es la fuente de verdad (ver la regla canónica de la sección "Cobertura temporal", que hoy dice que el **monto** sale de `ventas_ml.impuestos`).

### 4. Retenido acumulado por jurisdicción (a la fecha, ~$4,86M)

| Jurisdicción | Retenido |
|---|---|
| Santa Fe | $1.665.209,43 |
| Buenos Aires | $1.139.392,11 |
| Corrientes | $625.234,80 |
| CABA | $416.196,14 |
| Córdoba | $231.293,62 |
| Tucumán (ya unificado) | $157.443,45 |
| Mendoza | $110.403,04 |
| Neuquén | $75.169,82 |
| Entre Ríos | $75.107,67 |
| Río Negro | $58.961,35 |
| Salta | $51.876,52 |
| Chubut | $35.271,74 |
| San Luis | $30.818,43 |
| Misiones | $30.494,14 |
| Chaco | $26.825,53 |
| Jujuy | $23.410,55 |
| San Juan | $22.421,37 |
| Sgo. del Estero | $18.412,37 |
| Santa Cruz | $17.082,64 |
| La Pampa | $16.236,16 |
| La Rioja | $15.469,83 |
| Catamarca | $13.215,02 |
| Formosa | $5.336,33 |
| Tierra del Fuego | $1.874,51 |

**Dato:** los **regímenes provinciales propios** (`detail='tax_withholding'`, no SIRTAC) explican los saldos a favor — **Santa Fe −$1.469.001,49** y **Corrientes −$568.109,62** vienen casi todo por ahí.

### 5. Saldos a favor confirmados por el CM03: $2.867.481,49 inmovilizados

| Jurisdicción | Saldo a favor |
|---|---|
| Santa Fe | $886.893,95 |
| Santa Cruz | $725.600,97 |
| Corrientes | $707.678,62 |
| Catamarca | $327.197,08 |
| La Pampa | $143.246,24 |
| Río Negro | $54.113,39 |
| Tierra del Fuego | $22.751,24 |
| **Total** | **$2.867.481,49** |

En **Corrientes** retienen **~6x** y en **La Pampa** **~10x** lo determinado del mes.

Se **predijo antes de ver el CM03**, a partir de las retenciones desproporcionadas contra la base de cada provincia, y el papel lo confirmó.

---

## Documentos relacionados
- `ADARA-IIBB-CONVENIO-MULTILATERAL.md` — posición de IIBB por jurisdicción (Convenio Multilateral), cruce retenciones ↔ percepciones, saldos a favor y roadmap
- `ADARA-SCHEMA.md` — tabla `retenciones` + vista `v_retenciones_iibb`
- `ADARA-CHANGELOG.md` — sesión 3 junio 2026
- `ADARA-DECISIONES.md` — LN8 (posición fiscal informativa), P3 (imputación de devoluciones)
- `ADARA-IMPUESTOS.md` — IIBB pago a cuenta, Convenio Multilateral (ahora por jurisdicción desde el settlement)
- `ADARA-VENTAS-ML-V22.md` — colecta envío (doble liquidación) RESUELTO; conciliación viva (front + `vinculos`)
