# ADARA — Cancelaciones y Devoluciones (sistema v21)

Última actualización: 21 Junio 2026 (**reconciliación de canceladas en el Resultado — P13**: la proyección es insert-only y dejaba canceladas post-proyección como `aprobada` → 21 coladas (~$4,11M ingreso fantasma) + 3 `no_preparado` con CMV mal consumido ($384.062,20, violaba S4). `fn_reconciliar_ml_canceladas` las pasa a `estado='cancelada'` (salen del resultado) y revierte el CMV de las que no salieron; enganchada al hook post-sync (orden proyectar→reconciliar→consumir→congelar). Las despachadas/entregadas quedan con stock pendiente de recepción (GAP). Ver sección "Reconciliación de canceladas en el Resultado (P13)".) · 17 Junio 2026 (**flujo de recepción de 3 destinos + depósito `REAC`**: la recepción de una cancelada/devolución ahora tiene tres opciones — `ok` (sano→vuelve a vendible), `reacondicionar` (volvió no-sano→va a `REAC`, recuperable vía botón "Pasar a venta"), `no_disponible` (no volvió→pérdida). Funciones nuevas `fn_devolucion_a_reacondicionar` (casos A: consumió FIFO; B: cancelada despachada sin consumo) y `fn_reacondicionado_a_venta`. Backend `/ml/recepcion` + `POST /reacondicionar/a-venta`; front botón 🔧 Reacondicionar. Probadas en rollback contra datos reales. **Decisión confirmada**: la rentabilidad se reconoce por **fecha de venta**, no de entrega — se exploró y **descartó** el modelo "consumo por entrega" (mandaría la venta del 31 al mes siguiente). Ver sección "Flujo de recepción de 3 destinos" y `ADARA-DECISIONES.md`.) · 16 Junio 2026 (**regla nueva CANC1**: una venta cancelada **NO es neutra** — ML no reintegra el envío si ya se despachó. En el AS de MP la operación netea $0 (el envío no aparece); el costo del envío vive en la **facturación de ML**, fuente aún no integrada → pendiente cargarla para capturar el costo de envío de canceladas y cargos ML. Canceladas tienen `fecha_cobro` NULL (esperado); el dinero se concilia por el **par cobro↔devolución**, no por fecha de cobro. Abril: 147/166 movieron plata, 139 ya devueltas, **~8 cobradas con reintegro pendiente** (vigilar contra AS de mayo), 19 sin cobro. `GET /venta/:id/detalle` ahora trae movimientos por `mp_payment_id` aunque la venta no esté conciliada. **Clasificación (`cancelled` ≠ producto no salió)**: por `estado_envio`, abril 166 canceladas = **106 no_preparado** (no salió), **12 despachado**, **48 entregado** → 60 despacharon. ML mezcla cancelaciones reales y devoluciones de entregadas. Criterio ADARA: clasificar por `estado_envio` (¿salió?) + `recepcion_*` (¿volvió?), NO por la etiqueta de ML. **Flujo de recepción/stock (existe)**: `/ml/recepcion` `condicion='ok'` → reverso FIFO → vuelve al lote = vendible; `'no_disponible'` → pérdida (reclamable). **GAP**: los botones OK stock / No disp. hoy solo están en la solapa **Devueltas** (por `claim_status`), NO en **Canceladas** → una cancelada despachada/entregada no puede registrar recepción ni vendibilidad. **Conciliar una cancelada**: no hay botón conciliar (es cobro+devolución neto 0, se concilia del lado movimientos); lo que la cierra es la recepción. Pendiente: rutear canceladas despachadas/entregadas al flujo de recepción. Pantalla Ventas ML ahora muestra columna **Estado envío** (se sacó F. cobro) + nro de operación en retenciones. Ver CHANGELOG 16/6 — NOTA: este doc es referencia v21; el estado vigente vive en el schema del rediseño) · 8 Junio 2026 (`/ml/recepcion` ok → reverso FIFO real `fn_revertir_devolucion`, ya no `+1 catalogo_skus`; pata COGS RESUELTA) · 5 Junio 2026 (herramienta de conciliación de devoluciones por `op_id` construida, desplegada y validada; UI en chip Devueltas)

> ⚠️ **REFERENCIA v21 — CONGELADO. No es el estado actual.** Las tablas del v21 se borraron en el reset del 27/05/2026; la DB vive bajo el schema del rediseño (`ADARA-SCHEMA.md`). Este doc se conserva solo como referencia técnica de las integraciones. Estado vigente: `ADARA-DOCS-INDEX.md`.

> 📌 **Doc del sistema v21 en producción.** Documenta la lógica técnica actual de clasificación de cancelaciones/devoluciones usando la API de Mercado Libre (Claims, Shipments).
>
> El rediseño extiende el manejo de devoluciones físicas (ver `ADARA-FLUJO-OPERATIVO.md` sección "Estados físicos") y suma el módulo de reclamos a ML + proveedores (ver `ADARA-RECLAMOS.md`).

---

## Principio general

El sistema debe reflejar la realidad física del depósito. Todo se gestiona desde "Devol. / Cancelac." con tabs Pendientes y Resueltas. Las que generan movimientos financieros se aprueban → pasan a conciliación.

---

## Clasificación por tipo (24 marzo 2026)

| estado_envio | claim_id | Tipo | Badge | Qué pasó |
|---|---|---|---|---|
| no_preparado / preparado | no | Cancelación | ✗ Cancelación | Nunca salió del depósito |
| despachado | no | Entrega fallida | ↩ Entrega fallida | ML no pudo entregar, devuelve paquete |
| entregado | no | Devol. comprador | ↩ Devol. comprador | Comprador devolvió sin claim en API |
| cualquiera | sí | Devolución | ↩ Devolución | Comprador reclamó via Claims API |

---

## Pantalla — Tabs unificados

### Tab Pendientes
Todo sin resolver: cancelaciones + devoluciones (con y sin claim), todos los meses, una sola tabla.
Columnas: Fecha, Período, N° Venta, Producto, SKU, Envío, Importe, Tipo, Motivo/Estado, Acción.

### Tab Resueltas
Todo ya procesado (aprobadas + descartadas + reingresado/perdida). Solo consulta.

### KPIs
Pendientes, Devoluciones, Cancelaciones, En gestión, Recibidas, Resueltas.

---

## Claims API ML

| Endpoint | Uso |
|---|---|
| GET /claims/search?status=opened | Claims abiertos (50) |
| GET /claims/search?resource_id={order_id} | Buscar claim específico para huérfanas |
| GET /claims/{claim_id} | Detalle claim |
| GET /claims/{claim_id}/returns | Detalle return |

Búsqueda: opened global + por resource_id para ventas cancelled+entregado sin claim_id.
No se pisan claim_status manuales (reingresado/perdida).

### Motivos de devolución

| reason_id | name | Triage | ¿Cobran envío? |
|---|---|---|---|
| PDD9939 | repentant_buyer | repentant | No |
| PDD9942 | different_item_other | different | Sí |
| PDD9946 | broken_item | defective | Sí |
| PDD9949 | not_working_item | not_working | Sí |
| PDD9953 | damaged_package_missing_accessories | incomplete | Sí |

### Motivos de entrega fallida (substatus shipment)
returning_to_sender, receiver_absent, receiver_rejected, damaged, lost, stolen, not_delivered, cancelled, out_of_zone, wrong_address. Se capturan en sync y guardan en `motivo_cancelacion`.

---

## Acciones disponibles

### Cancelación (no_preparado / preparado)
- **🗑 Descartar** → aprobada=true, estado_conciliacion=descartada
- **🗑 Descartar todas sin preparar** → masivo

### Devolución / Entrega fallida
- **📦 Recibido** → claim_status=producto_recibido → aparecen botones OK stock / No disp.
- **📞 En gestión** → nota obligatoria → claim_status=en_gestion
- **📝 Nota** → agregar/editar nota a cualquier venta

### En gestión
- **✅ Aprobar** → aprobada=true, periodo_cobro=v.periodo → pasa a Conciliación del mes original
- **📦 Recibido** → si el producto finalmente volvió

### Producto recibido
- **📦 OK stock** → `POST /ml/recepcion condicion=ok` → **reverso FIFO real** (`fn_revertir_devolucion(ml_order_id, hoy)`): devuelve al lote original las unidades consumidas con el costo snapshot (revierte el CMV). Reemplaza el `+1 catalogo_skus` muerto del v21. Fechado a `recepcion_fecha` (P3: imputa al mes del evento).
- **🔧 Reacondicionar** → `POST /ml/recepcion condicion=reacondicionar` → `fn_devolucion_a_reacondicionar(ml_order_id, hoy)`: el producto volvió pero **no sano**. Revierte el CMV (si consumió) y mueve la unidad a un lote en depósito **`REAC`** (no vendible, recuperable). Nota opcional. Ver sección "Flujo de recepción de 3 destinos".
- **❌ No disp.** → `POST /ml/recepcion condicion=no_disponible` → nota obligatoria, **sin reverso de stock ni de CMV** → pérdida.

---

## Notas

Campo `recepcion_nota` en ventas_ml. Se puede agregar desde cualquier fila con botón 📝.
Se muestra: en columna Motivo de Devol/Canc (cyan), en detalle expandido, y en fila expandible de Conciliación.

---

## Campos en ventas_ml

| Campo | Tipo | Descripción |
|---|---|---|
| estado_envio | text | no_preparado / preparado / despachado / entregado |
| estado_cancelacion | text | pendiente / retirar_paquete / esperando_devolucion / etc. |
| claim_id | text | ID del claim en ML |
| claim_status | text | abierto / etiqueta_generada / en_transito / producto_recibido / en_gestion / reingresado / perdida / cerrado |
| motivo_devolucion | text | reason_id (PDD9939, etc.) — de Claims API |
| motivo_cancelacion | text | substatus del shipment o cancel_detail — del sync |
| devuelta | boolean | true cuando hubo reembolso |
| monto_reembolso | numeric | Cuánto se devolvió (informativo, verdad en AS) |
| cargo_envio_devolucion | numeric | Cargo envío retorno (desde AS) |
| fecha_devolucion | date | Fecha del claim |
| recepcion_condicion | text | ok / reacondicionar / no_disponible |
| recepcion_fecha | date | Cuándo llegó al depósito |
| recepcion_nota | text | Nota libre del operador |

---

## Tabla stock_devoluciones

Registra cada producto recibido con condición y nota.
→ Ver ADARA-STOCK.md para detalle.

---

## Impacto contable de la devolución (v22)

Imputación: **mes del movimiento de MP** (débito/crédito), regla P3 (ver `ADARA-DECISIONES.md` / `ADARA-PNL.md`). Tres patas:

1. **Financiero (CC de devoluciones con ML):** suma de los movimientos del AS vinculados a la venta con categoría `devolucion` / `venta_cancelada` / `cargo_envio_devolucion`, con **signo real** (débito < 0 / crédito > 0). Una devolución puede **acreditar** (el comprador devuelve algo distinto → se gana el caso).
2. **COGS / stock:** `recepcion_condicion='ok'` → `consumo_lote` tipo `reverso_devolucion` (el costo vuelve a inventario). `no_disponible` → no hay reverso → **pérdida** (el CMV original, ya cargado en el mes de la venta, no se recupera).
3. **Impuestos (diferido):** la nota de crédito la emite Tango; con la integración TFactura, el reintegro de IVA/IIBB/Ganancias se acredita en los ledgers fiscales (capa 6, A14). Hoy no se asienta — detalle en `ADARA-IMPUESTOS.md`.

**UI (jun 2026):** la herramienta vive **dentro de la pantalla Ventas ML, en el chip "Devueltas"** (no es una solapa aparte — decisión de unificar todo lo de ML en una pantalla, ver `ADARA-FRONTEND.md`). Usa el **mismo navegador Día/Mes** que el resto, filtrando por **fecha de la línea del AS**. La CC de devoluciones es una **vista derivada** (no persiste saldo — CB6/PT5): `v_cc_devoluciones`, una fila **por (venta, mes)** con el impacto financiero (signo real) imputado al mes de cada línea, más `linea_negocio_id` de la venta. Ver definición en `ADARA-SCHEMA.md`.

### Estado actual y cómo se engancha (CONSTRUIDO — jun 2026)

**Hecho:**
- Vista `v_cc_devoluciones` (pata financiera) **rediseñada jun 2026**: una fila **por (venta, mes)** (no más `min(fecha)` por venta), sumando las líneas vinculadas con **signo real** e imputando **cada línea al mes de su propia fecha** (regla dura 2 del P&L). Incluye `linea_negocio_id`. Es la materia prima de la línea "− Devoluciones del mes" del P&L (que aún no existe como pantalla).
- Detección de claims (`/ml/devoluciones`) operativa: pobla `claim_id`/`claim_status` (global, todos los meses; trae claims abiertos + busca por `resource_id` las canceladas enviadas/entregadas).

**Enganche de los movimientos de devolución a su venta — por `op_id` (RESUELTO jun 2026):**

Hay **533 movimientos categoría `devolucion`** en el extracto (al inicio, 0 vinculados → `v_cc_devoluciones` vacía). Se llena a medida que se vinculan.

Una devolución es un **bundle de varias líneas** del AS (dinero retenido, débito por deuda, reintegro de impuestos/comisiones, envío cancelado, liquidación cancelada) **agrupadas por el `op_id`** = campo 2 de `referencia_externa`.

> ⚠️ **Corrección (jun 2026): SÍ hay clave de enganche directa.** El doc previo afirmaba "no hay clave directa" y proponía match por **monto** — **era falso y peligroso** (los montos colisionan: en abril −678.944,02 aparece 3 veces, −300.266,17 dos veces). El `op_id` del bundle **es el id de la liquidación original** y se comparte con el cobro `"Liquidación de dinero · {op_id}"` de la venta. Ese `op_id` = `ventas_ml.mp_payment_id` (o está en `mp_payment_ids`). **Validado:** 189/213 `op_id` matchean directo.

**Match en capas (validado contra los 213 `op_id` reales, jun 2026):**

| Capa | Cómo resuelve | op_ids |
|---|---|---|
| 1 | `op_id == ventas_ml.mp_payment_id` (o en `mp_payment_ids`) | 189 |
| 2 | `op_id == retenciones.mp_source_id` → `COALESCE(retenciones.venta_id, ventas_ml por ml_order_id = retenciones.order_id)` | 16 (9 por `venta_id` + 7 por `order_id`) |
| 3 | `op_id` corto (< 12 díg.) → **agregado ML** (facturas vencidas, reintegros batcheados) → cola "Cargos ML", NO se imputa a venta | 7 |
| 4 | Resto → cola "Revisión" (residual, típicamente venta 2025 pre-snapshot; impacto de caja casi nulo) | 1 |

**212 de 213 resuelven a una venta o son agregados identificados.** Match **siempre por `op_id`, NUNCA por monto**.

**Causa de los "17 residuales" (RESUELTA):** no eran ventas no sincronizadas — el `op_id` de esos bundles es el **source_id de la liquidación**, distinto del `mp_payment_id` de la venta (ej. bundle `153468421205` → venta cuyo payment es `153468123919`). `retenciones` es el puente porque guarda `mp_source_id` + `venta_id`/`order_id`. 16/17 puentearon así; 1 (`150961858416`) no está en `retenciones`, netea $0 → Revisión.

**Endpoints (en `server.js`):**
- `GET /devoluciones/resolver` — agrupa los movimientos `categoria=devolucion` por `op_id`, resuelve la venta por las capas de arriba, y devuelve los bundles clasificados (`pendiente`/`parcial`/`vinculada`/`agregado`/`revision`) con sus líneas. Solo lectura.
- `POST /devoluciones/vincular {op_id, venta_id}` — vincula TODAS las líneas del bundle a la venta (recalcula montos desde `movimientos`, `monto=abs`, upsert idempotente) y marca `devuelta=true, monto_reembolso=abs(neto)` solo si `neto < 0`.
- `GET /venta/:id/detalle` — junta toda la "vida de plata" de una venta (cobro + retenciones + devoluciones) para el modal de detalle por venta.

**Mecanismo:** reusar **vínculos N:N** (`op_tipo='venta_ml'`, `op_id=ventas_ml.id`, `movimiento_id`), igual que la conciliación de cobros. Se **vinculan TODAS** las líneas del bundle de una sola vez, cada una imputada al **mes de su movimiento** (P3). La venta se marca `devuelta` **solo si el neto del bundle < 0** (pérdida real; neto 0 = retención + re-crédito que se cancelan → no se marca). **NO tocar `por_cobrar`.** Regla dura: `ADARA-DECISIONES.md` O10.

> ⚠️ **Guardrail `vinculos.monto`** — va en **magnitud positiva** (`r2(abs(monto))`); `/vincular` y `/vincular-lote` **rechazan `monto ≤ 0`**. El signo real vive en `movimientos.monto`; la CC de devoluciones suma los movimientos (no los vínculos). *(Corrige una versión previa de este doc/handoff que decía "monto con signo real" — habría roto el endpoint.)*

**Vocabulario de líneas del bundle (clasificar cada una):**

| Descripción empieza con… | categoria | signo | Qué es |
|---|---|---|---|
| Liquidación de dinero | cobro_venta | + | Cobro original (neto liquidado) |
| Dinero retenido Reclamos y devoluciones | devolucion | − | MP retiene al abrir el reclamo |
| Débito por deuda Devoluciones y reclamos en Mercado Libre | devolucion | − | El débito real (suele ser el bruto) |
| Devolución de dinero Reclamos y devoluciones | devolucion | + | Re-crédito de la retención |
| Devolución de dinero Reintegro de impuestos y comisiones | devolucion | + | Reintegro de impuestos/comisiones |
| Devolución de dinero Envío cancelado a {nombre} | devolucion | + | Reintegro del envío |
| Liquidación de dinero cancelada Venta cancelada | cobro_venta | + | Liquidación anulada |

El **neto del bundle completo** (todas las categorías, no solo `devolucion`) = impacto real en caja.

**Agregados que NO son devolución de venta (apartar, NO vincular por-venta):**
- `"Débito por deuda Facturas vencidas de Mercado Libre"` → facturación de ML (gasto). **BUG: está mal categorizado como `devolucion` en el clasificador del AS** (debería ser `gasto`/`comision_marketplace`). Ver `ADARA-MOVIMIENTOS.md`.
- `"Devolución de dinero Reintegro de comisiones"` (ids cortos familia `46…`) → reintegros batcheados, sin cobro asociado, no caen a una venta puntual.
- `"Devolución de dinero Reclamos y devoluciones"` sin cobro (ids cortos `30…`) → créditos agregados.

**RESUELTO (8/6/2026) — pata COGS/pérdida:** el puente `ventas_ml ↔ ventas`/`venta_items` existe (proyección por `ml_order_id`, CF1) y el FIFO está activo (CF5). El reverso de COGS lo hace `fn_revertir_devolucion(p_ml_order_id, p_fecha)`: une por `ventas.referencia_externa = ml_order_id` (`canal='ml'`), busca los `consumo_lote` tipo `consumo` de esa venta y crea el `reverso_devolucion` por el neto con el costo snapshot, devolviendo al lote original. Se dispara desde `/ml/recepcion` cuando `condicion='ok'`. `no_disponible` → sin reverso → pérdida.

---

## Flujo de recepción de 3 destinos + depósito REAC (17 jun 2026)

Cuando una cancelada/devolución que salió del depósito vuelve, la recepción (`/ml/recepcion`) tiene **tres destinos** para el stock:

| Condición | Qué pasó | Función | Efecto en stock |
|---|---|---|---|
| `ok` | Volvió sano | `fn_revertir_devolucion` | Reverso FIFO → vuelve al lote original (vendible) |
| `reacondicionar` | Volvió pero no sano | `fn_devolucion_a_reacondicionar` | Revierte CMV (si consumió) y mueve la unidad a un lote en depósito `REAC` |
| `no_disponible` | No volvió (pérdida) | `fn_ajuste_cancelada_no_retornada` | Faltante FIFO (pérdida) |

**`fn_devolucion_a_reacondicionar(p_ml_order_id, p_fecha)` cubre dos casos:**
- **Caso A** — la venta **consumió FIFO** (devolución del circuito costeado): inserta el reverso (revierte el CMV) y transfiere la unidad del lote original a un lote nuevo `REAC` que **hereda `compra_id` y costo** del original (trazabilidad + costo real). Ambos lados quedan con ajustes `compensacion` auditables.
- **Caso B** — cancelada despachada/entregada que **nunca consumió** (fuera del circuito costeado): no hay CMV que revertir; mueve la unidad de stock vendible (FIFO más viejo, excluyendo `REAC`) al lote `REAC`. Sin esto, apretar "reacondicionar" en una cancelada sería un no-op silencioso.

**`fn_reacondicionado_a_venta(p_lote_id, p_unidades, p_deposito, p_fecha)`** — el botón **"↪ Pasar a venta"**. Mueve unidades de un lote `REAC` al depósito de venta (default `DEP`), reusando un lote destino mismo sku/compra/costo si existe. La unidad vuelve a estar disponible/recomprable. `p_unidades` NULL = mueve todo el lote.

**Endpoints:** `POST /ml/recepcion {venta_id, condicion:'reacondicionar', nota?}` y `POST /reacondicionar/a-venta {lote_id, unidades?, deposito?}`. **Front:** botón 🔧 Reacondicionar en el modal de canceladas (entre OK stock y No disp.).

**Idempotencia:** el endpoint bloquea reproceso vía `recepcion_condicion` (409 si ya tiene recepción). El Caso A además es idempotente a nivel función (el reverso lleva el neto consumido a 0).

**Costo del reacondicionado (decisión):** vuelve a su **costo original** (sin desvalorización automática). Si después se descarta → pérdida en ese momento; si se recupera y pasa a venta → stock normal a ese costo.

**Disponible / PSI:** el depósito `REAC` **NO es stock vendible** → se excluye del disponible y de la recompra (PSI). Ver `ADARA-STOCK.md`.

> **Probadas en rollback contra datos reales (17/6/2026):** Caso A (order 2000016926104580): CMV revertido a 0, 2u a REAC, el lote original no recupera la unidad vendible, y el round-trip REAC→venta deja REAC en 0. Caso B (order 2000015608559562, cancelada despachada sin consumo): vendible 2→1, REAC 0→1. Nada persistido (ROLLBACK).

---

## Reconciliación de canceladas en el Resultado (P13) — 21 jun 2026

**Hallazgo.** La proyección (`fn_proyectar_ml`) es **insert-only**: proyecta `paid`/`partially_refunded` con `NOT EXISTS` + solo `INSERT`, y **nunca borra ni actualiza** una orden que pasó a `cancelled` *después* de proyectada. Quedaba como venta `estado='aprobada'` con ingreso + CMV intactos. El filtro `estado in ('aprobada','entregada')` de `v_resultado_mensual` era **no-op** (las 14.833 ventas ML nacen `aprobada`). Se colaron al resultado **21 canceladas** (16 entregado $3.202.322,79 + 3 no_preparado $536.590,32 + 2 despachado $375.090,75 = ~$4,11M de ingreso fantasma). Peor: las **3 `no_preparado` habían consumido CMV ($384.062,20)** de mercadería que **nunca salió** → violaba S4 (stock subvaluado).

**Fix — `fn_reconciliar_ml_canceladas(p_desde default null, p_hasta default null)`** (regla `ADARA-DECISIONES.md` **P13**):
- Para cada orden `ml_status='cancelled'` con venta `estado='aprobada'`: la pasa a **`estado='cancelada'`** → la vista la dropea (sale ingreso **y** su CMV del resultado).
- Si `estado_envio in ('no_preparado','preparado')` (**nunca salió**): además revierte el CMV vía `fn_revertir_devolucion` → las unidades vuelven al stock (S4).
- Si `despachado`/`entregado` (**salió**): **NO** revierte CMV — depende de la recepción (CANC2/S8). El resultado ya queda bien; el stock de esas unidades queda pendiente del flujo de recepción (ver GAP abajo).
- **Idempotente**: solo toca `estado='aprobada'`; una vez flipeada se ignora. El flip va **antes** de consumir (en el hook) para que `fn_consumir_fifo` (filtra `estado in aprobada/entregada`) no la re-consuma y el reverso no se deshaga.

**Hook (`server.js`):** orden **proyectar → reconciliar → consumir → congelar**. Full-scan (sin ventana de fecha) porque una cancelación puede ser de una venta vieja fuera del rango del sync. Toda futura cancelación post-proyección se limpia sola.

**Backfill 21/6/2026 (aplicado, verificado):** 21 canceladas → `cancelada` (0 quedan colándose), 3 `no_preparado` con CMV revertido (consumo 1 + reverso −1 = neto 0, 3 unidades de vuelta al stock).

**GAP que esto deja expuesto (diferido):** las **16 `entregado` + 2 `despachado`** canceladas salieron del depósito y su stock sigue descontado hasta registrar recepción (`ok`→vuelve / `no_disponible`→pérdida). Es el mismo GAP de siempre: el flujo de recepción (`/ml/recepcion` + funciones `fn_revertir_devolucion`/`fn_ajuste_cancelada_no_retornada`) existe, pero los botones solo están en la solapa **Devueltas**, no en **Canceladas**. Pendiente: rutear canceladas despachadas/entregadas al flujo de recepción.

**Lado P&L:** la línea "− Devoluciones del mes" (`v_resultado_mensual.devoluciones`) trata el otro evento (reembolso de venta real, imputado al mes del movimiento, P3). Ver `ADARA-PNL.md`.

---

## Pendientes

1. ✅ Fix balance devoluciones — Resuelto 24 marzo 2026 (ver `ADARA-CHANGELOG.md`)
2. Validación pre-cierre mes
3. Definir cómo conciliar bonificaciones canceladas
4. **(rediseño)** Integrar con módulo de reclamos: cuando `recepcion_condicion = 'no_disponible'`, ofrecer crear reclamo a ML o a Proveedor — ver `ADARA-RECLAMOS.md`
5. **GAP recepción de canceladas (PRIORITARIO, abierto 21/6):** rutear las canceladas `despachado`/`entregado` al flujo de recepción (hoy solo en solapa Devueltas) para cerrar su stock. Tras el fix P13, el resultado ya es correcto; lo que queda es marcar `ok`/`reacondicionar`/`no_disponible` de las 16 entregado + 2 despachado que salieron y se cancelaron.
