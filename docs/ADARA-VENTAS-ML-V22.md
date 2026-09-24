# ADARA — Ventas ML + Conciliación (implementación v22)

Última actualización: 13 Julio 2026 (**Conciliación manual bidireccional — tablero mensual**: se reemplazó el flujo automático "Conciliar todas" por una vista de **dos tablas** (Ventas del mes ↔ Extracto completo de MP del mes) con confirmación venta por venta. Novedades: columnas **Entrega** (`fecha_entrega`) + **SKU**; el extracto derecho trae **TODOS** los movimientos de MP del mes (`origen='mp_account_statement'`, no solo cobros); se **incluyen** conciliadas (marcadas), canceladas que salieron y devoluciones, se **excluyen** solo las canceladas sin envío (S4); **selección bidireccional** (clic en venta resalta sus movimientos y viceversa) con sugeridos-primero; **KPIs con progress-rings** (% ventas conciliadas, % movimientos vinculados); **filtro por mes** en ambas tablas; check **"Ocultar conciliadas"** + pendientes-primero para no marear; y el modelo de **período de conciliación diferible** (`ventas_ml.conciliacion_periodo`): botón **"Diferir →"** empuja una venta sin cobro al mes siguiente, **"↩"** la devuelve, y desde un movimiento del extracto cuya venta es de otro mes, **"traer ↰"** la trae al mes en curso. La conciliación es **solo del eje plata** — no toca stock/CMV (que se reconocen a fecha de venta, S9). Ver sección al final.) · 16 Junio 2026 (pantalla Ventas ML: **filtros por columna** + **fila de totales sticky** + columna **Estado envío** (chip no_preparado/despachado/entregado con filtro; **reemplazó a F. cobro**, vacía en canceladas) + **nro de operación** (`mp_source_id`) en cada retención del modal. **Hallazgo canceladas**: `ml_status='cancelled'` NO implica que el producto no salió (abril: 106 no_preparado, 12 despachado, **48 entregado** → 60 despacharon) — clasificar por `estado_envio` + recepción, no por la etiqueta de ML. **BUG RESUELTO — paginación no determinística**: `sbGet` pagina por Range de a 1.000 y ordenar por columnas con empates (`fecha`) saltea filas en el borde → cobros/ventas/vínculos faltantes (síntoma: cobro existente mostrado como "sin cobro", Cobradas=0). Fix: desempate único `id` en todas las cargas >1.000. **DATA: `periodo_cobro` NO confiable** (desfasado por exclusión del upsert) → usar `fecha_cobro`. **`ventas_ml.estado_conciliacion/conciliado/balance_conciliacion` están sin poblar** — el estado vive en `vinculos` (`op_tipo='venta_ml'`, `op_id=ventas_ml.id`). Backend `GET /venta/:id/detalle` ahora trae movimientos por `mp_payment_id` además de los vinculados. Ver CHANGELOG 16/6) · 8 Junio 2026 (`/ml/sync` ahora encadena el costeo `fn_proyectar_ml→fn_consumir_fifo`; `/ml/recepcion` ok → reverso FIFO) · 4 Junio 2026 (fix O8 colecta envío IMPLEMENTADO; RLS de `retenciones`; devoluciones por `op_id` O10; pagos en mediación DIAGNOSTICADO)

> 📌 **Complementa, NO reemplaza, `ADARA-VENTAS-ML.md` (v21).**
> El sync de ventas y las reglas de cálculo de cargos/`por_cobrar` siguen siendo
> los de la v21. Este doc cubre **solo lo nuevo del v22**: la pantalla "Ventas ML"
> y la conciliación de ventas ML enchufada al modelo de `vinculos` del v22.

> ⚠️ **LEER ANTES DE TOCAR (chat nuevo):** buena parte de esta lógica
> (bonificaciones de envío, varios cobros, packs, cálculo de envío Flex vs
> colecta/full) **ya estaba resuelta en la v21**. Antes de reimplementar nada,
> leer `ADARA-VENTAS-ML.md`, `ADARA-FLEX.md` y `ADARA-MOV-SIN-CONCILIAR.md`.
> La v21 es la fuente de verdad de la mecánica; el v22 la porta al modelo nuevo.

---

## Arquitectura de conciliación de ventas ML en v22

Las ventas de ML viven en la tabla **`ventas_ml`** (la misma del sync v21, recreada
tras el reset del 27/05/2026). Se concilian usando el modelo universal de `vinculos`
del v22, con un tipo de operación propio:

- **`vinculos.op_tipo = 'venta_ml'`**, `op_id = ventas_ml.id`.
- Se agregó `'venta_ml'` al CHECK de `vinculos.op_tipo` (ver `migracion_venta_ml.sql`)
  y a la lista `tiposOk` de los endpoints `POST /vincular` y `POST /vincular-lote`.
- No colisiona con `op_tipo='venta'` (reservado para ventas Tango / `v_ventas_ar`).
- `vinculos.monto` = magnitud positiva imputada = el monto de **cada movimiento**
  vinculado (no el `por_cobrar`). Una venta puede tener varios vínculos.

El cobro vive en `movimientos` (`categoria='cobro_venta'`), importado del Account
Statement de MP. La referencia del extracto queda embebida en
`movimientos.referencia_externa` con el formato `fecha|REFERENCE_ID|monto|saldo`,
donde `REFERENCE_ID` es el `mp_payment_id`.

> ⚠️ **Motor de conciliación: SOLO el front.** El motor de conciliación del server v21
> (`autoConciliarMP`, tabla **`movimientos_mp`**, columnas `balance_conciliacion` /
> `estado_conciliacion` / `conciliado` en `ventas_ml`) está **MUERTO**: `movimientos_mp`
> se borró en el reset del 27/05/2026 y no se recreó. Los endpoints `/mp/conciliar`,
> `/mp/recalcular-balance`, `/mp/vincular`, `/mp/desvincular`, `/mp/descartar` apuntan a
> esa tabla inexistente → fallan. La conciliación VIVA es **exclusivamente** esta: front
> `ventas-ml.js` + `vinculos`, vía `POST /vincular` (botón Conciliar). El estado real de
> conciliación se lee de `vinculos` (no de `balance_conciliacion`, que quedó en null).
> **Deuda técnica:** limpiar esos endpoints. (El `POST /vincular-lote` sigue existiendo
> pero ya no se usa desde el front — ver la sección del 13/7.)

---

## Pantalla "Ventas ML" (`public/js/screens/ventas-ml.js`)

Pantalla de control + conciliación. Registrada en `main.js` (`#ventas_ml`) y en el
menú de `index.html`. Tiene **dos vistas** (estado `VISTA`):

- **`control`** (default) — la tabla de control diario/mensual de siempre (columnas,
  filtros por columna, KPIs, totales sticky, modal de detalle, canceladas/devoluciones).
- **`conciliar`** — el tablero de conciliación manual bidireccional (ver sección 13/7).
  Se entra con el botón **"🔗 Conciliar a mano"** y se vuelve con "‹ Volver a la lista".

Notas de la vista de control:
- **Modo Día / Mes** (toggle). En Día navega día por día; en Mes trae todo el mes.
- **Sincronizar ventas**: modal desde/hasta → `POST /ml/sync`. Maneja "ML desconectado"
  con link a `/ml/auth`. **Desde 8/6/2026 el sync también encadena el costeo** (helper `sbRpc`): tras traer ventas/retenciones corre `fn_proyectar_ml → fn_consumir_fifo` (proyección al circuito costeado + consumo FIFO de lotes; CF1/CF5). La devolución recibida OK (`POST /ml/recepcion`) dispara el reverso de stock/CMV (`fn_revertir_devolucion`) — ver `ADARA-CANCELACIONES-DEVOLUCIONES.md`.
- **Cruce venta ↔ cobro por `mp_payment_id`** (`mp_payment_id` + `mp_payment_ids`,
  deduplicado por id de movimiento).

---

## Reglas de conciliación (validadas con datos reales, jun 2026)

1. **Una venta es conciliable cuando la SUMA de todos sus cobros** (los que matchean
   por `mp_payment_id`) **coincide con `por_cobrar`** (tolerancia `abs(dif) < 0.02`),
   ya sea sola o sumándole su bonificación de envío.
2. **Varios cobros (split payment):** una venta pagada con dos medios (transferencia +
   tarjeta, o dos tarjetas) genera dos líneas de "Liquidación de dinero" en el AS, ambas
   con el `mp_payment_id` de la venta. Si juntas dan el `por_cobrar`, se vinculan **todas**.
3. **Bonificación de envío:** ML liquida la bonificación de envío como un **movimiento
   aparte** del pago principal, con **número de operación distinto** (no contiguo) y por
   el **monto neto** (bruto − 0,6% imp. créditos y débitos). En el AS se rotula
   "Bonificación por envío Mercado Envíos". La app la identifica por la palabra
   **"bonific"** en `descripcion`. Si el pago principal no llega al `por_cobrar`, busca
   una bonificación **disponible** (no vinculada) cuyo monto complete la diferencia y
   vincula ambos movimientos. **Las bonificaciones del mismo monto son intercambiables**
   (se reparten una por venta, uso único). Regla v21: "primero el pago, después la
   bonificación de envío".
4. **Lo que NO cierra queda en "revisar"** (no se auto-concilia): varios cobros que no
   suman, o monto que no coincide. (El caso de envío colecta con doble liquidación está
   **resuelto** — ver sección "PENDIENTES → 2. ENVÍO COLECTA — RESUELTO".)
5. **Endpoint de lote** `POST /vincular-lote` (`{ vinculos: [...] }`): inserta muchos
   vínculos en un solo upsert (idempotente por `movimiento_id,op_tipo,op_id`). **Ya no lo
   usa el front** desde el 13/7 (se retiró "Conciliar todas"); sigue disponible en el backend.

### Cálculo de `por_cobrar` y envío (referencia v21 — NO tocar sin leer)

- El `por_cobrar` calculado por el sync es correcto e **incluye bien el envío**. No
  está inflado. (En el análisis de jun 2026 se confirmó que coincide al centavo con el
  "Total recibido" de ML.)
- Envío **Flex**: `cargo_envio = flexBonificacion`; la contribución del comprador
  (`shipping_amount`) se suma **directo a `por_cobrar`** (ML te la transfiere).
- Envío **Colecta/Full**: el costo de envío lo deduce ML directamente. ⚠️ Si el envío es
  **a cargo del comprador**, ML genera en el extracto **dos liquidaciones** (producto + pago
  del envío) con ids distintos; ver Pendiente 2 y `ADARA-DECISIONES.md`.
- Regla completa en `ADARA-VENTAS-ML.md` (reglas duras 1-4) y `ADARA-FLEX.md`.

---

## Fix de zona horaria (sync)

El sync v22 normaliza `date_created` / `money_release_date` a horario
**America/Argentina/Buenos_Aires** mediante un helper `fechaHoraARG(iso)` (usa
`Intl.DateTimeFormat` en-CA). De ahí salen `fecha`, `hora_venta`, `periodo`,
`periodo_cobro`, `fecha_cobro`. Validado contra los reportes de ML (la franja 00:00
ya no se corre de día). Antes el sync guardaba la hora cruda de la API, corrida.

---

## Layout v22 (afecta toda la app)

- **Menú horizontal arriba** (`nav.topnav` en `index.html` + `base.css`), en lugar del
  sidebar lateral fijo. El `<main>` usa todo el ancho. Activo marcado con borde inferior ámbar.
- **Tablas con scroll horizontal de respaldo** (`.table-wrap{overflow-x:auto}` + `min-width:0`
  en `.main`/`.content`).
- **Tabla de Ventas ML responsive**: fuente y padding fluidos con `clamp()`
  (`#vml-tabla`), de ~11px a 16px según el ancho del monitor; montos con `white-space:nowrap`
  (no se parten en dos líneas).

---

## ⚠️ Guardrails técnicos

- El `monto` de cada vínculo es el del **movimiento** que vincula, no el `por_cobrar`.
- Deduplicar cobros por `id` antes de sumar/vincular (evita doble conteo).
- Cada bonificación de envío se usa **una sola vez** (excluir las ya vinculadas).
- No tocar el cálculo de `por_cobrar`/envío del sync sin leer las reglas v21 — está bien.
- Re-sincronizar NO rompe conciliaciones: el upsert es por `ml_order_id` (mantiene el `id`)
  y no toca `vinculos`. Solo auto-descarta cancelaciones sin entrega (`ml_status=cancelled`
  + `estado_envio IN (no_preparado, preparado)`), que nunca tienen cobro.

---

## PENDIENTES (para el próximo chat)

### 1. PACKS (carrito multi-producto) — RESUELTO (no era un problema)

Análisis de jun 2026: **los packs concilian igual que las ventas sueltas.** Cada producto
del pack es una orden con su propio `mp_payment_id`:
- **Colecta:** cada orden matchea 1:1 contra su `por_cobrar` (el envío ya viene deducido).
- **Flex:** liquidación principal + bonificación de envío por el neto (mismo mecanismo que sueltas).

El código NO aparta packs en ningún lado. El síntoma "quedan en revisar" era simplemente
que el período no se había conciliado todavía. Verificado en abril 2026: tasa de
conciliación packs 766/1.172 (65%) ≈ no-packs 1.246/1.962 (64%) → prácticamente idéntica.

> El caso "cecilia thiene" que se creía un "reparto a nivel pack" era en realidad un Flex
> con bonificación de envío grande (en abril hay bonificaciones de 6.451 y 8.439), no un
> reparto entre productos. La sospecha de "no matchea 1:1 por `mp_payment_id`" quedó descartada.

### 2. ENVÍO COLECTA — doble liquidación — RESUELTO (jun 2026)

Las ventas **colecta con envío a cargo del comprador** quedaban en "revisar (monto ≠)" porque
ML genera en el extracto **dos liquidaciones separadas con `SOURCE_ID` distintos** (consecutivos):
una por el **producto** (con el `mp_payment_id` de la venta) y otra por el **pago del envío del
comprador** (con otro `SOURCE_ID`, que MP no asocia a la venta). `matchCobros` solo cruzaba por el
id de la venta → no sumaba la del envío.

**Caso testigo:** orden `2000015791986818` (pack `2000012312060309`), `por_cobrar 31.517,47`
(= Total recibido MP). Producto `152079796875` = **26.287,93** + envío `152079747157` = **5.218,77**.
La diferencia **$10,77** es la **retención de IIBB sobre la liquidación del envío** ($10,46 SIRTAC +
$0,31 Tucumán), capturada en `retenciones` como `transaction_type='SETTLEMENT_SHIPPING'`. **No** está
dentro de `por_cobrar`. Cierra al centavo: `26.287,93 + 5.218,77 + 10,77 = 31.517,47 = por_cobrar`.

**Identidad de cierre:** `por_cobrar = cash(producto + envío) + |IIBB envío|`.

**Fix (en el front `ventas-ml.js`):**
1. `loadVentasML` lee `retenciones` (`transaction_type=SETTLEMENT_SHIPPING`) y arma un índice
   `SHIP_BY_VENTA[venta] = { liqIds:[mp_source_id], retEnvio: Σ monto }`. Atado por `venta_id`
   (resuelto en packs) y, si null, por `ml_order_id`/`pack_id` (colecta suelta — ojo: `ingestRetenciones`
   resuelve `venta_id` por `mp_source_id`/`pack_id`, **no** por `order_id`, así que la colecta sin pack
   queda con `venta_id` null y hay que atarla por `ml_order_id`).
2. `matchCobros(v)` busca, además de los `mp_payment_id`, los `liqIds` del envío en `COBROS_BY_REF`
   (el `mp_source_id` del envío == el id embebido en `referencia_externa` del movimiento del envío).
3. `conciliable`/falta: el test pasa a `|Σ cobros − (por_cobrar + retEnvio)| < 0,02` con
   `retEnvio = Σ monto SETTLEMENT_SHIPPING ≤ 0`. El lote vincula producto + envío (cash); la retención
   **NO** se vincula (no es movimiento; ya vive en `retenciones` / `v_retenciones_iibb`).

**Validado a escala (jun 2026):** 58 ventas con retención de envío → **46 cierran exacto**; 3 sin cash
liquidado aún (esperado, no bug); ~9 residuales acotados (revisión menor, no bloquea).

`por_cobrar` **NO** se toca. La `SETTLEMENT` (producto) ya está dentro de `por_cobrar` (no se suma);
solo la `SETTLEMENT_SHIPPING` (envío) se contempla — **excepción** a la regla general de retenciones.
Regla de negocio: `ADARA-DECISIONES.md` O8. Retenciones: `ADARA-RETENCIONES-IIBB.md`.

> **Estado:** IMPLEMENTADO en `ventas-ml.js` (4 jun 2026) y desplegado. Helpers `shipDe` /
> `retEnvioDe` / `objetivoDe`; el puente se resuelve con PRIORIDAD `venta_id → ml_order_id → pack_id`
> (una sola clave por venta, nunca se suman las tres → no cuenta dos veces en un pack). `matchCobros`
> suma el cobro del envío (su `mp_source_id` ya vive en `COBROS_BY_REF`) y los puntos de cierre
> (`conciliable`, `asignarBonifs`, `conciliar`) usan `objetivoDe(v) = por_cobrar + retEnvio`. Validado
> con `node` y con el testigo `2000015791986818` (cierra al centavo). Cero cambios en `server.js` ni en `por_cobrar`.
>
> **⚠ Prerequisito:** la tabla `retenciones` DEBE tener RLS + policy `authenticated` (post-A17) o
> el front recibe `[]`, `SHIP_BY_VENTA` queda vacío y NINGUNA colecta-envío engancha (síntoma silencioso).

### 3. Devoluciones / cancelaciones / "ML debe y no paga"

El enganche de devoluciones al bundle por **`op_id`** (campo 2 de `referencia_externa` = id de
liquidación, compartido con el cobro original), **nunca por monto** — O10. Cada devolución es un
bundle de varias líneas (neto + envío + reintegro de impuestos/comisiones) que comparten el `op_id`;
se vinculan todas, imputadas al mes de su movimiento. En el tablero de conciliación (13/7) las
canceladas/devoluciones se cierran con **"Conciliar mov."** (endpoint `/venta/:id/conciliar-movimientos`).

### 4. Línea de negocio en ventas ML

Hoy queda `null`; se asigna en el paso de P&L (familia del SKU × canal). Ver
`ADARA-LINEAS-NEGOCIO.md`.

### 5. PAGOS EN MEDIACIÓN — `por_cobrar` inflado (sync) — DIAGNOSTICADO (4 jun 2026)

**Causa raíz confirmada:** mientras el pago está **`payment.status === 'in_mediation'`**, MP devuelve
`charges_details: []` y **sin** `net_received_amount`. El sync no tiene de dónde sacar envío ni impuestos,
así que `por_cobrar` queda en el **provisional** `bruto − comisión`, inflado. Caso testigo
`2000016031933056`: `por_cobrar 404.599,30` vs cobro real +392.470,63. **NO es una devolución.** Fix a
acordar (front estilo O8, o recalcular `por_cobrar` en el sync); **no editar a mano** (el re-sync lo pisa).

---

## Conciliación manual bidireccional — tablero mensual (13 Julio 2026)

Se rediseñó la conciliación de ventas ML: de **"Conciliar todas" automático** a un **tablero
mensual de dos tablas con confirmación a mano**. Decisión de producto de Sebastián: quiere ver
la sugerencia de la IA y confirmar venta por venta, no que vincule solo.

### Estructura de la vista (`renderConciliar` en `ventas-ml.js`, estado `VISTA='conciliar'`)

- **Header con KPIs** — dos **progress-rings** (SVG inline, paleta ADARA): **% ventas conciliadas**
  (verde `#0F6E56`) y **% movimientos MP vinculados** (ámbar `#D97706`) del mes, + tarjeta de
  pendientes. El % va como número (no color solo). Los totales cuentan todo el mes (aunque las
  tablas oculten lo conciliado).
- **Tabla izquierda "Ventas del mes"** — columnas **# Venta · Entrega (`fecha_entrega`) · SKU ·
  Producto · Objetivo · Estado · Acción**. Muestra **todas** las ventas del mes efectivo: pendientes,
  conciliadas (marcadas ✓), **canceladas que salieron** del depósito y **devoluciones**. **Excluye
  solo las canceladas sin envío** (`no_preparado`/`preparado`), que no mueven stock ni plata (S4).
- **Tabla derecha "Extracto de Mercado Pago del mes"** — **TODOS** los movimientos de MP del mes
  (`MOV_MP`, cargado de `movimientos` con `origen='mp_account_statement'`, todas las categorías, no
  solo cobros). Columnas **Fecha · N° op. (`referencia_externa` campo 2) · Descripción · Monto ·
  Estado** (→ #venta si está vinculado / `auto` / `pendiente` / `venta de {mes} · traer ↰` / `sin venta`).

### Selección bidireccional
- Clic en una **venta** (izq.) → sus cobros/movimientos sugeridos (motor `matchCobros` + bonif +
  colecta O8) se **resaltan y suben al tope** de la tabla derecha, con chip "sugerido".
- Clic en un **movimiento** (der.) → selecciona su venta a la izquierda (y la sube al tope).
- La confirmación es venta por venta, reusando el motor existente:
  - **Conciliar** (venta normal que cierra) → `conciliar(ventaId)` (vincula cobros + bonif).
  - **Conciliar mov.** (cancelada/devuelta con movimientos pendientes) → `conciliarMovimientos(ventaId)`
    (bundle por `op_id`, O10).
  - **Deshacer** (conciliada) → `desvincular(ventaId)`.

### Declutter (no marear)
- Lo **pendiente va arriba**, lo **ya conciliado baja al fondo** bajo un separador, y el check
  **"Ocultar conciliadas"** (estado `CONC_HIDE_DONE`) lo saca de la vista. Los KPIs igual lo cuentan.
- En el extracto: **sugeridos → pendientes → ya vinculados/automáticos**, con separadores.

### Filtro por mes + período de conciliación diferible
La conciliación se hace **por mes**. La vista filtra por **período efectivo** de la venta:
`efPeriodo(v) = ventas_ml.conciliacion_periodo || mes de la venta`. Esto permite mover ventas entre
meses (el cruce venta/cobro es inherente: una venta de fin de mes cobra el mes siguiente):
- **"Diferir →"** (venta sin cobro este mes) → `sbPatch ventas_ml.conciliacion_periodo = mes siguiente`;
  la venta desaparece de este mes y aparece en el próximo.
- **"↩"** → devuelve la venta a su mes de venta (`conciliacion_periodo = null`).
- **"traer ↰"** (desde un movimiento del extracto cuya venta es de otro mes) → trae esa venta al mes
  en curso (`conciliacion_periodo = mes actual`) y la deja seleccionada, lista para conciliar. Resuelve
  el caso "cobro este mes de una venta vieja" sin tener que procesar los meses en orden.

> **La conciliación es SOLO el eje plata.** Diferir/traer **no toca stock ni CMV ni P&L ni el mes
> fiscal**: el stock y el CMV se reconocen en la **fecha de la venta** (S9), no del cobro. `conciliacion_periodo`
> es solo un "casillero" de en qué mes trabajás el matching del dinero. Ver `ADARA-DECISIONES.md` (regla O13).

### Cola legítima que NO cierra contra una venta (esperado)
- Cargos de ML (publicidad / "Débito por deuda Facturas vencidas") → son **gastos**, se concilian en
  otra pantalla, no contra una venta.
- Cobros de ventas **anteriores al sistema** (2025) → no existen en ADARA.
El "100% de movimientos" real es el de los movimientos que corresponden a ventas de ADARA.

### Se retiró
- El botón **"Conciliar todas"** y las funciones `conciliarTodas` / `armarLoteConciliacion` (vinculado
  automático en lote). El endpoint `/vincular-lote` sigue en el backend, sin uso desde el front.

### Motor reusado (no se tocó)
`matchCobros`, `objetivoDe`/`retEnvioDe`/`shipDe` (colecta O8), `asignarBonifs`/`buscarBonif`,
`conciliable`, `conciliar`, `conciliarMovimientos`, `desvincular`, `estaConciliada`. Cero cambios en
`server.js` y en el cálculo de `por_cobrar`.

### Requisito de base de datos
Columna nueva **`ventas_ml.conciliacion_periodo text`** (nullable). Migración aplicada por Supabase MCP
(`add_conciliacion_periodo_ventas_ml`). Policy `ALL:authenticated` (A17) cubre el UPDATE del front. Ver
`ADARA-SCHEMA.md`.

---

## Archivos de código de esta tanda (en el repo)

- `public/index.html` — menú arriba (topnav).
- `public/css/base.css` — layout en columna + tabla scroll/responsive.
- `public/js/main.js` — ruta `#ventas_ml`.
- `public/js/screens/ventas-ml.js` — pantalla completa (control + conciliación bidireccional).
- `server.js` — `'venta_ml'` en `/vincular`; endpoint `/vincular-lote` (ya sin uso en el front);
  sync con `fechaHoraARG`; `matchLinea` como stub (la línea se asigna en P&L).
- `migracion_venta_ml.sql` — agrega `'venta_ml'` al CHECK de `vinculos.op_tipo`.
- Migración `add_conciliacion_periodo_ventas_ml` (columna `ventas_ml.conciliacion_periodo`).
