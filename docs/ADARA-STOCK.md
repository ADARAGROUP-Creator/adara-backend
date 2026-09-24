# ADARA — Stock

Última actualización: 5 Agosto 2026 (**auditoría de lotes + velocidad de rotación**: 2 lotes con `costo_unitario = 0` en la compra inicial #4 (SKU 6 y 19, con stock activo) → al venderse muestran margen 100%; **compras 1 y 3 con componentes de producto pero CERO lotes** (LEANVAL mayo $36,5M / 200u del SKU 139 y BUDDY abril $4,9M): mercadería cargada que nunca entró al stock — junio y julio sí generaron lote. **Rotación medida sobre 1.824 consumos**: la mercadería empieza a salir a los **1-5 días** del alta del lote, 51% se consume en el mismo mes del alta, promedio 24,7 días → **el stock tiene que entrar apenas llega la mercadería aunque falten facturas**, o el PSI queda ciego en los días de mayor rotación. Julio 2026: 4 compras por $104.803.138,21 → 15 lotes. Ver "Actualización 5 Agosto 2026" al final.) · 17 Junio 2026 (**depósito `REAC` (reacondicionar)**: los productos que vuelven no-sanos van a un lote `deposito='REAC'`, fuera del stock vendible/disponible/PSI, conservando costo original; entran con `fn_devolucion_a_reacondicionar` y salen con `fn_reacondicionado_a_venta` (botón "Pasar a venta" → depósito `DEP`). Recepción de canceladas/devoluciones ahora con 3 destinos: ok/reacondicionar/no_disponible. Reglas duras 8 y 9 nuevas. **Confirmado**: stock y CMV se reconocen por **fecha de venta**, no de entrega — modelo "consumo por entrega" evaluado y descartado. Ver `ADARA-CANCELACIONES-DEVOLUCIONES.md` y `ADARA-DECISIONES.md`.) · 16 Junio 2026 (**riesgo de sobrestimación por canceladas despachadas**: `ml_status='cancelled'` NO implica que el producto no salió — abril: de 166 canceladas, 12 `despachado` + 48 `entregado` = **60 despacharon**. Las canceladas se excluyen del circuito costeado (no consumen FIFO), correcto solo si el producto no salió o volvió; si salió y no volvió → **stock sobrestimado**. El retorno a stock se registra por `/ml/recepcion`: `condicion='ok'` → reverso FIFO real al lote original (**vendible**); `'no_disponible'` → sin retorno (**pérdida**, reclamable). **GAP**: ese flujo de recepción hoy solo se ofrece en la solapa **Devueltas** (por `claim_status`), no en **Canceladas** → pendiente rutear canceladas con `estado_envio` despachado/entregado al mismo flujo. Cruzar con `recepcion_fecha/condicion` para detectar stock potencialmente sobrestimado.) · 8 Junio 2026 parte 2 (**seed EJECUTADO**: 137 lotes / 89 SKUs / $60.836.417,17 = Tango; columna `lotes.deposito`; FIFO `cantidad_actual` vivo; valorización con costos de apertura cargados = $313.112.866,62; PSI vivo) · 8 Junio 2026 (seed de stock = foto de hoy desde Tango; PSI lee `Σ lotes.cantidad_actual`; el FIFO mantiene `cantidad_actual` vivo — ver `ADARA-COSTEO-FIFO.md`, CF3/CF4) · 15 Mayo 2026

Cómo se modela y valoriza el stock en ADARA. Cada unidad de cada SKU tiene **trazabilidad por lote** con costo unitario real, y se actualiza en tiempo real con cada venta, compra, devolución y ajuste.
**Reemplaza al viejo `ADARA-STOCK.md`** que solo manejaba cantidades.

---

## Objetivo

Tener la **foto exacta del stock** en cualquier momento, expresada en:

1. **Cantidades** por SKU (físico real)
2. **Disponibles** por SKU (físico menos comprometido)
3. **Valorización** en ARS (cantidad × costo unitario)
4. **Por familia de producto** (electrónica, luminaria, mochila sindical, mochila individual)
5. **Por lote** con costo histórico para trazabilidad y CMV exacto

---

## Conceptos

| Concepto | Definición |
|----------|-----------|
| **SKU** | Identificador de producto (ej: 23B = Redmi Buds 8 Active Black) |
| **Lote** | Conjunto de unidades del mismo SKU que entraron en una misma compra/importación con el mismo costo unitario |
| **Stock físico** | Total de unidades de un SKU que están físicamente en el depósito |
| **Stock comprometido** | Unidades ya vendidas pero todavía no despachadas (ML aprobada sin despachar, Tango facturada sin entregar) |
| **Stock disponible** | Stock físico − comprometido. Lo que se puede vender ahora |
| **Stock valorizado** | Σ(cantidad × costo unitario) por lote, expresado en ARS |
| **FIFO** | Política por defecto: las salidas consumen primero las unidades del lote más viejo |

---

## Modelo: lotes con cantidades y costos

Cada SKU puede tener N lotes activos a la vez. Cada lote mantiene cantidad y costo histórico.

```
SKU 23B (Redmi Buds 8 Active Black)
├── Lote #lote_inicial_23B (31/12/2025): 30u × $4.500  = $135.000   ← seed inicial
├── Lote #022 (15/01/2026):              80u × $4.890  = $391.200
├── Lote #023 (18/03/2026):             190u × $7.552  = $1.434.880   ← con gastos sin factura
└── Lote #024 (10/05/2026):              50u × $6.380  =  $319.000
                                        ───  ──────────  ───────────
                                  Físico: 350u    Valorizado: $2.280.080

Comprometido (ventas no despachadas):  8u
Disponible para venta:                342u
```

Tablas conceptuales:
- `lotes` — id, sku, compra_id (o lote_inicial), cantidad_inicial, cantidad_actual, costo_unitario, fecha_alta, **`deposito`** (nullable; un lote por SKU × depósito — DEP/DJ/MFUL/MENV en el seed)
- `consumo_lote` — venta_id, lote_id, unidades, costo_unitario_al_consumir (snapshot)
- `ajustes_inventario` — id, lote_id, fecha, tipo (faltante / sobrante / compensacion), unidades_delta, costo_delta, motivo

> **Seed de apertura ejecutado 8/6/2026** desde el export de Tango (compra `tipo='inicial'`, lotes fechados hoy). Match por la columna `SKU` del export (la columna `Código` interno de Tango colisiona, NO usar). El stock valorizado por depósito se expone en `v_valorizacion_stock`; los SKUs cuyo costo de apertura todavía está en $0 se listan en `v_skus_sin_costo` (13 pendientes al 8/6). Costo inmutable: nace en la compra→lote, no se edita a mano (CF6).

---

## Operaciones que afectan el stock

### Entradas

| Operación | Efecto en stock | Cómo |
|-----------|-----------------|------|
| Compra/importación recibida | Crea un **lote nuevo** con cantidad y costo unitario | Al cargar la compra y marcar "recibida" |
| Devolución OK (producto vendible) | Suma 1u **al lote del que salió originalmente** | Trazabilidad vía `consumo_lote` de la venta original |
| Ajuste positivo por conteo físico | Suma al lote elegido | Manual, con motivo registrado |

### Salidas

| Operación | Efecto en stock | Cómo |
|-----------|-----------------|------|
| Venta aprobada | Descuenta unidades por **FIFO**: del lote más viejo disponible | Automático al aprobar la venta |
| Cancelación con stock no entregado | **Sin efecto** (la venta nunca descontó porque no se aprobó) | — |
| Devolución no vendible (roto, faltante) | **Sin retorno a stock**, queda como "pérdida" o se reclama a ML | — |
| Ajuste negativo por conteo físico | Resta del lote elegido (típicamente el más viejo) | Manual, con motivo |

### Modificaciones de costo (sin cambio de cantidad)

| Operación | Efecto | Cómo |
|-----------|--------|------|
| Compensación entre lotes | Redistribuye costo entre lotes, **total invertido no cambia** | Pantalla "Compensar lotes" |
| Ajuste de costo por gasto adicional posterior | Eleva el costo de un lote retroactivo (raro) | Manual, con motivo registrado |

---

## Stock físico vs disponible

Para alertas de quiebre y decisión de recompra importa el **disponible**, no el físico.

```
STOCK SKU 23B AL 15/05/2026

  Físico (en depósito)         350u
  − Comprometido               −  8u    (ventas aprobadas sin despachar)
  ─────────────────────────────────
  Disponible para venta        342u
```

### Cómo se calcula el comprometido

Venta aprobada con `estado_despacho` distinto a "despachado" o "entregado". Es decir:
- ML: venta con label generado pero no marcada como entregada
- Tango B2B: venta con factura emitida pero remito pendiente

Cuando la venta queda como "entregada" (o equivalente), el comprometido pasa a ser cantidad neta consumida del lote.

> **El depósito `REAC` (reacondicionar) NO es stock vendible** → se excluye tanto del **físico vendible** como del **disponible** y de la **recompra (PSI)**. El disponible se calcula sobre los lotes con `deposito <> 'REAC'`. Ver sección "Depósito REAC".

---

## Stock valorizado en tiempo real

Para cada SKU, en cualquier momento:

```
valorizado(sku) = Σ ( cantidad_actual_lote × costo_unitario_lote ) por todos los lotes del SKU
```

Para una familia o el total empresa:

```
valorizado_familia = Σ valorizado(sku) por todos los SKUs de la familia
valorizado_total   = Σ valorizado_familia
```

Esta vista es el componente "Stock" del estado patrimonial (`ADARA-PATRIMONIAL.md`).

---

## Vista por familia de producto

| Familia | SKUs | Valorizado | % del stock |
|---------|------|------------|--------------|
| electronica | 124 | $ 8.450.000 | 58% |
| luminaria | 3 | $ 2.100.000 | 14% |
| mochila_sindical | 2 | $ 1.460.000 | 10% |
| mochila_individual | 3 | $ 1.000.000 | 7% |
| (sin clasificar) | 0 | $ 0 | 0% |
| **Total** | **132** | **$ 13.010.000** | 100% |

Nota: la vista es **por familia**, no por línea de negocio. Un SKU de la familia `electronica` puede venderse tanto en "ML Electrónica" como en "Electrónica off-ML" — no se puede dividir su stock físico entre líneas (es el mismo en el depósito). La asignación a línea ocurre en el momento de la venta, no antes.

---

## Sincronización con Tango Factura

**ADARA es master del stock.** Tango mantiene su propio stock para emitir comprobantes correctamente, pero no se sincroniza con ADARA automáticamente.

### Cómo se mantienen coherentes

| Evento | Efecto en Tango | Efecto en ADARA |
|--------|------------------|------------------|
| Venta ML aprobada en ADARA | (sin efecto inmediato) | Descuenta lote FIFO |
| Tango factura la venta ML (vía su API con ML) | Descuenta su stock | (sin efecto, ADARA ya descontó) |
| Venta Tango B2B / TN / WhatsApp en Tango | Descuenta su stock + emite factura/remito | Cuando llega sync Tango: ADARA descuenta lote FIFO |
| Compra/importación recibida | Manual cargas en Tango (factura proveedor) | Manual cargas en ADARA con desglose completo |

### Riesgo y mitigación

**Riesgo:** desfase entre Tango y ADARA mientras una venta no se sincronizó todavía.

**Mitigación:**
1. Sync automático Tango → ADARA cada 30 min (junto con sync ML)
2. Al cargar manualmente una compra/importación en ADARA, recordatorio "cargar también en Tango"
3. Auditoría mensual: comparar stock Tango vs ADARA, conciliar diferencias

---

## Alertas y PSI Recompra

El módulo PSI (Pantalla de Stock Insuficiente / Plan de Stock e Inventario) muestra los SKUs que están por quebrar.

### Cálculo de "días de stock"

```
velocidad_diaria(sku) = ventas_ultimas_4_semanas(sku) / 28
dias_stock(sku) = stock_disponible(sku) / velocidad_diaria(sku)
```

### Códigos de alerta

| Días disponibles | Color | Acción |
|------------------|-------|--------|
| > 30 días | Verde | Sin acción |
| 15 – 30 días | Amarillo | Programar reposición |
| 7 – 15 días | Naranja | Pedir OC urgente |
| < 7 días | Rojo | Stock crítico, riesgo de quiebre |
| 0 | Negro | Quebrado, sin disponible |

Los SKUs en naranja y rojo aparecen destacados en la home. Lista en pantalla PSI con la velocidad de venta, los días restantes y la sugerencia de cantidad a reponer.

---

## Ajustes por inventario físico

Cuando se hace conteo físico (mensual, trimestral, o cuando se detecta una diferencia), se cargan ajustes manuales.

### Pantalla "Ajuste de inventario"

1. Seleccionar SKU
2. Indicar cantidad **real** contada (vs la que aparece en sistema)
3. Indicar motivo: error de despacho, mercadería rota, robo, error de carga, otro
4. Indicar **a qué lote** afecta:
   - Faltante → típicamente al lote más viejo (FIFO inverso de baja)
   - Sobrante → al lote más reciente (o crear un nuevo "lote ajuste")
5. ADARA registra el ajuste en `ajustes_inventario` con timestamp y usuario (cuando se sume autenticación)

### Impacto en P&L

Los faltantes generan pérdida (sale stock valorizado sin venta) → entra al P&L de la línea correspondiente como "pérdida por inventario" (rubro de gastos extraordinarios).

Los sobrantes generan ganancia ("recupero de inventario") — raro, pero pasa cuando hubo error de carga previo.

---

## Depósito REAC (reacondicionar) — 17 jun 2026

Cuando un producto vuelve de una devolución/cancelación **no sano**, no se descarta como pérdida total ni vuelve a stock vendible: va a un depósito aparte **`REAC`** (lote con `deposito='REAC'`), conservando su **costo original** (`compra_id` y costo heredados del lote de origen → trazabilidad). Queda fuera del stock vendible hasta que se recupere.

**Cómo entra a REAC** — desde la recepción de una cancelada/devolución con `condicion='reacondicionar'` (`fn_devolucion_a_reacondicionar`):
- Si la venta consumió FIFO → revierte el CMV y transfiere la unidad del lote original a `REAC`.
- Si es una cancelada despachada que no consumió → mueve la unidad de stock vendible (FIFO más viejo) a `REAC`.

**Cómo sale de REAC** — botón **"↪ Pasar a venta"** (`fn_reacondicionado_a_venta(lote_id, unidades?, deposito?, fecha)` vía `POST /reacondicionar/a-venta`). Mueve unidades de un lote `REAC` al depósito de venta (default **`DEP`**, el depósito general), reusando un lote destino mismo sku/compra/costo si existe. La unidad vuelve a estar disponible/recomprable.

**Valuación (decisión):** el reacondicionado vuelve a su **costo original**, sin desvalorización automática. Descarte posterior = pérdida en ese momento; recupero a venta = stock normal a ese costo. Ver `ADARA-DECISIONES.md`.

Las transferencias REAC↔venta se registran con ajustes `compensacion` (con `lote_contrapartida_id`) en `ajustes_inventario`, auditables. Ver `ADARA-CANCELACIONES-DEVOLUCIONES.md` (flujo de 3 destinos) y `ADARA-SCHEMA.md` (funciones).

---

## Reglas duras

1. **FIFO por defecto**, override solo en casos justificados.
2. **El costo del lote no se modifica** salvo por compensación o ajuste explícito con motivo.
3. **El consumo de la venta queda registrado** en `consumo_lote` para reconstruir el CMV en cualquier momento.
4. **Una devolución vuelve al lote original** cuando se conoce. Si no se conoce, va al lote más reciente con su costo.
5. **Las cancelaciones sin entrega no afectan stock** (la venta nunca descontó).
6. **ADARA es master del stock.** Tango mantiene el suyo pero no es la verdad oficial.
7. **Ajustes por inventario quedan registrados** con motivo y afectan P&L como pérdida o recupero.
8. **El depósito `REAC` no es stock vendible.** Se excluye del disponible y de la recompra (PSI). El reacondicionado conserva su costo original; solo vuelve a vendible vía "Pasar a venta".
9. **El stock se descuenta y el CMV se reconoce en la fecha de la VENTA**, no de la entrega. (Se evaluó y descartó el modelo "consumo por entrega": mandaría una venta de fin de mes al mes siguiente. Ver `ADARA-DECISIONES.md`.)
10. **El stock entra apenas llega la mercadería, aunque falten facturas** (5/8/2026). La rotación medida muestra que la mercadería empieza a salir a los 1-5 días del alta del lote; si el lote se crea recién cuando llega el papeleo, el PSI queda ciego justo en los días de mayor rotación. Ver "Actualización 5 Agosto 2026".

---

## Pendientes y TBD

- **Lotes con `costo_unitario = 0`** (5/8/2026): SKU **6** y **19** en la compra inicial #4. Corregir antes de que se vendan y ensucien el Resultado con margen 100%.
- **Compras 1 y 3 sin lotes** (5/8/2026): LEANVAL (mayo) y BUDDY (abril) tienen componentes de producto pero nunca generaron lote → esa mercadería no está en stock. Decidir si se regeneran los lotes o se documenta como gap de apertura.
- **Clasificación de los 132 SKUs por familia**: tarea inicial manual antes del arranque
- **Costo unitario real al 31/12/2025 de los 132 SKUs**: a cargar por Sebastián
- **Frecuencia de inventario físico**: definir (mensual / trimestral / semestral)
- **Política de SKUs nuevos**: cómo se agregan SKUs nuevos (importación, manual, sync Tango)
- **Sync de stock entre Tango y ADARA** (futuro opcional): si en algún momento Tango ofrece API de stock, se podría hacer reconciliación automática

---

## Documentos relacionados

- `ADARA-FLUJO-OPERATIVO.md` — operaciones diarias que afectan stock
- `ADARA-COMPRAS-IMPORTACIONES.md` — generación de lotes con costos reales
- `ADARA-PNL.md` — uso del CMV exacto desde el consumo de lotes
- `ADARA-PATRIMONIAL.md` — stock valorizado en el balance de la empresa
- `ADARA-TFACTURA.md` — sincronización con Tango
- `ADARA-LINEAS-NEGOCIO.md` — cómo se asignan ventas a línea (familias de SKUs)
- `ADARA-DECISIONES.md` — reglas consolidadas

---

## Costeo y seed de stock (actualización 8/6/2026)

El costeo de ventas (CMV) y la conexión venta↔lote viven en **`ADARA-COSTEO-FIFO.md`** (reglas CF1-CF4 en `ADARA-DECISIONES.md`). Lo que toca a este doc:

- **Seed inicial (CF3):** el stock de apertura se carga desde un **export de Tango** (stock actual valorizado) como lotes fechados hoy bajo una compra `tipo='inicial'`. No se reconstruye el 31/12 ni las compras 2026 (eso requeriría además las ventas off-ML de Tango). El FIFO consume de hoy en adelante; el CMV histórico ene–hoy queda diferido.
- **`cantidad_actual` lo mantiene vivo el FIFO** una vez activa la Fase 2 (`consumo_lote`). Hallazgo (CF4): hasta el 8/6/2026 las ventas ML **no** lo descontaban (`consumo_lote=0`, valor congelado).
- **PSI** deriva el stock de `Σ lotes.cantidad_actual` por SKU; sus columnas Stock/Días/Recompra se activan al cargar lotes.

---

## Actualización — 5 Agosto 2026 (auditoría de lotes + velocidad de rotación)

### 1. Lotes con `costo_unitario = 0` — margen falso del 100%

En la **compra inicial #4** (snapshot del seed del 8/6/2026) quedaron **2 lotes con `costo_unitario = 0`**: **SKU 6** y **SKU 19**, ambos **con stock activo**.

**Consecuencia:** cuando esas unidades se vendan, la venta va a registrar **CMV = 0** → **margen del 100%** (venta sin costo). Ensucia el Resultado y el margen de la línea.

**Acción:** revisar y corregir el costo **antes** de que se consuman. Se relaciona con `v_skus_sin_costo` (13 pendientes al 8/6).

### 2. Compras 1 y 3 — mercadería cargada que nunca entró al stock

Dos compras tienen **componentes de tipo producto pero CERO lotes** generados:

| Compra | Proveedor | Mes | Monto | Detalle |
|---|---|---|---|---|
| #1 | **BUDDY** | Abril 2026 | $4.900.000 aprox. ($4,9M) | Sin lotes |
| #3 | **LEANVAL** | Mayo 2026 | $36.500.000 aprox. ($36,5M) | 200 unidades del **SKU 139**, sin lotes |

Es decir: **la mercadería se cargó en la compra pero nunca entró al stock**. Las compras de **junio y julio sí generaron lote**, así que parece un problema **de las cargas más viejas** (del flujo anterior), no del flujo actual.

**Acción:** decidir si se regeneran los lotes de esas dos compras o se documenta como gap de apertura. Hasta entonces, el stock de esos SKUs está **subestimado** y la valorización también.

### 3. Velocidad de rotación — medida sobre 1.824 consumos

Medición del 5/8 sobre los **1.824 consumos de lotes que tienen compra asociada** (días entre el alta del lote y el consumo):

| Métrica | Valor |
|---|---|
| Primera salida típica | **1 – 5 días** desde el alta del lote (mínimo **0**) |
| Consumos dentro de la **primera semana** | **427** |
| Consumos dentro de los **30 días** | **1.118** |
| Consumos en el **mismo mes** del alta | **933 (51%)** |
| Promedio | **24,7 días** |

Casos concretos: **ELIT** vendió **al día siguiente** del alta del lote; **LEANVAL**, **a los 5 días**.

**Consecuencia de diseño (regla dura 10):** el stock tiene que **entrar apenas llega la mercadería, aunque falten facturas**. Si el lote se crea recién cuando llega el papeleo del proveedor, **el PSI queda ciego justo en los días de mayor rotación** — más de la mitad de las unidades ya se vendieron dentro del mismo mes del alta, y una parte importante dentro de la primera semana.

Impacto cruzado: esto también afecta el **CMV** (una venta sin lote cae en `costo_referencia`, ver `ADARA-COSTEO-FIFO.md`) y la **recompra** (PSI no ve stock que físicamente está).

### 4. Volumen de julio 2026

**4 compras** cargadas por **$104.803.138,21**, que generaron **15 lotes**.
