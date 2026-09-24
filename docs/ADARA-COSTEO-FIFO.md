# ADARA — Costeo de Ventas (Proyección ML + FIFO)

Última actualización: 7 Agosto 2026 (**dos precisiones sobre `fn_consumir_fifo` que costaron un bug cada una**: es **canal-agnóstica** — costea cualquier venta con `estado in ('aprobada','entregada')`, venga de ML o de una carga manual — y su **`unidades_faltantes` es GLOBAL por rango de fechas**, no por venta. Además **CF14**: la `fecha_alta` del lote define qué ventas puede costear hacia atrás. **Foto de julio: el 86,7 % del CMV es estimado.** Ver "Actualización 7 Agosto 2026" al final.) · 5 Agosto 2026 (**regla del costo tardío + re-costeo**: `consumo_lote.costo_unitario_al_consumir` **congela** el CMV al consumir; el costo tardío se reparte sobre **TODAS** las unidades del lote y se re-costea lo ya vendido. Medición: el 51 % de los consumos ocurre en el mismo mes del alta del lote.) · 21 Junio 2026 (**reconciliación de canceladas en el hook**: orden `proyectar→reconciliar→consumir→congelar`, P13) · 16 Junio 2026 (re-sync de un mes incompleto: **idempotente en costeo**, **aditivo en ventas**) · 10 Junio 2026 (hook de congelamiento enganchado al `/ml/sync`; cobertura CMV 100 %) · 9 Junio 2026 (CMV estimado a costo actual; `skus.costo_referencia`; **congelamiento del CMV**) · 8 Junio 2026 (huérfanos resueltos vía `sku_map`/`combo_map`, CF8)

Cómo ADARA conecta las **ventas** con el **stock por lotes** para calcular el **CMV**, base del P&L.

> **Lectura obligatoria** antes de tocar `ventas`, `venta_items`, `consumo_lote`, `lotes` o el sync de costeo. Reglas duras en `ADARA-DECISIONES.md` (CF1–CF14).

> ✅ **Circuito completo desde el 8/6/2026.** Seed ejecutado (137 lotes / 89 SKUs / $60.836.417,17 = Tango). `fn_consumir_fifo` + `fn_revertir_devolucion` + hook al `/ml/sync`. **`compras.linea_id` ES NULLABLE** (el seed va con `linea_id=NULL`).

---

## 0. El problema que originó esto (hallazgo 8/6/2026)

`ventas_ml` (14.741 filas) y el circuito costeado `ventas`/`venta_items` vivían **separados**, y el costeado estaba **vacío**. `consumo_lote` = **0 filas**: **las ventas de ML nunca consumieron stock**. Evidencia: SKU 300 con `cantidad_inicial=9 = cantidad_actual=9` pese a 17 ventas.

La doc daba por hecho que "venta ML descuenta FIFO" — en los datos **no estaba implementado**.

---

## 1. Decisión de arquitectura

**Proyectar las ventas de ML al circuito costeado** (`ventas` + `venta_items`) y de ahí **consumir lotes por FIFO** (`consumo_lote`).

`consumo_lote.venta_item_id` es **NOT NULL** → no se puede consumir un lote sin un `venta_item`. La proyección es prerrequisito del FIFO.

---

## 2. Proyección `ventas_ml → ventas + venta_items` (CF1)

`ml_order_id` es único por fila → relación 1:1. No se agrupa por pack.

| Origen `ventas_ml` | Destino |
|---|---|
| `ml_order_id` | `ventas.referencia_externa` (llave de idempotencia) |
| `fecha` | `ventas.fecha` |
| `ml_status` | `ventas.estado`: `paid`/`partially_refunded` → `aprobada`; `cancelled` → **se omite** |
| — | `ventas.canal = 'ml'` (**FK a `canales`**) |
| `sku` (= `skus.codigo`) | `venta_items.sku_id` |
| `skus.familia` + `'ml'` → `lineas_negocio_reglas` | `ventas.linea_id` (**automático**) |
| `cantidad` | `venta_items.cantidad` |
| `importe_bruto`, `skus.alicuota_iva` | `venta_items.precio_unitario_neto` |

### Modelo neto-driven (CF2, CRÍTICO)

La **fuente de verdad es `precio_unitario_neto`**. Son **columnas GENERADAS** (no se insertan): `iva_unitario`, `precio_unitario_bruto`, `neto_linea`, `iva_linea`, `bruto_linea`; en `ventas`, `periodo` y `es_gravada`.

```
precio_unitario_neto = importe_bruto / (1 + alicuota_iva) / cantidad   ← SIN redondear al guardar
```

Hay **±1 centavo** en ~13 % de las líneas: es inherente a todo esquema neto+IVA y **se acepta**.

### Idempotencia

`ux_ventas_canal_refext` = `UNIQUE (canal, referencia_externa) WHERE referencia_externa IS NOT NULL`.

> ⚠️ Este índice es también el motivo por el cual la **referencia de una venta en efectivo lleva prefijo** (`EFVO-000123`) y nunca es numérica pelada: `v_resultado_mensual` hace `LEFT JOIN ventas_ml ON vm.ml_order_id = b.referencia_externa`, y una colisión arrastraría comisiones de ML a una venta que no es de ML.

### Resultado de la corrida (2026)

**13.918 ventas proyectadas.** Revenue ML 2026: neto $1.133.687.557,72 · IVA débito $148.661.718,41 · bruto $1.282.349.276,13.

**14 excepciones resueltas (CF8):** la equivalencia vive en la **proyección**, no en la tabla (el sync pisa `ventas_ml.sku`). Tablas `sku_map` (alias 1:1) y `combo_map` (un código = N SKUs); resolución **combo → directo → alias código → alias título**. Mapeos: `97→98`, `95→PA001N`, foco→`401`, combo `86+ac001` → reloj `86` (principal) + film `AC001` (regalo: `neto_factor=0`, $0 de venta pero descuenta stock).

---

## 3. `fn_consumir_fifo(p_desde, p_hasta)` — la mecánica

Por cada `venta_item` de una venta con `estado in ('aprobada','entregada')` en el rango:

- Consume lotes del mismo `sku_id` con `cantidad_actual > 0` y **`fecha_alta <= venta.fecha`**, en orden `fecha_alta ASC, id ASC`.
- Crea `consumo_lote` (`tipo='consumo'`, `costo_unitario_al_consumir` = **snapshot** del lote) y baja `lotes.cantidad_actual`.
- **Quiebre**: `cantidad_actual >= 0` por CHECK → consume lo disponible y **marca faltante**, nunca negativo.
- **Idempotente** por `venta_item` (descuenta lo ya consumido).

Ejemplo: compra el 3/5 a $10 (lote A) y el 20/5 a $11 (lote B). Ventas del 3 al 19/5 → solo existe A → $10. Desde el 20/5 → siguen tomando $10 mientras quede A; al agotarse, pasan a $11.

### ⚠️ Dos cosas que no son obvias (7/8/2026)

**Es canal-agnóstica.** No filtra por `canal`: costea **cualquier** venta con estado válido, venga de la proyección de ML o de una carga manual. Por eso la venta en efectivo (`canal='efectivo'`) se costea sola, **sin tocar la función**. Ver `ADARA-VENTAS-EFECTIVO.md`.

**Su `unidades_faltantes` es GLOBAL por rango de fechas, no por venta.** Llamarla con `(fecha, fecha)` procesa **todas** las ventas de ese día. Si se la usa dentro del alta de una venta puntual para avisar "no hay stock", el número devuelto incluye faltantes de **otras** ventas. Para avisar sobre una venta concreta hay que **consultar `consumo_lote` de sus ítems** y comparar contra la cantidad vendida. (Bug detectado en la prueba de punta a punta del endpoint de venta en efectivo, antes de desplegar.)

---

## 4. Reversas (CF5)

Cancelación o devolución vendible posterior al consumo → `consumo_lote` **negativo** (`tipo='reverso_cancelacion'` / `'reverso_devolucion'`, `unidades<0`, lo exige un CHECK) que devuelve unidades **al lote original** con el **costo snapshot**. Las devoluciones no vendibles **no** retornan a stock.

`fn_revertir_devolucion(ml_order_id, fecha)` se dispara desde `/ml/recepcion` con `condicion='ok'`.

---

## 5. Hook al `/ml/sync`

Orden garantizado: **`fn_proyectar_ml` → `fn_reconciliar_ml_canceladas` → `fn_consumir_fifo` → `fn_congelar_cmv_estimado`**.

Corre dentro de un `try` que **no bloquea el sync**. La respuesta trae `costeo.{canceladas_reconciliadas, cmv_congelados}`.

> **`reconciliar` va ANTES de `consumir`** (P13): pasa a `estado='cancelada'` las órdenes canceladas después de proyectadas y revierte el CMV de las que nunca salieron. Como `fn_consumir_fifo` filtra por estado, una vez flipeada **ya no se re-consume**. Si corriera después, el FIFO podría re-crear el consumo recién revertido.

---

## 6. Constraints del schema relevantes

- **Generadas** (no insertar): `ventas.{periodo, es_gravada}`; `venta_items.{iva_unitario, precio_unitario_bruto, neto_linea, iva_linea, bruto_linea}`.
- `ventas.canal` → **FK a `canales`** (valores: `ml`, `b2b`, `tienda_nube`, `whatsapp_efectivo`, **`efectivo`**).
- `ventas.estado` ∈ {pendiente, aprobada, entregada, cancelada}.
- `ventas.tipo_comprobante` ∈ {factura_a, factura_b, factura_c, remito, **sin_comprobante**}.
- `venta_items.alicuota_iva` ∈ [0,1] (**fracción**: 0.21 / 0.105 / **0**). `cantidad>0`. `precio_unitario_neto>=0`.
- `consumo_lote.tipo` ∈ {consumo, reverso_cancelacion, reverso_devolucion}; CHECK liga signo.
- `lotes.cantidad_actual>=0`. `lotes.compra_id` NOT NULL.
- `compras.tipo` ∈ {local, importacion, inicial}. **`compras.linea_id` ES NULLABLE.**
- **No hay triggers** sobre `lotes`, `compras` ni `compra_componentes`: el costo del lote lo calcula el **backend una sola vez**, al dar de alta la compra.

---

## 7. CMV estimado + congelamiento (CF9, CF10)

### El problema
El seed es la foto del **8/6/2026**: todas las ventas anteriores quedaban con **CMV = 0** (no había lote que consumir). El costeo histórico exacto es inviable.

### CMV estimado a costo actual
Las ventas históricas sin FIFO real se costean con el **costo actual del SKU** (`cantidad × costo_unit`), etiquetado **estimado**. Las nuevas siguen por FIFO real.

### `skus.costo_referencia` — fallback para SKUs sin lote
`v_costo_sku_actual = COALESCE(promedio ponderado de lotes con costo>0, costo_referencia)`. El **lote real siempre tiene prioridad**: el día que se carga la compra, el lote pisa la referencia solo, sin tocar nada más.

> **Caso Targus (SKU 153):** tenía lotes del seed con `costo_unitario = 0`. Las ventas históricas se congelaron bien con `costo_referencia`, pero las **futuras** iban a consumir esos lotes a $0 (el FIFO real tiene prioridad sobre el congelado). Fix: bajar el costo real **al lote**.
>
> **Regla general:** un SKU con `costo_referencia` pero lotes en $0 necesita que el costo baje **también al lote**, o sus ventas futuras costean a 0. **Pendiente vigente:** lotes de la compra inicial #4 (SKU 6 y 19) con stock activo y costo 0.

### Congelamiento — el CMV, una vez calculado, queda FIJO
Tabla **`cmv_estimado_congelado`** + **`fn_congelar_cmv_estimado()`**, idempotente (`ON CONFLICT (venta_item_id) DO NOTHING` → **nunca re-pisa**). `origen` = `'lote_promedio'` o `'costo_referencia'`.

`v_resultado_mensual` lee con prioridad: **FIFO real → congelado → dinámico residual**.

---

## 8. Actualización — 5 Agosto 2026 (regla del costo tardío y re-costeo)

### Hecho técnico central

`consumo_lote.costo_unitario_al_consumir` **congela el CMV en el momento del consumo**. Por lo tanto:

> **Actualizar `lotes.costo_unitario` NO reescribe el pasado: solo afecta consumos futuros.**

Eso hace **viable ajustar costos tardíos sin tocar meses cerrados** — y también obliga a un paso explícito si se **quiere** mover el CMV ya consumido.

### La regla (CF13)

El neto del costo tardío **se reparte sobre TODAS las unidades del lote, no sobre las que quedan.**

```
Ejemplo: 10 unidades a $100.000 + flete tardío de $50.000

  Repartido sobre el REMANENTE (1 unidad):  esa unidad queda a $150.000  ✗
  Repartido sobre TODAS (10 unidades):      +$5.000 c/u → $105.000        ✓
```

| Unidades | Qué se toca |
|---|---|
| **En stock** | Sube `lotes.costo_unitario` |
| **Ya vendidas** | Se **re-costean**: `consumo_lote.costo_unitario_al_consumir` → mueve el CMV de esos períodos |

**Se re-costean TODOS los renglones del lote, incluidos los `reverso_devolucion`.** Si una unidad volvió, su reverso tiene que usar el mismo costo que su consumo o el CMV neto queda descuadrado.

### Medición que respalda la regla (1.824 consumos)

La mercadería **empieza a salir a los 1-5 días** del alta del lote (mínimo **0 días**). 427 consumos en la primera semana · 1.118 dentro de los 30 días · **933 (51 %) en el mismo mes en que entró el lote** · promedio 24,7 días.

**Conclusión:** el corte por mes abierto resuelve la mayoría de los casos sin partir el costo entre períodos.

### Caso verificado — compra #6 (LEANVAL, SKU 302)

```
Lote: 200 unidades a $247.106,61 · Ya vendidas: 21 u, CMV $5.189.238,81
Costo tardío de $500.000 → $2.500 por unidad

  Costo unitario nuevo:     $249.606,61
  CMV de las 21 vendidas:   + $ 52.500
  Valor del stock (179 u):  + $447.500
  ─────────────────────────────────────
  Total                       $500.000   ✓ cierra exacto
```

---

## 9. Actualización — 7 Agosto 2026 (CF14 + la foto del CMV de julio)

### CF14 — la `fecha_alta` del lote define qué ventas puede costear hacia atrás

`fn_consumir_fifo` sólo consume lotes con **`fecha_alta <= fecha de la venta`**. Si el lote se carga con la fecha de la **factura** en vez de la de **llegada al depósito** (FO5), las ventas anteriores quedan **sin costo real aunque hoy haya stock disponible**.

Caso detectado: `302` Televisor Enova 43" — los lotes viejos (8/6) se agotaron y el lote nuevo de 200 unidades entró con `fecha_alta = 23/07`. Las ventas de julio anteriores al 23 no lo pueden tocar: quedan 164 unidades en stock y 26 ventas sin costear.

**Implicancia para la Fase 2 del simulador:** la fecha de ingreso al depósito que se pida al confirmar un despacho **no es un dato administrativo** — define cuántas ventas del pasado se recuperan. Ver `ADARA-IMPORTACIONES-SIM.md`.

### Foto del CMV de julio 2026 — el 86,7 % es estimado

| | |
|---|---|
| Ventas ML de julio | 2.095 |
| **Sin costo real** | **1.273 (61 %)** |
| CMV total | $343.862.983,22 |
| CMV real | $45.763.559,75 |
| **CMV estimado** | **$298.099.423,47 (86,7 %)** |

El margen de julio ($171.126.352,44) está construido casi enteramente sobre costos estimados. Tres causas, con acción distinta cada una:

| Caso | SKUs | Ítems | CMV estimado | Qué lo destraba |
|---|---|---|---|---|
| **A — sin ninguna compra cargada** | 18 | 353 | **$133.755.790,72** | Los despachos de importación sin confirmar (Fase 2). Tienen `costo_referencia` de las simulaciones pero **cero lotes** |
| **B — lotes agotados** | 16 | 861 | **$156.335.715,26** | Compras de reposición. El peor: SKU `178` (Tablet Redmi Pad 2 8/256 Gris) con **320 unidades** sin costear = $98,4M, un tercio del problema |
| **C — hay stock, lote posterior a la venta** | 5 | 59 | **$8.007.917,49** | **CF14** — corregir la `fecha_alta` si la mercadería entró antes de lo que dice el lote |

**C es el único que puede ser un error de carga** y no una falta de datos: se arregla verificando la fecha real de entrada al depósito y re-corriendo el FIFO.

### Cómo diagnosticar esto de nuevo

La consulta que separa los tres casos: por cada `venta_item` del período sin `consumo_lote` de tipo `consumo`, contar los lotes del SKU y su `cantidad_actual`.

```
nlotes = 0            → caso A (nunca entró una compra)
nlotes > 0, stock = 0 → caso B (se agotó)
nlotes > 0, stock > 0 → caso C (fecha del lote posterior a la venta)
```

Sirve para cualquier mes y es lo primero a correr cuando el margen de un período parece demasiado bueno.

---

## 10. Pendientes

- [ ] **Fase 2 del simulador** → destraba el caso A ($133,8M de CMV estimado + $199,3M de crédito fiscal).
- [ ] **Compras de reposición** de los 16 SKUs del caso B.
- [ ] **Verificar fechas de entrada al depósito** de los 5 SKUs del caso C (CF14).
- [ ] **Lotes con `costo_unitario = 0`** en la compra inicial #4 (SKU 6 y 19) con stock activo → margen del 100 % al venderse.
- [ ] **`TA002V` Kindle** sin costo. **BLOQUEADO** por la alícuota (en `skus` figura 21 %, Sebastián indica 10,5 %; la alícuota recostea ventas y débito fiscal).
- [ ] **`DS001N` Disco SSD** con lote del seed sobrecargado (CMV 95,6 % vs ~69 % real). Fix: corregir `lotes.costo_unitario` + re-snapshot de las ventas de junio. Libera ~$7,4M de margen.
- [ ] **CMV histórico ene–hoy**: diferido a la integración de ventas Tango.
- [ ] **Corte por mes cerrado en el re-costeo** (`meses_cerrados`, capa 9, en pausa).

---

## Documentos relacionados

- `ADARA-DECISIONES.md` — CF1–CF14, P13, S9, S10
- `ADARA-STOCK.md` · `ADARA-PNL.md` · `ADARA-COMPRAS-IMPORTACIONES.md` · `ADARA-GASTOS.md` (costo tardío) · `ADARA-IMPORTACIONES-SIM.md` (Fase 2) · `ADARA-VENTAS-EFECTIVO.md` (FIFO canal-agnóstico) · `ADARA-CANCELACIONES-DEVOLUCIONES.md`
