# ADARA — Compras e Importaciones

Última actualización: 10 Agosto 2026 (**verificación del circuito C20 de compra sin comprobante** (sano, 0 compras cargadas) · **hallazgo estructural: no hay dónde poner la deuda con los terceros de una compra** → separar COSTO de DEUDA · **regla 17: costo fiscal de comprar sin comprobante y vender con comprobante (45,5% / 56%)** · **impacto del CMV congelado al cargar compras con fecha retroactiva**. Ver "Actualización — 10 Agosto 2026" al final.) · 7 Agosto 2026 (**compra sin comprobante — regla C20**: nueva columna `compras.sin_comprobante` + CHECK + trigger que impide componentes fiscales. Distinta de "factura pendiente" y distinta de `compra_componentes.tipo='sin_factura'`. Ver "Actualización — 7 Agosto 2026" al final.) · 5 Agosto 2026 (**alta de compra con gastos prorrateables desbloqueada**: `clase` es GENERATED ALWAYS y no se puede insertar → el criterio de reparto pasó a `descripcion`; el CHECK de `tipo` ahora admite `gasto_prorrateable` y `extra_directo`; IVA multi-alícuota por comprobante. Reglas nuevas: **el cargo accesorio se clasifica por quién lo factura** (`flete` suma al AP, `gasto_prorrateable`/`extra_directo` no) y **un tratamiento fiscal = un renglón** para el despacho de aduana. Decisión: el lote de importación nace con el **costo estimado del simulador**. Ver "Actualización — 5 Agosto 2026" al final.) · 18 Junio 2026 · 11 Junio 2026 · 9 Junio 2026

> **Nota:** este documento es el **diseño** del dominio compras/importaciones. Lo que ya está **implementado en v22** (compras locales, lotes, cuenta corriente, anular) está descripto en la sección [Implementación v22](#implementación-v22-mayo-2026) más abajo. Lo no marcado como implementado sigue siendo diseño/pendiente.

Cómo se registran compras locales e importaciones, cómo se forman los **lotes** con costos reales, y cómo alimentan el CMV exacto del P&L.
Reemplaza al doc de importaciones del v21 (que era solo un placeholder).

---

## Objetivo

Tener costos **reales por unidad** de todo lo que se vende. No promedios. No estimaciones. Cada venta sabe exactamente cuánto costó la unidad que se vendió.

Eso habilita:
- CMV exacto en el P&L
- Stock valorizado real (en ARS) en el patrimonial
- Análisis de margen por SKU / línea
- Separación clara entre **costo real** (forma parte del costo) y **crédito fiscal** (recupero impositivo)

---

## Conceptos clave

| Concepto | Definición |
|----------|-----------|
| **Compra** | Operación de adquisición de mercadería a un proveedor (local o del exterior). Una compra puede incluir N SKUs. |
| **Importación** | Tipo especial de compra que entra al país por aduana. Tiene componentes adicionales (aranceles, IVA aduana, percepciones). |
| **Lote** | El conjunto de unidades de **un mismo SKU** que vienen de una misma compra/importación con un mismo costo unitario. Una compra genera tantos lotes como SKUs distintos tenga. |
| **Costo unitario real** | Lo que efectivamente te costó cada unidad, incluyendo prorrateo de gastos comunes (flete, despacho, etc.). NO incluye créditos fiscales. |
| **Crédito fiscal** | Lo que pagaste en impuestos que se recupera contra otros impuestos. No es parte del costo. |
| **FIFO** | First In, First Out. Las ventas consumen primero las unidades del lote más viejo. |

---

## Modelo: cada compra es uno o más lotes

Estructura conceptual:

```
COMPRA / IMPORTACIÓN
├── Cabecera (proveedor, fecha, moneda, tipo)
├── Componentes de costo
│   ├── Precio de los productos (FOB en importación)
│   ├── Flete (interno o internacional)
│   ├── Seguro
│   ├── Aranceles aduaneros (solo importación)
│   ├── Tasa estadística (solo importación)
│   ├── Despacho y gastos de aduana (solo importación)
│   └── Otros gastos asignables al producto
├── Componentes fiscales (créditos)
│   ├── IVA pagado
│   ├── IIBB percepción
│   ├── Ganancias percepción
│   └── Otros impuestos
├── Pagos realizados (uno o varios)
│   └── Cada pago vinculado a un movimiento bancario
└── Lotes generados (uno por SKU)
    ├── Lote SKU 23B: cantidad, costo_unitario_real
    ├── Lote SKU 24A: cantidad, costo_unitario_real
    └── ...
```

### Ejemplo: una importación con dos SKUs

```
IMPORTACIÓN #023 — Proveedor AODELI-KAVEH — 18/03/2026 — USD

PRODUCTOS
  SKU 23B (Redmi Buds 8 Active Black)   200 unidades   FOB USD 4,50/u   = USD 900
  SKU 24A (Redmi Buds 8 Active White)    50 unidades   FOB USD 4,50/u   = USD 225
  FOB total                                                              USD 1.125

COMPONENTES DE COSTO COMUNES (se prorratean)
  Flete internacional                                                    USD   100
  Seguro                                                                 USD    25
  Aranceles aduaneros (16% sobre FOB+flete)                              USD   200
  Tasa estadística (0,5%)                                                USD     7
  Despacho y honorarios aduaneros                                        USD    45
  Total componentes comunes                                              USD   377

TIPO DE CAMBIO (blue al despacho)                                        $ 1.020/USD

COMPONENTES FISCALES (créditos, NO son costo)
  IVA pagado en aduana (21% sobre FOB+flete+aranceles+tasa)              USD   258 → crédito IVA
  IIBB percepción importación                                            USD    37 → crédito IIBB
  Ganancias percepción                                                   USD    56 → crédito Ganancias

PAGOS REALIZADOS
  20/03: Transferencia USD 600 a TC $1.020 = $612.000   (mov. banco)
  22/03: Transferencia ARS $530.000                      (mov. banco)
  Pendiente de pago                                      $   ~ 145.000  → cuentas por pagar

PRORRATEO DEL COSTO (común sobre FOB)
  SKU 23B absorbe: 900/1125 × 377 = USD 301,60
  SKU 24A absorbe: 225/1125 × 377 = USD  75,40

LOTES GENERADOS
  Lote #023-23B: 200u, costo unitario = (900 + 301,60)/200 × $1.020 = $ 6.124
  Lote #023-24A: 50u,  costo unitario = (225 +  75,40)/50  × $1.020 = $ 6.124
```

(En este caso ambos lotes tienen el mismo costo unitario porque los SKUs tienen mismo FOB y la prorrata fue proporcional. Si tuvieran FOB distintos, los costos finales también lo serían.)

---

## Componentes del costo real (forman parte)

Lo que se suma para calcular el costo unitario del lote:

| Componente | Aplica a | Notas |
|------------|----------|-------|
| Precio de productos (FOB en importación, precio de lista en compra local) | Todas | El núcleo del costo |
| Flete | Todas | Interno (Argentina) o internacional |
| Seguro | Todas | Cobertura del traslado |
| Aranceles aduaneros | Solo importación | Generalmente % sobre FOB+flete |
| Tasa estadística | Solo importación | 0,5% sobre FOB+flete (Argentina) |
| Despacho y honorarios de aduana | Solo importación | Servicio del despachante |
| Otros gastos asignables al producto | Todas | Inspección, certificaciones, transporte interno, etc. |
| **Gastos sin factura (efectivo)** | Todas, frecuente en importación | Honorarios informales, coimas, servicios sin comprobante. Pagados en efectivo desde caja física. **Forman parte del costo real** pero no generan crédito fiscal. Ver sección dedicada más abajo. |

Todos estos componentes se prorratean entre los SKUs de la compra **según su naturaleza** (corregido 18/6/2026, ver "Actualización — 18 Junio 2026" al final): el **flete** se reparte por **PESO (kg)**, el **seguro** por **FOB**, los **tributos** (derechos/estadística) son % sobre el CIF de cada producto y el **despachante** es % del CIF. Los **gastos fijos** (bolsón) se reparten por % o $ con criterio precargable por kg o FOB. La asunción previa de "todo por proporción de FOB" quedó **superada**: solo el seguro va por FOB.

> **Dónde se carga cada uno (5/8/2026).** Que un costo capitalice al lote **no** define dónde se registra. Si el cargo viene en la factura del propio proveedor va como componente `flete` de la compra; si es de un tercero **con factura propia** se carga como **gasto vinculado a la compra** (`gastos.capitaliza_compra_id`, ver `ADARA-GASTOS.md`); si no tiene comprobante, como componente `sin_factura`. Ver la tabla completa en la actualización del 5/8.

---

## Componentes fiscales (créditos, NO son costo)

Lo que se paga al fisco en el momento de la compra/importación pero se recupera vía DDJJ. No forma parte del costo del producto.

| Componente | Aplica a | Va al ledger de |
|------------|----------|-----------------|
| IVA pagado | Compras factura A + importaciones | Crédito IVA |
| IIBB percepción | Importaciones + compras con percepción | Crédito IIBB |
| Ganancias percepción | Importaciones + algunos servicios | Crédito Ganancias |
| Otros impuestos (impuesto país, etc.) | Según caso | Según naturaleza |

Cuando se carga la compra, ADARA separa automáticamente:
- Lo que forma parte del costo → eleva el costo unitario del lote
- Lo que es crédito fiscal → entry en el ledger del impuesto correspondiente

> ⚠️ **Falta el tipo `iva_percepcion`** (percepción de IVA de importación, libre disponibilidad). Hoy no existe y cargarla como `tipo='iva'` la mandaría al colchón equivocado (crédito técnico). Ver pendientes.

---

## Gastos sin factura (efectivo)

En la realidad operativa de importaciones argentinas hay costos que **no tienen factura asociada** y se pagan en efectivo:

- Honorarios informales (despachante, gestores)
- Coimas
- Servicios prestados sin comprobante
- Gastos chicos varios no documentados

Estos gastos son **costo real** de traer la mercadería. Si no se contemplan, el P&L interno está mintiendo (mostraría más margen del real).

### Cómo se cargan en ADARA

1. Al cargar la compra/importación, se agregan como ítems del tipo **"Gasto sin factura"** con:
   - Monto (en ARS o USD)
   - Descripción libre (opcional, para tu propio control)
   - Caja de origen del pago: caja física ARS o caja física USD
2. ADARA genera **automáticamente un movimiento de salida** en la caja correspondiente.
3. El monto **suma al costo del lote** y se prorratea entre SKUs igual que el resto de los componentes comunes.
4. **NO genera crédito fiscal** (ni IVA, ni IIBB, ni Ganancias).
5. Queda **etiquetado** como "sin factura" para que sea fácilmente identificable. El contador decide cómo tratarlo (en general, no se incluye como deducible en la DDJJ).

### Implicancia para el P&L

| P&L interno (ADARA) | P&L fiscal (contador) |
|---------------------|----------------------|
| Incluye los gastos sin factura como costo real | Excluye los gastos sin factura (no deducibles) |

Esto es esperado: tu P&L interno refleja la realidad económica; el P&L fiscal refleja la realidad declarable.

### Ejemplo

```
IMPORTACIÓN #023 (continuación del ejemplo anterior)

GASTOS SIN FACTURA (efectivo)
  Despachante — honorarios informales      USD  150   (caja USD)
  Aduana — gestiones varias                USD  200   (caja USD)
  Total                                    USD  350

Al TC $1.020 = $ 357.000

PRORRATEO ENTRE SKUs (proporción de FOB)
  SKU 23B absorbe: 900/1125 × 357.000 = $ 285.600
  SKU 24A absorbe: 225/1125 × 357.000 = $  71.400

LOTE 23B nuevo costo unitario:
  Costo previo: $ 6.124
  + $ 285.600 / 200u = $ 1.428
  = $ 7.552

LOTE 24A nuevo costo unitario:
  Costo previo: $ 6.124
  + $ 71.400 / 50u = $ 1.428
  = $ 7.552

MOVIMIENTOS GENERADOS
  Salida de caja USD: − USD 350   (fecha = fecha de la importación)
```

---

## Pagos a proveedores

Una compra puede tener **N pagos** (anticipo, contra documentos, cuota final, parciales).

| Caso | Manejo |
|------|--------|
| Pago único al contado | Un movimiento bancario vinculado a la compra |
| Pago en cuotas (3 cheques diferidos, ej.) | N cheques anotados como "promesas de pago" con fechas. Cuando cada cheque se debita del banco, se vincula al movimiento |
| Pago anticipo + saldo | Dos movimientos vinculados a la misma compra |
| Pago en USD desde caja física USD | Movimiento de caja USD, no de banco |
| Pago en ARS desde caja física ARS | Movimiento de caja ARS, no de banco |
| Pago con tarjeta de crédito | El gasto va a la tarjeta. El pago de la tarjeta al banco no es pago de la compra (es pago de la tarjeta) |

**Estado de la compra:**
- `pagado_total` — suma de pagos = total
- `pagado_parcial` — suma de pagos < total. Diferencia → cuentas por pagar
- `sin_pagar` — todavía no se pagó nada (común en B2B con plazo)

---

## Tipo de cambio en compras USD

Regla: **TC blue al momento de la operación**, criterio histórico.

| Momento | TC aplicable |
|---------|--------------|
| Fecha de la compra/importación | TC blue del día → fija el costo del lote en ARS |
| Cada pago en USD | TC del día del pago → registra el monto efectivamente erogado |

**Diferencia de cambio:**
- Si pagás con USD que compraste a TC menor → ganancia cambiaria
- Si pagás con USD que compraste a TC mayor → pérdida cambiaria

Estas diferencias **van al P&L como "diferencia de cambio realizada"**, no modifican el costo del lote.

**El costo del lote queda congelado al TC del momento de la compra.** No se recalcula con TCs posteriores.

---

## FIFO al consumir stock

Cada venta consume unidades del lote más antiguo disponible del SKU.

```
LOTE #022 (15/01/2026)  100u → quedan 6u disponibles
LOTE #023 (18/03/2026)  200u → quedan 200u disponibles
LOTE #024 (10/05/2026)   50u → quedan 50u disponibles

Venta del 14/05: 10 unidades de SKU 23B

Consumo FIFO:
  6u del Lote #022 a $4.890 = $29.340
  4u del Lote #023 a $6.124 = $24.496
  CMV total = $53.836
```

### Override manual

En casos especiales (combo, garantía, lote físicamente identificable) se puede asignar manualmente de qué lote sale cada venta. Queda registrado como override.

---

## Compensación de costos entre lotes (ajuste de inventario)

Caso típico (mencionado al inicio del rediseño): comprar el mismo producto a precios muy distintos hace inviable vender el caro. Solución: **redistribuir costos entre lotes** sin cambiar el valor total invertido.

### Ejemplo

```
ANTES
  Lote A: 100u × $100  = $10.000
  Lote B: 100u × $ 90  = $ 9.000
  Total invertido: $19.000

OBJETIVO: que ambos queden a $95

AJUSTE
  Lote A: −$500  (de 100u × $100 → 100u × $95)
  Lote B: +$500  (de 100u × $90  → 100u × $95)

DESPUÉS
  Lote A: 100u × $95 = $9.500
  Lote B: 100u × $95 = $9.500
  Total invertido: $19.000   ✓ no cambió
```

### Reglas

1. El **total invertido no cambia**. Es redistribución, no creación de valor.
2. Las **ventas anteriores al ajuste mantienen su CMV original** — no se recalcula retroactivo.
3. Las **ventas posteriores al ajuste usan el nuevo costo**.
4. Queda un **registro de ajuste** con fecha, motivo, lotes afectados, deltas. Auditable.

### UI

Pantalla "Compensar lotes": seleccionás 2+ lotes del mismo SKU, definís un costo objetivo (o costo por lote), ADARA calcula deltas y registra el ajuste.

---

## Cuentas por Pagar (AP)

Subset de compras con `pagado < total`. Cada renglón muestra:

- Proveedor
- Fecha de la compra
- Monto total
- Pagado a la fecha
- Pendiente
- Fecha estimada de vencimiento (si hay)
- Línea de negocio que la generó

Vista agrupada por proveedor con cuenta corriente (lo que le debés vs lo que pagaste históricamente).

Al pagar (cuando aparece el movimiento bancario), se vincula al saldo pendiente y se reduce.

**Qué entra al total facturado (`v_compras_ap`):** productos, `flete` y componentes fiscales. **Quedan excluidos** `sin_factura`, `gasto_prorrateable` y `extra_directo` — capitalizan al lote pero se le deben a un tercero, no al proveedor de la factura.

---

## Diferencia entre compra local e importación

| Aspecto | Compra local | Importación |
|---------|--------------|-------------|
| Moneda | ARS o USD (TC blue) | USD generalmente |
| Componentes de costo | Precio + flete (si aplica) + otros | + aranceles + tasa estadística + despacho |
| IVA | Crédito IVA si es factura A | Crédito IVA aduana (21%) |
| Percepción IIBB | Solo si proveedor la aplica | Casi siempre aplica |
| Percepción Ganancias | Solo en algunos servicios | Casi siempre aplica |
| Forma de pago | Transferencia ARS, cheques, efectivo | Transferencia USD, generalmente al exterior |
| Documentos | Factura A/C | Factura proveedor + despacho aduanero |

ADARA presenta un formulario de carga distinto para cada tipo, con los campos específicos.

---

## Snapshot inicial al 31/12/2025

Para arrancar el sistema con stock valorizado:

- Cada SKU tiene un **lote inicial único** con la cantidad actual y el costo unitario real estimado al 31/12/2025.
- No es una compra real — es una "carga inicial de stock" registrada como pseudo-compra con fecha 31/12/2025.
- A partir del 1/1/2026, las compras nuevas generan lotes nuevos. Los lotes iniciales se consumen primero (son los más viejos).
- Es responsabilidad de Sebastián cargar el costo unitario real de cada SKU al arrancar (132 SKUs aprox.).

---

## Implementación v22 (Mayo 2026)

Lo que está **construido y en producción** (pantalla **Compras**, `public/js/screens/compras.js` + endpoints en `server.js`). Cubre **compras locales en ARS y en USD** (con tipo de cambio). Importaciones (despacho de aduana multi-factura) todavía no (ver pendientes).

### Pantalla

Dos pestañas:
- **Facturas** — lista de compras (desde la vista `v_compras_ap`) + KPIs (cantidad, comprado, pendiente de pago) + alta + anular.
- **Cuenta corriente** — saldo por proveedor uniendo **compras + gastos** (facturado / pagado / saldo, con signo: le debés / te debe).

### Alta de compra local — `POST /compras` (atómico)

Una sola operación arma:
- **Cabecera** en `compras`: `tipo='local'`, `moneda` (`ARS` o `USD`), `tc_blue` (TC del día si es USD), fecha, línea, estado. El **N° de factura** se guarda en su **columna dedicada** `compras.nro_factura` (puede quedar `NULL` = factura diferida; ver más abajo).
- **Componentes producto** en `compra_componentes` (`tipo='producto'`): por cada SKU con cantidad y costo, guardados en la **moneda de la factura**. Cada uno **genera un lote** en `lotes` con `costo_unitario` = costo **neto** (sin IVA) **siempre en ARS**: si la factura es en USD, se **congela** al TC del día (`costo_usd × tc_blue`). `cantidad_inicial = cantidad_actual = cantidad`.
- **Componentes fiscales** en `compra_componentes` (`tipo='iva' | 'iibb_percepcion' | 'ganancias_percepcion'`): créditos fiscales, **no** suman al costo del lote.

Si algo falla, hace rollback best-effort.

### IVA automático por alícuota de SKU

- Cada producto toma la **alícuota de su SKU** (`skus.alicuota_iva`, default 0.21).
- Selector por línea editable: **21% / 10,5% / 27% / Exento**.
- El IVA se calcula y se carga como **componente fiscal** (crédito IVA), separado del costo. IIBB y Ganancias percepción se cargan a mano.
- **Multi-alícuota (5/8/2026):** el comprobante puede tener **más de un renglón de IVA**. El total se arma como (cada producto × su alícuota) + (cada gasto prorrateable "de esta factura" × su alícuota).

### Anular compra — `POST /compras/:id/anular`

Soft-delete con reversa de stock (mismo criterio que el "anular" de Gastos: deja rastro):
- Marca `compras.estado='anulada'` + guarda el motivo en `compras.motivo`.
- **Borra los lotes** (revierte el stock que había generado) y los `compra_componentes`. La cabecera queda como tombstone.
- **Bloquea (409)** si:
  - ya se **consumió/vendió** stock de algún lote (`cantidad_actual < cantidad_inicial`), o
  - tiene **pagos vinculados** (`vinculos` con `op_tipo='compra'`) → primero hay que desvincular desde Conciliación.
- La lista, los KPIs y la cuenta corriente **filtran** `estado_compra='anulada'` **y** el seed de apertura (`tipo_compra='inicial'`).

**Editar una compra** todavía no está implementado. Workflow actual: **anular + volver a cargar**.

### Proveedores

- **Get-or-create por CUIT** (`POST /proveedores`): normaliza el CUIT a 11 dígitos y, si ya existe un proveedor con ese CUIT, **lo reutiliza** (no duplica).
- **Autocompletado por padrón ARCA**: al tipear el CUIT en el alta rápida se trae la razón social. Ver `ADARA-ARCA-PADRON.md`.
- **Anti-olvido**: al **guardar** una compra (o un gasto), si quedó un CUIT cargado en el alta rápida pero no se tocó "Crear", el proveedor **se crea y se vincula solo**. Antes se guardaba sin proveedor.

### Extensión Junio 2026 — USD/TC, percepciones por jurisdicción, factura diferida

Sobre el alta de compra local se agregaron, manteniendo el flujo atómico de `POST /compras`:

**Moneda y tipo de cambio (compra local en USD).** El alta acepta `moneda` (`ARS`/`USD`) y `tc_blue`. Los componentes (productos y fiscales) se guardan en la **moneda de la factura**; el **lote siempre queda en ARS** congelando `costo_usd × tc_blue`. La vista `v_compras_ap` convierte los componentes USD a ARS con ese mismo TC para los totales y la cuenta corriente.

**Percepciones múltiples por jurisdicción (repeater).** Antes había dos campos fijos (IIBB / Ganancias). Ahora es un **repeater**: se cargan **N percepciones**, cada una con su **tipo** (`iibb` / `ganancias`) y su **jurisdicción**. Cada fila se materializa como un `compra_componentes` (`tipo='iibb_percepcion'` o `'ganancias_percepcion'`), con la **jurisdicción guardada en `descripcion`**.
- La **jurisdicción es un desplegable** con las 24 jurisdicciones de Convenio Multilateral. El valor guardado es el **código `snake_case`** (`buenos_aires`, `caba`, `tucuman`, …), **el mismo vocabulario que usan las retenciones de IIBB de ventas** (`retenciones.jurisdiccion` / `v_retenciones_iibb`). Así percepciones de compra y retenciones de venta **cruzan por provincia** (clave para la posición de IIBB del Convenio Multilateral).
- Ganancias es nacional → se carga con jurisdicción vacía (`— Sin jurisdicción`).

**N° de factura diferido.** El N° vive en la **columna dedicada `compras.nro_factura`** y **puede quedar vacío** al cargar (la factura del proveedor a veces llega después). En la lista de Facturas, una compra `local` activa sin N° muestra el chip **"factura pendiente"** + acción **"asignar"**. El endpoint **`POST /compras/:id/factura`** asigna/actualiza solo el N° (edición acotada: no recalcula lotes/costos/IVA; rechaza si la compra está anulada).

**Seed de stock fuera de Facturas.** El stock de apertura (compra `tipo='inicial'`, sin proveedor, total $0) **no es una factura de mercadería**: se **excluye** de la pestaña Facturas, de la Cuenta corriente y de los KPIs (su valorización vive en Inventario). Tampoco ofrece "Anular" (anularlo rompería el costeo FIFO de las ventas que ya consumieron ese stock).

### Qué del diseño de arriba todavía NO está implementado

- **Importaciones** (USD, prorrateo de nacionalización, aranceles/tasa/despacho) — pendiente, ver abajo.
- **Gastos sin factura dentro de la compra** (con salida de caja automática y prorrateo) — pendiente.
- **Compensación de costos entre lotes** (ajuste de inventario) — pendiente.
- **Snapshot inicial 31/12/2025** y carga de costos de los 132 SKUs — pendiente.
- **FIFO / consumo de lotes en la venta** — depende del sync de ventas ML.

---

## Reglas duras

1. **Costo del lote inmutable**, salvo por compensación explícita (ajuste de inventario) o por **re-costeo de costo tardío** (regla del 5/8/2026, ver `ADARA-COSTEO-FIFO.md`).
2. **Pagos a la compra ≠ compra**. Una compra del 18/03 con pago el 22/03 sigue siendo "compra del 18/03" en el P&L (devengado).
3. **Componentes fiscales NO suman al costo**. Van al ledger del impuesto.
4. **FIFO por defecto**. Override solo en casos justificados, queda registrado.
5. **Ajuste de inventario es delta, no edición directa del costo**. Mantiene el total invertido.
6. **TC del lote queda fijo** al TC del día de la compra. Diferencias posteriores → P&L como dif. de cambio.
7. **Gastos sin factura suman al costo del lote pero NO generan crédito fiscal**. Quedan etiquetados como "sin factura" para tratamiento fiscal externo.
8. **El costo del lote siempre se persiste en ARS.** Si la factura es en USD, se congela al TC del día (`costo_usd × tc_blue`). La moneda original queda registrada en el componente.
9. **La jurisdicción de las percepciones usa el código normalizado compartido con ventas** (`snake_case`, igual que `retenciones.jurisdiccion`). Nunca texto libre, para que compra↔venta crucen por provincia.
10. **El N° de factura puede ser diferido** (columna `nro_factura`, nullable); asignarlo después no recalcula nada. El seed de apertura (`tipo='inicial'`) no se lista como compra ni se anula.
11. **El cargo accesorio se clasifica por QUIÉN lo factura, no por qué es** (5/8/2026). En la factura del propio proveedor → `flete` (suma al AP de ese proveedor). De un tercero → `gasto_prorrateable` (se reparte) o `extra_directo` (un solo SKU), que **no** suman al AP. Los tres capitalizan al costo del lote.
12. **Un tratamiento fiscal = un renglón** (5/8/2026). Un despacho de aduana no es una factura: es un papel con conceptos de tratamiento distinto. De un despacho salen más renglones que papeles.
13. **El lote de importación nace con el costo estimado completo del simulador**, no con el FOB pelado (5/8/2026). Las facturas reales se cargan después y al cerrar el despacho se ajusta la diferencia.
14. **`compra_componentes.clase` es GENERATED ALWAYS** — nunca se inserta desde la app. Se deriva de `tipo`. El criterio de reparto del prorrateo viaja en `descripcion`.
15. **Compra sin comprobante ≠ factura pendiente** (7/8/2026, C20). `sin_comprobante = true` significa que esa factura **no existe y nunca va a existir**: cero crédito fiscal, `nro_factura` obligatoriamente `NULL`, prohibido cargarle componentes fiscales. "Factura pendiente" (`nro_factura IS NULL` con `sin_comprobante = false`) es lo opuesto: la factura existe, todavía no llegó el número.
16. **La compra sin comprobante sí genera deuda con el proveedor.** No tener factura no borra que hay que pagarla: sigue entrando a `v_compras_ap` y a la cuenta corriente. Lo que se pierde es el crédito fiscal, no el pasivo.
17. **Comprar sin comprobante y vender con comprobante cuesta 45,5% / 56% de cada peso no documentado** (10/8/2026). Por cada peso sin factura se paga la **alícuota de IVA** (no hay crédito que compensar el débito) **más la tasa de Ganancias** (el costo no es deducible): **45,5%** con mercadería al **10,5%** de IVA y tasa de Ganancias del 35%; **56%** con IVA al 21%. **No incluye IIBB**, que lo empeora. Solo tendría sentido si esa mercadería **también se vende sin facturar**. Ver la actualización del 10/8 para el cálculo del descuento mínimo aceptable.

---

- ~~**Importaciones — multi-factura / multi-proveedor por despacho** (decisión de fondo, identificada 30/05/2026)~~ → **RESUELTO 5/8/2026**: no hace falta proveedor por componente. Cada factura de un tercero (forwarder, despachante, TCA, almacenaje) se carga como **gasto vinculado a la compra** (`gastos.capitaliza_compra_id`), con su propio proveedor, fecha, IVA y AP. El despacho queda como una compra (la del FOB) más N gastos capitalizables. Ver `ADARA-GASTOS.md`.

## Pendientes y TBD (diseño previo)

- **Tipo `iva_percepcion` para la percepción de IVA de importación (5/8/2026, imprescindible).** Aparece en casi todos los despachos y hoy **no existe**. Cargarla como `tipo='iva'` la pondría en el colchón equivocado: la percepción es de **libre disponibilidad**, no crédito técnico. Impacto: CHECK de `compra_componentes.tipo`, la expresión generada de `clase`, `v_compras_ap` y `v_posicion_fiscal`.
- **Tabla de equivalencias código de proveedor → SKU (5/8/2026).** Los códigos que trae la factura (ej. `0418340` de Invid) son del **proveedor**, no de ADARA. Hoy hay que deducir a mano qué SKU es cada renglón. Una tabla de equivalencias permitiría auto-completar la carga.
- **Sin restricción de unicidad sobre (proveedor, nro_factura) (5/8/2026).** Se puede cargar dos veces la misma factura sin que nada avise. De hecho las compras **2 y 3** comparten el número `00006-00000451`.
- **Costo unitario real al 31/12/2025 de los 132 SKUs**: a cargar por Sebastián al arrancar
- **Listado completo de proveedores** (locales + del exterior): a cargar en la tabla `proveedores`
- ~~**Método de prorrateo alternativo** (peso, volumen): por ahora solo FOB, ampliar si surge necesidad~~ → **CERRADO 18/6/2026**: el flete se prorratea por **peso (kg)** y el seguro por **FOB** (validado contra planillas reales en el Simulador de Importaciones). Ver `ADARA-IMPORTACIONES-SIM.md` y la "Actualización — 18 Junio 2026" al final de este doc.
- **Importaciones históricas 2025**: no se cargan retroactivas (decisión confirmada). Solo lotes iniciales únicos.
- **Reparto de costo de lote entre líneas de negocio**: cuando un lote (ej. 100 mochilas importadas) alimenta ventas de dos líneas distintas (ej. 50 sindical + 50 individual), el pago al proveedor se imputa a UNA línea pero el CMV se atribuye por venta. Definir cómo se reconcilia el saldo por línea: ¿se imputa la compra a una línea "primaria" y se ajusta al consumir? ¿se reparte la compra anticipadamente? Decisión a tomar al construir la pantalla de Compras/Importaciones. Identificado el 28/05/2026 durante clasificación de SKUs.

---

## Documentos relacionados

- `ADARA-FLUJO-OPERATIVO.md` — cuándo se cargan las compras y se reciben las importaciones
- `ADARA-GASTOS.md` — gastos capitalizables vinculados a una compra (`capitaliza_compra_id`): costos accesorios de terceros con factura propia
- `ADARA-COSTEO-FIFO.md` — costo tardío, re-costeo de lotes y CMV congelado
- `ADARA-STOCK.md` — valorización del stock por lote, descuento por venta
- `ADARA-PNL.md` — uso del CMV exacto en el resultado
- `ADARA-IMPUESTOS.md` — créditos fiscales generados en compras/importaciones
- `ADARA-IMPORTACIONES-SIM.md` — simulador de importaciones (costo estimado que da de alta el lote)
- `ADARA-LINEAS-NEGOCIO.md` — imputación de lotes a líneas
- `ADARA-CONCILIACION-BANCARIA.md` — pagos a proveedores como movimientos bancarios
- `ADARA-DECISIONES.md` — reglas consolidadas

---

## Actualización — 11 Junio 2026 (gastos prorrateables + proveedor sin CUIT + compra informal)

### Gastos al costo del lote (IMPLEMENTADO — antes figuraba pendiente)
El alta de compra (`POST /compras` + `compras.js`) ahora permite sumar al **costo del lote** dos tipos de gasto, que **conviven**:

- **Extra directo por producto:** un monto que se asigna a un producto puntual y suma solo a ese lote.
- **Prorrateables compartidos:** flete / comisión / despacho, repartidos entre todos los productos por **criterio** `costo neto` (default) o `unidades`.

**Fórmula del costo unitario del lote** (en moneda de la factura, luego congelado a ARS al TC, redondeo final una sola vez):
```
costo_lote_unit = costo_base + (extra_directo / cantidad) + (parte_prorrateo / cantidad)
parte_prorrateo_i = total_prorrateables × (base_i / Σ base)    [criterio costo]
                  = total_prorrateables × (cant_i / Σ cant)    [criterio unidades]
```

**Regla dura:** flete / comisión / despacho / coima pagados a **terceros** (transportista, despachante, vendedor) **van al costo del lote (CMV) pero NO a la cuenta corriente del proveedor** del producto. Se guardan como `compra_componentes` con `tipo='extra_directo'` (con `sku_id`) o `tipo='gasto_prorrateable'`, como rastro auditable. La vista **`v_compras_ap` fue recreada para excluir esos dos tipos** del total facturado y del saldo (igual que ya excluía `sin_factura`). No generan crédito fiscal.

> ⚠️ **Dos correcciones del 5/8/2026 sobre este bloque:**
> 1. **La regla vale solo para cargos de terceros.** Si el cargo viene **dentro de la factura del propio proveedor** (ej. "Gastos adicionales" de Invid), va como `tipo='flete'` y **sí suma al AP** de ese proveedor. Ver la tabla de clasificación en la actualización del 5/8.
> 2. **`clase` no se insertaba: es GENERATED ALWAYS.** El texto original de este bloque decía que se guardaba `clase='directo'` / `clase=criterio`; eso era exactamente el bug que impedía guardar cualquier compra con prorrateables (Postgres devolvía `428C9`). El criterio de reparto viaja ahora en `descripcion`. Además, hasta el 5/8/2026 el CHECK de `compra_componentes.tipo` **no admitía** ninguno de los dos tipos, así que en la práctica esta funcionalidad **nunca se pudo ejecutar** entre el 11/6 y el 5/8 (0 filas de esos tipos).

### Proveedor sin CUIT
`POST /proveedores`: el **CUIT es opcional** (si se carga, valida 11 dígitos). Dedup: con CUIT por CUIT; sin CUIT por **nombre normalizado** (solo entre los que tampoco tienen CUIT). Pensado para proveedores informales / compras sin factura. Excepción al get-or-create por CUIT.

### Compra informal / sin factura
- Se carga con alícuota **Exento** en cada renglón → **cero crédito IVA** (sin factura = sin IVA recuperable; meterlo inflaría la Posición Fiscal). Sin percepciones.
- **Moneda USD + TC del USDT:** `compras.moneda='USD'` y el campo `tc_blue` admite el **TC del USDT** (es genérico: guarda el TC usado, sea blue o USDT). El lote se congela a ARS = `costo_usd × tc`.
- **El lote entra a stock al instante** al guardar la compra (`estado='activa'`, `lotes.fecha_alta = compra.fecha`, `cantidad_actual = cantidad`). No hay paso de habilitación. El FIFO lo consume en ventas con fecha ≥ esa fecha. Conviene cargar la compra **con el flete ya incluido**; si el costo llega tarde, se aplica la regla de costo tardío (5/8/2026, `ADARA-COSTEO-FIFO.md`).

---

## Actualización — 18 Junio 2026 (prorrateo de importación corregido + simulador)

Validado contra planillas reales de Sebastián (importaciones de smartwatches/auriculares y de notebooks), al construir el **Simulador de Importaciones** (`ADARA-IMPORTACIONES-SIM.md`, Fase 1). Tres correcciones al modelo de costeo de importación de este doc:

### 1. El flete se prorratea por PESO (kg), no por FOB
La asunción original ("todo por proporción de FOB") era incorrecta para el flete. En la realidad el flete internacional se reparte **por kilo**. Sebastián trackea peso unitario y total con precisión. Reparto por componente:

| Componente | Criterio de prorrateo |
|---|---|
| Flete (declarado y real) | **Peso (kg)**: `flete · (peso_total_i / Σpeso)` |
| Seguro | **FOB declarado**: `seguro_total · (fob_decl_total_i / Σfob)`, con `seguro_total = seguro% · (Σfob + flete_declarado)` |
| Derechos / Estadística | **% sobre el CIF** de cada producto (capitalizan) |
| Despachante | **% del CIF** (`despachante% · CIF_i`) |
| Fijos (bolsón) | Por **%** o **$**, criterio de precarga **kg** o **FOB** |

### 2. Existen DOS fletes: declarado y real
- **Flete declarado**: entra al **CIF** y por lo tanto a la base de los tributos aduaneros (derechos, estadística) y del crédito fiscal (IVA/IIBB/Ganancias percepción).
- **Flete real** (`USD/kg × kg`): el costo verdadero que capitaliza al lote. Suele incluir, sobre todo en **consolidados**, gastos en origen (THC, desconsolidado, almacenaje, transporte) mezclados con el flete internacional.

La diferencia (real − declarado) **capitaliza al costo** del lote, pero **no** a la base aduanera ni genera crédito fiscal.

> **Importante (22/7/2026).** Esa diferencia flete real−declarado es **costeo puro**: capitaliza vía el bolsón de fijos, pero **NO es subdeclaración aduanera y NO integra el cálculo del ahorro/coima** del simulador. La coima nace solo de subdeclaración de **producto** (FOB, cantidad, posición arancelaria). El caso `flete_real > flete_declarado` es normal —y frecuente en consolidados— sin que haya "subfacturación de flete": simplemente el número real trae gastos en origen. Ver P15 en `ADARA-DECISIONES.md` y el CIF real sombra en `ADARA-IMPORTACIONES-SIM.md`.

### 3. Subfacturación de FOB
Se declara un **FOB menor** al real. Los tributos y el crédito fiscal caen sobre el **FOB declarado**; la diferencia no declarada (`(fob_real_u − fob_decl_u) · cantidad`) **capitaliza al costo del lote SIN generar crédito fiscal** — mismo trato que los gastos sin factura (P12). El monto no declarado es salida de caja por fuera del circuito bancario.

### Impacto en `compra_componentes` (cuando se construya el alta real de importación, Fase 2)
- El costo del lote (CMV s/IVA) debe contemplar: FOB **real** (incluye lo no declarado) + flete **real** (por peso) + seguro (por FOB) + derechos + estadística + despachante + fijos.
- Las percepciones de IVA/IIBB/Ganancias **no** capitalizan (P12): son crédito fiscal → Posición Fiscal.
- Hoy el alta `POST /compras` solo soporta `tipo='local'`, `moneda='ARS'`, un proveedor y prorrateo por costo/unidades. El circuito de importación (multi-componente con prorrateo por peso/FOB) se construye en la **Fase 2** del simulador (botón "Confirmar" → compra + lotes), todavía pendiente.

### Estado del simulador
**Fase 1 desplegada**: pantalla `#importaciones_sim` (`public/js/screens/importaciones-sim.js`) + tabla `importacion_sim` (JSONB con el escenario completo, patrón post-A17). Solo **calcula y guarda/recupera** borradores; **no toca** stock, lotes, compras ni caja. Detalle completo en `ADARA-IMPORTACIONES-SIM.md`.

---

## Actualización — 5 Agosto 2026 (alta con prorrateables desbloqueada + clasificación de cargos accesorios + ciclo del despacho)

### Bugs encontrados y corregidos

**1. `clase` es GENERATED ALWAYS y el alta la mandaba explícita → error `428C9`.**
`compra_componentes.clase` se **deriva** de `tipo` con un CASE:

| `tipo` | `clase` derivada |
|---|---|
| `iva`, `iibb_percepcion`, `ganancias_percepcion`, `otro_impuesto` | `fiscal` |
| `sin_factura` | `sin_factura` |
| todo el resto (ELSE) | `costo` |

`POST /compras` en `server.js` la enviaba en el insert (`clase: 'directo'` para el extra directo y `clase: criterio` para el prorrateable) y Postgres rechazaba con **`428C9` "cannot insert a non-DEFAULT value into column clase"**. Efecto: **no se podía guardar NINGUNA compra que tuviera gastos prorrateables**. Encima usaba esa columna para guardar el criterio de reparto, que no es su propósito.
**Fix:** se sacó `clase` del payload. El criterio ahora va en la **`descripcion`** del componente, con formato `"<concepto> [reparto: costo|unidades]"`.

**2. El CHECK de `compra_componentes.tipo` no admitía `gasto_prorrateable` ni `extra_directo`.**
El doc los daba por implementados desde el 11/6/2026 y `v_compras_ap` ya los excluía por nombre, pero el CHECK nunca se había ampliado: había **0 filas** de esos tipos en la base.
**Fix:** migración **`ampliar_tipos_compra_componentes`**, que agrega ambos valores al CHECK. Caen automáticamente en `clase='costo'` por el ELSE de la expresión generada.

**3. La pantalla admitía un solo renglón de IVA por comprobante.**
Una factura con dos alícuotas (productos al 10,5% + un cargo al 21%) no se podía cargar completa.
**Fix:** el IVA se calcula como **(cada producto × su alícuota) + (cada gasto prorrateable × su alícuota)**.

### De quién es el cargo accesorio (los tres tipos y cuándo usar cada uno)

Los tres **capitalizan al costo del lote**. Lo que cambia es **a quién se le debe la plata**:

| Tipo | ¿Suma al AP del proveedor? | Cuándo se usa |
|---|---|---|
| `flete` | **Sí** | El cargo viene **EN la factura del mismo proveedor** (ej. "Gastos adicionales" de Invid dentro de su propia factura) |
| `gasto_prorrateable` | No | Es de un **tercero con factura aparte**; se reparte entre los productos |
| `extra_directo` | No | Es de un **tercero** pero aplica a **un solo SKU** |

`v_compras_ap` excluye del total facturado `sin_factura`, `gasto_prorrateable` y `extra_directo`; **`flete` sí suma**.

**En la pantalla:** el renglón de gasto prorrateable tiene ahora un tilde **"de esta factura"** que decide entre `flete` y `gasto_prorrateable`, y habilita un **selector de alícuota de IVA (0 / 21 / 10,5 / 27)** solo cuando está tildado. Si el cargo es de un tercero **no lleva IVA acá**: su crédito entra con la factura de ese tercero (que se carga como **gasto vinculado a la compra**, ver `ADARA-GASTOS.md`).

### Un tratamiento fiscal = un renglón (despacho de importación)

El **despacho de aduana (DUA) no es una factura**: es un solo papel que contiene conceptos con tratamientos fiscales distintos. De un despacho típico salen **más renglones que papeles**.

| Concepto | Origen | IVA | Tratamiento |
|---|---|---|---|
| FOB mercadería | Invoice del exterior | — | capitaliza al lote |
| Flete internacional | Factura forwarder / BL | según quién factura | capitaliza |
| Seguro | Póliza | 21% si es local | neto capitaliza · IVA crédito |
| Derechos de importación | DUA | — | capitaliza |
| Tasa estadística | DUA | — | capitaliza |
| IVA aduana | DUA | — | crédito técnico |
| IVA percepción adicional | DUA | — | libre disponibilidad ⚠ **NO HAY TIPO todavía** |
| Percepción Ganancias | DUA | — | crédito Ganancias |
| Percepción IIBB | DUA | — | crédito IIBB |
| Honorarios despachante | FC A | 21% | neto capitaliza · IVA crédito |
| TCA / terminal | FC A | 21% | neto capitaliza · IVA crédito |
| Almacenaje / depósito fiscal | FC A | 21% | neto capitaliza · IVA crédito |
| Flete interno al depósito | FC A | 21% | neto capitaliza · IVA crédito |
| Coima / gestiones | — | — | capitaliza · sin crédito · sale de caja |

> Las **alícuotas concretas varían por posición arancelaria y régimen**. Hay que confirmarlas con el despachante en cada despacho.

### Ciclo del despacho de importación (decisión 5/8/2026)

**El lote debe nacer con el costo estimado completo del simulador, no con el FOB pelado.**

Fundamento con datos reales:
- En despachos es **normal vender antes de tener todas las facturas** (dato del usuario).
- La mercadería **empieza a consumirse a los 1-5 días** del alta del lote (medición del 5/8, ver `ADARA-COSTEO-FIFO.md`).
- Si el lote nace con FOB pelado el error inicial del costo es de **30-40%**; con el costo del simulador, de **2-5%**.

**Secuencia:**
1. **Costo estimado** (simulador) al ingresar la mercadería → nace el lote.
2. Las **facturas reales se cargan como gastos vinculados** a medida que llegan (`capitaliza_compra_id`).
3. Al **cerrar el despacho** se compara estimado vs real y se **ajusta la diferencia**.

**Momento de confirmar:** cuando la **mercadería llega al depósito** (decisión del usuario). Salvedad: el **crédito fiscal del DUA se devenga unos días antes** → hace falta una **fecha de DUA separada de la fecha de recepción**.

### Compras cargadas el 5/8 (casos testigo)

- **#9 — Jukebox S.A., FC A 0005-00267117 del 24/7 — $6.431.377,79.** Cargada **con el bug activo**: hubo que agregar por SQL el componente `flete` de $38.820,80 y un segundo renglón `iva` de $8.152,37, y corregir a mano los lotes 150 y 151 a $503.531,90 y $629.032,26.
- **#10 — Jukebox S.A., FC A 0005-00267118 del 24/7 — $8.937.538,74.** Primera compra que entró **completa y sin corrección manual** tras el fix: flete de $59.827,20 como `flete`, IVA de $832.521,48 sumando las dos alícuotas ($819.957,77 al 10,5% + $12.563,71 al 21%), percepción IIBB CABA $236.068,46. Los 3 lotes salieron con el prorrateo ya adentro: **AI001N $1.100.931,12 · NT004 $503.911,21 · TA014G $629.506,10**. **Valida el prorrateo automático, que nunca se había podido ejecutar.**
- **#11 — Stylus S.A., FC A-0011-00288835 del 24/7 — $27.756.411,74.** 9 renglones, envío de $15.600 tildado como **"de esta factura"** (Stylus lo incluye en la base del IVA 21%), 3 percepciones de IIBB (CABA 3%, Buenos Aires 0,8%, Tucumán 1,75%). **Un centavo de diferencia** contra el papel por redondeo: el proveedor calcula el IVA sobre la base total y ADARA renglón por renglón.

**Julio 2026 quedó con 4 compras por $104.803.138,21**, todas con comprobante adjunto.

---

## Actualización — 7 Agosto 2026 (compra sin comprobante — C20)

Módulo pedido por Sebastián: cargar **compras de mercadería que no tienen factura** (proveedor informal, compra en efectivo, mercadería sin documentar). Hasta ahora el workaround era cargar todos los renglones con alícuota **Exento**, pero eso deja una compra que *parece* facturada: tiene lugar para un N° de factura, se le puede asignar uno después, y nada impide meterle un componente de IVA por error.

### Tres cosas distintas que se venían confundiendo

| Concepto | Dónde vive | Qué significa |
|---|---|---|
| **Compra sin comprobante** | `compras.sin_comprobante = true` | La **compra entera** no tiene factura. Nunca va a tener número ni crédito fiscal. |
| **Factura pendiente** | `compras.nro_factura IS NULL` con `sin_comprobante = false` | La factura **existe**, todavía no llegó el número. Se asigna después con `POST /compras/:id/factura`. |
| **Componente sin factura** | `compra_componentes.tipo = 'sin_factura'` | Un **renglón de costo** sin comprobante (coima, gestión informal) **dentro de una compra que sí está facturada**. Capitaliza al lote, no da crédito. |

> ⚠️ La columna arrancó llamándose `sin_factura` y se **renombró a `sin_comprobante` a mitad de la implementación**, justamente porque `compra_componentes.tipo` ya usa `'sin_factura'` con el tercer significado de la tabla. Dos cosas distintas con el mismo nombre en tablas vecinas era deuda técnica garantizada.

### Implementación

**Base de datos** (migraciones `compras_sin_factura` → `compras_sin_comprobante_rename`):
- Columna `compras.sin_comprobante boolean NOT NULL DEFAULT false`.
- **CHECK**: si `sin_comprobante = true` entonces `nro_factura IS NULL`. No se puede tener las dos cosas.
- **Trigger `fn_chk_componente_sin_factura`** sobre `compra_componentes`: rechaza insertar cualquier componente de `clase = 'fiscal'` (`iva`, `iibb_percepcion`, `ganancias_percepcion`, `otro_impuesto`) si la compra padre está marcada `sin_comprobante`. La regla se defiende **en la base**, no solo en el endpoint: si mañana entra un INSERT por SQL o por otro camino, el crédito fantasma sigue siendo imposible.
- Migración `v_compras_ap_expone_sin_comprobante`: la vista devuelve la columna para que el frontend pueda marcar la fila.

**Backend** (`POST /compras`):
- Si viene `sin_comprobante: true`, **fuerza a 0** el IVA y todas las percepciones y **nulea** `nro_factura` antes de insertar. No confía en que el frontend lo haya hecho.
- `POST /compras/:id/factura` responde **409** si la compra es `sin_comprobante`: asignarle un número sería exactamente la contradicción que el CHECK impide.

**Frontend** (`compras.js`):
- Tilde **"Compra sin comprobante"** en el alta. Al tildarlo se deshabilitan y ponen en cero el N° de factura, el selector de alícuota de cada renglón y el repeater de percepciones (variable de cierre `SIN_COMP` + `aplicarSinComp()`).
- Badge **`sin comprobante`** en la lista de Facturas, para que se distinga de un vistazo del chip `factura pendiente`.

### Efecto en costo, AP y posición fiscal

| Dimensión | Efecto |
|---|---|
| **Costo del lote** | Entra **completo**. El costo real de la mercadería no depende de que haya papel. |
| **Cuenta corriente / AP** | **Sí suma.** La deuda con el proveedor existe igual y hay que pagarla. |
| **Crédito fiscal** | **Cero.** Ni IVA, ni IIBB, ni Ganancias. Garantizado por el trigger. |
| **Posición fiscal** | No aporta crédito. Ver `ADARA-IMPUESTOS.md`. |
| **P&L interno vs fiscal** | Igual que los gastos sin factura: el P&L interno la incluye como costo real, el fiscal la excluye. |

### Lo que esto NO resuelve

- **La venta correlativa.** Comprar sin factura y vender con factura deja débito sin crédito; comprar con factura y vender sin factura deja crédito sin débito (ese es el caso de la venta en efectivo, ver `ADARA-VENTAS-EFECTIVO.md`). ADARA registra los dos lados como son; el tratamiento fiscal es criterio del contador.
- **La salida de caja.** Hoy la compra sin comprobante no genera automáticamente el movimiento de caja del pago — se concilia como cualquier otra compra. El automatismo de "gasto sin factura paga de caja" que describe la sección de arriba sigue pendiente.

---

## Actualización — 10 Agosto 2026 (verificación C20 + deuda con terceros + costo fiscal de comprar sin comprobante + CMV retroactivo)

### (a) Verificación del circuito de compra sin comprobante (C20)

Verificado **contra la base y contra el repo**. El circuito está **sano y completo**:

| Pieza | Estado |
|---|---|
| Columna `compras.sin_comprobante` (`boolean NOT NULL DEFAULT false`) | ✅ |
| CHECK `chk_sin_comprobante_sin_nro` — `(NOT sin_comprobante) OR (nro_factura IS NULL)` | ✅ |
| Trigger `trg_componente_sin_factura` **BEFORE INSERT OR UPDATE** sobre `compra_componentes`, ejecutando `fn_chk_componente_sin_factura()` | ✅ (cubre también el UPDATE, no solo el alta) |
| `POST /compras` — fuerza IVA y percepciones a 0 y devuelve **400** si le mandan N° de factura | ✅ |
| `POST /compras/:id/factura` — devuelve **409** | ✅ |
| Tilde + badge en `compras.js` | ✅ |
| `v_compras_ap` expone la columna | ✅ |

> ⚠️ **Todavía hay 0 compras cargadas con `sin_comprobante = true`.** El circuito está construido y probado a nivel estructura, pero **nunca se ejerció con datos reales**.

### (b) HALLAZGO ESTRUCTURAL: no hay dónde poner la deuda con los terceros de una compra

`v_compras_ap` calcula `total_facturado_ars` **EXCLUYENDO** `sin_factura`, `gasto_prorrateable` y `extra_directo`, pero **`pagado_ars` suma TODOS los vínculos de la compra**.

**Consecuencia:** si se cargan el pasero / flete / comisión como **componentes** y después se **vinculan esos pagos a la compra**, el **saldo de cuenta corriente queda NEGATIVO** y el proveedor de la mercadería figura **sobrepagado**.

#### La arquitectura correcta: SEPARAR COSTO DE DEUDA

Verificado: **las tres vistas ya la soportan**, no hace falta migración.

| Qué | Dónde se registra | Para qué sirve |
|---|---|---|
| **Costo** (para que el lote nazca completo) | **Componentes de la compra**: el cargo asignado a un SKU puntual como `extra_directo`, los repartibles como `gasto_prorrateable` | Capitalizan al lote y **no inflan el AP** del proveedor de la mercadería |
| **Deuda y pago de cada tercero** | Un **`gasto` por tercero** con `capitaliza_compra_id` apuntando a la compra, `sin_comprobante`, **sin crédito IVA**, con **su propio proveedor** | Tiene **AP propio en `v_gastos_ap`**; su pago se vincula con **`op_tipo='gasto'`** |

#### Verificado que NO hay doble conteo

| Vista | Comportamiento | Efecto |
|---|---|---|
| `v_gastos_mensual` | **filtra `capitaliza_compra_id IS NULL`** | el gasto capitalizable **no duplica el costo en el P&L** (ya está en el CMV vía el lote) |
| `v_gastos_ap` | **no filtra** | la **deuda con el tercero sí existe** |
| `v_compras_ap` | **excluye** los componentes de terceros | **no inflan el AP** del proveedor de la mercadería |

#### Deuda técnica que se asume

Cada monto se carga **DOS veces**: una como **componente de la compra** (para costear el lote) y otra como **gasto** (para la deuda y el pago). **Nada avisa si divergen.**

> ⏳ **Pendiente:** un endpoint que haga las dos cosas en **una sola operación** (alta de "cargo de tercero sobre una compra" → componente + gasto, atómico y consistente).

### (c) Regla nueva 17 — costo fiscal de comprar sin comprobante y vender con comprobante

Comprar sin comprobante y vender con comprobante cuesta, **por cada peso no documentado**, la **alícuota de IVA** más la **tasa de Ganancias**:

| Alícuota de IVA de la mercadería | Costo por peso no documentado |
|---|---|
| **10,5%** + Ganancias 35% | **45,5%** |
| **21%** + Ganancias 35% | **56%** |

**No incluye IIBB**, que lo empeora.

**Consecuencia práctica.** Una oferta de **"50% facturado / 50% en efectivo"** solo conviene con un **descuento del 45,5% sobre la parte en efectivo** — es decir **22,75% sobre el total de la compra**. **Ningún proveedor ofrece eso.**

Solo tendría sentido si **esa mercadería también se vende sin facturar**, y **hoy no hay canal para eso a escala** (ver `ADARA-VENTAS-EFECTIVO.md`).

> ⚠️ **A confirmar con el contador:** la tasa del **35%** es la **marginal de sociedades**. El número sigue pendiente de validación.

### (d) Impacto del CMV congelado al cargar compras con fecha retroactiva

**Situación:** el CMV está **congelado hasta el 8/8/2026** — no hay mes abierto.

**Pero `fn_consumir_fifo(p_desde, p_hasta)` es re-ejecutable.** Su **idempotencia es por ítem**: `v_rem = cantidad − suma de consumo_lote`. Por lo tanto:

- Los ítems con **CMV estimado** (que **no tienen filas en `consumo_lote`**) **SÍ** los consume un **lote nuevo con `fecha_alta` vieja**.
- `v_resultado_mensual` **prioriza FIFO real sobre congelado**.

**Conclusión:** cargar una compra con **fecha vieja SÍ puede recuperar el CMV real de ventas pasadas**.

**PERO** el hook del `/ml/sync` corre con **ventana de 7 días**, así que **nunca va a re-costear un mes viejo solo**: es un **paso manual y deliberado**, y **mueve el resultado de ese mes**.

> ⚠️ **Si no se corre:** el lote **queda entero en stock** y, si esa mercadería **ya se vendió**, el **stock queda inflado**.

Ver `ADARA-COSTEO-FIFO.md` (CMV congelado, re-costeo, costo tardío) y `ADARA-STOCK.md`.
