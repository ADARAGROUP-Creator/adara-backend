# ADARA — Schema de Base de Datos

Última actualización: 10 Agosto 2026 (**Módulo IIBB / Convenio Multilateral: 7 migraciones**. Dos tablas nuevas — **`iibb_parametros`** (un renglón por período, cargado desde el CM03 presentado, con `alicuota_efectiva` GENERATED) e **`iibb_jurisdiccion`** (los 24 renglones del CM03 de referencia) — y cuatro vistas nuevas: `v_iibb_base`, `v_iibb_determinado`, `v_iibb_jurisdiccion_mensual`, `v_impuesto_cheque_mensual`. `v_resultado_mensual` y `v_resultado_linea_mensual` se extienden **de forma aditiva**. Fix de jurisdicción en `v_retenciones_iibb` + backfill de 9.980 filas. **Gotcha confirmada en vivo: `CREATE OR REPLACE VIEW` sólo permite agregar columnas al final (`42P16`).** Detalle completo del dominio en `ADARA-IIBB-CONVENIO-MULTILATERAL.md`. Ver "Actualización 10 Agosto 2026" al final.) · 7 Agosto 2026 (**5 migraciones: ventas y compras sin comprobante**. `v_control_mensual.iva_debito` pasa a `sum(vi.iva_linea) FILTER (WHERE v.es_gravada)`; alta del canal **`efectivo`** en `canales`; `compras.sin_comprobante` + CHECK + **trigger** `fn_chk_componente_sin_factura` que rechaza componentes fiscales; `v_compras_ap` expone la columna. **Hallazgo que corrige lo documentado: `ventas.canal` tiene FK a `canales`** — no es texto libre. Ver "Actualización 7 Agosto 2026" al final.) · 5 Agosto 2026 (**Compras nacionales — 4 migraciones + verificación del schema real**: CHECK de `compra_componentes.tipo` ampliado a 14 valores; `gastos.capitaliza_compra_id`; exclusión del P&L de gastos capitalizados; `v_gastos_ap` expone la columna. **Hallazgos:** `compra_componentes.clase` es **GENERATED** → nunca mandarla en un insert (`428C9`); `lotes.costo_unitario` está **persistido**; **no hay ningún trigger** sobre lotes/compras/componentes; `gastos.estado` usa **`'activo'`**.) · 4 Agosto 2026 (`ventas_ml`: columnas de Tango Factura) · 3 Agosto 2026 (`posicion_fiscal_apertura` + `v_posicion_fiscal` con doble arrastre; 20 SKUs nuevos) · 16 Julio 2026 (`ventas_ml.importe_facturado` + `aporte_ml`) · 13 Julio 2026 (`ventas_ml.conciliacion_periodo`) · 21 Junio 2026 (`gasto_imputacion`, G12) · 17 Junio 2026 (funciones de reacondicionar, depósito `REAC`) · 16 Junio 2026 (**regla `sbGet`**: queries >1.000 filas ordenan por columna única) · 13 Junio 2026 (`v_saldo_cuenta`; `workspace_config`) · 9-10 Junio 2026 (`costo_referencia`, `cmv_estimado_congelado`, `v_control_mensual`, `v_posicion_fiscal`) · 8 Junio 2026 (columnas generadas, `fn_proyectar_ml`, `fn_consumir_fifo`, `sku_map`/`combo_map`)

Documentación técnica del modelo de datos del rediseño en Supabase (PostgreSQL). Acá vive el **cómo** del schema, no el **por qué** del negocio (eso está en `ADARA-DECISIONES.md`).

---

## Convenciones del schema

| Aspecto | Convención |
|---------|------------|
| Schema Postgres | `public` (sin prefijo) |
| Idioma | Español |
| PK por defecto | `bigint generated always as identity` |
| PK de lookups puros | `text` con el código natural (`familias`, `canales`, `categorias_gasto`) |
| Devengado (`periodo`) | Columna **generada inmutable** desde `fecha`. **Nunca usar `to_char`** — está marcada STABLE y rompe los generated columns |
| Columnas derivadas | `generated always as (...) stored`. **Una columna GENERATED nunca debe viajar en el payload de un insert/update** — `428C9` |
| Tipos monetarios | `numeric(16,2)` para ARS |
| Timestamps | `timestamptz` default `now()` |
| RLS | **ON** en todo `public` (A17): policy `authenticated`, revoke `anon`. Toda tabla/vista nueva nace así |
| Validaciones complejas | En la app. En la DB, CHECK simples, FKs **y triggers cuando el dato es fiscal** (ver `fn_chk_componente_sin_factura`) |
| Triggers | **Casi no se usan.** No hay ninguno sobre `lotes`, `compras` ni `compra_componentes`: el costo lo calcula el backend al dar de alta. **Excepción (7/8/2026):** `trg_componente_sin_factura` sobre `compra_componentes`, porque ahí lo que está en juego es crédito fiscal |
| AR/AP | **Jamás persistidos** (CB6 / PT5) |
| Mes cerrado | Vía tabla `meses_cerrados`. **Todavía NO existe** |
| Montos en `movimientos` | **Signed**: + entrada, − salida |
| Vínculos N:N | `vinculos` polimórfica: `op_tipo` + `op_id` sin FK formal |
| Snapshot en transacciones | Si un atributo del maestro puede cambiar (ej. `alicuota_iva`), se **copia** a la tabla transaccional |
| Idempotencia de orígenes externos | `UNIQUE (origen, referencia_externa)` |
| Extensiones de vistas | **Siempre aditivas al final.** `CREATE OR REPLACE VIEW` no deja renombrar ni reordenar columnas existentes (`42P16`) |
| Códigos de jurisdicción | `snake_case`, **vocabulario único** para `retenciones.jurisdiccion`, `compra_componentes.descripcion` (percepciones) e `iibb_jurisdiccion.jurisdiccion` |
| Verificar antes de codear | La doc puede no reflejar todas las restricciones reales (NOT NULL, CHECK, GENERATED, **FK**). Pasó con `skus`, `movimientos`, `compra_componentes` y **`ventas.canal`** (7/8) |

---

## Mapa de capas

| # | Capa | Estado | Tablas |
|---|------|--------|--------|
| 1 | Maestros / Dimensiones | ✅ | `familias`, `canales`, `categorias_gasto`, `lineas_negocio`, `lineas_negocio_reglas`, `cuentas`, `skus`, `proveedores`, `empleados`, `empleado_linea_pct`, `saldos_iniciales` |
| 2 | Compras / Stock | ✅ | `compras`, `compra_componentes`, `lotes`, `ajustes_inventario` |
| 3 | Ventas + CMV | ✅ | `ventas`, `venta_items`, `consumo_lote` + `v_ventas_totales`, `v_stock_check` |
| 4 | Movimientos + Conciliación | ✅ | `movimientos`, `vinculos` + `v_movimientos_estado`, `v_ventas_ar`, `v_compras_ap` |
| 5 | Gastos | ✅ | `gastos`, `gasto_imputacion`, `gasto_fiscal` + `v_gastos_ap`, `v_gastos_mensual`, `v_gastos_categoria_mensual` |
| — | Fiscal (parcial) | ✅ | `retenciones`, `posicion_fiscal_apertura`, **`iibb_parametros`**, **`iibb_jurisdiccion`** + `v_retenciones_iibb`, `v_posicion_fiscal`, `v_control_mensual`, **`v_iibb_base`**, **`v_iibb_determinado`**, **`v_iibb_jurisdiccion_mensual`**, **`v_impuesto_cheque_mensual`** |
| 6-9 | Fiscal / Reclamos / Tesorería / Cierres | ⏸ A14 | incluye `meses_cerrados` (**no existe**) |

---

## Capa 1 — Maestros

### `canales`
PK `codigo text`. **Seeds (5):** `ml`, `tienda_nube`, `b2b`, `whatsapp_efectivo`, **`efectivo`** (alta 7/8/2026).

> ⚠️ **`ventas.canal` tiene FK a esta tabla** — no es texto libre. Se descubrió al intentar insertar `'efectivo'` sin darlo de alta (`23503 ventas_canal_fkey`). Antes de usar un canal nuevo, hay que crearlo acá.
>
> **`efectivo` vs `whatsapp_efectivo`:** el segundo es para ventas off-ML que **sí** se emiten en Tango (remito) y que entrarán por el sync de ventas no-ML el día que exista. `efectivo` es **sin comprobante**. Se separaron a propósito: mezclarlas las volvería inseparables en el Resultado. Ver V1 en `ADARA-DECISIONES.md`.

### `cuentas`
`tipo` ∈ {banco, mp, caja}, `moneda` ∈ {ARS, USD}. **Seeds (6):** `supervielle_ars`, `mp_ars`, `caja_ars`, `caja_usd`, `trust_wallet`, `santi_financiera`.

**⚠ La moneda de la cuenta no se valida contra la moneda del monto** en `sin_factura_auto` — ver movimiento 16325. **El endpoint `/ventas/efectivo` sí valida** (rechaza si la cuenta no es ARS).

### `skus`
UNIQUE `codigo`. `familia` FK nullable. **`alicuota_iva`** `numeric(5,4)` not null default `0.2100` con CHECK `[0,1]` — la tasa es propiedad del producto. **`costo_referencia`** numeric nullable (CF9): fallback del CMV cuando no hay lote.

**Invariante crítico**: NO contiene `costo` ni `stock`. Ambos derivan de lotes (S7).

**Columnas reales:** `id`, `codigo` UNIQUE NOT NULL, `descripcion`, `familia` (FK nullable), `activo` NOT NULL DEFAULT true, `creado_en`, `alicuota_iva`, `costo_referencia`.

### Otras
`familias` (4 seeds) · `categorias_gasto` (17 seeds) · `lineas_negocio` · `lineas_negocio_reglas` (familia → línea, LN2) · `proveedores` (CUIT nullable, get-or-create por CUIT; **⚠ LEANVAL duplicado ids 1 y 2**) · `arca_ta` (caché del TA de ARCA) · `empleados` + `empleado_linea_pct` · `saldos_iniciales` (A2: el saldo se **deriva**, no se carga) · `workspace_config` (token ML, RLS on, solo backend).

---

## Capa 2 — Compras / Stock

### `compras` (cabecera)
- `tipo` ∈ {`local`, `importacion`, `inicial`}.
- `moneda` ∈ {ARS, USD}; **USD obliga `tc_blue`** (CHECK + C6).
- `linea_id` **nullable**.
- `estado` ∈ {`activa`, `anulada`}. **No existe** el ciclo `abierta`/`cerrada`.
- `nro_factura text` nullable = **factura diferida**. **Sin unicidad `(proveedor_id, nro_factura)`** — las compras 2 y 3 comparten número.
- **`sin_comprobante boolean NOT NULL DEFAULT false`** (7/8/2026) — ver abajo.
- **No se guarda `total` ni `estado_pago`.**

#### `sin_comprobante` (migración `compras_sin_factura` + `compras_sin_comprobante_rename`)

Distingue **"sin comprobante"** (nunca va a tener factura) de **"factura pendiente"** (`nro_factura IS NULL` y la factura va a llegar). Antes eran indistinguibles.

```sql
ALTER TABLE compras ADD COLUMN sin_comprobante boolean NOT NULL DEFAULT false;

ALTER TABLE compras ADD CONSTRAINT chk_sin_comprobante_sin_nro
  CHECK (NOT sin_comprobante OR nro_factura IS NULL);
```

> **Se renombró de `sin_factura` a `sin_comprobante` sobre la marcha.** Motivo: `compra_componentes.tipo` **ya tiene** un valor `'sin_factura'` con otro significado (la porción de una compra pagada sin comprobante, con su `cuenta_id`). Dos significados para la misma palabra en el mismo dominio es una trampa. `sin_comprobante` además es el término que ya usa `ventas.tipo_comprobante`.

**Trigger `trg_componente_sin_factura`** (BEFORE INSERT OR UPDATE en `compra_componentes`): rechaza `iva`, `iibb_percepcion`, `ganancias_percepcion` y `otro_impuesto` si la compra padre tiene `sin_comprobante = true`, con `errcode = 'check_violation'`. Va como trigger y no como CHECK porque la condición **cruza dos tablas**.

Es el mismo criterio que el filtro por `es_gravada` en `v_control_mensual`: **que el dato fiscal no dependa de que quien carga se acuerde.**

### `compra_componentes`
`tipo` enumerado (**14 valores**) cubre tres mundos:
- **Costo**: `producto`, `flete`, `seguro`, `arancel`, `tasa_estadistica`, `despacho`, `otro_costo`, `gasto_prorrateable`, `extra_directo`.
- **Sin factura** (suma al costo, sin crédito, dispara mov de caja — C7): `sin_factura`. Requiere `cuenta_id`.
- **Fiscal** (no suma al costo — C3): `iva`, `iibb_percepcion`, `ganancias_percepcion`, `otro_impuesto`.

Columna **`clase` GENERATED ALWAYS**:

```
clase = CASE
  WHEN tipo IN ('iva','iibb_percepcion','ganancias_percepcion','otro_impuesto') THEN 'fiscal'
  WHEN tipo = 'sin_factura' THEN 'sin_factura'
  ELSE 'costo'
END
```

**⚠ `clase` NUNCA debe mandarse en un insert** (`428C9`). Era el bug que impedía guardar compras con gastos prorrateables.

Para percepciones, la **jurisdicción** va en `descripcion` con código normalizado (`snake_case`), el mismo de `retenciones.jurisdiccion`. **(10/8/2026: y el mismo de `iibb_jurisdiccion.jurisdiccion` — es lo que permite cruzar determinado, retenido y percibido por provincia.)**

### `lotes`
- `cantidad_inicial` inmutable; `cantidad_actual` y `costo_unitario` son **running state**. `v_stock_check` detecta drift.
- **`costo_unitario` está PERSISTIDO, no generado**, y **no hay trigger**: lo calcula el backend al dar de alta la compra.
- **No lleva `linea_id`**.
- `deposito text` — un lote por SKU × depósito. **`deposito='REAC'`** = reacondicionar, fuera del stock vendible.
- **`fecha_alta` ordena el FIFO** e índice `(sku_id, fecha_alta)`. Ver **CF14**: define qué ventas puede costear hacia atrás.

### `ajustes_inventario`
`tipo` ∈ {`faltante`, `sobrante`, `compensacion`, `ajuste_costo`}. `motivo` obligatorio (S5). Compensación entre lotes como **par** vía `lote_contrapartida_id`.

---

## Capa 3 — Ventas + CMV

### `ventas` (cabecera)
- **`canal` → FK a `canales`** (⚠️ no es texto libre), `linea_id` NOT NULL, `linea_override` boolean.
- `fecha` + `periodo` (generado inmutable).
- `tipo_comprobante` ∈ {`factura_a`, `factura_b`, `factura_c`, `remito`, **`sin_comprobante`**}.
- **`es_gravada` GENERADA**: `tipo_comprobante IN ('factura_a','factura_b','factura_c')`. Existía desde el principio y **no la usaba nadie** hasta el 7/8/2026 (ver `v_control_mensual`). **Desde el 10/8/2026 también manda en IIBB: sólo la base gravada tributa (`v_iibb_base`).**
- `cliente_nombre` + `cliente_doc` libres.
- `referencia_externa` genérico + índice `ux_ventas_canal_refext` = `UNIQUE (canal, referencia_externa) WHERE referencia_externa IS NOT NULL`.
- `estado` ∈ {`pendiente`, `aprobada`, `entregada`, `cancelada`}.
- **No guarda totales** — viven en items, expuestos por `v_ventas_totales`.

### `venta_items`
- `cantidad` > 0, `precio_unitario_neto` (base imponible).
- `alicuota_iva` **snapshot** de `skus.alicuota_iva`, ∈ [0,1] (**fracción**: 0.21 / 0.105 / **0**).
- 5 columnas generadas: `iva_unitario`, `precio_unitario_bruto`, `neto_linea`, `iva_linea`, `bruto_linea`.

### `consumo_lote`
- `unidades`: **positivo cuando sale** del lote, **negativo cuando vuelve**.
- `tipo` ∈ {`consumo`, `reverso_cancelacion`, `reverso_devolucion`}; CHECK liga el signo. Uso real: `consumo` 1.824+ filas, `reverso_devolucion` 12.
- **`costo_unitario_al_consumir`: snapshot que CONGELA el CMV** al momento del consumo. Es la razón por la que un costo mal cargado es irreversible una vez vendido (C19).
- `fecha` + `periodo` generado: devenga el reverso en el mes correcto (P3).

### Capa 3 bis — `ventas_ml`

| Columna | Origen | Significado |
|---------|--------|-------------|
| `ml_order_id` | ML | Llave de vínculo (= `ExternalID` de Tango) |
| `pack_id` | ML | Combo real = **≥2** `ventas_ml` con el mismo `pack_id` |
| `importe_bruto` | ML sync | `total_amount` (incluye el aporte de ML embebido) |
| `aporte_ml` | ML sync | Aporte de ML a la promo |
| `importe_facturado` | ML sync | **Neto del comprador** = `bruto − aporte`. **NO es el total de Tango** |
| `nro_factura`, `tipo_factura`, `total_facturado_tango`, `tango_movimiento_id` | Tango sync | Vínculo fiscal |
| `conciliacion_periodo` | Front | `text` nullable; mes al que se difiere la venta en el eje plata |
| `shipment_id` | ML sync | Puente de bonificaciones Flex |

**⚠ `ventas_ml.estado_conciliacion/conciliado/balance_conciliacion` están SIN poblar** — no usar. **`periodo_cobro` NO es confiable** → usar `fecha_cobro`.

### Funciones de costeo

- **`fn_proyectar_ml(desde, hasta)`** — proyecta `ventas_ml → ventas/venta_items` (CF1). Idempotente. Resuelve el SKU: **combo → directo → alias código → alias título** (CF8). **Una venta cuyo SKU no resuelve queda huérfana**, fuera del Resultado.
- **`fn_consumir_fifo(desde, hasta)`** — consume lotes con `cantidad_actual>0 AND fecha_alta<=venta.fecha`, orden `fecha_alta,id`. Idempotente por `venta_item`. **Es canal-agnóstica** (toma cualquier venta `aprobada`/`entregada`) y **su `unidades_faltantes` es GLOBAL por rango**, no por venta. Ver `ADARA-COSTEO-FIFO.md`.
- **`fn_revertir_devolucion(ml_order_id, fecha)`** — reverso al lote original con costo snapshot.
- **`fn_reconciliar_ml_canceladas()`** — P13.
- **`fn_congelar_cmv_estimado()`** — CF10, idempotente.
- **`fn_devolucion_a_reacondicionar` / `fn_reacondicionado_a_venta`** — depósito `REAC` (S8).
- El sync encadena: **`fn_proyectar_ml → fn_reconciliar_ml_canceladas → fn_consumir_fifo → fn_congelar_cmv_estimado`**.

### Vistas de ventas / costeo

- `v_ventas_totales` · `v_stock_check` · `v_cmv_mensual` · `v_valorizacion_stock` · `v_margen_ventas` (flag `costeada`) · `v_skus_sin_costo`.
- **`v_costo_sku_actual`** = `COALESCE(promedio ponderado de lotes con costo>0, skus.costo_referencia)`. El lote real tiene prioridad (CF9).
- **`v_resultado_mensual`** — por `periodo` × `linea` × **`canal`**. CMV: FIFO real → congelado → dinámico. Columna `devoluciones` (P13). **Extendida el 10/8/2026 con `base_gravada`, `iibb_determinado`, `impuesto_cheque` e `iibb_base_confirmada` (al final, aditivas).**
  > ⚠️ Hace `LEFT JOIN ventas_ml ON vm.ml_order_id = b.referencia_externa`. Por eso la referencia de una venta no-ML **nunca puede ser numérica pelada**: una colisión arrastraría comisiones de ML.
- **`v_control_mensual`** — drill-down por período + **fuente única del IVA** para Resultado y Posición Fiscal. Ver abajo el cambio del 7/8.
- `cmv_estimado_congelado` + `fn_congelar_cmv_estimado()` (CF10).
- `sku_map` / `combo_map` (CF8).

---

## Capa 4 — Movimientos + Conciliación

### `movimientos`
`monto` **signed** (USD nativo en cuentas USD). `origen` text (`'manual'`, `'supervielle'`, `'mp_account_statement'`, `'sin_factura_auto'`, **`'venta_efectivo'`**). **`referencia_externa` y `categoria` NOT NULL.** **UNIQUE (`origen`, `referencia_externa`)**. `linea_id` nullable (uso acotado a huérfanos, LN9).

### `vinculos`
`op_tipo` ∈ {`venta`, `venta_ml`, `compra`, `gasto`, `reclamo`, `transferencia`, `ajuste`} (CHECK). `op_id` bigint **sin FK formal**. **`monto`: MAGNITUD POSITIVA imputada** (nunca el signo del movimiento). UNIQUE `(movimiento_id, op_tipo, op_id)`.

> `op_tipo='transferencia'` existe en el CHECK pero **no hay tabla ni pantalla detrás**.

### Vistas
- `v_movimientos_estado`: `saldo_pendiente = abs(monto) − Σ vinculos.monto`, estado con tolerancia 0,02.
- `v_ventas_ar` / **`v_compras_ap`**: AR/AP en vivo. `v_compras_ap` excluye del total facturado `sin_factura`, `gasto_prorrateable` y `extra_directo`; **`flete` SÍ suma** (viene en la factura del proveedor). Expone `nro_factura`, `notas`, `capitaliza_compra_id` y **`sin_comprobante`** (7/8).
- `v_cc_devoluciones`: una fila por (venta, mes), por `mes_imputacion` (P3).

---

## Capa 5 — Gastos

### `gastos`
`tipo_comprobante` ∈ {`factura_a`,`factura_b`,`factura_c`,`ticket`,`sin_factura`}. **`genera_credito_iva` generated** = `tipo_comprobante='factura_a' AND monto_iva>0`. CHECK G5: no factura A → `monto_iva=0`. **`estado` usa `'activo'`** (masculino).

**`capitaliza_compra_id`** (nullable, FK `compras`) — gasto cuyo neto capitaliza al lote: **fuera del P&L**, **dentro del AP**, IVA como crédito del período de SU factura (C15).

### `gasto_imputacion` (G12)
`linea_id` NOT NULL + `canal` nullable + `porcentaje` (Σ=100 validado por backend + `v_gasto_imputacion_check`).

### `gasto_fiscal`
`tipo` ∈ ret/perc; `clase` generated. `a pagar = bruto + percepciones − retenciones` (G10).

---

## Adjuntos · Retenciones · Fiscal

- **`adjuntos`** — polimórfica (`op_tipo`+`op_id`), bucket privado `comprobantes`.
- **`retenciones`** — settlement de MP. `UNIQUE (mp_source_id, transaction_type, detail, financial_entity)`. `periodo` es el del **release**, no el de la venta. + `v_retenciones_iibb`.
- **`posicion_fiscal_apertura`** — una fila por mes de corte = total de la DDJJ (I9). Seed junio 2026 (F.2051, tx 1182373081): débito 70.183.307,66 / crédito 50.415.389,17 / **saldo_favor 13.808.267,01** / a_pagar 0 / **libre_disponibilidad 26.306,80**.
- **`v_posicion_fiscal`** — mantiene las 7 columnas originales y agrega **al final** `saldo_tecnico_favor`, `libre_disponibilidad`, `estado_fiscal`. Corte = `max(periodo)` de la apertura. Arrastre recursivo:
  - `a_pagar_técnico = máx(0, débito − crédito − técnico_previo)`
  - `saldo_tecnico_favor = máx(0, técnico_previo + crédito − débito)`
  - `iva_a_pagar = máx(0, a_pagar_técnico − libre_previa − ret_mes)`
  - `ret_mes` = **0** hoy (placeholder → FISC-RET-IVA).

> **Nota 7/8/2026:** el saldo técnico a favor **se consumió íntegro en julio** — quedó en $0,00.

### IIBB / Convenio Multilateral (10/8/2026)

> **El detalle completo del dominio — cómo se lee el CM03, cómo se deriva la alícuota, qué significa cada columna de las vistas y cómo se interpreta la cobertura — vive en `ADARA-IIBB-CONVENIO-MULTILATERAL.md`.** Acá va sólo el schema.

Ambas tablas nacen con el patrón estándar: **RLS habilitada** + policy `auth_all for all to authenticated using (true) with check (true)`.

#### `iibb_parametros` — un renglón POR PERÍODO

Se carga desde **cada CM03 presentado**. No se estima ni se tipea a mano una alícuota.

| Columna | Tipo | Notas |
|---|---|---|
| `id` | `bigserial` | PK |
| `periodo_referencia` | `text NOT NULL UNIQUE` | ej. `'2026-06'` |
| `determinado_total` | `numeric NOT NULL` | CHECK `<> 0` |
| `base_referencia` | `numeric NOT NULL` | CHECK `> 0`. Es la base imponible **TOTAL declarada** (todas las líneas, no sólo ML) |
| `base_confirmada` | `boolean NOT NULL DEFAULT false` | mientras esté en `false`, la alícuota es un **techo** |
| `alicuota_efectiva` | `numeric` | **GENERATED ALWAYS AS (determinado_total / base_referencia) STORED** |
| `origen` | `text` | de qué CM03 salió |
| `vigente` | `boolean NOT NULL DEFAULT true` | |
| `notas` | `text` | |
| `creado_en` | `timestamptz` | |

> ⚠️ `alicuota_efectiva` es GENERATED → **nunca viaja en el payload de un insert/update** (`428C9`).

#### `iibb_jurisdiccion` — los 24 renglones del CM03 de referencia

| Columna | Tipo |
|---|---|
| `id` | `bigserial` PK |
| `periodo_referencia` | `text NOT NULL` |
| `jurisdiccion` | `text NOT NULL` |
| `determinado_ref` | `numeric` |
| `valores_restan` | `numeric` |
| `valores_suman` | `numeric` |
| `a_favor_contribuyente` | `numeric` |
| `a_favor_fisco` | `numeric` |

`UNIQUE (periodo_referencia, jurisdiccion)` · índice **`ix_iibb_jur_periodo`**.

> **Los códigos de jurisdicción usan el MISMO vocabulario `snake_case`** que `retenciones.jurisdiccion` y que las percepciones de compra (`compra_componentes.descripcion` con `tipo='iibb_percepcion'`). Es la condición para que determinado, retenido y percibido **crucen por provincia**.

#### Vistas nuevas

| Vista | Qué devuelve |
|---|---|
| **`v_iibb_base`** | Base imponible por **período × `linea_id` × canal**. Sólo ventas gravadas (`filter (where v.es_gravada)`) y estados `'aprobada'`/`'entregada'` |
| **`v_iibb_determinado`** | `periodo`, `base_gravada`, `alicuota_efectiva`, `base_confirmada`, `origen_periodo`, `iibb_determinado`, `alicuota_del_periodo`, `base_declarada`, `cobertura_pct`, `facturacion_fuera_de_adara`. **Si no hay fila de `iibb_parametros` para el período, hereda la alícuota del último disponible y marca `alicuota_del_periodo = false`** |
| **`v_iibb_jurisdiccion_mensual`** | `periodo` × `jurisdiccion` con `determinado`, `retenido`, `percibido`, `saldo`, `pct_del_total`. El determinado se reparte con el factor **`determinado_ref_j / base_referencia`**, que es coeficiente × alícuota |
| **`v_impuesto_cheque_mensual`** | `periodo`, `impuesto_cheque`, desde `retenciones where tipo='impuesto_cheque'` |

#### Vistas extendidas (aditivas)

Las dos se extendieron **de forma ADITIVA**: todas las columnas previas conservan **nombre, orden y semántica**, y las nuevas van **al final**, de modo que el frontend viejo no se rompe.

- **`v_resultado_mensual`** suma `base_gravada`, `iibb_determinado`, `impuesto_cheque`, `iibb_base_confirmada`. Los dos costos vienen con **SIGNO NEGATIVO**, igual que `comision`/`envio`/`costo_financiero`/`impuestos`.
- **`v_resultado_linea_mensual`** suma `base_gravada`, `iibb_determinado`, `impuesto_cheque`, `margen_contribucion_real`, `resultado_operativo_real`, `iibb_base_confirmada`. **Las columnas `margen_contribucion` y `resultado_operativo` viejas quedan intactas por compatibilidad.**

El **prorrateo entre línea y canal** va por peso de base gravada dentro del período, con window function:

```sql
sum(sum(base_gravada)) over (partition by periodo)
```

…y `coalesce` sobre el peso, para el caso de un período **sin ninguna venta gravada** (división por cero / NULL).

#### Fix en `v_retenciones_iibb`

La vista ahora deriva la jurisdicción así, para no depender de que la ingesta escriba bien el campo:

```sql
case when tipo = 'iibb_tucuman' then 'tucuman'
     else coalesce(jurisdiccion, financial_entity) end
```

Además se hizo **backfill de `retenciones.jurisdiccion = 'tucuman'`** en las filas con `tipo='iibb_tucuman'` — **9.980 filas**.

---

## Invariantes transversales

1. `lotes.costo_unitario` solo cambia vía `ajustes_inventario` con motivo (C1) o por el recálculo explícito de un costo tardío (CF13). **Está persistido y no hay trigger** — lo escribe el backend.
2. `consumo_lote` **siempre** hace snapshot del costo del lote (S2).
3. `compra_componentes` con `clase='fiscal'` generan entries en el ledger fiscal.
4. `compra_componentes` con `tipo='sin_factura'` generan movimiento `origen='sin_factura_auto'` + vínculo.
5. AR/AP **no se persisten** (CB6, PT5).
6. Toda tabla transaccional nueva debe tener `fecha` + `periodo` (generado).
7. `venta_items.alicuota_iva` **siempre** snapshot de `skus.alicuota_iva`.
8. Deducciones de marketplace → vínculos (mini-capa pendiente).
9. Cuando exista `meses_cerrados`, ninguna inserción impacta un período cerrado. **No existe.**
10. La línea de un `movimiento` no se almacena: se hereda del vínculo (CB6), salvo huérfanos (LN9).
11. **Costeo del CMV**: FIFO real → congelado → dinámico.
12. **Una venta ML sin SKU resoluble no se proyecta** (CF1/CF8).
13. Toda tabla/vista nueva nace con **RLS on + policy `authenticated`** (A17).
14. **El dato fiscal de la venta ML vive en `ventas_ml`**; la tabla `facturas_tango` **no existe**.
15. **Una columna GENERATED nunca viaja en el payload** (`428C9`).
16. **Un gasto con `capitaliza_compra_id` no es gasto del P&L** pero **sí es deuda** y su IVA **sí es crédito**.
17. **(7/8/2026) El IVA débito depende del TIPO DE COMPROBANTE, no de la alícuota del ítem.** `v_control_mensual` filtra por `es_gravada`.
18. **(7/8/2026) Una compra con `sin_comprobante = true` no puede tener componentes fiscales ni `nro_factura`.** Lo garantizan un CHECK y un trigger, no sólo el front.
19. **(10/8/2026) Sólo la base GRAVADA genera IIBB.** `v_iibb_base` filtra por `es_gravada`, igual que el IVA débito (V2). Una venta sin comprobante no se declara y no tributa.
20. **(10/8/2026) Toda extensión de una vista es ADITIVA al final.** Renombrar o intercalar columnas rompe con `42P16`; además, las columnas viejas son contrato con el frontend desplegado.

---

## Gotchas técnicas

1. **`to_char` no es IMMUTABLE** — no funciona en generated columns.
2. **`fecha::text` tampoco** (depende de `DateStyle`).
3. **Generated columns deben ser `STORED`**.
4. **PostgREST expone solo el schema `public`**.
5. **Identity columns** no admiten valor del cliente en INSERT.
6. **`round(numeric, integer)` SÍ es IMMUTABLE**.
7. **Vínculos polimórficos = sin FK formal en `op_id`** (deliberado).
8. **CHECKs cross-table no se enforcen con CHECK simple** — van a app o **trigger** (caso `sin_comprobante`).
9. **La doc del schema puede ir atrás de la tabla real.** Y una feature documentada como implementada **puede no existir en la base** (los tipos `gasto_prorrateable`/`extra_directo` figuraban desde el 11/6 pero el CHECK no los admitía).
10. **`create or replace view` no deja renombrar ni intercalar columnas** — las nuevas van **al final** (`42P16`). Pasó con `v_posicion_fiscal`, `v_gastos_ap` y `v_compras_ap`.
11. **Un CTE que modifica datos no ve sus propios efectos en el mismo statement.**
12. **`fn_proyectar_ml` es idempotente** — re-proyectar tras dar de alta SKUs solo agrega lo que faltaba.
13. **Crear usuarios de Supabase Auth por SQL:** las columnas de token van en `''` (no NULL) o GoTrue tira "Database error querying schema".
14. **Mandar una columna GENERATED en el insert = `428C9`.**
15. **(7/8/2026) `CREATE TEMP TABLE ... AS INSERT ... RETURNING` no es válido.** Hay que envolverlo: `create temp table t as with x as (insert ... returning ...) select * from x;`.
16. **(7/8/2026) Un `DO $$ ... $$` es un solo statement**: si falla en el medio, revierte todo el bloque. Útil para probar triggers y CHECKs sin ensuciar datos.
17. **(7/8/2026) `BEGIN ... ROLLBACK` funciona en `execute_sql` del MCP** — es la forma de probar un circuito completo contra datos reales sin persistir nada. Se usó para validar la venta en efectivo de punta a punta antes de desplegar.
18. **(10/8/2026) La gotcha 10, confirmada en vivo.** Un intento de **insertar una columna en el medio** de `v_iibb_determinado` falló con **`42P16: cannot change name of view column`**. `CREATE OR REPLACE VIEW` sólo permite **AGREGAR columnas al final**: no deja renombrar ni reordenar las existentes. **Por eso todas las extensiones de vistas del proyecto deben ser aditivas al final** (invariante 20). Si hiciera falta reordenar, es `DROP VIEW` + recrear, con todo lo que eso arrastra en vistas dependientes y grants/RLS.

---

## Pendientes / TBD

- **Capas 6-9 en pausa por A14.**
- **`meses_cerrados` no existe** → la inmutabilidad del mes cerrado hoy es solo convención. **Primer guardarriel real (7/8):** `POST /ventas/efectivo` exige confirmación si la fecha cae en un mes anterior.
- **Tipo `iva_percepcion`** en `compra_componentes`: no existe.
- **Unicidad `(proveedor_id, nro_factura)`** en `compras`: sin restricción.
- **Validación de moneda en `sin_factura_auto`**: `server.js` no chequea. (El endpoint de venta en efectivo sí.)
- **Notas de crédito de compra**: sin soporte.
- **Mini-capa ML — deducciones de marketplace** como vínculos.
- **Soporte multi-currency en pagos** (`v_compras_ap` asume ARS).
- **FISC-RET-IVA** — alimentar `ret_mes` de `v_posicion_fiscal`.
- **FISC-CRED-ML** — crédito IVA de comisiones/envíos de ML sin duplicar el costo.
- **Conciliación fiscal Tango vs ADARA** — calculable en SQL; UI pendiente.
- **Sync Tango de ventas no-ML** — 4 de las 5 líneas no pueden entrar al sistema.
- **(10/8) `iibb_parametros.base_confirmada` sigue en `false` para 2026-06** — falta la base imponible TOTAL declarada del CM03. Hasta que llegue, `alicuota_efectiva` es un **techo**.
- **(10/8) Posible `iibb_parametros` por ACTIVIDAD.** Si la DDJJ declara más de una actividad con alícuotas distintas, la tabla necesita un renglón por actividad además de por período. Es un cambio chico si se define ahora.
- **Deuda de datos abierta:** LEANVAL duplicado · movimiento 16325 · cajas sin circuito de transferencias · compras 1 y 3 sin lotes · lotes con costo 0 (compra #4, SKU 6 y 19) · **el 86,7 % del CMV de julio es estimado**.

---

## Documentos relacionados

`ADARA-DECISIONES.md` (la constitución) · `ADARA-DOCS-INDEX.md` · `ADARA-LINEAS-NEGOCIO.md` · `ADARA-COMPRAS-IMPORTACIONES.md` · `ADARA-STOCK.md` · `ADARA-GASTOS.md` · `ADARA-CONCILIACION-BANCARIA.md` · `ADARA-MOVIMIENTOS.md` · `ADARA-VENTAS-ML-V22.md` · `ADARA-ML-BONIFICACIONES.md` · `ADARA-COSTEO-FIFO.md` · `ADARA-IMPUESTOS.md` · `ADARA-RETENCIONES-IIBB.md` · `ADARA-IIBB-CONVENIO-MULTILATERAL.md` · `ADARA-TFACTURA.md` · `ADARA-AUTH.md` · `ADARA-ADJUNTOS.md` · `ADARA-VENTAS-EFECTIVO.md`

---

## Actualización — 11 Junio 2026 (RLS)

**28 tablas + 18 vistas** con `ENABLE ROW LEVEL SECURITY` + policy `auth_all FOR ALL TO authenticated` + `REVOKE ALL FROM anon`. `service_role` bypassa. **Patrón para lo nuevo:** RLS + policy `authenticated` + revoke `anon`.

> **⚠ Corrección 5/8/2026:** la nota del 11/6 sobre los tipos `extra_directo`/`gasto_prorrateable` documentaba algo **no aplicado** — el CHECK no los admitía y no había ninguna fila. Se aplicó recién el 5/8.

---

## Actualización — 13 Junio / 21 Junio / 13 Julio / 16 Julio 2026

- **13/6:** `saldos_iniciales.linea_id` NULLABLE + `v_saldo_cuenta`; cuenta `santi_financiera`; `ventas_ml.shipment_id`; **`workspace_config`** (token ML).
- **21/6:** bucket `comprobantes` + `adjuntos`; dominio **Flex** (4 tablas + 3 vistas); **`gasto_imputacion`** (G12) y `gastos.linea_id` deprecado.
- **13/7:** `ventas_ml.conciliacion_periodo` — **SOLO eje plata** (S9).
- **16/7:** `ventas_ml.importe_facturado` + `aporte_ml`. Clave: el facturado vive a **nivel pago** (`transaction_amount`), no a nivel orden.

---

## Actualización — 3 Agosto 2026 (apertura fiscal + 20 SKUs)

Tabla `posicion_fiscal_apertura` + `v_posicion_fiscal` con doble arrastre (ver arriba). 20 SKUs nuevos que dejaban **272 órdenes / $187,6M** fuera del Resultado: tras el alta y la re-proyección, el neto de julio pasó de $214M a **$384M**.

**Recordatorio operativo (CF1/CF8):** cuando aparezcan ventas ML de SKUs no catalogados, `fn_proyectar_ml` las saltea. Diagnóstico:

```sql
select vm.sku, count(*), sum(vm.importe_bruto) from ventas_ml vm
where vm.periodo='YYYY-MM' and vm.ml_status in ('paid','partially_refunded')
  and not exists (select 1 from ventas v where v.canal='ml' and v.referencia_externa=vm.ml_order_id)
group by vm.sku;
```

---

## Actualización — 4 Agosto 2026 (Tango Factura en `ventas_ml`)

Columnas `nro_factura`, `tipo_factura`, `total_facturado_tango`, `tango_movimiento_id`. Las llena `syncTangoFacturas` matcheando `ml_order_id = ExternalID` (fallback `pack_id`).

**Separación crítica:** `importe_facturado` la escribe SOLO el sync de ML; `total_facturado_tango` SOLO el de Tango. Al principio se reusó la misma columna y los dos syncs se pisaban.

---

## Actualización — 5 Agosto 2026 (compras nacionales: 4 migraciones + verificación del schema real)

| Migración | Qué hace |
|---|---|
| `ampliar_tipos_compra_componentes` | El CHECK de `tipo` pasa a admitir los **14** valores (suma `gasto_prorrateable` y `extra_directo`) |
| `gastos_capitaliza_compra` | `gastos.capitaliza_compra_id` + índice parcial |
| `excluir_gastos_capitalizados_del_pnl` | `v_gastos_mensual` y `v_gastos_categoria_mensual` agregan `AND g.capitaliza_compra_id IS NULL`. **`v_gastos_ap` NO lo filtra** |
| `v_gastos_ap_expone_capitaliza` | La vista expone la columna, **al final** por `42P16` |

### Hallazgos sobre el schema real
`clase` es **GENERATED** (`428C9` — era el bug de los prorrateables) · `lotes.costo_unitario` **persistido** · **no hay triggers** sobre lotes/compras/componentes · `gastos.estado` usa **`'activo'`** · `compras.estado` solo `activa`/`anulada` · `v_compras_ap`: **`flete` sí suma** al AP · `adjuntos` es polimórfica.

### Deuda de datos
LEANVAL duplicado (ids 1 y 2, mismo CUIT, ambos en uso) · movimiento 16325 ($2.000.000 en `caja_usd`) · cajas en negativo · compras 1 y 3 con componentes de producto y **cero lotes** · lotes con `costo_unitario = 0` en la compra #4.

---

## Actualización — 7 Agosto 2026 (ventas y compras sin comprobante: 5 migraciones)

| Migración | Qué hace |
|---|---|
| `iva_debito_solo_ventas_gravadas` | `v_control_mensual.iva_debito` pasa de `sum(vi.iva_linea)` a **`sum(vi.iva_linea) FILTER (WHERE v.es_gravada)`** |
| `canal_efectivo_sin_comprobante` | Alta del canal **`efectivo`** en `canales` |
| `compras_sin_factura` | `compras.sin_factura` + CHECK + trigger `fn_chk_componente_sin_factura` |
| `compras_sin_comprobante_rename` | Renombre a **`sin_comprobante`** + CHECK renombrado + trigger actualizado |
| `v_compras_ap_expone_sin_comprobante` | La vista expone la columna (al final, `42P16`) |

### El filtro por `es_gravada` — por qué y con qué riesgo

`ventas.es_gravada` es una columna generada que existía **desde el principio** y **no la usaba nadie**. `v_control_mensual` sumaba `iva_linea` **sin filtrar**, así que el IVA débito dependía de la alícuota tipeada en el ítem, no del tipo de comprobante.

Con la llegada de las ventas sin comprobante eso pasaba a ser un agujero concreto: una venta no facturada cargada por error con 21 % habría sumado **débito fiscal fantasma**, aumentando el IVA a pagar sin que nada avisara.

**Verificación antes de aplicar:** con los datos existentes (8 períodos, 18.259 ventas, **todas `factura_b`** → `es_gravada = true`) la diferencia es **$0,00 en todos los períodos**. Regresión cero.

> **Sólo se filtró el agregado de `iva_debito`.** Poner `AND v.es_gravada` en el `WHERE` del CTE `val` habría filtrado también `neto`, `ordenes_validas` y `bruto_validas`, rompiendo el panel de control. Es un `FILTER` sobre una sola función de agregación, no un filtro de la consulta.

### `ventas.canal` tiene FK — corrección a lo documentado

Se descubrió al intentar insertar `canal='efectivo'`: `23503 ventas_canal_fkey`. **`ventas.canal` referencia `canales(codigo)`** — antes este doc no lo decía y el CHECK-only sugería texto libre.

**Implicancia:** un canal nuevo hay que darlo de alta en `canales` **antes** de usarlo. Se verificó también que ya existía `whatsapp_efectivo`, y se decidió **no reusarlo** (ver `canales` arriba y V1 en `ADARA-DECISIONES.md`).

### Verificación de los guardarrieles

Ambos se probaron contra la base real dentro de un `DO $$ ... $$` con transacción revertida:

| Prueba | Resultado |
|---|---|
| Componente `producto` en compra sin comprobante | ✅ pasa (el stock entra igual, S10) |
| Componente `iva` | ✅ rechazado |
| Componente `iibb_percepcion` | ✅ rechazado |
| `UPDATE ... SET nro_factura` | ✅ rechazado por el CHECK |
| Compra normal con `iva` | ✅ sigue aceptando |

Y el circuito de venta en efectivo completo (venta + ítems + movimiento + vínculo + FIFO) con `BEGIN ... ROLLBACK`: `es_gravada=false`, IVA débito sin cambio, caja +$100.000, stock −2 unidades, Resultado con ingreso y CMV real, comisión ML $0.

---

## Actualización — 10 Agosto 2026 (módulo IIBB / Convenio Multilateral: 7 migraciones)

**El detalle del dominio está en `ADARA-IIBB-CONVENIO-MULTILATERAL.md`.** Acá queda el schema y las lecciones técnicas.

| Migración | Qué hace |
|---|---|
| `iibb_parametros_y_jurisdicciones` | Crea **`iibb_parametros`** (con `alicuota_efectiva` GENERATED) y **`iibb_jurisdiccion`** (24 renglones del CM03), ambas con RLS + policy `auth_all` |
| `fix_jurisdiccion_iibb_tucuman` | `v_retenciones_iibb` deriva la jurisdicción con `case when tipo='iibb_tucuman' then 'tucuman' else coalesce(jurisdiccion, financial_entity) end` + **backfill de 9.980 filas** de `retenciones.jurisdiccion='tucuman'` |
| `vistas_iibb_determinado` | Crea `v_iibb_base`, `v_iibb_determinado`, `v_iibb_jurisdiccion_mensual` y `v_impuesto_cheque_mensual` |
| `resultado_con_iibb_determinado` | `v_resultado_mensual` suma **al final** `base_gravada`, `iibb_determinado`, `impuesto_cheque` |
| `resultado_linea_con_iibb` | `v_resultado_linea_mensual` suma **al final** `base_gravada`, `iibb_determinado`, `impuesto_cheque`, `margen_contribucion_real`, `resultado_operativo_real` |
| `iibb_peso_base_null_safe` | `coalesce` sobre el peso del prorrateo, para el período sin ninguna venta gravada |
| `iibb_alicuota_por_periodo_y_cobertura` | `v_iibb_determinado` pasa a **alícuota por período** (con herencia + `alicuota_del_periodo`) y agrega `base_declarada`, `cobertura_pct`, `facturacion_fuera_de_adara`; las tres vistas de resultado exponen `iibb_base_confirmada` |

### Lo que cambia conceptualmente

El gasto de IIBB del período es el **impuesto determinado**, no la retención de ML ni las percepciones de compra — esas son **anticipos** (ver IIBB1 en `ADARA-DECISIONES.md`). El schema refleja eso: `iibb_parametros` guarda lo que dice el CM03 presentado, `v_iibb_base` mide la base gravada que ADARA sí ve, y `v_iibb_determinado` cruza las dos.

**El signo importa:** `iibb_determinado` e `impuesto_cheque` entran a `v_resultado_mensual` con **signo negativo**, igual que `comision`, `envio`, `costo_financiero` e `impuestos`. Sumarlos en positivo habría inflado el resultado en vez de reducirlo.

**El prorrateo entre línea y canal** usa el peso de base gravada dentro del período (`sum(sum(base_gravada)) over (partition by periodo)`). Es contabilidad de gestión: el coeficiente del Convenio Multilateral es de la empresa, no de la línea (IIBB6).

### La gotcha que costó una migración: `42P16`

Un intento de **intercalar una columna en el medio** de `v_iibb_determinado` falló con **`42P16: cannot change name of view column`**. `CREATE OR REPLACE VIEW` sólo permite **agregar columnas al final**.

Regla que queda: **toda extensión de vista es aditiva al final** (invariante 20). Además de la limitación de Postgres, hay una razón de producto: las columnas viejas son contrato con el frontend desplegado, y `v_resultado_linea_mensual` conserva `margen_contribucion` y `resultado_operativo` intactas justamente por eso — las versiones con IIBB adentro son columnas nuevas (`*_real`), no un reemplazo.

### Vocabulario de jurisdicciones

`iibb_jurisdiccion.jurisdiccion`, `retenciones.jurisdiccion` y la `descripcion` de las percepciones de compra comparten el mismo código `snake_case`. Sin eso, `v_iibb_jurisdiccion_mensual` no podría poner determinado, retenido y percibido en la misma fila. El fix de `iibb_tucuman` existe precisamente porque la ingesta no siempre escribía el campo: **la vista no debe depender de que el dato venga bien cargado.**

---

## Actualización — 25 Septiembre 2026 (pricing: 6 tablas propias)

Migración **`20260925120000_pricing_tablas`** (copia en `supabase/migrations/`). Paso 2b de la unificación de pricing (`ADARA-PRICING.md`). Son datos de **simulación** (PRC1): **nunca** se escriben en `skus`, `lotes` ni `iibb_parametros` (PRC2). Tasas en **porcentaje** (21 = 21 %), como `core/pricing.js`.

| Tabla | Clave | Qué guarda | Origen en pricing |
|---|---|---|---|
| `pricing_producto` | `sku_id` PK → `skus.id` (CASCADE) | `costo_sin_iva`, `iva_pct` (21 / 10,5), `categoria`, `envio_ml_monto` y `cargo_fijo_ml_monto` (**brutos**, manuales hasta el sync de publicaciones), `estado` | `products` |
| `pricing_costo_historial` | `id` identity | cada cambio de costo o IVA, con `cambiado_por = auth.uid()`; lo llena el trigger `trg_pricing_costo_historial` | `product_cost_history` |
| `pricing_tasas` | `id = 1` (una sola fila) | `iibb_pct`, `idc_pct`, `iigg_pct` (< 100), `estructura_pct` | `tax_settings` |
| `pricing_canal` | `codigo` PK | los 9 canales: `tipo`, `cuotas`, `costo_financiacion_pct`, `margen_default_pct`, `redondeo_a`, `modo_redondeo`, `aplica_*` (**NULL = default de `normalizeOption`**: ML sí, directo no), `orden` | `mercadolibre_installment_fees` |
| `pricing_comision_categoria` | `id`; UNIQUE `lower(categoria)` | `comision_pct` por categoría, `ml_category_ids` | `mercadolibre_category_fees` |
| `pricing_margen` | UNIQUE `(sku_id, canal_codigo)`; FK a `skus` y `pricing_canal` | `margen_pct`, `utilidad_neta` (manda sobre margen), `pvp_manual` (manda sobre todo), montos de estructura y envío manual, `comision_venta_pct`, `venta_con_iva`, `iva_costo_pct`, `descuento_promo_pct` | `product_channel_margins` |

- `actualizado_en` lo mantiene `fn_pricing_actualizado()` (BEFORE UPDATE). No se reutilizó `update_updated_at()` porque escribe `updated_at`.
- Seguridad: patrón post-A17 en las 6 (RLS + policy `<tabla>_authenticated` + GRANT `authenticated` + REVOKE `anon`). Verificado: 0 grants a `anon`.
- Datos iniciales: `pricing_tasas` con IIBB 5 % (el resto en 0, pendiente de copiar de pricing); `pricing_canal` con los 9 canales y los costos de cuotas MP3 8,4 · MP6 12,3 · MP9 15,7 · MP12 19,2 de la migración 037 de pricing. `pricing_producto`, `pricing_margen` y `pricing_comision_categoria` vacías.
- Verificado con una transacción de prueba revertida: el historial registra alta + cambio de costo (no registra cambios de otras columnas) y la FK a `pricing_canal` rechaza canales inexistentes.
