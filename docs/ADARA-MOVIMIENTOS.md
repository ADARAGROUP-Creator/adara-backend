# ADARA — Movimientos

Última actualización: 7 Agosto 2026 (**botón "+ Venta en efectivo" + origen `venta_efectivo` + regla CB14** — el primer movimiento de **entrada** que registra el sistema en una caja. `caja_ars` pasó de −$4.000.000 a **+$4.116.312,41**. Ver "Actualización 7 Agosto 2026" al final.) · 5 Agosto 2026 (**REGLA DEL MOVIMIENTO: lo crea quien tiene la evidencia externa** — banco y MP generan **deuda**; efectivo y dólar billete generan el **movimiento directo**. · Estado real de las cajas: solo salidas, nunca una entrada. · **BUG — moneda no validada**: el movimiento **16325** metió $2.000.000 en `caja_usd`. · **Pendiente: pantalla de transferencias entre cuentas**. · **Gap: Compras no puede registrar pagos**.) · 4 Junio 2026 (flag: "Débito por deuda Facturas vencidas" mal categorizado como `devolucion`; bundles de devolución por `op_id`)

Pantalla y reglas de la tabla `movimientos` (capa 4): el extracto unificado de todas las cuentas. Este doc cubre la **carga, importación y visualización**. La **conciliación** vive en `ADARA-CONCILIACION-BANCARIA.md`; las reglas duras, en `ADARA-DECISIONES.md` (CB1–CB14).

---

## Qué es

`movimientos` es la única tabla por la que pasa toda la plata (Supervielle, MP, caja ARS, caja USD). Cada fila es una entrada o salida real de una cuenta.

Pantalla: `public/js/screens/movimientos.js` → `loadMovimientos()`. Lista + filtros + KPIs + **importar extracto Supervielle** + **carga manual de caja** + **"+ Venta en efectivo"** + **borrado de manuales**.

---

## Regla del movimiento — lo crea quien tiene la evidencia externa (5/8/2026)

| Se paga con | Quién crea el movimiento | Qué genera la operación |
|---|---|---|
| **Transferencia Supervielle** | El **extracto**, al subirlo | Un **pendiente de pago** (deuda) |
| **Mercado Pago** | El **extracto AS**, al subirlo | Un **pendiente de pago** (deuda) |
| **Efectivo (caja ARS)** | La **app**, en el acto | El **movimiento directo** |
| **Dólares billete (caja USD)** | La **app**, en el acto | El **movimiento directo** |

**Por qué:** si la app creara el movimiento de una transferencia, el extracto después traería **el mismo pago duplicado**. Para efectivo y dólar billete no hay extracto que lo traiga nunca: si la app no lo crea, no lo crea nadie.

Es la formulación operativa de CB11 y del límite de la carga manual a cuentas de caja.

> **Corolario CB14 (7/8/2026):** cuando la app crea el movimiento **y** la operación en el mismo acto (gasto sin factura en efectivo, venta en efectivo), **el vínculo se crea junto** y el movimiento **nace conciliado**. No tiene sentido que caiga en "movimientos sin conciliar" para que alguien lo cruce a mano contra una operación que la propia app acaba de crear.

---

## Columnas reales de `movimientos` (verificadas contra Supabase)

| Columna | Tipo | Notas |
|---------|------|-------|
| `id` | bigint identity | PK |
| `cuenta_id` | bigint NOT NULL | FK `cuentas` |
| `fecha` | date NOT NULL | |
| `periodo` | text generado | mes del flujo (percibido), inmutable |
| `monto` | numeric(16,2) NOT NULL | **signed**: + entrada, − salida. En cuentas USD se guarda en **USD nativo** (TI5) |
| `origen` | text NOT NULL | `'manual'`, `'supervielle'`, `'mp_account_statement'`, `'sin_factura_auto'`, **`'venta_efectivo'`**. Texto libre, sin CHECK |
| `referencia_externa` | text **NOT NULL** | UNIQUE `(origen, referencia_externa)` |
| `categoria` | text **NOT NULL** | clasificación operativa (ver abajo) |
| `descripcion` | text | lo que dice la fuente |
| `linea_id` | bigint nullable | **solo para huérfanos** (ver Opción A) |
| `conciliado_auto` | boolean | `true` si el mov "se cierra solo" sin vínculo |
| `creado_en` | timestamptz default now() | |

Categorías convencionales: `cobro_venta`, `pago_proveedor`, `gasto`, `comision_bancaria`, `comision_marketplace`, `impuesto`, `transferencia_interna`, `devolucion`, `interes`, `ajuste_manual`.

> ⚠️ **Clasificador del AS — bug a corregir:** el AS rotula como **"Débito por deuda …"** tanto las devoluciones de venta como cargos que **NO son devolución** (**"Débito por deuda Facturas vencidas de Mercado Libre"** = facturación/publicidad de ML), que hoy caen en `categoria='devolucion'` por el prefijo. **Debería ser `gasto` / `comision_marketplace`.**
>
> **Bundles de devolución (`op_id`):** las líneas de una devolución comparten el **`op_id`** (campo 2 de `referencia_externa`) con el cobro original. El enganche a la venta es por ese `op_id`, **nunca por monto** (O10).

---

## Carga manual (v1)

Modal "Movimiento de caja". **Se limita a cuentas de caja (efectivo).** Banco y MP entran por importación de extracto (CB11). **Todos los campos son obligatorios.**

- **Tipo (Entrada/Salida)**: toggle. El usuario ingresa el monto **positivo** y la app le pone el signo.
- **Moneda**: sale de `cuentas.moneda`. En caja USD el monto se guarda en USD nativo.
- **Categoría**: obliga a elegir una real.
- `origen='manual'`.

### Dedup / duplicados

La carga manual **NO se deduplica por contenido** (dos movimientos iguales pueden ser reales). Cada carga genera una **referencia surrogate única** (`'manual-' + crypto.randomUUID()`). El seguro contra el doble-cargado es un **aviso al guardar** (no un bloqueo) si ya existe uno con misma `cuenta + fecha + monto`, más la lista pasiva "Ya cargados ese día".

### Estado

La lista lee **`v_movimientos_estado`** (única fuente de verdad): `auto` | `pendiente` | `parcial` | `conciliado` (en la UI, **"Vinculado"**).

---

## Borrado de movimientos manuales

Solo se borran los `origen='manual'`. Los **importados** son inmutables (CB2). Endpoint `DELETE /movimientos/:id` con **guarda en backend** que rechaza (`403`) cualquier origen distinto y limpia los `vinculos`.

---

## Importación de extracto Supervielle

Endpoint `POST /supervielle/import`. Acepta el **export de movimientos** por subcuenta en **.xlsx o .csv** (Fecha, Hora, Concepto, Detalle, Débito, Crédito, Saldo). Las 3 subcuentas se agregan en `supervielle_ars` (CB10).

**Mapeo:** `monto = crédito − débito` · `descripcion = concepto — detalle` · `origen='supervielle'` · `referencia_externa = fecha|hora|monto|saldo` (**idempotente**).

**Clasificación automática:** el ruido se cierra solo (`conciliado_auto=true`: impuestos, intereses, comisiones bancarias, FCI) y quedan pocos pendientes reales (Comex, ResumenVisa, pagos de servicios, cobros).

**Red de seguridad:** continuidad de saldo (`saldo[i] == saldo[i+1] + monto[i]`). Si hay saltos, avisa que faltan movimientos.

**Gotcha del CSV:** Supervielle escribe los acentos como `?` y `cellDates` confunde DD/MM. CSV se decodifica `latin1` y se parsea con `raw:true`; la detección de columnas usa regex tolerantes (`/d.?bito/`).

---

## Línea de negocio — Opción A, reabierta acotada

La línea se **hereda de la operación vinculada** (CB6). **Excepción:** los movimientos **huérfanos** sí pueden llevar `movimientos.linea_id` a mano.

- **Precedencia:** `línea = COALESCE(línea de la operación, movimientos.linea_id)`.
- **Asignación a toda la operación:** al elegir línea en un huérfano que comparte N° de operación de MP, se aplica a **todos los huérfanos de esa operación**.

### Pantalla

Una fila por movimiento (no agrupa). Filtro por mes, KPIs y pills acotados al mes. **N° de operación SOLO para MP** — en Supervielle ese campo es la **HORA** y agrupar por él fue un bug. Columna "Vinculado a" y columna "Línea" (editable en huérfanos, bloqueada "(heredada)" en los que tienen contraparte).

---

## ⚠ Guardrails técnicos

- Nunca mandar `null` en `referencia_externa` ni `categoria` (NOT NULL → `23502`).
- Carga manual: referencia surrogate única, **sin** dedup por contenido; **solo cuentas de caja**.
- Banco/MP **solo por extracto** (CB11).
- **La operación no crea el movimiento cuando el pago va por banco o MP** — genera deuda. Romper esto duplica el pago cuando entra el extracto.
- **Validar la moneda antes de crear un movimiento automático.** Hoy `server.js` toma `cuenta_origen_intencion` sin chequearlo → ya metió pesos en `caja_usd` (mov 16325). **El endpoint `/ventas/efectivo` sí valida** (rechaza si la cuenta no es ARS) — replicar ese criterio en gastos.
- **Ningún movimiento de caja se carga suelto**: nace de un par (transferencia) o de una operación. El arranque se resuelve con **saldo inicial por caja** (`saldos_iniciales` + `v_saldo_cuenta`, criterio A2).
- Idempotencia del import Supervielle: NO cambiar la fórmula de `referencia_externa` sin migrar.
- CSV Supervielle: `latin1` + `raw:true` + regex tolerante a `?`.
- Nunca borrar `origen != 'manual'`.
- `movimientos.linea_id` es **solo para huérfanos**.
- **N° de operación se agrupa SOLO para MP.**
- `monto` signed; en USD es nativo.
- La lista lee `v_movimientos_estado`.
- `sbGet` pagina obligatoriamente y debe ordenar por columna única.

---

## Historial de errores

| Fecha | Error | Causa | Fix |
|-------|-------|-------|-----|
| 28/05/2026 | Pantalla mostraba Home en `#movimientos` | `movimientos.js` quedó en `public/js/` en vez de `screens/` | Mover a `screens/` |
| 28/05/2026 | `23502` en `referencia_externa` | columna NOT NULL no documentada | Referencia surrogate única |
| 28/05/2026 | `23502` en `categoria` | columna NOT NULL no documentada | Categoría real obligatoria |
| 28/05/2026 | Toast de error sin texto visible | texto blanco sobre `var(--red)` claro | `var(--red-strong,#B42318)` |
| 02/06/2026 | Movimientos del banco agrupados sin relación | `nroOperacion()` usaba el 2.º campo para todo; en Supervielle es la HORA | Agrupar **solo** si `origen='mp_account_statement'` |
| 02/06/2026 | Fila de operación mostraba "$0" | El agrupado colapsaba a **neto**; una venta revertida da 0 | Una fila por movimiento |
| 05/08/2026 | **Mov 16325: $2.000.000 en `caja_usd`** | `server.js` no verifica que la moneda del gasto coincida con la de la cuenta | **Pendiente**: validar moneda antes de insertar; corregir el movimiento |
| 07/08/2026 | **Venta en efectivo con un dígito de menos** (precio $100.171,51 en vez de $1.000.171,51) | Error de tipeo en la carga; el precio va a mano cuando se vende al costo | Corregido por SQL: precio del ítem + monto del movimiento + monto del vínculo. **El CMV no se vio afectado: lo fija el costo del lote al consumir, no el precio de venta** |

---

## Documentos relacionados

- `ADARA-DECISIONES.md` — CB1–CB14, A2
- `ADARA-SCHEMA.md` — capa 4 (`movimientos`, `vinculos`, vistas)
- `ADARA-CONCILIACION-BANCARIA.md` — matching universal, AR/AP dinámico
- `ADARA-FRONTEND.md` — patrón de pantalla, helper `SB`
- `ADARA-GASTOS.md` — tipo `sin_factura`
- `ADARA-VENTAS-EFECTIVO.md` — la venta que crea el movimiento de entrada
- `ADARA-COMPRAS-IMPORTACIONES.md` — gap: Compras no puede registrar pagos

---

## Actualización — 5 Agosto 2026 (cajas, transferencias y el gap de pagos en Compras)

### 1. Estado real de las cajas y su causa

`caja_ars` **−$4.000.000** y `caja_usd` **−USD 2.000.000**, con solo salidas. Los 4 movimientos existentes son todos de origen **`sin_factura_auto`**. **Nunca se registró una sola entrada.**

Además: hay **un único** movimiento `transferencia_interna` en toda la base, y `op_tipo='transferencia'` **existe en el CHECK de `vinculos`** pero **no hay tabla ni pantalla detrás**.

Los saldos negativos no son un error de cálculo: son la consecuencia de que **no existe todavía la forma de meter plata en la caja**.

### 2. Bug — la moneda no se valida al crear el movimiento automático

El movimiento **16325** metió **$2.000.000 en `caja_usd`** con descripción "Alquiler deposito", 1/7/2026. `server.js` toma `cuenta_origen_intencion` **sin verificar la moneda**, y como en cuentas USD el monto se guarda en **USD nativo**, esos $2.000.000 se leen como **USD 2.000.000**.

### 3. Pendiente — pantalla de transferencias entre cuentas

Falta una pantalla que cree el **par de movimientos de forma atómica**: origen **−X**, destino **+X**, con **TC** si hay cambio de moneda y la **diferencia de cambio al P&L** (es resultado financiero, **no** costo del lote).

Cadena típica que hoy NO se puede registrar:

```
extracción de banco → caja ARS  →  cambio a USD (caja USD)  →  pago al proveedor
        ✗ falta                        ✗ falta                     ✓ funciona
```

### 4. Regla que hace que las cajas no mientan

**Todo movimiento de caja nace de un par (transferencia) o de una operación. Ninguno se carga suelto.** Para el arranque se fija un **saldo inicial por caja a una fecha de corte** (criterio A2), no se cargan las extracciones históricas una por una.

### 5. Gap — Compras no puede registrar pagos

El tipo **`sin_factura`** —el único que dispara movimiento automático de caja— **solo está en Gastos**. En **Compras** todo queda como **deuda** esperando un movimiento del extracto que, para un pago en efectivo, **nunca va a aparecer**.

---

## Actualización — 7 Agosto 2026 (venta en efectivo: la primera entrada de caja del sistema)

### Botón "+ Venta en efectivo"

Se sumó al toolbar, junto a "Importar" y "+ Movimiento de caja". Abre un modal con fecha, línea de negocio, **ítems múltiples** (SKU con buscador + cantidad + precio + subtotal en vivo), cliente opcional y total a cobrar.

No crea un movimiento suelto: postea a **`POST /ventas/efectivo`**, que es atómico y crea **venta + ítems + movimiento + vínculo + FIFO**. Detalle completo en `ADARA-VENTAS-EFECTIVO.md`.

### `origen = 'venta_efectivo'` — el primer ingreso de caja

Es el **primer movimiento de entrada** que el sistema genera en una caja. Hasta el 6/8 las dos cajas tenían **exclusivamente salidas**. Con la primera venta real, `caja_ars` pasó de **−$4.000.000 a +$4.116.312,41**.

| Campo | Valor |
|---|---|
| `cuenta_id` | `caja_ars` (validado: rechaza si la cuenta no es ARS) |
| `monto` | **positivo** — entra plata |
| `origen` | `venta_efectivo` |
| `referencia_externa` | `venta_efectivo-<venta_id>` (UNIQUE con el origen) |
| `categoria` | `cobro_venta` |
| `linea_id` | la de la venta |

Mismo patrón que `sin_factura_auto` en gastos, en el sentido contrario.

### Nace conciliado (CB14)

El vínculo `op_tipo='venta'` → `op_id = venta_id` se crea en el mismo acto. El movimiento **no cae en "sin conciliar"**: sería absurdo pedirle a alguien que lo cruce a mano contra una venta que la app acaba de crear en la misma transacción.

### Lo que esto NO resuelve

Sigue faltando lo estructural del 5/8: **las transferencias entre cuentas y el saldo inicial**. La venta en efectivo mete plata real en la caja, pero la **extracción del banco que la originó** —si la hubo— sigue sin poder cargarse. Mientras no exista el circuito de transferencias, los saldos de caja siguen sin ser leíbles como saldo: son la suma de lo poco que el sistema sabe.
