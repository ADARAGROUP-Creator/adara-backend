# ADARA — Gastos

Última actualización: 5 Agosto 2026 (**gasto capitalizable a una compra** (`gastos.capitaliza_compra_id`): el costo accesorio de un TERCERO con comprobante propio (flete, despachante, TCA) se carga como gasto vinculado a la compra — su neto se reparte entre los lotes por costo neto y sube `costo_unitario`, se **excluye del P&L** pero **no** de `v_gastos_ap`, y el IVA sigue siendo crédito con la **fecha de la factura del tercero**. Endpoint de **preview de impacto** `GET /compras/:id/impacto-capitalizacion` + confirmación obligatoria (409 `requiere_confirmar_recosteo`) si la compra ya tuvo ventas. Tabla de decisión "dónde se carga cada costo accesorio". Corrección: `gastos.estado` es `'activo'`, no `'activa'`.) · 21 Junio 2026 (**reparto por imputaciones (G12)**: un gasto se reparte en `gasto_imputacion` — línea + canal opcional + %, Σ=100; el % reparte el gasto completo (neto al P&L por línea/canal, IVA/perc/ret al ledger fiscal por línea). Reemplaza el modelo "un gasto = una línea" (deroga reglas duras 1 y 2). Editor de alta con "repartir equitativo" + control Σ=100. Vistas `v_gastos_mensual` (ahora por línea×canal) + `v_gastos_categoria_mensual` (drill-down) + `v_resultado_linea_mensual` recreadas; **fix** del crédito IVA de gastos (`v_control_mensual.iva_cred_gastos` leía `gasto_fiscal`, ahora `gastos.monto_iva`). Canal `b2b_tango`→`b2b`.) · 9 Junio 2026 (selector único de "Pago" — fusiona forma_pago + cuenta_origen; cuenta Trust Wallet USDT; aclaración honorarios monotributista ≠ sueldo) · 29 Mayo 2026 (pantalla v1 + retenciones/percepciones)

Cómo se cargan, clasifican y concilian los gastos operativos del negocio: alquiler, servicios, honorarios, software, comisiones, comidas, comunicaciones, etc.

**Reemplaza al viejo `ADARA-GASTOS.md`** que solo cubría carga manual.

---

## Objetivo

1. Capturar **todos los egresos operativos** del negocio (con o sin factura)
2. Imputar cada gasto a una **línea de negocio** y a una **categoría**
3. Vincular cada gasto a su **movimiento bancario/MP/caja** correspondiente
4. Alimentar el P&L (gastos operativos por línea) y el ledger fiscal (crédito IVA cuando aplica)

---

## Concepto: qué es un gasto y qué no

Para evitar confusión, los **gastos** son todo egreso operativo que **no es**:

| No es gasto | Dónde vive |
|-------------|------------|
| Costo de mercadería | En lotes de compras → CMV vía FIFO (ver `ADARA-COMPRAS-IMPORTACIONES.md`) |
| Sueldos | En módulo de sueldos con % por línea (ver `ADARA-LINEAS-NEGOCIO.md`) |
| Impuestos | En ledger fiscal por impuesto (ver `ADARA-IMPUESTOS.md`) |
| **Pérdidas por mercadería no vendible** | En el P&L como **rubro propio "Pérdida por mercadería"** — incluye reclamos no cubiertos (ML o proveedor) y faltantes de inventario. Ver `ADARA-RECLAMOS.md` y `ADARA-STOCK.md` |
| Devoluciones de ventas | Descuento de ventas brutas en P&L del mes del reembolso |

Todo lo demás es **gasto**.

> ⚠️ **Excepción con forma de gasto: el gasto capitalizable.** Un costo accesorio de mercadería facturado por un **tercero** (flete del depósito del proveedor al nuestro, despachante, TCA) **se carga en esta pantalla** aunque termine siendo costo de mercadería: entra como gasto con `capitaliza_compra_id` apuntando a la compra, su neto sube al costo de los lotes y sale del P&L por CMV al vender. Ver la sección dedicada más abajo.

> 💡 **Aclaración importante**: cuando una devolución llega rota y nadie cubre el reclamo (ni ML ni el proveedor), **NO es gasto operativo**. Es pérdida del costo de mercadería que pagaste y que no recuperaste. Va al P&L como rubro propio "Pérdida por mercadería no vendible". Esto permite analizar correctamente el margen sin contaminar la lectura de los gastos fijos.

> 💡 **Honorarios vs sueldos**: un contratista **monotributista que te emite factura (C) es gasto/honorarios**, NO sueldo — se carga en la pantalla de Gastos imputado a su línea (ej. Franco y Alicia → Electrónica 100%, confirmado 9/6/2026). El "módulo de sueldos con % por línea" (`empleado_linea_pct`) es solo para empleados en **relación de dependencia** (cobran por recibo, no facturan); de ahí salen las **cargas sociales** como gasto. Regla práctica: si hay factura → honorarios-gasto; si hay recibo de sueldo → módulo de sueldos.

---

## Categorías de gastos

Lista de categorías base (configurables, se pueden agregar más a futuro):

| Categoría | Ejemplos |
|-----------|----------|
| Servicios básicos | Luz (EDESUR), gas, agua, internet, ABL |
| Alquiler | Alquiler del depósito o oficina |
| Honorarios profesionales | Contador, abogado, consultor |
| Logística | Servicios de transporte que no son costo directo (mudanza interna, fletes operativos) |
| Publicidad y marketing | Ads ML, ads Facebook/Google, diseño, fotografía |
| Insumos y papelería | Cinta de embalar, etiquetas, impresoras, papelería |
| Comunicaciones | Telefonía, planes corporativos, dominios web |
| Software / SaaS | Tango Factura (AXOFT), Adobe, Slack, hosting, dominios |
| Mantenimiento | Reparaciones del depósito, limpieza, seguridad |
| Combustible | Cargas para vehículos del negocio |
| Transporte y viajes | Pasajes, taxis, peajes operativos |
| Comidas y representación | Almuerzos con clientes, café con proveedores |
| Comisiones bancarias | Mantenimiento de cuenta, comisiones por cheques, transferencias |
| Comisiones financieras | Impuesto al cheque (Ley 25413), descuento de cheques |
| Cargas sociales | Aportes patronales, ART (asociadas a sueldos pero registradas como gasto) |
| Capacitación | Cursos, libros, suscripciones a contenido |
| Otros | Lo que no encaja arriba |

Cada categoría se asigna **al cargar el gasto**, no automáticamente.

---

## Doble flujo: proactivo y reactivo

Hay dos maneras de que un gasto entre en ADARA:

### Flujo A — Proactivo (cargás vos al hacer el gasto)

Pasos:
1. Pagaste algo (efectivo, transferencia, MP, tarjeta)
2. Vas a la pantalla "Gastos" en ADARA
3. Completás:
   - Fecha
   - Categoría
   - Descripción
   - Monto bruto
   - Monto neto (sin IVA, si tenés factura A)
   - IVA (si aplica)
   - Línea de negocio
   - Forma de pago (efectivo / transferencia / MP / tarjeta)
   - Cuenta de origen (Supervielle / MP / caja ARS / caja USD / tarjeta)
   - Archivo adjunto (factura, foto del recibo, opcional)
4. Quedó como "pendiente de conciliar"
5. Cuando aparezca el movimiento bancario / extracto MP correspondiente, **ADARA matchea automáticamente** por monto + fecha y lo concilia

### Flujo B — Reactivo (aparece desde el extracto)

Pasos:
1. Subís extracto Supervielle o AS de MP
2. Aparecen movimientos sin vincular en el buzón "Mov sin conciliar"
3. Por cada movimiento que es un gasto:
   - Click en **"Pasar a gasto"**
   - Se abre el modal de creación con monto y fecha pre-llenados
   - Completás los datos restantes (categoría, línea, descripción)
   - Al confirmar, queda conciliado al instante (con el movimiento que originó)

### Cuál usar cuándo

| Caso | Flujo recomendado |
|------|-------------------|
| Pagás en efectivo o por transferencia con factura en mano | A (proactivo) |
| Cargás factura del contador apenas la recibís | A (proactivo) |
| Aparece débito automático en el extracto que no tenías cargado | B (reactivo) |
| Servicio recurrente que se debita solo (AXOFT, EDESUR, plan AFIP) | B (reactivo) o A si querés llevar control previo |
| Pago de la tarjeta — desglose en N gastos | B (reactivo, con desglose — ver abajo) |

Ambos flujos son válidos. El resultado final es el mismo: **un gasto registrado, conciliado con su movimiento, imputado a línea y categoría**.

---

## Gastos sin factura (efectivo)

Realidad operativa: hay gastos que se pagan en efectivo y **no tienen factura**. Pueden ser:

- Honorarios informales (despachantes, gestores, fletes chicos)
- Coimas / propinas
- Servicios sin comprobante
- Compras chicas sin factura
- Mantenimiento informal

### Cómo se cargan

Igual que cualquier gasto en flujo proactivo, pero:
- Marcás el checkbox **"Sin factura"**
- No se ingresa neto / IVA — solo monto total
- La cuenta de origen suele ser **caja física** (ARS o USD)
- Quedan **etiquetados como "sin factura"** para que el contador los pueda excluir en la DDJJ

### Implicancias

| P&L interno (ADARA) | P&L fiscal (contador) |
|---------------------|----------------------|
| Suma el gasto sin factura como egreso real | Excluye los gastos sin factura (no deducibles) |
| Refleja la realidad económica del negocio | Refleja la realidad declarable a AFIP |

(Lo mismo aplica para gastos sin factura como parte de **compras / importaciones** — ver `ADARA-COMPRAS-IMPORTACIONES.md`.)

---

## Gasto capitalizable a una compra (`gastos.capitaliza_compra_id`)

**Qué modela:** un costo accesorio de mercadería con **comprobante propio de un TERCERO** (flete del depósito del proveedor al nuestro, honorarios del despachante, TCA/terminal, almacenaje) se carga como **gasto vinculado a una compra**, NO como componente de esa compra.

### Por qué gasto y no componente de la compra (fundamento de la decisión, 5/8/2026)

1. **El crédito fiscal del tercero se devenga en la fecha de SU factura, no en la de la compra.** Como componente heredaría el período de la compra y el crédito quedaría imputado al **mes equivocado**.
2. **`gastos` ya tiene todo lo necesario**: `proveedor_id`, `tipo_comprobante`, `nro_comprobante`, `monto_neto`, `monto_iva`, `genera_credito_iva`, `cuenta_origen_intencion`, `forma_pago`, adjuntos y el flujo de pago proactivo/reactivo. Construirlo dentro de compras sería **duplicar una pantalla que ya funciona**.

### Comportamiento cuando `capitaliza_compra_id` NO es NULL

- El **neto** se reparte entre los lotes de esa compra **por su valor** (`cantidad_inicial × costo_unitario`) — el mismo criterio "por costo neto" del prorrateo de compras — y **sube `lotes.costo_unitario`**.
- El gasto **se excluye del P&L**: `v_gastos_mensual` y `v_gastos_categoria_mensual` filtran `capitaliza_compra_id IS NULL`. Ya vive en el costo del lote y sale por **CMV al vender**.
- **NO se excluye de `v_gastos_ap`**: la deuda con el tercero existe y hay que pagarla.
- El **IVA sigue siendo crédito fiscal normal**, con la **fecha de la factura del tercero**.
- **No lleva imputación por línea** (la hereda de la compra vía el lote): el backend no la exige y la pantalla oculta ese bloque.

### Preview de impacto antes de guardar

Endpoint **`GET /compras/:id/impacto-capitalizacion?neto_ars=X`**, que devuelve:
- qué **lotes** cambian y su **costo nuevo**
- cuántas **unidades ya se consumieron**
- el **delta de CMV agrupado por período**
- el **delta de stock**

La pantalla lo consulta cada vez que cambia la compra elegida, el monto, la moneda o el TC, y muestra un cuadro con el resultado.

Si la compra **ya tuvo ventas**, aparece un **checkbox de confirmación obligatorio** ("Entiendo que cambia el CMV de esos meses"); sin tildarlo el POST responde **409 `requiere_confirmar_recosteo`**.

La mecánica de re-costeo (por qué se reparte sobre TODAS las unidades del lote y cómo se mueven los períodos) está en `ADARA-COSTEO-FIFO.md`.

### Validaciones

- Gasto en **USD que capitaliza: exige TC** (el lote vive en ARS).
- **No se puede anular un gasto ya capitalizado** sin revertir antes el costo de los lotes (la pantalla lo frena con un mensaje).
- El **rollback del endpoint** restaura tanto `lotes.costo_unitario` como `consumo_lote.costo_unitario_al_consumir`.

### Tabla de decisión — dónde se carga cada costo accesorio

| Situación | Dónde se carga |
|---|---|
| Cargo **en la factura del propio proveedor** | Renglón `flete` en la compra, tildado **"de esta factura"** |
| Flete / servicio de un **tercero con factura** | **Gasto** con su proveedor y su fecha, vinculado a la compra (`capitaliza_compra_id`) |
| Coima o pago **sin comprobante** | Componente `sin_factura` en la compra, con su cuenta de caja |

> **Por qué no hace falta un mecanismo para RESTAR costo.** La asimetría es real: una **coima** significa que el costo real es **MAYOR** que el facturado → hace falta sumar, y para eso está `sin_factura`. Un costo **facturado de más** significa que el real es **MENOR** → ahí alcanza con **no cargarlo de más desde el principio**.

---

## Gastos compartidos entre líneas

Si un mismo gasto sirve a varias líneas (ej: alquiler del depósito que se usa para todo), se carga **un solo gasto repartido en imputaciones** (`gasto_imputacion`): un renglón por línea/canal con su `porcentaje` (suma 100). El botón **"repartir equitativo"** del alta divide el 100% entre los renglones. (Modelo anterior — N gastos separados — derogado el 21/6/2026, G12.)

**Ejemplo: alquiler mensual de $600.000**

```
Alquiler total: $ 600.000

Distribución por uso del depósito:
  ML Electrónica         50% → gasto 1: $ 300.000 línea ML Electrónica
  Electrónica off-ML     10% → gasto 2: $  60.000 línea Electrónica off-ML
  Luminarias              5% → gasto 3: $  30.000 línea Luminarias
  Mochilas Sindicatos    20% → gasto 4: $ 120.000 línea Mochilas Sindicatos
  Mochilas Individuos    15% → gasto 5: $  90.000 línea Mochilas Individuos
  ──────────────────────────────────────────────────────────────────────
  Total                  100%             $ 600.000

(En el extracto bancario aparece un único movimiento por $ 600.000.
 Cuando se concilia, se vincula a los 5 gastos en simultáneo.)
```

**Cómo se reparte (G12)**:
- El `porcentaje` de cada renglón reparte el gasto completo: el **neto** baja al P&L por línea/canal (`v_gastos_mensual`); el **IVA** crédito y las percepciones/retenciones se distribuyen por el mismo % para la lectura fiscal por línea.
- `canal` opcional: vacío = toda la línea (no atribuible a canal); o un canal puntual (`ml` / `tienda_nube` / `b2b` / `whatsapp_efectivo`).
- Σ% = 100 validado en el alta (`POST /gastos`) y en la vista de salud `v_gasto_imputacion_check`.
- El total de la DDJJ no cambia (Σ%=100); lo que se habilita es abrir cada gasto por línea/canal.
- **Excepción:** el **gasto capitalizable** no lleva imputaciones — la línea la hereda de la compra a través del lote.

---

## Pago combinado de N gastos (regla N:N)

Caso típico: pagás varias facturas juntas con una sola transferencia.

**Ejemplo: pagás contador (marzo + abril) en mayo con una transferencia de $200.000**

```
Estado en ADARA:
  31/03  Gasto "Honorarios contador marzo"   $100.000   pendiente
  30/04  Gasto "Honorarios contador abril"   $100.000   pendiente

Extracto Supervielle del 05/05:
  05/05  Transferencia − $ 200.000

Conciliación:
  Click "Vincular múltiples" en el movimiento del 05/05 →
  Marcás los dos gastos del contador →
  Suma $200.000 = monto del movimiento → ✓ Conciliado

Resultado:
  Ambos gastos pasan a "Pagado"
  El movimiento queda cerrado, vinculado a los 2 gastos
  P&L: marzo registra el gasto de marzo; abril el de abril
  (No se toca P&L de mayo porque el devengado ya pasó)
```

Mecánica completa en `ADARA-CONCILIACION-BANCARIA.md`.

---

## Desglose de pago de tarjeta

Cuando aparece un movimiento con concepto `Cobranzas ResumenVisa` (pago de tarjeta de crédito) en el extracto, ADARA ofrece una pantalla especial **"Desglosar pago"**:

```
MOVIMIENTO: 06/04  Cobranzas ResumenVisa  − $ 975.864,73

DESGLOSE:
  Software Adobe Cloud         $ 32.000   ML Electrónica  · Software
  Combustible (cargas mes)     $138.000   Mochilas Sind. · Combustible
  Almuerzos clientes           $ 85.500   Luminarias     · Comidas
  Insumos papelería            $ 24.500   ML Electrónica · Insumos
  Compras MercadoLibre         $195.864   ML Electrónica · Insumos
  ...
  ──────────────────────────────────────────────────────────────
  Suma desglosada              $ 975.864,73  ✓ coincide
```

Cada renglón del desglose **crea un gasto nuevo** con:
- Fecha del cargo (la del resumen, o aproximada)
- Categoría + línea
- Descripción del cargo
- Forma de pago: tarjeta

Los gastos quedan vinculados al movimiento del pago de la tarjeta. La fecha de cada gasto es la del cargo original (no la del pago de la tarjeta) — eso respeta el principio devengado del P&L.

---

## Gastos recurrentes

Hay gastos que se repiten todos los meses con monto similar (alquiler, servicios, software, etc.). Para evitar carga repetida, ADARA puede tener una **plantilla** de gastos recurrentes:

```
PLANTILLA: Tango Factura (AXOFT)
  Frecuencia: mensual, día 13 aprox.
  Monto esperado: $ 360.000 ± 10%
  Categoría: Software / SaaS
  Línea: (selección al instanciar — suele ser ML Electrónica)
  Forma de pago: débito automático
  Cuenta origen: Supervielle ARS /3
```

Cada mes, cuando aparece el movimiento `Débito Automáticode Servicio AXOFT` en el extracto:
1. ADARA detecta automáticamente que matchea la plantilla (concepto + monto en rango + fecha en rango)
2. Crea el gasto automáticamente con todos los campos pre-llenados
3. Lo concilia con el movimiento al instante

El usuario solo confirma. Si el monto se salió mucho del rango (ej: subió 30%), ADARA pide confirmación manual.

(Esta funcionalidad es **opcional / de evolución futura**, no crítica para el arranque.)

---

## Vínculo con conciliación bancaria

Cada gasto **debe** estar vinculado a un movimiento (banco / MP / caja / tarjeta). Sin vínculo, queda como "pendiente de pago" en AP (cuentas por pagar).

Estados del gasto:

| Estado | Significado |
|--------|-------------|
| Pendiente de pago | Cargado, sin movimiento bancario asociado todavía (es AP) |
| Parcialmente pagado | Tiene movimiento(s) vinculado(s) pero la suma no completa el monto |
| Pagado | Conciliado totalmente |
| Anulado | Decisión explícita (error, duplicado, etc.) |

Ver mecánica completa en `ADARA-CONCILIACION-BANCARIA.md`.

---

## Vínculo con impuestos (crédito IVA)

Cuando el gasto tiene **factura A** del proveedor, genera **crédito IVA**:

```
Gasto: Honorarios contador marzo
  Bruto: $ 121.000
  Neto:  $ 100.000
  IVA:   $  21.000

Imputaciones:
  P&L (gasto operativo, línea ML Electrónica):  − $ 100.000  (sin IVA)
  Ledger IVA (crédito del mes):                 + $  21.000
```

Si el gasto tiene **factura B / C** o **sin factura**, NO genera crédito IVA. El monto bruto entero va al P&L.

**Gasto capitalizable:** el IVA se comporta igual (crédito con la fecha de la factura del tercero); lo que cambia es el **neto**, que no baja al P&L sino al costo del lote.

---

## Retenciones y percepciones (renglones fiscales)

Una factura de proveedor puede traer **percepciones** (te las cobran de más, suman al total) y vos podés practicar **retenciones** (le retenés al proveedor, restan lo que le pagás). En ADARA se cargan como **renglones aparte** del gasto, en la tabla `gasto_fiscal` (un renglón por concepto), no como campos sueltos del gasto.

Tipos soportados: retenciones `ret_ganancias`, `ret_iva`, `ret_iibb`, `ret_suss`, `otro_ret`; percepciones `perc_iva`, `perc_iibb`, `otro_perc`.

El **a pagar** lo calcula la vista `v_gastos_ap`:

```
a pagar = bruto + percepciones − retenciones
```

- El `monto_neto` / `monto_iva` del gasto **no se tocan** (siguen alimentando P&L y crédito IVA igual).
- El total del comprobante = bruto + percepciones; lo que efectivamente transferís = a pagar.
- El alta del gasto + sus renglones fiscales es **atómica** vía `POST /gastos` (ver nota técnica).

---

## Vista en la app

### Pantalla "Gastos"

Lista filtrable de todos los gastos del período seleccionado, con filtros por:
- Categoría
- Línea de negocio
- Estado (pendiente / pagado / anulado)
- Con factura / sin factura
- Forma de pago
- Rango de fechas

Cada renglón: fecha, descripción, categoría, línea, monto, estado, forma de pago, link al movimiento bancario si está conciliado.

**Modal de alta:** fecha, comprobante, categoría, **imputaciones** (uno o más renglones línea + canal opcional + %, con botón "repartir equitativo" y control Σ=100), descripción, proveedor (alta rápida por CUIT con padrón ARCA), N° comprobante, moneda + TC (si USD), neto/IVA, renglones fiscales, y un **único selector "Pago"** que reemplaza a los antiguos dos campos separados "Forma de pago" + "Cuenta origen" (cambio 9/6/2026, G11 — ver Nota técnica). Elegir el pago auto-sugiere la moneda.

**Bloque "capitaliza a una compra" (5/8/2026):** al elegir una compra se oculta el bloque de imputaciones, se exige TC si el gasto es en USD, y se muestra el **cuadro de impacto** (lotes afectados, costo nuevo, unidades ya consumidas, delta de CMV por período, delta de stock) que se recalcula ante cada cambio de compra / monto / moneda / TC. Si la compra ya tuvo ventas, hay que tildar la confirmación de re-costeo para poder guardar.

### Resumen mensual por categoría

```
GASTOS DE ABRIL 2026 POR CATEGORÍA

  Servicios básicos              $   145.000     2,7%
  Alquiler                       $   600.000    11,3%
  Honorarios profesionales       $   180.000     3,4%
  Logística                      $   320.000     6,0%
  Publicidad                     $   890.000    16,8%   ← RoAS calculable
  Insumos                        $   210.000     3,9%
  Comunicaciones                 $    95.000     1,8%
  Software / SaaS                $   420.000     7,9%
  Comisiones bancarias           $   180.000     3,4%
  Cargas sociales                $ 1.620.000    30,5%
  Comidas / representación       $   140.000     2,6%
  Otros                          $   500.000     9,7%
  ───────────────────────────────────────────────────────
  TOTAL                          $ 5.300.000   100,0%
```

(Los gastos capitalizables **no aparecen** en este resumen: no son gasto del período, son costo de mercadería.)

### Resumen por línea

Mismo análisis pero agrupado por línea de negocio, para entender qué línea consume más recursos.

---

## Reglas duras

1. **Cada gasto se imputa a UNA categoría y a una o más líneas/canales** vía `gasto_imputacion` (% que suma 100). No hay gastos sin imputación. **Excepción:** el gasto capitalizable no lleva imputaciones (hereda la línea de la compra vía el lote).
2. **Gastos compartidos = un gasto repartido en imputaciones** (línea + canal opcional + %), no N gastos separados (G12, deroga el criterio anterior).
3. **Devengado, no percibido**: el gasto cuenta en el mes de la factura/fecha del cargo, no del pago.
4. **Sin factura ≠ inválido**: los gastos sin factura se cargan igual, etiquetados como tales. Suman al P&L interno, no generan crédito fiscal.
5. **Crédito IVA solo con factura A**. Facturas B/C o sin factura no generan crédito.
6. **Mes cerrado es inmutable**: gastos posteriores se imputan al mes en curso, no se retroactúan.
7. **Devolver un gasto cargado**: en lugar de borrarlo, se anula con motivo (mantiene auditoría).
8. **Gasto en USD sin TC traba el cierre del mes (G9)**. El TC se completa al primer pago.
9. **Retenciones y percepciones van en renglones aparte (`gasto_fiscal`)**. El a pagar = bruto + percepciones − retenciones; el neto/IVA del gasto no se tocan (G10).
10. **Un costo accesorio de mercadería facturado por un tercero se carga como GASTO capitalizable, no como componente de la compra** (5/8/2026). El motivo es fiscal: el crédito se devenga con la fecha de la factura del tercero, no con la de la compra.
11. **El gasto capitalizable sale del P&L pero NO del AP** (5/8/2026). El neto ya vive en el costo del lote (llega al resultado por CMV); la deuda con el tercero sigue siendo deuda.

---

## Pendientes y TBD

- **Reversa de un gasto capitalizado (5/8/2026)**: la anulación hoy se **frena en el front**, pero **no hay endpoint** que revierta el costo del lote (`lotes.costo_unitario` + `consumo_lote.costo_unitario_al_consumir`). Hoy solo existe el rollback interno del alta.
- **Corte por mes cerrado en la capitalización (5/8/2026)**: falta `meses_cerrados` (capa 9, en pausa) para prohibir el re-costeo de un período ya cerrado. Regla objetivo en `ADARA-COSTEO-FIFO.md`: re-costeo libre con el mes abierto; si cerró, la parte consumida no se toca y va al P&L del mes de la factura.
- **Plantillas de gastos recurrentes**: funcionalidad opcional para automatizar carga de servicios mensuales. Para evolución futura.
- **Adjuntar archivos** (foto del recibo, PDF de factura): definir storage (Supabase Storage probablemente) y límites.
- **Carga de gastos retroactivos enero-abril 2026**: se cargan progresivamente al subir los extractos del período.
- **Categorías personalizadas**: si surgen categorías nuevas con uso recurrente, agregarlas como configurables.
- **Aprobación de gastos > X**: ¿hace falta workflow de aprobación para gastos grandes? Probablemente no en esta etapa (4 usuarios, confianza alta).
- **Reportes de gastos por proveedor**: vista alternativa agrupada por proveedor, para ver con quién se gasta más.

---

## Nota técnica — implementación

Schema implementado el 27/05/2026 (capa 5). Detalle completo en `ADARA-SCHEMA.md`. Puntos clave:

- **Tabla `gastos`** atómica (sin items — el gasto operativo es plano, a diferencia de ventas/compras).
- **Montos en moneda origen**: `monto_neto` + `monto_iva` se guardan libres (no derivados por alícuota fija). Soporta redondeos del proveedor. `monto_bruto` es generated.
- **Moneda USD**: `tc` se completa al primer pago (regla operativa **B1**). Mientras esté NULL, el gasto **traba el cierre del mes** (regla **G9**). **Excepción:** si el gasto **capitaliza a una compra**, el TC es **obligatorio en el alta** (el lote se persiste en ARS).
- **AP dinámico vía `v_gastos_ap`** (CB6 — nada persistido). Expone `estado_pago` derivado: `usd_sin_tc` / `pendiente` / `parcial` / `pagado` (tolerancia 0,02). **No filtra** los capitalizables: la deuda con el tercero existe igual.
- **`genera_credito_iva`** es columna generada (`tipo_comprobante='factura_a' AND monto_iva > 0`). La capa 6 (Fiscal) la consume cuando exista.
- **Gastos sin factura en efectivo**: la app debe disparar un movimiento automático en caja (`origen='sin_factura_auto'`) + vínculo `op_tipo='gasto'`. Análogo a `compra_componentes.tipo='sin_factura'`.
- **Intención vs fuente de verdad**: `cuenta_origen_intencion` + `forma_pago` ayudan al matching/UI pero NO son la verdad del pago. La verdad vive en `vinculos` + `movimientos.cuenta_id`.
- **Selector único "Pago" (9/6/2026, G11)**: el modal de alta tiene un solo campo que materializa `forma_pago` + `cuenta_origen_intencion` (antes eran dos selects que se llenaban redundante — ej. "Mercado Pago" había que elegirlo dos veces). Mapeos: Efectivo→`caja_ars`/`caja_usd`, Transferencia/Débito automático→`supervielle_ars`, MP→`mp_ars`, Tarjeta→`tarjeta` + cuenta **vacía** (concilia vía desglose del resumen, A7), USDT→`transferencia` + `trust_wallet` (el CHECK `gastos_forma_pago_check` solo admite efectivo/transferencia/mp/tarjeta/debito_automatico, así que `usdt` no es valor válido de `forma_pago` — lo cripto lo marca la **cuenta**). Elegir el pago auto-setea la moneda sugerida (USD para Caja USD y Trust Wallet), editable. Cambio 100% frontend en `gastos.js`; no toca tabla, backend ni matching. La cuenta `trust_wallet` (Trust Wallet USDT, `tipo=caja`, `moneda=USD`) se creó en `cuentas` el 9/6/2026 (TI7).
- **`gastos.estado` es `'activo'`** (corrección de dato del 5/8/2026: **no** es `'activa'`). Las tres vistas (`v_gastos_ap`, `v_gastos_mensual`, `v_gastos_categoria_mensual`) filtran por ese valor.
- **Anular** preserva vínculos para auditoría; la vista los oculta (`where estado='activo'`). Un gasto **capitalizado** no se puede anular sin revertir antes el costo de los lotes.
- **Tabla `gasto_fiscal`** (29/05): renglones de retenciones/percepciones (`tipo` + `monto`, `clase` generated). El **a pagar = bruto + percepciones − retenciones** lo calcula `v_gastos_ap`, que se recreó para exponer `a_pagar_origen`/`a_pagar_ars`, `percepciones_origen`, `retenciones_origen` y `total_factura_origen`.
- **Alta atómica vía `POST /gastos`** (Express, A16): inserta el gasto + sus renglones `gasto_fiscal` + (si `sin_factura` + efectivo) el movimiento `origen='sin_factura_auto'` en caja con vínculo `op_tipo='gasto'`. Rollback best-effort si algo falla.
- **Tabla `gasto_imputacion` (21/6/2026, G12)**: reparto del gasto por `linea_id` + `canal` (FK `canales`, nullable = toda la línea) + `porcentaje` (CHECK 0–100; Σ=100 lo valida el backend). Patrón post-A17 (RLS + policy `authenticated` + GRANT, sin anon). `gastos.linea_id` quedó **nullable** (deprecado; la verdad vive en las imputaciones). `POST /gastos` recibe `imputaciones[]`, valida Σ=100 + canal FK, inserta atómico (el rollback borra imputaciones + fiscal + gasto). Back-compat: si llega `linea_id` sin imputaciones, sintetiza un renglón 100%. Las vistas `v_gastos_mensual` (período×línea×**canal**, neto repartido por %), `v_gastos_categoria_mensual` (drill-down) y `v_gastos_ap` (expone `imputaciones` JSON + `lineas_resumen` + `n_lineas`) leen de esta tabla.
- **Columna `gastos.capitaliza_compra_id` (5/8/2026)**: FK a `compras`, nullable. Cuando no es NULL, el alta reparte el **neto** entre los lotes de esa compra por `cantidad_inicial × costo_unitario` y actualiza `lotes.costo_unitario` + `consumo_lote.costo_unitario_al_consumir` de las unidades ya vendidas (re-costeo, ver `ADARA-COSTEO-FIFO.md`). `v_gastos_mensual` y `v_gastos_categoria_mensual` filtran `capitaliza_compra_id IS NULL`; `v_gastos_ap` **no**. El POST devuelve **409 `requiere_confirmar_recosteo`** si la compra tiene ventas y no vino la confirmación. Rollback: restaura costo de lotes y de consumos.
- **Endpoint `GET /compras/:id/impacto-capitalizacion?neto_ars=X` (5/8/2026)**: preview de solo lectura — lotes afectados con costo nuevo, unidades consumidas, delta de CMV por período y delta de stock. Lo consume el modal de alta de gastos.
- **Pantalla `gastos.js`** (`loadGastos()`) implementada el 29/05: lista desde `v_gastos_ap`, KPIs (gastos del período, pendiente AP, USD sin TC, crédito IVA), filtros (período/categoría/línea/estado/búsqueda), modal de alta con renglones fiscales y anular con motivo.
- **Proveedores (30/05)**: alta rápida con **CUIT primero** y **autocompletado de razón social desde el padrón ARCA** (`/padron`, ver `ADARA-ARCA-PADRON.md`). `POST /proveedores` es **get-or-create por CUIT** (no duplica). Al **guardar** el gasto, si quedó un CUIT en el alta rápida sin tocar "Crear", el proveedor se crea y vincula solo (antes se guardaba sin proveedor).

---

## Historial de errores

| Fecha | Síntoma | Causa | Fix |
|-------|---------|-------|-----|
| 30/05/2026 | Al guardar un gasto eligiendo **Cuenta origen** (ej. "Caja ARS"), error `22P02 invalid input syntax for type bigint: "caja_ars"`. | La columna `gastos.cuenta_origen_intencion` estaba creada como **`bigint`**, pero por diseño guarda el **código** de cuenta (texto). Front y backend la usan como `codigo`; solo la columna estaba en desacuerdo. Nunca había saltado porque nunca se había elegido una cuenta origen (quedaba NULL). | Migración: como `v_gastos_ap` depende de la columna, se hizo `DROP VIEW v_gastos_ap` → `ALTER COLUMN ... TYPE text` → recrear la vista (capturando `pg_get_viewdef` para no reproducirla a mano) → `GRANT SELECT ... TO anon, authenticated, service_role` (los grants se pierden al recrear). Sin cambios de código. |
| 30/05/2026 | Gasto/compra se guardaba **sin proveedor** aunque el CUIT había traído el nombre. | El alta rápida requería tocar "Crear"; si se guardaba directo, el proveedor nunca se creaba ni quedaba seleccionado. | Al guardar, si hay un CUIT de 11 dígitos en el alta rápida y no hay proveedor seleccionado, se crea/vincula solo (`crearProveedorDesdeCuit`). |
| 05/08/2026 | Consultas y filtros escritos contra `gastos.estado = 'activa'` no devolvían nada. | Dato mal documentado: el valor real del estado es **`'activo'`** (masculino). | Se corrigió la referencia; las tres vistas (`v_gastos_ap`, `v_gastos_mensual`, `v_gastos_categoria_mensual`) filtran por `'activo'`. |

---

## Documentos relacionados

- `ADARA-FLUJO-OPERATIVO.md` — flujos proactivo y reactivo en la cadencia diaria
- `ADARA-CONCILIACION-BANCARIA.md` — matching automático contra extractos + regla N:N
- `ADARA-PNL.md` — gastos operativos como egresos en el resultado
- `ADARA-IMPUESTOS.md` — crédito IVA generado por gastos con factura A
- `ADARA-LINEAS-NEGOCIO.md` — imputación por línea y manejo de compartidos
- `ADARA-COMPRAS-IMPORTACIONES.md` — gastos sin factura asociados a compras + clasificación de cargos accesorios (`flete` / `gasto_prorrateable` / `extra_directo`)
- `ADARA-COSTEO-FIFO.md` — re-costeo de lotes por costo tardío y efecto en el CMV de meses ya vendidos
- `ADARA-DECISIONES.md` — reglas consolidadas
