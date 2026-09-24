# ADARA — Ventas en efectivo sin comprobante

Última actualización: 7 Agosto 2026 — **documento nuevo**. Circuito implementado y en uso (primera venta real cargada el 6/8/2026).

Ventas cobradas en efectivo que **no se facturan**. Descuentan stock por FIFO, suman a `caja_ars` y entran al Resultado, pero **no generan IVA débito**.

Es la cara simétrica de los gastos sin factura (`ADARA-GASTOS.md`): el sistema ya modelaba la compra sin comprobante y le faltaba la venta.

---

## Por qué existe

Antes de esto, una venta en efectivo **no se podía cargar en ningún lado**. Al 6/8/2026 la tabla `ventas` tenía 18.259 registros y el **100 % era `canal='ml'`**: ninguna venta off-ML había entrado nunca al sistema.

El diseño documentado en `ADARA-FLUJO-OPERATIVO.md` dice que efectivo / WhatsApp / consumidor final entran **por sync de Tango**. Ese sync no existe: el implementado el 4/8 sólo pega N° de factura a ventas ML por `ml_order_id ↔ ExternalID` y **no trae ventas que no sean de ML**. Ver "Pendiente estructural" al final.

Lo único que se podía hacer era registrar el cobro como movimiento suelto en `caja_ars` — el eje plata, sin stock, sin CMV y sin Resultado.

---

## Modelo de datos

El schema ya estaba preparado a medias: `ventas.tipo_comprobante` aceptaba `'sin_comprobante'` y `ventas.es_gravada` es una **columna generada**:

```sql
es_gravada = (tipo_comprobante IN ('factura_a','factura_b','factura_c'))
```

Faltaba el canal, el guardarriel fiscal y el circuito de alta.

### Convención de carga

| Campo | Valor | Por qué |
|---|---|---|
| `ventas.canal` | `'efectivo'` | canal propio (ver abajo) |
| `ventas.tipo_comprobante` | `'sin_comprobante'` | dispara `es_gravada = false` |
| `ventas.estado` | `'entregada'` | requisito de `fn_consumir_fifo` y de `v_resultado_mensual` |
| `ventas.moneda` | `'ARS'` | no hay caso en USD |
| `ventas.referencia_externa` | `'EFVO-000123'` | **prefijo obligatorio** — ver gotcha abajo |
| `venta_items.alicuota_iva` | `0` | sin factura no hay IVA que discriminar |
| `venta_items.precio_unitario_neto` | **lo que se cobra**, sin desglosar | no hay comprobante del cual separar el IVA: la plata es toda ingreso |

Con eso `bruto_linea` = el efectivo cobrado = el monto del movimiento de caja → concilia 1:1.

### Por qué `efectivo` y no `whatsapp_efectivo`

`ventas.canal` tiene **FK a la tabla `canales`** (no es texto libre — no estaba documentado). Ya existía el canal `whatsapp_efectivo` ("WhatsApp / Efectivo"), pero según `ADARA-FLUJO-OPERATIVO.md` **ese canal es para ventas que SÍ se emiten en Tango** (remito) y que van a entrar por ahí el día que exista el sync de ventas no-ML.

Si las ventas sin comprobante cayeran en el mismo canal, después sería **imposible separarlas en el Resultado** — que es justamente lo que hay que poder mirar aparte (ver "Comparabilidad del margen").

---

## El doble candado del IVA

Que una venta en efectivo no genere débito depende de **dos** cosas, a propósito:

1. **La convención**: `alicuota_iva = 0` en el ítem → `iva_linea = 0`.
2. **El filtro en la vista**: `v_control_mensual` computa

```sql
sum(vi.iva_linea) FILTER (WHERE v.es_gravada) AS iva_debito
```

Antes del 6/8/2026 la vista sumaba `iva_linea` **sin mirar `es_gravada`**: la columna generada existía y no la usaba nadie. Con sólo la convención, una venta no facturada cargada por error con alícuota 21 % habría sumado **débito fiscal fantasma** y aumentado el IVA a pagar sin que nada avisara.

Con el filtro, el débito depende del **tipo de comprobante** — la verdad fiscal — y no de lo que se tipeó.

> Verificado antes de aplicar: en los 8 períodos cargados (2026-01 a 2026-08, 18.259 ventas, todas `factura_b`) la diferencia es **$0,00 en todos**. Regresión cero.

---

## Cómo juega en la posición fiscal

**El crédito de la compra de esa mercadería no se toca**: ya se devengó en el mes de la compra. El IVA no se empareja producto por producto — es una cuenta mensual.

Lo que pasa es que **ese mes no suma débito**. El resultado depende del mes:

- **Si el débito sigue siendo mayor al crédito** (el caso normal en ADARA: julio 2026 tuvo débito $60,7M contra crédito $19,8M), la venta en efectivo simplemente hace que el "a pagar" **no crezca**. No genera saldo a favor.
- **Si el crédito supera al débito**, el excedente cae en **saldo técnico a favor** y arrastra al mes siguiente vía `v_posicion_fiscal` (doble arrastre, implementado el 3/8). Ver `ADARA-IMPUESTOS.md`.

> ⚠️ Confusión frecuente: *"el crédito de esa compra queda como saldo a favor"*. No — el crédito ya se computó y se aplica contra **todo** el débito del mes. Sólo sobra si el mes cierra con crédito > débito.

---

## Endpoint `POST /ventas/efectivo`

Alta atómica: **venta + ítems + movimiento de caja + vínculo + FIFO**, con rollback best-effort (PostgREST no da transacción).

```
{ fecha, linea_id, items:[{sku_id, cantidad, precio_unitario}],
  cliente_nombre?, descripcion?, cuenta_codigo?, confirmar_mes_anterior? }
```

Pasos:

1. Valida fecha, línea, ítems, existencia de los SKUs y que la cuenta sea ARS.
2. Inserta la venta y le pone `referencia_externa = 'EFVO-' + id` (después del insert, para que sea única sin adivinar).
3. Inserta los ítems con alícuota 0.
4. Crea el movimiento en `caja_ars` (`origen='venta_efectivo'`, `categoria='cobro_venta'`, monto **positivo**) — mismo patrón que el gasto `sin_factura_auto`.
5. Crea el vínculo `op_tipo='venta'` → **nace conciliada**, no cae en "movimientos sin conciliar".
6. Corre `fn_consumir_fifo(fecha, fecha)`.

### Guardarriel de fecha

Si la fecha cae en un **mes anterior** al actual, devuelve **409** con `error:'fecha_mes_anterior'` y exige reenviar con `confirmar_mes_anterior: true`. No lo bloquea —a veces es legítimo— pero obliga a que sea una decisión consciente.

Es el primer lugar donde se implementó el guardarriel pendiente #9 de `ADARA-FLUJO-OPERATIVO.md`. Mientras no exista `meses_cerrados`, esto es lo único que frena una carga retroactiva silenciosa.

### Gotchas que costaron sangre

**El FIFO va último y NO tumba la operación si falla.** `fn_consumir_fifo` es idempotente por `venta_item`, así que el próximo consumo la levanta igual. Tumbar ahí obligaría a deshacer lotes ya decrementados, que es peor. Por eso el rollback no borra la venta si el FIFO ya corrió.

**`fn_consumir_fifo(fecha, fecha)` procesa TODAS las ventas de ese día**, no sólo la que se acaba de crear. Su `unidades_faltantes` es **global** y no sirve para avisar faltante de stock: daría falsos positivos por otras ventas. El aviso se calcula consultando `consumo_lote` de los ítems de **esa** venta. (Bug detectado en la prueba de punta a punta antes de desplegar.)

**`referencia_externa` nunca puede ser numérica pelada.** `v_resultado_mensual` hace `LEFT JOIN ventas_ml ON vm.ml_order_id = b.referencia_externa`: una colisión arrastraría comisiones y envíos de ML a una venta en efectivo. De ahí el prefijo `EFVO-`.

---

## Impacto por capa

| Capa | Qué pasa | ¿Hubo que tocar algo? |
|---|---|---|
| **Stock / FIFO** | `fn_consumir_fifo` es **canal-agnóstica**: agarra cualquier venta con `estado in ('aprobada','entregada')` | No |
| **Resultado** | `v_resultado_mensual` también es canal-agnóstica: aparece con ingreso y CMV real | No |
| **Comisión / envío ML** | LEFT JOIN por `referencia_externa`; con prefijo no matchea → 0 | No (pero ver gotcha) |
| **IVA débito** | `sum(iva_linea)` sin filtro | **Sí — único cambio de vista** |
| **Caja ARS** | Movimiento + vínculo | Endpoint nuevo |
| **PSI / valorización** | Salen solos del stock consumido | No |

---

## Frontend

Botón **"+ Venta en efectivo"** en la pantalla Movimientos (junto a "+ Movimiento de caja"). Modal con fecha, línea, ítems múltiples (SKU + cantidad + precio + subtotal), cliente opcional y total en vivo.

El selector de SKU usa `core/skuPicker.js` (ver `ADARA-FRONTEND.md`). Los SKUs **se traen cada vez que se abre el modal**: cachearlos en una variable de módulo hacía que un SKU dado de alta después de entrar a Movimientos no apareciera nunca, porque la app es SPA y no recarga la página. Fue un bug real reportado el 6/8.

---

## Riesgos conocidos

**Comparabilidad del margen.** El ingreso de una venta en efectivo entra al **100 %**; el de una venta ML entra **neto de IVA**. A igual precio, la venta en efectivo muestra ~21 % más de margen en el Resultado. No es un error de cálculo, pero mirar líneas mezcladas sin tener esto en la cabeza lleva a conclusiones falsas. Por eso `efectivo` es canal propio: se ve aparte de un vistazo.

**Ventas al costo inflan la facturación sin contribución.** Si se usan para neutralizar stock (ver caso EFVO-018262), el canal muestra ingreso con margen cero. Mirar "ventas totales" sin abrir por canal da un número inflado.

**Exposición fiscal.** Comprar con factura A (tomando el crédito) y vender sin débito corre el ratio crédito/débito hacia un saldo a favor sin sustento en ventas, y deja el stock real **por debajo** del stock teórico reconstruible desde comprobantes. ADARA refleja la realidad —que es lo correcto para gestionar— pero no la vuelve neutra. Criterio a confirmar con el contador; está en la lista de pedidos junto con la política de gastos sin factura.

---

## Caso testigo — EFVO-018262 (6/8/2026)

Primera venta real cargada. Sirve como ejemplo de uso y de los dos errores posibles.

- Fecha 31/07/2026 (mes anterior → disparó el guardarriel, se confirmó).
- Dos ítems, SKUs `1000001` y `1000002`, vendidos **al costo** para neutralizar stock creado por una compra.
- **Error de carga detectado en la revisión:** el segundo ítem se cargó a $100.171,51 cuando el costo del lote era $1.000.171,51 (un dígito de menos) → **margen falso de −$900.000** en julio. Corregido por SQL el 7/8; el CMV no se vio afectado porque **lo fija el costo del lote al consumir, no el precio de venta**.
- Resultado final: ingreso $8.116.312,41 = CMV $8.116.312,41, margen **$0,00**. IVA débito de la venta **$0,00**. `caja_ars` pasó de −$4.000.000 a +$4.116.312,41.

> Lección: cuando el precio va al costo, conviene verificar contra el lote. Se evaluó un botón "= al costo" que lo trajera solo; Sebastián lo descartó (7/8).

---

## Pendiente estructural — sync de ventas no-ML

**4 de las 5 líneas de negocio no tienen forma de entrar al sistema.** Este circuito cubre sólo las ventas en efectivo sin comprobante; las ventas facturadas off-ML (Tienda Nube, B2B, WhatsApp con remito) siguen sin camino.

La solución es extender el sync de Tango: hoy busca por `ExternalID` para pegarle la factura a una venta ML; habría que **traer también los movimientos sin `ExternalID` de ML y proyectarlos a `ventas` / `venta_items`**. Es la misma integración que ya funciona, extendida.

Regla asociada a discutir (`ADARA-DECISIONES.md`): *toda venta off-ML se emite en Tango y entra por sync; ADARA no acepta alta manual de ventas facturadas.* La venta en efectivo **sin comprobante** es la excepción, precisamente porque no hay comprobante que emitir.

---

## Documentos relacionados

- `ADARA-DECISIONES.md` — reglas V1–V3
- `ADARA-IMPUESTOS.md` — posición fiscal, arrastre de saldos a favor
- `ADARA-GASTOS.md` — el caso simétrico (gastos sin factura)
- `ADARA-COSTEO-FIFO.md` — `fn_consumir_fifo`, canal-agnóstica
- `ADARA-MOVIMIENTOS.md` — el botón y el origen `venta_efectivo`
- `ADARA-SCHEMA.md` — canal `efectivo`, FK `ventas.canal → canales`, filtro de `v_control_mensual`
- `ADARA-FLUJO-OPERATIVO.md` — canales de venta y el pendiente del sync Tango
