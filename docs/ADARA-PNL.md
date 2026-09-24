# ADARA — Estado de Resultado (P&L)

Última actualización: 10 Agosto 2026 (**IIBB Convenio Multilateral e impuesto al cheque en el Resultado**: el margen operativo estaba **inflado**. `v_resultado_mensual.impuestos` salía de `ventas_ml.impuestos`, que es la **retención** que descuenta ML (~1,01%–1,11% del ingreso neto, muy estable) y se usaba como si fuera el costo de IIBB del mes. No lo es: la retención es **pago a cuenta**; el costo es el **impuesto determinado** (base × coeficiente × alícuota, por cada una de las 24 jurisdicciones). CM03 anticipo 202606 presentado 15/07/2026: determinado **$20.765.310,42** contra **$4.346.103,85** que restaba ADARA → **$16.419.206,57 no contabilizados**, más de la mitad del resultado operativo de junio. Se incorpora además el **impuesto al cheque**, que no estaba en ningún lado del Resultado: **$8.205.589,40** acumulados desde marzo 2026 en `retenciones`. Márgenes corregidos: junio pasa de 6,38% a **1,59%**; el patrón que aparece es **cuanto más se vende, peor margen**. `v_resultado_mensual` y `v_resultado_linea_mensual` extendidas de forma **aditiva**. Detalle completo en `ADARA-IIBB-CONVENIO-MULTILATERAL.md`. Ver sección al final.) · 21 Junio 2026 (**canceladas/devoluciones en el Resultado**: filtro `estado` que era no-op corregido vía `fn_reconciliar_ml_canceladas` (P13) — 21 canceladas coladas sacadas (~$4,11M) + 3 `no_preparado` con CMV revertido; columna `v_resultado_mensual.devoluciones` por mes del reembolso (P3) con guard anti doble conteo; cascada `contribución − CMV − gastos + devoluciones`. Ver sección al final + `ADARA-CANCELACIONES-DEVOLUCIONES.md`.) · 21 Junio 2026 (**gastos integrados al P&L**: la capa Gastos existe y baja al resultado vía `v_resultado_linea_mensual` (margen de contribución − gastos = resultado operativo); los gastos se reparten por `gasto_imputacion` (línea + canal opcional + %, G12), `v_gastos_mensual` ahora abre por línea×canal. **Fix** del IVA crédito de gastos. Drill-down por categoría: vista `v_gastos_categoria_mensual` lista, UI pendiente.) · 9 Junio 2026 (**CMV estimado a costo actual + congelado** para el histórico ML → resultado operativo realista por mes; **margen operativo real** sobre neto s/IVA; aclaración de los **3 montos** de la venta ML; **drill-down de control mensual** vía `v_control_mensual`; **IVA débito/crédito/a pagar**) · 8 Junio 2026 (**Nivel 1 implementado**: pantalla Resultado `#resultado` + vista `v_resultado_mensual` = contribución antes de CMV por mes/línea/canal) · 8 Junio 2026 (revenue e IVA débito por línea ya disponibles para canal ML desde la proyección — ver `ADARA-COSTEO-FIFO.md`) · 15 Mayo 2026

Estado de resultado mensual: cuánto **generó** y cuánto **ganó** cada línea de negocio.
**Es el destino del sistema.** Toda la lógica de conciliación, lotes, gastos e impuestos se construye para que este número sea correcto y confiable.

---

## Estado de implementación (8/6/2026)

**Nivel 1 — contribución antes de CMV: IMPLEMENTADO.** La pantalla **Resultado** (`#resultado`,
`resultado.js`) muestra el estado de resultado mensual por línea: `ventas netas − comisión ML −
envío − costo financiero − IIBB = margen de contribución (antes de CMV)`. Una fila por mes + Total,
con **selector de línea** y **selector de canal** (la rentabilidad por canal se obtiene cruzando esa
dimensión dentro de la línea — modelo línea=producto, canal=dimensión, LN2). Lee la vista
`v_resultado_mensual` (período × línea × canal). El % mostrado **no es el margen real**: todavía no
baja CMV ni gastos.

**Pendiente para bajar al resultado neto:**
- **CMV masivo** — hoy sólo 19 ventas tienen costo cargado; el resto espera el costeo histórico de Tango (las ventas pre-seed no consumen FIFO).
- **Gastos de estructura** — capa Gastos ✅ implementada: pantalla `#gastos` + reparto por `gasto_imputacion` (línea + canal opcional + %, G12), ya integrada al resultado vía `v_resultado_linea_mensual`. Pendiente: **sueldos** vía `empleado_linea_pct` (módulo aparte, no construido).
- **Otros ingresos/egresos** — financieros (intereses del banco), que se imputan a Electrónica (P8/TI2).
- **Ganancias** — capa fiscal.
- **Detalle mensual desplegable** — al abrir un mes: ingresos por canal + gastos por categoría + otros ingresos. La capa Gastos ya está; la vista `v_gastos_categoria_mensual` (período × línea × canal × categoría) está creada y lista para cablear en `resultado.js`.

---

## Objetivo

Responder dos preguntas en cualquier momento:

1. **¿Cuánto generó cada línea de negocio este mes?** (ventas netas)
2. **¿Cuánto ganó cada línea de negocio este mes?** (resultado neto, después de costos, gastos e impuestos)

Y de paso:
- ¿Qué línea es la más rentable en margen?
- ¿Qué línea está perdiendo plata?
- ¿Cómo viene el mes en curso comparado con los anteriores?

---

## Principios contables

| Principio | Aplicación en ADARA |
|-----------|---------------------|
| **Devengado** | El P&L se calcula sobre la fecha de la venta o el gasto, NO sobre la fecha de cobro o pago. Una venta del 28 de marzo cuenta en marzo aunque MP la deposite el 5 de abril. Una factura del contador con fecha 31 de marzo cuenta en el P&L de marzo aunque se pague junto con la de abril en mayo. |
| **Sin IVA** | Los montos del P&L se expresan netos de IVA (lo que la empresa realmente factura). El IVA va a `ADARA-IMPUESTOS.md`. |
| **Por línea** | Cada concepto se imputa a una línea según las reglas de `ADARA-LINEAS-NEGOCIO.md`. El total empresa es suma de líneas. |
| **Costos reales** | El costo de mercadería vendida (CMV) sale del costo exacto de los lotes consumidos (FIFO), no de promedios ni estimaciones. |
| **Devoluciones en el mes del reembolso** | Una venta de marzo cuyo reembolso aparece en abril se descuenta del P&L de **abril**, no se retoca el de marzo. |

---

## Ejemplo: pago combinado de varias facturas

Caso típico: una transferencia bancaria cubre **varios gastos** de meses distintos. Aplica a honorarios profesionales, sueldos atrasados, resúmenes de tarjeta de crédito, o cuenta corriente con proveedores.

| Momento | Evento | Impacto en ADARA |
|---------|--------|-------------------|
| 31 marzo | Factura del contador por honorarios marzo: $100.000 | Gasto cargado en marzo con estado "pendiente de pago". **P&L marzo: −$100.000** |
| 30 abril | Factura del contador por honorarios abril: $100.000 | Gasto cargado en abril con estado "pendiente de pago". **P&L abril: −$100.000** |
| 5 mayo | Transferencia única por $200.000 al contador | Movimiento bancario del 5/5 se **vincula a los dos gastos previos**. Cierra ambos pagos. **P&L mayo: no se toca** (los gastos ya estaban devengados en marzo y abril) |

**Regla N:N**: un movimiento bancario puede vincularse a varios gastos, y un gasto puede vincularse a varios movimientos bancarios (pagos parciales / cuotas / anticipos). La conciliación se considera completa cuando la suma de los gastos vinculados al movimiento iguala el monto del movimiento.

Lo mismo aplica a cobros: si un cliente B2B te paga 3 facturas con una sola transferencia, ese movimiento se vincula a 3 ventas.

Mecánica detallada del matching → `ADARA-CONCILIACION-BANCARIA.md`.

---

## Estructura del estado de resultado

Para cada línea, cada mes:

```
LÍNEA: ML ELECTRÓNICA — MARZO 2026                       (montos netos de IVA)
═══════════════════════════════════════════════════════════════════════════════

INGRESOS
  + Ventas brutas                                    + $ 10.000.000
  − Devoluciones (reembolsadas en este mes)          −      280.000
  = Ventas netas                                       $  9.720.000   ─────────

COSTOS DIRECTOS
  − Costo de mercadería vendida (CMV)                −   5.800.000   FIFO por lote
  = Margen bruto                                       $  3.920.000   ▼ 40,3%

GASTOS DE LA OPERACIÓN
  − Comisiones de plataforma (ML / TN)               −   1.090.000
  − Logística (MEF + otras transportadoras)          −     420.000
  − Retenciones netas (a favor)                      +      90.000   netas, según signo
  − IIBB sobre ventas netas (3%)                     −     292.000
  − Gastos operativos (alquiler, servicios, etc.)    −     347.000
  − Sueldos (asignados por %)                        −     650.000
  = Resultado operativo                                $  1.211.000   ▼ 12,5%

PÉRDIDAS POR MERCADERÍA (rubro propio, NO gasto operativo)
  − Pérdida por reclamos no cubiertos (ML/proveedor) −      85.000
  − Pérdida por inventario / ajustes negativos       −      15.000
  = Resultado después de pérdidas                      $  1.111.000

RESULTADO FINANCIERO (solo ML Electrónica — Tesorería General)
  + Rendimiento saldo remunerado                     +      69.602
  − Retención Ganancias sobre rendimientos           −       1.100
  + Rendimiento realizado FCI/PF                     +           0
  = Subtotal financiero                                $     68.502

OTROS
  − Impuesto a las Ganancias (estimación)            −     363.000
  = Resultado neto del mes                             $    916.502   ▼ 9,4%
```

> **Nota importante:** la sección "Resultado financiero" **solo aparece en ML Electrónica**. Las otras 4 líneas no la tienen — la tesorería es central y su rendimiento se imputa íntegramente a ML Electrónica. Ver `ADARA-INVERSIONES.md`.

Misma estructura replicada para cada línea. **Total empresa = suma de las 5 líneas.**

> **Actualización 10/8/2026 — el renglón de IIBB de este cuadro está desactualizado.** El "IIBB sobre ventas netas (3%)" era un placeholder: el costo real es el **impuesto determinado del Convenio Multilateral** (base × coeficiente × alícuota por jurisdicción), no un porcentaje plano ni la retención de ML. Además falta en el cuadro el **impuesto al cheque**, que ahora sí baja al resultado. Ver la sección "IIBB determinado e impuesto al cheque en el Resultado (10/8/2026)" al final y `ADARA-IIBB-CONVENIO-MULTILATERAL.md`.

---

## Detalle por concepto

### Ventas brutas

Suma de ventas con `fecha_venta` dentro del mes y `estado != descartada`, imputadas a la línea. Netas de IVA.

Fuentes:
- Sync ML (ventas con origen `ml`)
- Sync Tango (resto: TN, B2B, WhatsApp/efectivo)

### Devoluciones

Suma de los reembolsos a clientes/ML con `fecha_movimiento` dentro del mes en curso. Incluyen:
- Devoluciones aprobadas con reembolso total
- Cancelaciones post-entrega con reembolso
- Ajustes por reclamos cubiertos por ML donde el dinero vuelve

**Regla dura**: la devolución impacta el P&L del mes en que se procesa el reembolso, no del mes de la venta original.

### Costo de mercadería vendida (CMV)

Calculado al momento de aprobar la venta, sumando el costo exacto de las unidades consumidas de los lotes según FIFO.

Ejemplo:

```
Venta del 14/03: 10 unidades de Redmi Buds 8 Active

Consumo FIFO:
  Lote #022 (compra 15/01): 6 unidades restantes × costo_unitario $4.890 = $29.340
  Lote #023 (importación 18/03): 4 unidades × costo_unitario $5.255   = $21.020

CMV de la venta = $50.360
```

El CMV se imputa al mes de la venta (devengado), no al mes de la compra del lote.

Si hubo ajuste de inventario (compensación entre lotes) posterior a la venta, el CMV de la venta queda congelado — no se recalcula retroactivo.

### Comisiones de plataforma

Para ML: viene del campo `cargo_venta` de cada venta, sin IVA.
Para Tienda Nube: viene del % que cobra TN, sin IVA.
Para Tango B2B / WhatsApp / efectivo: no hay comisión.

### Logística

Suma de costos de envío imputados al mes:
- MEF: viene del `cargo_envio` (Flex bonificación) y de la factura mensual de MEF auditada
- Otras logísticas: viene de la factura que emite cada transportadora

Se imputa por línea según las ventas que llevó cada envío.

### Retenciones netas

Resultado de sumar todas las retenciones aplicadas a las ventas del mes (Créd/Déb, SIRTAC, IIBB Buenos Aires, etc.) **menos** las percepciones que vos pagaste anticipadamente. Cuando son negativas significa que generaste créditos fiscales.

Vienen del campo `impuestos` de cada venta.

> **Corrección 10/8/2026:** este campo (`ventas_ml.impuestos`) es la **retención de IIBB que descuenta ML**, y es un **pago a cuenta**, no un costo del período. Usarlo como el IIBB del mes subestimaba el impuesto. El costo del período es el **determinado**. Ver sección del 10/8/2026 al final.

### IIBB sobre ventas netas

Cálculo automático: ventas netas de la línea × 3% (o la alícuota correspondiente según jurisdicción del Convenio Multilateral).

Es lo que **se devenga** en el mes — lo que efectivamente se paga puede ser menor por retenciones ya sufridas (se compensan en el ledger de IIBB).

> **Actualización 10/8/2026:** el 3% era un placeholder. El devengado real es el **impuesto determinado del CM03** (base × coeficiente × alícuota, 24 jurisdicciones), que en junio 2026 dio una **alícuota efectiva implícita del 5,2852%** sobre la base que conoce ADARA. Ver sección del 10/8/2026.

### Gastos operativos

Suma de gastos cargados en ADARA con `fecha` en el mes y `linea_negocio_id` igual a la línea. Netos de IVA.

Incluye: alquiler, servicios, papelería, fletes internos, transporte, mantenimiento, marketing, etc. — todo lo que no es costo directo de mercadería ni sueldo.

### Sueldos

Suma del sueldo bruto + cargas sociales del mes, multiplicado por el % de la línea (ver `ADARA-LINEAS-NEGOCIO.md` sección "Asignación de sueldos a línea").

### Impuesto a las Ganancias

Estimación mensual sobre el resultado operativo. La tasa exacta a definir con el contador. Sirve para tener una visión realista del resultado neto — la liquidación efectiva es anual y la hace el contador.

### Pérdidas por mercadería (rubro propio)

Distinción importante: las pérdidas por mercadería **NO son gastos operativos**. Se imputan como rubro separado en el P&L.

| Tipo | Origen |
|------|--------|
| Pérdida por reclamos no cubiertos | Reclamos cerrados sin recupero (ML rechazó + proveedor rechazó) — ver `ADARA-RECLAMOS.md` |
| Pérdida por inventario | Faltantes detectados en conteo físico — ver `ADARA-STOCK.md` |
| Pérdida por mercadería rota / vencida | Producto que no se puede vender por estado físico |

Conceptualmente: es **costo de mercadería que ya pagaste y que no recuperaste con la venta**. Mantenerlo separado de los gastos operativos permite analizar correctamente el margen.

---

## Devoluciones en el P&L

No se reabre el mes de la venta (queda cerrado, intacto). La devolución se imputa al **mes en que MP debita o acredita** el movimiento (regla P3), con signo real según el resultado. Tres patas:

1. **Reversa de ingreso** — lo que MP debita (refund) o **acredita** (cuando el comprador devuelve algo distinto y se gana el caso).
2. **Envío de la devolución** — `cargo_envio_devolucion` (costo del retorno).
3. **COGS** — si el producto vuelve vendible (`recepcion_condicion='ok'`) se revierte vía `consumo_lote.reverso_devolucion` (el costo vuelve a inventario); si `no_disponible`, no hay reverso → **pérdida** (el CMV original, ya cargado en el mes de la venta, no se recupera).

El neto entre el mes de la venta y el mes de la devolución da correcto sin tocar meses cerrados. El reintegro fiscal (IVA/IIBB/Ganancias vía nota de crédito) está diferido a la capa fiscal (A14) / TFactura — ver `ADARA-IMPUESTOS.md`.

---

## Métricas derivadas

Además de los conceptos del cuadro, en cada vista se muestran indicadores calculados:

| Métrica | Cómo se calcula | Para qué sirve |
|---------|-----------------|----------------|
| Margen bruto % | Margen bruto / Ventas netas | Eficiencia del costeo de productos |
| Margen operativo % | Resultado operativo / Ventas netas | Eficiencia de la operación |
| Margen neto % | Resultado neto / Ventas netas | Rentabilidad final del mes |
| **RoAS** (Return on Ad Spend) | Ventas netas / Gasto publicitario | Cuánto generan tus ventas por cada peso de publicidad. Requiere categoría "Publicidad" en los gastos. |
| Ticket promedio | Ventas brutas / cantidad de ventas | Tamaño promedio de operación |
| Ventas por día | Ventas netas / días hábiles del mes | Velocidad de facturación |

Las métricas se muestran al lado del concepto del que dependen. RoAS aparece destacado en la vista por línea cuando la línea tiene gasto publicitario imputado.

---

## Vistas en la app

### Vista 1: P&L de una línea, mes específico

El cuadro detallado de arriba, para una línea + mes seleccionado. Cada renglón es drill-down: click en "Ventas brutas" lleva al listado de ventas que componen el total. Click en "CMV" lleva al detalle de lotes consumidos. Y así.

### Vista 2: Comparativo mensual de una línea

```
ML ELECTRÓNICA           Ene     Feb     Mar     Abr     May (parcial)
─────────────────────────────────────────────────────────────────────
Ventas netas          8.450   9.100   9.720  10.230   3.890
CMV                   5.040   5.420   5.800   6.100   2.310
Margen bruto %         40,4%   40,4%   40,3%   40,4%   40,6%
Gastos operación      2.350   2.560   2.799   2.870   1.120
Resultado operativo     720     720     711   1.110     390
Resultado neto %        8,5%    7,9%    7,3%   10,8%   10,0%
```

Permite ver tendencias: ¿el margen se mantiene? ¿los gastos crecen más que las ventas? ¿el resultado mejora?

### Vista 3: Total empresa = suma de líneas

```
MAYO 2026 (parcial al 15/05)
─────────────────────────────────────────────────────────────────────────
                       ML Elec   off-ML   Lumi    Moch S  Moch I   TOTAL
Ventas netas            3.890     420    1.250    830     390    6.780
CMV                     2.310     250    580      450     180    3.770
Margen bruto             1.580     170    670     380     210    3.010
Gastos+sueldos          1.190     105    310     240     110    1.955
Resultado neto            390      65    360     140     100    1.055
Margen neto %            10,0%   15,5%  28,8%   16,9%   25,6%   15,6%
```

Vista comparativa entre líneas: ¿cuál es la más rentable? ¿cuál tiene más volumen?

### Vista 4: Posición del mes en curso

P&L del mes corriente actualizado al momento, con la marca "parcial" para indicar que el mes no terminó. Permite ver si el mes va bien o mal sin esperar al cierre.

---

## Reglas duras (no cambiar sin discusión)

1. **Devengado, no percibido.** Una venta del 28 de marzo cuenta en marzo aunque MP la deposite el 5 de abril.
2. **Devoluciones en el mes del reembolso**, no en el mes de la venta original.
3. **CMV congelado al momento de la venta.** Ajustes de inventario posteriores no recalculan ventas pasadas.
4. **Sin IVA en ningún concepto del P&L.** El IVA va a `ADARA-IMPUESTOS.md`.
5. **Mes cerrado es inmutable.** Una vez cerrado, no se modifican ventas, gastos ni costos del mes — los ajustes van al mes en curso como "regularizaciones".
6. **Total empresa = suma exacta de líneas.** Si no da, hay un movimiento mal imputado.
7. **El gasto de IIBB es el DETERMINADO, no la retención** (10/8/2026). La retención de ML y las percepciones de compra son **anticipos**: reducen la caja, no el costo del período. Ver sección del 10/8/2026.
8. **Solo la base GRAVADA genera IIBB** (10/8/2026). Una venta sin comprobante no se declara y no tributa — mismo criterio que el IVA débito (regla V2 de `ADARA-IMPUESTOS.md`). Por eso el prorrateo del IIBB entre línea y canal va por **base gravada** y el canal `efectivo` no absorbe IIBB.

---

## Pendientes y TBD

- **Tasa de Ganancias por línea** (o tasa global): a definir con contador antes de la primera estimación
- **Carga retroactiva enero–abril 2026**: el P&L de los primeros meses se completa a medida que se cargan extractos, compras e historial
- **Vista de exportación a contador** (mensual, formato a confirmar): opcional, lo definimos cuando esté el módulo

---

## Documentos relacionados

- `ADARA-FLUJO-OPERATIVO.md` — cómo se generan los datos que alimentan el P&L
- `ADARA-LINEAS-NEGOCIO.md` — cómo se asigna cada concepto a una línea
- `ADARA-INVERSIONES.md` — Tesorería General y resultado financiero imputado a ML Electrónica
- `ADARA-COMPRAS-IMPORTACIONES.md` — lotes y costos que alimentan el CMV
- `ADARA-STOCK.md` — valorización y descuento por venta
- `ADARA-IMPUESTOS.md` — posición fiscal en vivo (IVA, IIBB, Ganancias)
- `ADARA-IIBB-CONVENIO-MULTILATERAL.md` — IIBB determinado del Convenio Multilateral: CM03, coeficientes, alícuotas por jurisdicción y su bajada al Resultado
- `ADARA-PATRIMONIAL.md` — valor neto de la empresa (la otra mitad: el balance)
- `ADARA-CONCILIACION-BANCARIA.md` — cómo se cierran ventas y gastos contra extractos
- `ADARA-DECISIONES.md` — reglas consolidadas

---

## Revenue + CMV + margen bruto ML disponibles (actualización 8/6/2026 parte 2)

Con la proyección al circuito costeado (CF1) **más el FIFO ya activo** (CF5) y el **seed de stock cargado** (CF3), para el canal **ML** ya están disponibles:
- **Revenue neto e IVA débito** por línea (`ventas`/`venta_items`, `canal='ml'`). 2026: neto $1.133.687.557,72 / IVA débito $148.661.718,41 (todo ML Electrónica).
- **CMV devengado** vía `v_cmv_mensual` (junio: $2.122.782 sobre 19 u costeadas).
- **Margen bruto** vía `v_margen_ventas` (a nivel venta_item, `ingreso_neto − cmv`, flag `costeada`). La pantalla **Costeo** (`#costeo`) ya expone estas 3 vistas + `v_valorizacion_stock`.

**Estas vistas son el primer insumo del P&L**, pero el P&L completo **sigue bloqueado**:
- El **margen de Costeo es BRUTO** — todavía no descuenta comisión ML, IIBB sobre ventas, retenciones ni logística (esa es la capa de **contribución**, que vive en el P&L, no en Costeo).
- La pantalla **Gastos** ya existe e integra al P&L (resultado operativo por línea vía `v_resultado_linea_mensual`, con gastos repartidos por `gasto_imputacion`). Pendientes para el resultado neto completo: **sueldos** (`empleado_linea_pct`) y **Ganancias** (capa fiscal).
- El **revenue off-ML** depende del sync de ventas Tango (capa 6); el **CMV histórico ene–hoy** queda diferido (CF3, snapshot no se recalcula).
- Sólo se costean las ventas **≥ fecha del seed**; las anteriores dan `costeada=false` en `v_margen_ventas` (sin CMV por diseño).

---

## CMV en el Resultado: estimado + congelado (9/6/2026)

El P&L de Electrónica/ML dejó de estar bloqueado por el CMV. El histórico (ventas anteriores al seed del 8/6) se costea con **CMV estimado a costo actual** del SKU y luego se **congela** (queda fijo). Detalle del mecanismo en `ADARA-COSTEO-FIFO.md` (parte 4) y `ADARA-SCHEMA.md` (parte 3). Reglas CF9-CF11 y P11 en `ADARA-DECISIONES.md`.

La pantalla **Resultado** ahora muestra, por mes/línea/canal: `ingreso_neto`, `cmv` (con detalle `cmv_real` FIFO vs `cmv_estimado` congelado), comisión, envío, costo financiero, impuestos, y el **resultado operativo**. *(10/8/2026: se sumaron las columnas `base_gravada`, `iibb_determinado`, `impuesto_cheque`, `iibb_base_confirmada` y, en la vista por línea, `margen_contribucion_real` y `resultado_operativo_real` — ver sección del 10/8/2026.)*

Resultado operativo por mes (Electrónica/ML, ya con CMV congelado):

| Período | Ingreso neto | CMV | CMV real | CMV estimado | Resultado op. | Margen |
|---|--:|--:|--:|--:|--:|--:|
| 2026-01 | 106.318.808 | 64.883.260 | 0 | 64.883.260 | 12.209.348 | 11,5% |
| 2026-02 | 165.012.471 | 96.089.694 | 0 | 96.089.694 | 23.192.675 | 14,1% |
| 2026-03 | 216.129.319 | 133.587.384 | 0 | 133.587.384 | 25.175.843 | 11,6% |
| 2026-04 | 261.755.654 | 167.142.556 | 0 | 167.142.556 | 22.789.780 | 8,7% |
| 2026-05 | 295.504.513 | 187.558.603 | 0 | 187.558.603 | 21.777.134 | 7,4% |
| 2026-06 | 110.258.832 | 72.019.621 | 15.249.358 | 56.770.263 | 9.223.498 | 8,4% |

Junio ya tiene CMV real (FIFO): sus ventas (≥ 8/6) consumen el seed.

> ⚠️ **ADVERTENCIA (10/8/2026) — los márgenes de la tabla de arriba están SOBRESTIMADOS.**
> Son **anteriores** a la incorporación del **IIBB determinado** del Convenio Multilateral y del **impuesto al cheque**. Restaban como IIBB únicamente la **retención** que descuenta ML (~1,01%–1,11% del ingreso neto) en vez del impuesto determinado, y no restaban impuesto al cheque en absoluto. La tabla se conserva como histórico; **la tabla vigente es la de abajo**.

Tabla corregida (línea **Electrónica**, resultado operativo sobre ingreso neto, datos al 10/8/2026):

| Período | Ventas netas | Resultado ANTES | Margen antes | IIBB determinado | Imp. cheque | Resultado REAL | Margen real |
|---|--:|--:|--:|--:|--:|--:|--:|
| 2026-01 | 106.318.808 | 11.880.738 | 11,17% | −5.619.123 | — | 7.445.419 | 7,00% |
| 2026-02 | 165.076.234 | 23.195.641 | 14,05% | −8.724.549 | — | 16.229.078 | 9,83% |
| 2026-03 | 236.694.998 | 26.606.241 | 11,24% | −12.509.717 | +9.192 | 16.702.848 | 7,06% |
| 2026-04 | 261.425.076 | 21.162.148 | 8,09% | −13.816.742 | −1.100.185 | 9.021.421 | 3,45% |
| 2026-05 | 294.518.047 | 21.697.004 | 7,37% | −15.565.760 | −2.374.435 | 6.985.072 | 2,37% |
| 2026-06 | 392.898.174 | 25.073.177 | 6,38% | −20.765.310 | −2.414.469 | 6.239.501 | 1,59% |
| 2026-07 | 525.000.062 | 45.661.154 | 8,70% | −27.318.151 | −1.885.617 | 21.830.311 | 4,16% |
| 2026-08 (parcial) | 262.447.574 | 31.008.232 | 11,82% | −13.870.783 | −440.075 | 19.381.120 | 7,38% |

**El patrón que aparece cuando el número está bien: cuanto más se vende, peor margen.** De enero a junio la facturación se multiplicó por 3,7 y el margen cayó de **7,00% a 1,59%**.

## Margen operativo y los 3 montos de la venta ML (P11)

El **margen** se calcula sobre el **ingreso neto sin IVA**, no sobre el bruto (el IVA no es ingreso). El KPI de la pantalla es **Margen operativo** = resultado operativo / ingreso neto (rentabilidad sobre la venta), no markup sobre el costo.

Una misma venta ML tiene **3 montos distintos** que no hay que confundir (ejemplo mayo 2026):
- **Bruto c/IVA** = total ML = $356,1M (incluye órdenes canceladas; es lo que muestra el panel grande de ML).
- **Neto s/IVA** = $295,5M (base del estado de resultado; bruto válidas − IVA débito).
- **Por cobrar** = $265,1M (neto − comisión − envío − costo financiero − IIBB; lo que ML efectivamente liquida).

## Drill-down de control mensual (`v_control_mensual`)

La pantalla Resultado permite **desplegar cada mes** para reconciliar contra ML (doble control), con 4 bloques: **cuadre con ML** (bruto ML incl. canceladas vs bruto válidas, y órdenes excluidas), **cascada a resultado** (bruto → neto → resultado), **IVA del mes** y **cobranza** (por cobrar). Datos de la vista `v_control_mensual` (ver `ADARA-SCHEMA.md`). El drill-down es a nivel ML global del mes (no respeta filtro línea/canal todavía; OK mientras todo sea Electrónica/ML).

## IVA débito / crédito / a pagar

- **IVA débito** = de `venta_items.iva_linea` (ventas válidas). Mayo: $38,5M. El IVA promedio ronda 13% porque ~76% del neto va a 10,5% (régimen electrónico: tablets/smartwatches/bands/SSD/mouse/teclado), el resto a 21%. Alícuotas validadas por Sebastián.
- **IVA crédito** = `compra_componentes` tipo `iva` + `gastos.monto_iva` (sólo `genera_credito_iva`, USD×`tc`). **Fix 21/6/2026**: antes leía `gasto_fiscal` tipo `iva` (perc/ret, vacío) y el crédito de gastos daba 0. **Incompleto** hasta cargar todas las compras/gastos.
- **IVA a pagar** = débito − crédito (mayo: $38,5M − $6,3M = $32,2M, provisorio).

Una **pantalla de Posición Fiscal** aparte (IVA + IIBB retenciones/percepciones por jurisdicción + Ganancias) queda como workstream futuro; las piezas base ya existen (`retenciones`/`v_retenciones_iibb`, percepciones de compra con jurisdicción, `v_control_mensual`).

## IIBB, Ganancias y anticipos en el resultado (9/6/2026)

Cierre del criterio fiscal del P&L (detalle completo en `ADARA-IMPUESTOS.md`, regla `ADARA-DECISIONES.md` P12):

- **IIBB determinado** del período → **resta** al resultado (impuesto sobre ingresos, no recuperable). Hoy resta parcialmente lo que ML retiene (`impuestos`); falta completar el determinado por jurisdicción.
- **Impuesto a las Ganancias** → **resta** como renglón final (sobre la utilidad), no mezclado con costos operativos.
- **Percepciones / retenciones / anticipos** (IVA, IIBB, Ganancias, incl. importación) → **no restan al resultado**: son pagos a cuenta (crédito/activo). Restar el anticipo además del impuesto determinado sería doble conteo. Ejemplo IIBB: impuesto del período $100 (resta) − anticipos $70 ya pagados = $30 a pagar; el golpe a la rentabilidad es $100, una vez.
- En **importación**: derechos, tasa estadística, flete, seguro y nacionalización **capitalizan al costo** (CMV); las percepciones de IVA/IIBB/Ganancias **no** (son anticipos).

Cascada objetivo del P&L:
```
Ingreso neto (s/IVA)
− CMV
− comisión ML / envío / costo financiero
− gastos operativos (netos de IVA)
− IIBB determinado
= Resultado operativo
− Impuesto a las Ganancias
= Resultado neto
```
El **IVA** y los **anticipos/percepciones** quedan fuera de esta cascada → pantalla de **Posición Fiscal** (caja/fisco).

> **10/8/2026:** el pendiente marcado arriba ("falta completar el determinado por jurisdicción") quedó **resuelto**. El determinado del CM03 ya baja al Resultado y el impuesto al cheque se sumó a la cascada. Ver sección final.

## Canceladas y devoluciones en el Resultado (21/6/2026)

**Hallazgo.** `v_resultado_mensual` filtra `estado in ('aprobada','entregada')`, pero **todas** las ventas ML nacen `aprobada` (14.833/14.833) → el filtro era **no-op**. Como la proyección (`fn_proyectar_ml`) es insert-only, una orden que pasa a `cancelled` después de proyectada quedaba `aprobada` con ingreso + CMV intactos. Se colaron **21 canceladas** (~$4,11M de ingreso fantasma) y **3 `no_preparado`** habían consumido CMV ($384.062,20) de mercadería que nunca salió (violaba S4).

**Dos eventos distintos:**

1. **Cancelación** (`ml_status='cancelled'`) → **no es venta**. `fn_reconciliar_ml_canceladas` la pasa a `estado='cancelada'` y la vista la dropea (sale ingreso **y** su CMV). Si nunca salió (`no_preparado`/`preparado`), además revierte el CMV (vuelve al stock, S4). Si salió (`despachado`/`entregado`), el CMV ya no pesa en el resultado pero el stock queda pendiente de recepción (GAP). Regla `ADARA-DECISIONES.md` **P13**.
2. **Devolución** (reembolso de una venta real, p.ej. `partially_refunded`) → la venta **queda en su mes**; el reembolso es **línea negativa en el mes del movimiento** (P3). Columna `v_resultado_mensual.devoluciones`.

**Columna `devoluciones`** (append a `v_resultado_mensual`): fuente `v_cc_devoluciones` por `mes_imputacion` (mes del reembolso, **no** el de la venta), atribución de línea por la **venta proyectada** (`ventas.referencia_externa = ml_order_id`, porque `ventas_ml.linea_negocio_id` viene NULL), **signo real** (refund negativo / caso ganado positivo) y **guard anti doble conteo**: solo netea reembolsos de ventas aún contadas (`estado in aprobada/entregada`); las canceladas ya salieron del resultado y no se netean de nuevo.

**Cascada actualizada** (en `resultado.js`): `contribución − CMV − gastos + devoluciones`.

**Cobertura.** La línea depende de la conciliación de devoluciones (O10): hoy solo abril tiene movimientos conciliados (1 caso atribuible a venta 2026, +$17.130,57; 8 huérfanas −$712.201,34 que son de 2025 pre-snapshot o sin venta proyectada → fuera de la línea hasta resolverse). Se irá completando a medida que avance la conciliación de los otros meses.

**Auto-mantenimiento.** `fn_reconciliar_ml_canceladas` corre en el hook post-sync (`server.js`), orden **proyectar → reconciliar → consumir → congelar**, así toda futura cancelación post-proyección se limpia sola.

---

## IIBB determinado e impuesto al cheque en el Resultado (10/8/2026)

Detalle completo del módulo (CM03, coeficientes, alícuotas por jurisdicción, mecánica de cálculo y prorrateo) en **`ADARA-IIBB-CONVENIO-MULTILATERAL.md`**. Acá queda lo que le pega al P&L.

### El hallazgo

Sebastián preguntó si el Resultado tenía calculados los IIBB a pagar a fin de mes. **No los tenía.** `v_resultado_mensual.impuestos` salía de `ventas_ml.impuestos`, que es la **retención que descuenta ML** (~1,01%–1,11% del ingreso neto, muy estable mes a mes), y se usaba como si fuera el costo de IIBB del mes.

**No lo es.** La retención es un **pago a cuenta**; el costo del período es el **impuesto determinado**: base × coeficiente × alícuota, por cada una de las **24 jurisdicciones** del Convenio Multilateral.

### El número duro

CM03 anticipo **202606** (junio 2026), presentado el **15/07/2026**:

| | Monto |
|---|--:|
| Impuesto determinado (CM03) | **$20.765.310,42** |
| Lo que restaba ADARA (retención ML) | $4.346.103,85 |
| **Diferencia no contabilizada** | **$16.419.206,57** |

Esa diferencia es **más de la mitad del resultado operativo de junio**.

**Alícuota efectiva implícita: 5,2852%** (determinado / base). Es un **TECHO**: la base usada es la que conoce ADARA ($392.898.173,69, **solo canal ML**), y Sebastián confirmó que la base declarada del CM03 incluye **facturación B2B de luminarias** que ADARA no tiene cargada. Con la base real la alícuota efectiva baja, y el IIBB imputado a ML también.

### El impuesto al cheque tampoco estaba

No aparecía en **ningún lado** del Resultado: **$8.205.589,40** acumulados desde marzo 2026 en la tabla `retenciones`. Ahora se incorpora a la cascada.

### Márgenes corregidos

Ver la tabla corregida en la sección "CMV en el Resultado" más arriba. Resumen: **cuanto más se vende, peor margen**. De enero a junio la facturación se multiplicó por 3,7 y el margen operativo cayó de **7,00% a 1,59%**.

### Regla nueva

- **El gasto de IIBB es el DETERMINADO.** La retención de ML y las percepciones de compra son **anticipos**: reducen la caja, no el costo del período. Restar las dos cosas sería doble conteo (coherente con P12).
- **Solo la base GRAVADA genera IIBB.** Una venta sin comprobante no se declara y no tributa — mismo criterio que el IVA débito (regla **V2** de `ADARA-IMPUESTOS.md`).

### Cambios técnicos

`v_resultado_mensual` y `v_resultado_linea_mensual` se extendieron de forma **ADITIVA**: todas las columnas viejas conservan **nombre, orden y semántica** (nada del frontend existente se rompe).

| Columna nueva | Vista | Qué es |
|---|---|---|
| `base_gravada` | ambas | Base sobre la que se prorratea el IIBB (solo ventas gravadas) |
| `iibb_determinado` | ambas | IIBB determinado del período, **signo negativo** |
| `impuesto_cheque` | ambas | Impuesto a débitos/créditos bancarios, **signo negativo** |
| `iibb_base_confirmada` | ambas | Flag: si la base del período está confirmada contra el CM03 |
| `margen_contribucion_real` | `v_resultado_linea_mensual` | Margen de contribución ya con los dos costos nuevos |
| `resultado_operativo_real` | `v_resultado_linea_mensual` | Resultado operativo real (el de la tabla corregida) |

- Los **dos costos nuevos vienen con signo negativo**, igual que `comision` / `envio` / `costo_financiero`.
- El **prorrateo entre línea y canal va por base gravada**, así que el canal **`efectivo`** (venta sin comprobante) **no absorbe IIBB**.
