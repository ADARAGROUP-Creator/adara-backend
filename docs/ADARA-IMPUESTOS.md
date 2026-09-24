# ADARA — Impuestos y Posición Fiscal

Última actualización: 10 Agosto 2026 (**IIBB Convenio Multilateral — módulo implementado**: el Resultado no tenía calculado el IIBB a pagar. Se usaba `ventas_ml.impuestos` (la **retención** de ML, ~1,01%–1,11% del ingreso neto) como si fuera el costo del mes; es un **pago a cuenta**. El costo es el **impuesto determinado** (base × coeficiente × alícuota, 24 jurisdicciones). CM03 202606 presentado 15/07/2026: determinado **$20.765.310,42** vs $4.346.103,85 que restaba ADARA → **$16.419.206,57 no contabilizados**. **Alícuota efectiva implícita 5,2852%**, que es un **techo** (la base de ADARA es solo ML; la del CM03 incluye B2B de luminarias). Se incorpora también el **impuesto al cheque** ($8.205.589,40 desde marzo 2026), que no estaba en ningún lado. **Regla nueva: determinado = gasto, retención/percepción = anticipo**, y solo la base **gravada** genera IIBB. Brecha de retenciones (falta SIRCREB) y percepciones aduaneras de IIBB pendientes. Detalle completo en `ADARA-IIBB-CONVENIO-MULTILATERAL.md`. Incluye además el **análisis de comprar 50% facturado / 50% en efectivo**. Ver "Actualización — 10 Agosto 2026" al final.) · 7 Agosto 2026 (**venta sin comprobante — reglas V1/V2/V3**: `v_control_mensual.iva_debito` ahora filtra por `ventas.es_gravada`; una venta en efectivo no facturada NO genera IVA débito. Corrección conceptual importante: eso **no crea saldo a favor**, solo deja de sumar débito. Ver "Actualización — 7 Agosto 2026" al final.) · 5 Agosto 2026 (**CORRECCIÓN de regla mal generalizada**: los gastos de logística/comisión a terceros (flete, despacho, terminal) **sí generan crédito fiscal cuando hay factura A** — el neto capitaliza al costo del lote **y** el IVA discriminado va como crédito. Lo que no genera crédito es lo que **no tiene comprobante** (coima/pago informal). La redacción del 11/6 se escribió pensando en las coimas y se llevó puestas las facturas legítimas. · **N alícuotas de IVA por comprobante** implementado en Compras. · **FISC-RET-IVA**: asimetría Gastos (sí captura perc/ret de IVA) vs Compras (no existe el tipo `iva_percepcion`) + criterio operativo provisorio. · **FISC-CRED-ML** dimensionado: $120.723.837,16 de deducciones de ML en julio → ~$20.952.070,91 de crédito potencial, con la advertencia de **no cargar la factura de ML como gasto** (duplicaría el costo). · **Diseño de la tira de Posición Fiscal en la Home**: el número va acompañado de su completitud, si no engaña. Ver "Actualización 5 Agosto 2026" al final.) · 27 Julio 2026 (**Apertura fiscal de IVA IMPLEMENTADA — corte 30/06/2026**: se creó la tabla `posicion_fiscal_apertura` y se extendió `v_posicion_fiscal` con **doble arrastre** de saldos a favor. Junio se cargó como **total de la DDJJ** (F.2051 v.100, período 202606, presentada 16/07/2026, tx 1182373081): débito 70.183.307,66 / crédito 50.415.389,17 / **saldo técnico a favor 13.808.267,01** / a pagar **0** / **libre disponibilidad 26.306,80**. Julio+ = fino (factura por factura por Compras/Gastos); ene–may = `estado_fiscal='incompleto'` (no oficial; el saldo a favor previo de $33,5M ya viene dentro de junio). Pendiente nuevo **FISC-RET-IVA** (capturar retenciones/percepciones de IVA sufridas para que crezca la libre disponibilidad) + UI de `posicion-fiscal.js`. Corre el corte que estaba anotado en 31/05. Ver sección "Actualización 27 Julio 2026" al final.) · 21 Junio 2026 (**fix crédito IVA de gastos**: `v_control_mensual.iva_cred_gastos` leía `gasto_fiscal` (tabla de perc/ret, 0 filas) en vez del IVA propio del gasto → el crédito IVA de gastos con factura A se perdía. Corregido a `gastos.monto_iva WHERE genera_credito_iva` (USD×`tc`), **abrible por línea** vía `gasto_imputacion` (el % reparte el IVA igual que el neto; total DDJJ sin cambios). Ver `ADARA-GASTOS.md` G12.) · 16 Junio 2026 (**confirmado**: IVA fuera del P&L es correcto (pass-through); el flujo a pagar/a favor vive en **Posición Fiscal** `v_posicion_fiscal` (`iva_debito/iva_credito/iva_a_pagar`). Hoy el **IVA crédito está en ~0** (ene/feb/mar = 0) porque **faltan cargar las compras** → el "a pagar" está sobrestimado (es el peor escenario, como si todo fuera sin factura). **DECISIÓN NUEVA — apertura fiscal (análoga a A2)**: en vez de reconstruir el histórico de compras, cargar el **saldo a favor de IVA al mes de corte** (de la DDJJ del contador) en una filita `posicion_fiscal_apertura`, y que `v_posicion_fiscal` lo **arrastre** mes a mes (`a_pagar = débito − crédito − saldo_a_favor_previo`, como ARCA: el saldo a favor no se pierde, se traslada). De ese corte en adelante, cargar compras+ventas fino; meses previos = "fiscal incompleto". Pendiente: mes de corte + saldo a favor (del contador), también saldos a favor/anticipos de IIBB y Ganancias. Ganancias en Resultado sigue pendiente de la tasa. Ver CHANGELOG 16/6) · 10 Junio 2026 (**pantalla Posición Fiscal `#posicion_fiscal` + vista `v_posicion_fiscal`** — IVA delegado a `v_control_mensual` (fuente única), IIBB retenido canónico desde `ventas_ml.impuestos`, percepciones e impuesto al cheque; determinado IIBB y Ganancias pendientes del contador) · 9 Junio 2026 (tratamiento de impuestos en el **P&L vs Posición Fiscal**: IVA fuera del resultado (es financiero); **IIBB determinado** + **Ganancias** restan a la rentabilidad; **percepciones/anticipos** = pagos a cuenta, no costo; en importación distinguir costo capitalizable de percepciones) · 15 Mayo 2026

Cómo se trackea la posición fiscal de la empresa en vivo: **IVA, Ingresos Brutos y Ganancias**.
Permite saber en cualquier momento cuánto se va a pagar al fisco y tomar decisiones operativas (ej: comprar facturas antes del cierre).

---

## Objetivo

Responder en tiempo real:

1. **¿Cuánto IVA voy a tener que pagar este mes?** (decisión: comprar facturas para reducirlo)
2. **¿Cómo vengo de IIBB?** (cuánto generé, cuánto me retuvieron, cuánto pagar)
3. **¿Cuánto Ganancias acumulo a cuenta del año?**
4. **¿Tengo saldo a favor en algún impuesto?**

Y por línea de negocio (aunque la DDJJ es única) para analizar la rentabilidad real de cada una.

---

## Principios fiscales

| Principio | Aplicación |
|-----------|-----------|
| **Devengado** | El movimiento fiscal cuenta en el período de la factura, no del pago. DDJJ de marzo incluye las ventas del 1 al 31 de marzo aunque algunas se cobren en abril. |
| **Por período** | IVA e IIBB son mensuales (DDJJ mensual). Ganancias es anual con anticipos. |
| **Ledger por impuesto** | Cada impuesto tiene su tabla de movimientos (débitos y créditos). La posición se calcula desde ahí, no se persiste. |
| **Por línea (interno)** | Cada movimiento fiscal se imputa a una línea de negocio. AFIP/ARBA ve solo la suma, pero internamente desagregamos. |
| **Pago al fisco no afecta posición** | Cuando se paga la DDJJ, es un movimiento bancario. La posición fiscal se calcula desde las operaciones del período, no desde los pagos. |

---

## Los tres impuestos a trackear

| Impuesto | Frecuencia DDJJ | Quién paga | Quién retiene/percibe |
|----------|-----------------|------------|------------------------|
| **IVA** | Mensual | La empresa (RI) | ML, MP, bancos pueden retener; importaciones tienen percepción |
| **IIBB** (Convenio Multilateral) | Mensual + DDJJ anual | La empresa | ML, MP, bancos retienen; proveedores aplican percepción |
| **Ganancias** | DDJJ anual + anticipos | La empresa | ML, MP, bancos retienen; algunas operaciones tienen percepción |

---

## IVA — Cuenta Corriente

Modelo: ledger con débitos (lo que cobraste de IVA en tus ventas) y créditos (lo que pagaste de IVA en tus compras y gastos). El neto se paga al fisco.

### Ejemplo: marzo 2026

```
                                          DÉBITO         CRÉDITO        SALDO
─────────────────────────────────────────────────────────────────────────────────
DÉBITOS (IVA que cobraste de tus ventas)
  Ventas ML netas × 21%                $ 2.520.000
  Ventas Tango B2B netas × 21%         $   480.000
  Ventas Tienda Nube netas × 21%       $   150.000
  Ventas WhatsApp/efectivo × 21%       (sin IVA, son remitos)

CRÉDITOS (IVA que pagaste en compras y gastos)
  Compras locales c/factura A                          $   180.000
  Importaciones (IVA aduana)                           $ 1.134.000
  Servicios c/factura A (gastos)                       $    95.000

RETENCIONES Y PERCEPCIONES SUFRIDAS (pago a cuenta)
  Retenciones IVA en pagos MP                          $    65.000
  Retenciones IVA bancarias                            $    20.000
  Percepciones IVA en importaciones                    $    50.000
─────────────────────────────────────────────────────────────────────────────────
  TOTAL                                $ 3.150.000     $ 1.544.000

  POSICIÓN NETA DEL MES               $ 1.606.000 a pagar
```

Si el resultado es negativo (más crédito que débito), queda **saldo a favor** que se traslada al próximo período (carryforward).

### Reglas clave de IVA

- Las ventas con **factura B / consumidor final** tienen IVA incluido en el precio. ADARA lo desagrega: 21% va al ledger de IVA débito, el resto al P&L como ingreso.
- Las ventas con **remito** (WhatsApp/efectivo sin factura) NO generan IVA débito — pero la regularización fiscal queda al criterio del contador (fuera de ADARA).
- Las compras con **factura A** generan IVA crédito.
- Las compras con **factura B** NO generan crédito.
- Las importaciones tienen un IVA aduana (21% en general) que va como crédito.
- **Un comprobante puede tener N alícuotas** (ej. productos al 10,5% + un cargo adicional al 21%). El IVA de la compra se calcula renglón por renglón, no con una alícuota única de cabecera. Implementado el 5/8/2026 — ver la sección de esa fecha.

---

## IIBB — Cuenta Corriente

Convenio Multilateral. Mensual con DDJJ por jurisdicción + anual.

### Ejemplo: marzo 2026

```
                                                      MONTO
─────────────────────────────────────────────────────────────
GENERADO (3% sobre ventas netas)
  Ventas netas del mes × 3%                       $   840.000

RETENCIONES SUFRIDAS (pago a cuenta)
  Retenciones IIBB en pagos ML/MP                 −    85.000
  Retenciones IIBB bancarias (SIRCREB, otros)     −    35.000

PERCEPCIONES SUFRIDAS (pago a cuenta)
  Percepciones IIBB en importaciones              −   180.000
  Percepciones IIBB en compras locales            −    25.000

ANTICIPOS PAGADOS PREVIAMENTE
  Anticipo del mes anterior aplicado              −   200.000

─────────────────────────────────────────────────────────────
  SALDO A PAGAR ARBA / CABA / OTROS              $   315.000
```

> **Actualización 10/8/2026:** el "3% sobre ventas netas" de este ejemplo es un placeholder. El generado real es el **impuesto determinado del CM03** (base × coeficiente × alícuota, 24 jurisdicciones), que en junio 2026 arrojó una **alícuota efectiva implícita del 5,2852%** sobre la base conocida por ADARA. Ver "Actualización — 10 Agosto 2026" al final.

### Reglas clave de IIBB

- Se genera por **ventas netas** del mes (sin IVA), no por ventas brutas.
- La alícuota base es 3% pero **puede variar** por jurisdicción y rubro. Configurable por línea.
- Las **retenciones** son lo que vos sufriste (ej: ML retiene IIBB cuando te liquida). Son pago a cuenta.
- Las **percepciones** son cuando un proveedor te suma IIBB en sus facturas. También son pago a cuenta.
- En Convenio Multilateral, el porcentaje de ventas en cada jurisdicción determina cuánto se paga en cada una. Eso lo calcula el contador con la DDJJ trimestral/anual — ADARA mantiene el agregado.
- **(10/8/2026)** El **gasto** del período es el **impuesto determinado**, no la retención. Y **solo la base gravada** genera IIBB: una venta sin comprobante no se declara y no tributa.

---

## Ganancias — Cuenta Corriente (anual con anticipos)

A diferencia de IVA e IIBB, Ganancias es **anual**. Pero hay anticipos mensuales y retenciones a lo largo del año que son pagos a cuenta.

### Ejemplo: año fiscal 2026 (acumulado a mayo)

```
                                                      MONTO
─────────────────────────────────────────────────────────────
PAGOS A CUENTA YA REALIZADOS
  Anticipos mensuales pagados a AFIP             $   500.000
  Retenciones sufridas en cobros ML/MP           $   180.000
  Retenciones bancarias                          $    65.000
  Percepciones en importaciones                  $   140.000
─────────────────────────────────────────────────────────────
  ACUMULADO A CUENTA                            $   885.000

ESTIMACIÓN DE GANANCIAS DEL AÑO
  Resultado operativo acumulado año             $ 8.200.000
  × Tasa estimada (25% / 35%)                   ─────────────
  IMPUESTO ESTIMADO                             $ 2.870.000

  POSICIÓN PROYECTADA AL CIERRE                  $ 1.985.000 a pagar
```

### Reglas clave de Ganancias

- La tasa real depende del tipo de empresa (SRL, SA, monotributo no aplica) — a definir con contador.
- Los anticipos los liquida AFIP y se pagan mensualmente. Cada pago es un movimiento bancario que se vincula al ledger de Ganancias como pago a cuenta.
- La DDJJ es anual: en mayo del año siguiente se calcula el saldo real y se ajusta.
- Si los anticipos + retenciones superan el impuesto real, queda saldo a favor.

---

## Posición fiscal por línea

Aunque la DDJJ se presenta como suma de todas las líneas, **internamente cada impuesto se desagrega**.

```
MARZO 2026 — IVA NETO POR LÍNEA

                       Débito       Crédito        Neto
─────────────────────────────────────────────────────────
ML Electrónica       2.100.000      720.000   +1.380.000
Electrónica off-ML     420.000      120.000   +  300.000
Luminarias             310.000      450.000   −  140.000  (saldo a favor de la línea)
Mochilas Sindicatos    180.000       95.000   +   85.000
Mochilas Individuos    140.000      109.000   +   31.000
─────────────────────────────────────────────────────────
TOTAL                3.150.000    1.494.000   +1.656.000
```

Esto es informativo, no fiscal. Sirve para entender qué línea "paga más IVA" y por qué.

---

## Vista en la home (destacado permanente)

Arriba de la home, una **tira de posición fiscal del mes en curso**:

```
┌────────────────────────────────────────────────────────────────────────┐
│  POSICIÓN FISCAL — MAYO 2026 (parcial al 15/05)                        │
│                                                                          │
│  IVA del mes        $ 980.000 a pagar  ↑ vs abril (+12%)               │
│  IIBB del mes       $ 215.000 a pagar  ≈ vs abril                       │
│  Ganancias acum.    $ 685.000 a cuenta del año                         │
│                                                                          │
│  Faltan 16 días para cierre. Cargá facturas para reducir IVA neto.     │
└────────────────────────────────────────────────────────────────────────┘
```

Click en cada renglón abre la pantalla de detalle del impuesto.

> **Diseño acordado el 5/8/2026:** la tira NO puede mostrar solo el "a pagar" — tiene que mostrar **el número y su completitud juntos**. Ver "Actualización 5 Agosto 2026".

---

## Pantallas detalladas

### Pantalla "Impuestos" con tabs por impuesto

Una pantalla con 3 tabs (IVA, IIBB, Ganancias). Cada tab tiene:

1. **Posición del mes/año en curso** con el cuadro completo
2. **Histórico mensual** comparativo
3. **Detalle de movimientos del período** con drill-down a las ventas/compras que los originan
4. **Desagregado por línea de negocio**
5. **Botón "Snapshot para contador"** que exporta el cuadro listo para la DDJJ (formato a definir)

### Anticipos y pagos

Sección que muestra los anticipos pagados de Ganancias e IIBB, sus fechas, y el movimiento bancario asociado.

---

## Decisiones operativas que habilita

| Decisión | Cuándo | Qué te muestra ADARA |
|----------|--------|----------------------|
| Comprar facturas para bajar IVA del mes | Días previos al cierre | IVA neto actual del mes, monto que reduciría c/factura nueva |
| Pagar más anticipos de Ganancias | Cualquier momento | Acumulado vs estimación, gap a cubrir |
| Reclamar retenciones excesivas | Si retenciones > posición | Te avisa cuando hay sobreretención sistemática |
| Cambiar de jurisdicción IIBB | Largo plazo | Comparativo de ventas por jurisdicción (a futuro) |

---

## Reglas duras (no cambiar sin discusión)

1. **Devengado** — IVA débito en el mes de la factura, no del cobro.
2. **Remitos sin IVA débito** — las ventas WhatsApp/efectivo no generan IVA débito en ADARA. Regularización fiscal externa.
3. **Pagos al fisco no afectan posición** — son movimientos bancarios. La posición se calcula desde el ledger del período.
4. **Mes cerrado fiscalmente inmutable** — una vez cerrado, no se modifica el ledger del período. Ajustes van al mes siguiente como regularizaciones.
5. **Retenciones y percepciones sufridas son pago a cuenta** — siempre se imputan al período en que ocurrieron, nunca al período de origen de la operación.
6. **El crédito fiscal nace del comprobante, no del tipo de gasto** (5/8/2026) — si hay **factura A**, hay crédito, aunque el neto capitalice al costo del lote. Si no hay comprobante, no hay crédito. No generalizar "este tipo de gasto no da crédito".
7. **El IVA débito nace del comprobante, no de la venta** (7/8/2026, V2). Una venta sin factura mueve stock y mueve caja, pero **no genera débito**. `v_control_mensual` lo aplica con `sum(vi.iva_linea) FILTER (WHERE v.es_gravada)`: si la venta no es gravada, su IVA de línea no entra al débito **aunque los ítems tengan alícuota cargada**.
8. **Menos débito ≠ más crédito** (7/8/2026, V3). No facturar una venta **no genera saldo a favor**. El crédito de la compra ya estaba tomado en el mes de la compra. El efecto es que el débito del período es menor, nada más. Solo hay saldo a favor si el crédito del período supera al débito del período.
9. **El gasto de IIBB es el DETERMINADO; la retención es anticipo** (10/8/2026). El impuesto determinado del CM03 es el costo que resta al Resultado. La retención de ML y las percepciones de compra **no son costo**: reducen la caja (lo que queda por pagar), no la rentabilidad. Restar ambas cosas sería doble conteo.
10. **Solo la base GRAVADA genera IIBB** (10/8/2026). Una venta sin comprobante no se declara y no tributa — mismo criterio que el IVA débito (regla 7 / V2). Por eso el canal `efectivo` no absorbe IIBB en el prorrateo del Resultado.

---

## Pendientes y TBD

- **Captura de retenciones/percepciones de IVA sufridas (FISC-RET-IVA, nuevo 27/7/2026; ampliado 5/8/2026):** hoy ADARA captura IIBB retenido (`ventas_ml.impuestos`) y percepciones IIBB (`compra_componentes`), pero **no las retenciones/percepciones de IVA**. Sin eso, el **saldo de libre disponibilidad** de la Posición Fiscal (que ya arrastra el valor de apertura de junio, $26.306,80) **no crece** mes a mes. Fuente a definir: settlement MP (retención IVA), banco, percepción IVA de importación. En `v_posicion_fiscal` el término `ret_mes` está fijado en 0 (placeholder) hasta implementarlo.
  - **Asimetría del modelo (5/8/2026):** en **Gastos** el lugar existe (`gasto_fiscal` admite `perc_iva` y `ret_iva`); en **Compras no existe el tipo** — `compra_componentes` solo acepta `iva`, `iibb_percepcion`, `ganancias_percepcion` y `otro_impuesto`. Falta `iva_percepcion`, **imprescindible para despachos** (aparece en casi todos). Y en ninguno de los dos casos alimenta todavía la Posición Fiscal.
  - **Criterio operativo mientras tanto:** cargar la compra **SIN** esa percepción y anotarla aparte. **No** meterla como `tipo='iva'`: eso la mandaría al colchón equivocado (saldo **técnico**, que solo tapa débito futuro) cuando en realidad es **libre disponibilidad** (aplicable a otros impuestos o pedible en devolución).
- **Crédito fiscal de las deducciones de ML (FISC-CRED-ML, dimensionado 5/8/2026):** las comisiones, envíos y costo financiero que ML cobra son servicios facturados; si vienen con IVA incluido, hay crédito fiscal que hoy no se toma. Julio 2026 ≈ **$20.952.070,91** de crédito potencial. **Advertencia: no cargar la factura de ML como gasto** — ese costo ya resta en `v_resultado_mensual` vía `ventas_ml` y se duplicaría. Ver la sección del 5/8.
- **Ingesta de SIRCREB (nuevo 10/8/2026):** las recaudaciones bancarias de IIBB del Supervielle no se ingestan. El CM03 de junio computó $15.471.267,94 de "Valores Restan" y ADARA solo ve $1,53M (settlement MP) + $4,35M (API de ML). No afecta el margen (la retención no es gasto) pero sí el **saldo a pagar**.
- **Percepciones aduaneras de IIBB (nuevo 10/8/2026):** la hoja de Tucumán del CM03 muestra $59.316,88 de percepciones aduaneras de importaciones que ADARA no tiene, porque **los despachos siguen sin cargar** (los 9 borradores del simulador). Ver `ADARA-IMPORTACIONES-SIM.md`.
- **Base declarada del CM03 vs base de ADARA (nuevo 10/8/2026):** la base del CM03 incluye facturación B2B de luminarias que ADARA no tiene cargada. Hasta cargarla, la alícuota efectiva implícita (5,2852%) es un **techo** y el IIBB imputado a ML está sobrestimado.
- **Reintegro fiscal de devoluciones (gateado a TFactura):** en cada devolución, la nota de crédito emitida en Tango revierte el IVA/IIBB/Ganancias que ML reintegra; ese reintegro debe acreditarse en los ledgers fiscales (capa 6, A14). Se implementa con la integración TFactura (vínculo NC ↔ devolución por `ml_order_id` / `ExternalID`). Mientras tanto, el impacto financiero/COGS de la devolución se maneja en `ADARA-CANCELACIONES-DEVOLUCIONES.md`, sin asentar el reintegro fiscal.
- **Tasa real de Ganancias** según tipo de sociedad: a definir con contador. **(10/8/2026: el análisis de la compra 50/50 usa la tasa marginal de sociedades del 35%, que sigue sin confirmar. Si la efectiva es menor, el costo calculado baja proporcionalmente.)**
- **Alícuotas IIBB por jurisdicción** según Convenio Multilateral: a configurar con contador. **(10/8/2026: parcialmente cubierto — el CM03 202606 aporta coeficientes y alícuotas de las 24 jurisdicciones; ver `ADARA-IIBB-CONVENIO-MULTILATERAL.md`.)**
- **Saldos fiscales iniciales al 31/12/2025**: a pedir al contador antes de arrancar la operación
- **DDJJ Anual de Ganancias 2025**: si quedó saldo a favor o en contra, cargarlo como saldo inicial del ledger
- **Snapshot exportable para contador**: definir formato (Excel, CSV, PDF) con el contador
- **Tira de Posición Fiscal en la Home**: diseño definido el 5/8/2026 (número + completitud), implementación pendiente.

### Pedidos al contador (consolidado)

1. **Tasa efectiva de Ganancias** de la sociedad (hoy se usa 35% marginal, sin confirmar).
2. **Saldos fiscales iniciales al 31/12/2025** y resultado de la **DDJJ Anual de Ganancias 2025**.
3. **Formato del snapshot** mensual para la DDJJ.
4. **(10/8/2026)** **Base declarada del CM03**: confirmar qué facturación B2B de luminarias entra, para separar la base de ML de la base total.
5. **(10/8/2026)** **SIRCREB**: de dónde bajar el detalle de recaudaciones bancarias del Supervielle para cerrar la brecha de retenciones.
6. **(10/8/2026)** **Régimen de salidas no documentadas**: qué implica para la compra con parte en efectivo (ver análisis 50/50 al final).
7. **(10/8/2026)** **Impuesto al cheque**: confirmar qué porción computa como pago a cuenta de Ganancias (pendiente abierto desde el 10/6).

---

## Documentos relacionados

- `ADARA-FLUJO-OPERATIVO.md` — eventos diarios que alimentan el ledger fiscal
- `ADARA-PNL.md` — P&L sin IVA, complementario a esta vista
- `ADARA-IIBB-CONVENIO-MULTILATERAL.md` — IIBB determinado del Convenio Multilateral: CM03, coeficientes, alícuotas por jurisdicción, retenciones y bajada al Resultado
- `ADARA-LINEAS-NEGOCIO.md` — cómo se imputa cada movimiento fiscal a una línea
- `ADARA-COMPRAS-IMPORTACIONES.md` — generación de crédito IVA en compras
- `ADARA-CONCILIACION-BANCARIA.md` — pagos al fisco como movimientos bancarios
- `ADARA-DECISIONES.md` — reglas consolidadas

---

## Tratamiento en el Resultado vs Posición Fiscal (decisión 9/6/2026)

Regla madre: **el Resultado (P&L) mide si el negocio gana plata vendiendo; la Posición Fiscal mide cuánto le debe la empresa al fisco y cómo pega en la caja.** Son dos tableros distintos y no hay que mezclarlos. Ver `ADARA-DECISIONES.md` P12.

### IVA — NO entra al resultado
El IVA no es ingreso ni costo de la empresa: es cobranza/pago por cuenta del fisco. El P&L trabaja **todo neto de IVA** (ventas sin IVA, costos y gastos sin IVA), así que el IVA ya está fuera por diseño. Restar el IVA a pagar sería doble conteo y trataría como pérdida algo que el cliente ya pagó. El IVA vive **solo** en Posición Fiscal (débito − crédito = a pagar), donde importa como **flujo de caja**, no como rentabilidad.

**Sobre "comprar facturas para bajar IVA":** el efecto es reducir lo que se deposita de IVA (caja), no aumentar la rentabilidad operativa. Además, la factura mete un gasto que no es real, que ensuciaría el resultado y Ganancias. Por eso **no se modela como ganancia en el P&L** — rompería justamente la confiabilidad del número. Es una operación de naturaleza fiscal (y de riesgo), ajena a la rentabilidad del negocio.

### IIBB — SÍ resta al resultado (el determinado del período)
Ingresos Brutos es impuesto sobre la facturación, **no recuperable** → es costo real. Lo que resta a la rentabilidad es el **IIBB determinado del período** (alícuota × base por jurisdicción, Convenio Multilateral), una sola vez. Hoy el P&L ya resta parcialmente lo que **ML retiene** (campo `impuestos` de `v_resultado_mensual`); falta completar con el determinado del período.

> **10/8/2026 — RESUELTO.** El determinado del CM03 ya baja al Resultado por columna propia (`iibb_determinado`), y la retención dejó de usarse como proxy del costo. Ver la sección final.

### Ganancias — SÍ, como renglón final
El Impuesto a las Ganancias se calcula **sobre la utilidad**, así que va como **última línea** del P&L (después del resultado operativo → resultado neto), no mezclado con los costos operativos.

### Percepciones, retenciones y anticipos = pagos a cuenta (NO costo)
Las percepciones (IVA, IIBB, Ganancias — incluidas las de **importación**) y las retenciones que sufre la empresa **no son costo**: son **plata adelantada** a cuenta del impuesto que se va a pagar igual. Son un **crédito/activo** contra el fisco. Descuentan lo que queda por pagar de cada impuesto; **deducirlas además del impuesto determinado sería contar dos veces.**

Ejemplo (IIBB): impuesto del período $100 (esto resta al resultado) — anticipos ya pagados $70 → quedan $30 por pagar. El golpe a la rentabilidad es **$100**, no $70 + $100, ni $30. Los anticipos solo cambian el *timing* del pago.

Si se adelanta más de lo que se determina, queda **saldo a favor** (activo fiscal). Relevante para caja y patrimonio, no para rentabilidad. Caso típico: percepciones de IIBB en importaciones que se acumulan.

### Importación: qué capitaliza y qué no
Al nacionalizar, separar:
- **Costo de la mercadería** (capitaliza al `lotes.costo_unitario` → CMV): derechos de importación, tasa estadística, flete, seguro, gastos de nacionalización. **El IVA discriminado de esos servicios, cuando vienen con factura A, es crédito fiscal aparte** (el neto capitaliza, el IVA no).
- **Percepciones de IVA / IIBB / Ganancias**: NO capitalizan, NO son costo → son anticipos (crédito fiscal). En el sistema ya van separadas en `compra_componentes` (`iva` / `iibb_percepcion` / `ganancias_percepcion`), justamente para que no ensucien el costo del lote. **Falta el tipo `iva_percepcion`** (FISC-RET-IVA).

### Pantalla Posición Fiscal — IMPLEMENTADA (10/6/2026, nivel 1)
Pantalla `#posicion_fiscal` (`screens/posicion-fiscal.js`, `loadPosicionFiscal()`) que muestra, por período, tres secciones: **IVA** (débito − crédito = a pagar), **IIBB** (retenido y percepciones como pago a cuenta; determinado y saldo marcados *pendiente*) y **Otros** (impuesto al cheque + nota de Ganancias). KPIs arriba (IVA a pagar acumulado, IIBB pagos a cuenta, impuesto al cheque, IIBB determinado=pendiente).

Lee la vista **`v_posicion_fiscal`** (mensual). Diseño de fuentes (evita doble conteo y divergencia):
- **IVA** (`iva_debito`, `iva_credito`, `iva_a_pagar`): **delegado a `v_control_mensual`** — la misma vista que usa el panel de Resultado, así ambas pantallas no pueden mostrar números distintos. Crédito de compras con conversión USD×`tc_blue`. El **crédito IVA de gastos** sale de `gastos.monto_iva` (sólo `genera_credito_iva = factura_a AND iva>0`, USD×`tc`) — **fix 21/6/2026**: antes leía `gasto_fiscal` (perc/ret) y daba 0. Se reparte por línea con el % de `gasto_imputacion` (G12); el total mensual no cambia. **(27/7/2026: para los meses posteriores al corte de apertura, estos valores pasan por el arrastre — ver sección de abajo.)**
- **`iibb_retenido`**: **fuente canónica = `ventas_ml.impuestos`** (`sum(-impuestos)` por mes de venta). Completo desde enero, venta por venta, al centavo. Es **pago a cuenta**, no costo. La tabla `retenciones` se usa solo para la apertura por jurisdicción (gap histórico ene/feb — ver `ADARA-RETENCIONES-IIBB.md`), nunca para sumar el monto (sería doble conteo).
- **`iibb_percepciones`**: `compra_componentes` tipo `iibb_percepcion` (compras activas). Pago a cuenta.
- **`impuesto_cheque`**: `retenciones` tipo `impuesto_cheque` (impuesto a débitos/créditos bancarios).

**Criterio de período (a afinar):** `iibb_retenido` se imputa al **mes de la venta** (`ventas_ml.fecha`); `retenciones`/`impuesto_cheque` al **release** (su `periodo`), de ahí que aparezca un período "futuro" con solo impuesto al cheque (release programado). I5 pide imputar al período en que ocurrió la retención (release); por eso el monto canónico desde `ventas_ml` es una aproximación al mes de venta, suficiente para nivel 1.

### Pendiente (workstream Posición Fiscal — nivel 2)
- **IIBB determinado** del período (alícuota × base por jurisdicción, Convenio Multilateral): falta cargar las **alícuotas del contador**. Con eso se calcula el saldo (determinado − pagos a cuenta) y el determinado se **suma al Resultado** (hoy el Resultado resta `ventas_ml.impuestos` re-etiquetado como retención provisoria; ver `ADARA-PNL.md`). **(10/8/2026: resuelto — ver sección final.)**
- **Ganancias**: pendiente de la **tasa del contador**; va como renglón final del Resultado, no en Posición Fiscal.
- **Impuesto al cheque**: definir con el contador si va como costo financiero al Resultado y qué porción computa como pago a cuenta de Ganancias. **(10/8/2026: ya baja al Resultado como costo; falta la definición sobre el cómputo a cuenta de Ganancias.)**
- **IVA crédito incompleto**: el "a pagar" sale alto hasta cargar las compras (Tango/B2B) y los gastos con factura A. El débito es exacto.
- Validar con el contador la determinación de IIBB y la base de Ganancias. Tira de Posición Fiscal en la Home: diseño definido el 5/8/2026, implementación pendiente.

---

## Actualización — 11 Junio 2026 (compra sin factura)

- **Compra sin factura ≠ crédito IVA.** Una compra sin factura se carga con alícuota **Exento** en todos los renglones → no genera crédito fiscal de IVA. Tomar crédito sin factura inflaría falsamente el IVA a favor y distorsionaría la Posición Fiscal. (Refuerza la regla: el crédito fiscal solo nace de factura A.)
- **Gastos de logística/comisión a terceros (flete, despacho, coima):** el **neto capitaliza al costo del lote** (CMV). Ver `ADARA-COMPRAS-IMPORTACIONES.md` (CF12 en `ADARA-DECISIONES.md`).
  > ⚠️ **CORREGIDO el 5/8/2026.** La redacción original de este bullet decía que estos gastos "**no son crédito fiscal**", y eso está **mal generalizado**: vale para lo informal (la coima, el pago sin comprobante), pero **si el fletero, el despachante o la terminal son responsables inscriptos y facturan A, ese IVA SÍ es crédito fiscal**. La regla se escribió pensando en las coimas y se llevó puestas las facturas legítimas. Redacción correcta: **el neto capitaliza al costo del lote y el IVA discriminado es crédito, cuando hay factura A**. Ver la sección del 5/8/2026.

---

## Actualización — 7 Julio 2026 (IVA al día — pendientes)

> ✅ **Cerrado el 27/7/2026** (ver sección siguiente): el corte de apertura quedó en **30/06/2026** (no 31/05) y el saldo a favor se cargó desde la DDJJ de junio. Esta nota se conserva como historia.

- **Apertura fiscal (FISC-APERTURA):** corte confirmado **31/05/2026** (DDJJ de mayo presentada). Falta el dato de Sebastián: **saldo a favor de IVA que la DDJJ de mayo arrastra a junio** (casilla "saldo técnico a favor del período siguiente"); si mayo cerró a pagar → arrastre $0. Construir tabla `posicion_fiscal_apertura` (patrón post-A17) + modificar `v_posicion_fiscal` para arrastrar el saldo a favor mes a mes (`a_pagar = máx(0, débito − crédito − saldo_previo)`). Marcar meses pre-corte como "fiscal incompleto".
- **Junio fino:** el débito ML ya fluye; falta cargar crédito (compras vía `compra_componentes`, gastos factura A con `genera_credito_iva`+`monto_iva`). **Aviso:** el débito es solo ML; el B2B de Tango (Capa 6) no está sincronizado → la posición será exacta para ML pero no cuadra la DDJJ total hasta conectar Tango.
- **Alícuota Kindle `TA002V`:** a resolver 10,5% vs 21% (`skus`=21%). Afecta el **débito** de las ventas del Kindle. Ver `ADARA-COSTEO-FIFO.md`.

---

## Actualización — 27 Julio 2026 (Apertura fiscal de IVA implementada — corte 30/06/2026)

Se implementó la **apertura fiscal de IVA** (FISC-APERTURA, pendiente desde 16/6 y 7/7). El corte quedó en **30/06/2026** (la DDJJ de junio ya presentada), no en 31/05: junio se carga como **total de la DDJJ** y julio en adelante fino, factura por factura (compras + gastos factura A, por sus flujos existentes).

### Fuente cargada (DDJJ de junio, F.2051 v.100)
Período 202606, secuencia Original, presentada 16/07/2026, tx 1182373081, CUIT 30-71747647-2 (ADARA RS). El PDF queda como respaldo del número.
- Total del débito fiscal del período: **$70.183.307,66**
- Total del crédito fiscal del período: **$50.415.389,17**
- Saldo técnico a favor del período anterior: **$33.576.185,50**
- **Saldo técnico a favor del contribuyente: $13.808.267,01** ← arrastra a julio
- Saldo técnico a favor de ARCA (a pagar): **$0,00**
- Total de retenciones/percepciones/pagos a cuenta = **saldo de libre disponibilidad: $26.306,80**

### Qué se construyó
- **Tabla `posicion_fiscal_apertura`** (una fila por mes de corte): `periodo` (PK), `iva_debito`, `iva_credito`, `saldo_favor_previo`, `saldo_favor` (técnico, el que arrastra), `iva_a_pagar`, `libre_disponibilidad`, `fuente`. RLS on + policy `authenticated` (patrón A17). Detalle de schema en `ADARA-SCHEMA.md`.
- **`v_posicion_fiscal` extendida** (NO es vista nueva — se hizo `create or replace` sobre la existente para no duplicar):
  - `periodo = 2026-06` → **congelado** con los números de la DDJJ (`estado_fiscal='apertura'`).
  - `periodo > 2026-06` → **fino**, con arrastre recursivo de los dos colchones (`estado_fiscal='fino'`).
  - `periodo < 2026-06` → `estado_fiscal='incompleto'` (no oficial; el saldo a favor previo de $33,5M ya viene dentro de la DDJJ de junio → **no se reconstruye ene–may**).
  - **Columnas nuevas** (agregadas al final para no romper el frontend): `saldo_tecnico_favor`, `libre_disponibilidad`, `estado_fiscal`. Las 7 originales quedan iguales.
  - **`v_control_mensual` NO se tocó** (la usa Resultado) → el P&L no se ve afectado.

### Los dos saldos a favor (no se mezclan)
1. **Saldo técnico a favor** (nace de crédito > débito): solo tapa IVA **débito** futuro.
   - `a_pagar_técnico = máx(0, débito − crédito − técnico_previo)`
   - `técnico_saliente = máx(0, técnico_previo + crédito − débito)`
2. **Saldo de libre disponibilidad** (retenciones/percepciones/pagos a cuenta de IVA sufridos): reduce el IVA a pagar que quede **después** del técnico, y puede aplicarse a otros impuestos o pedir devolución.
   - `a_pagar = máx(0, a_pagar_técnico − libre_previa − ret_mes)`
   - `libre_saliente = máx(0, libre_previa + ret_mes − a_pagar_técnico)`
   - `ret_mes` (retenciones/percepciones de IVA del mes) hoy = **0** (placeholder — ver FISC-RET-IVA en Pendientes).

Ambos se guardan y arrastran **por separado**, porque se usan distinto. Sumarlos en un solo número rompe la mecánica de ARCA (aunque el total a favor de la empresa es la suma: $13,8M + $26 mil).

### Verificación (27/7/2026)
- **Junio:** a pagar $0, técnico $13.808.267,01, libre disponibilidad $26.306,80. ✓ Igual a la DDJJ.
- **Julio (parcial):** débito ML $27.565.888,63, crédito aún $0 → consume todo el técnico y da **a pagar $13.731.314,82**. Es el **peor caso** (todo sin factura); baja a medida que se cargue el crédito de julio por Compras/Gastos. El débito es **solo ML** hasta conectar Tango B2B.
- **Ene–may:** `estado_fiscal='incompleto'` (números derivados, no oficiales).

### Pendientes que quedan
- **FISC-RET-IVA:** capturar las retenciones/percepciones de IVA sufridas cada mes para que la **libre disponibilidad** crezca de julio en adelante (hoy solo arrastra el valor de apertura). Ver Pendientes y TBD.
- **Frontend (`posicion-fiscal.js`):** todavía no muestra los renglones nuevos (saldo técnico a favor / libre disponibilidad) ni usa `estado_fiscal` para marcar/ocultar los meses "incompleto". El número de "IVA a pagar" ya sale correcto (con arrastre) porque respeta la columna `iva_a_pagar`; falta la UI para explicar el colchón y flaggear los meses no oficiales.

---

## Actualización — 5 Agosto 2026 (corrección de crédito fiscal, N alícuotas, tira de Home, FISC-CRED-ML)

### 1. CORRECCIÓN — el IVA de flete/despacho/terminal con factura A SÍ es crédito

**Qué decía mal el doc:** que los gastos de logística/comisión a terceros (flete, despacho, coima) "no son crédito fiscal: capitalizan al costo del lote".

**Por qué está mal:** la regla es correcta **solo para lo informal**. Si el fletero, el despachante o la terminal son **responsables inscriptos y facturan A**, ese **IVA es crédito fiscal como cualquier otro**. Lo que no genera crédito es **lo que no tiene comprobante**, no el tipo de gasto. La regla se escribió pensando en las coimas y se llevó puestas las facturas legítimas.

**Magnitud del error:** en un despacho, honorarios del despachante + TCA + flete interno pueden sumar varios millones. Se estaba perdiendo el 21% de todo eso.

**Regla reformulada:**

| Comprobante | Neto | IVA |
|---|---|---|
| **Factura A** (fletero/despachante/terminal RI) | Capitaliza al **costo del lote** (CMV) | **Crédito fiscal** |
| **Sin comprobante** (coima, pago informal) | Capitaliza al **costo del lote** (CMV) | No hay crédito |

El neto capitaliza en los dos casos: lo que cambia es si hay o no IVA que computar. Ver `ADARA-COMPRAS-IMPORTACIONES.md` y CF12 en `ADARA-DECISIONES.md` — la regla CF12 hay que releerla con esta corrección.

### 2. FISC-RET-IVA — asimetría del modelo y criterio operativo

Hay un lugar donde cargar percepciones/retenciones de IVA en **Gastos** y no lo hay en **Compras**:

| Módulo | Tabla | Tipos disponibles | ¿Percepción de IVA? |
|---|---|---|---|
| Gastos | `gasto_fiscal` | incluye `perc_iva` y `ret_iva` | **Sí** |
| Compras | `compra_componentes` | `iva`, `iibb_percepcion`, `ganancias_percepcion`, `otro_impuesto` | **No existe el tipo** |

Y en **ninguno de los dos casos** alimenta todavía la Posición Fiscal: `ret_mes` sigue fijo en 0.

- **Falta el tipo `iva_percepcion` en `compra_componentes`.** Es **imprescindible para despachos de importación**: aparece en casi todos.
- **Criterio operativo mientras tanto:** cargar la compra **SIN** la percepción de IVA y anotarla aparte. **No usarla como `tipo='iva'`** — eso la mete en el colchón equivocado: quedaría como **saldo técnico** (que solo tapa débito de IVA futuro) cuando en realidad es **libre disponibilidad** (aplicable a otros impuestos o pedible en devolución). Los dos saldos se arrastran por separado justamente porque se usan distinto.

### 3. Tira de Posición Fiscal en la Home — diseño (decisión del 5/8)

El usuario quiere el número **siempre visible**. Pero mostrar solo el "a pagar" **engaña**: hoy la tira diría "**$36M a pagar**" todos los días, y ese número es el **peor caso** — calculado como si todo se comprara sin factura, porque el crédito está incompleto.

**Decisión: la tira muestra el número y su completitud juntos.** Renglones:

1. **A pagar estimado** (el número grande)
2. **Crédito cargado** del período
3. **Cantidad de comprobantes** cargados
4. **Días desde la última carga** de crédito
5. **Colchón disponible** (saldo técnico + libre disponibilidad)

Sin el renglón de completitud, el número **no es accionable**: no se distingue "debo $36M" de "todavía no cargué las facturas". Implementación pendiente.

### 4. N alícuotas de IVA por comprobante — IMPLEMENTADO

Una misma factura puede traer renglones con distintas alícuotas. Implementado el 5/8 en Compras:

```
IVA de la compra = Σ (cada producto × su alícuota)
                 + Σ (cada gasto prorrateable de esta factura × su alícuota)
```

**Caso real que lo motivó:** factura de **Jukebox** con productos al **10,5%** y un cargo adicional al **21%** → **$590.520,00 + $8.152,37**.

Consecuencia: la alícuota es un atributo del **renglón** (SKU / gasto prorrateable), no de la cabecera de la factura. De ahí la importancia de que la alícuota del SKU esté bien cargada (ver `ADARA-FRONTEND.md`, select de alícuota 5/8).

### 5. FISC-CRED-ML — dimensionamiento del crédito de las deducciones de ML

Medición del 5/8 sobre ventas `paid` / `partially_refunded`:

| Concepto | Julio 2026 |
|---|---|
| Comisiones | $75.895.446,57 |
| Envíos | $9.650.337,09 |
| Costo financiero | $35.178.053,50 |
| **Total deducciones** | **$120.723.837,16** |
| **Crédito fiscal potencial** (si vienen con IVA incluido) | **~$20.952.070,91** |
| A pagar de julio (referencia) | $35.336.059,70 |

| Concepto | Agosto 2026 (parcial) |
|---|---|
| Total deducciones | $12.859.565,02 |
| Crédito fiscal potencial | ~$2.231.825,33 |

**A confirmar:** que esos importes vengan con **IVA incluido** — hay que verificarlo contra la **factura real de ML**.

> ⚠️ **Advertencia operativa — NO cargar la factura de ML como gasto.** El costo de esas comisiones/envíos/financiero **ya está restando** en `v_resultado_mensual` vía `ventas_ml`. Cargar la factura de ML como gasto **duplicaría el costo** en el Resultado. Hay que resolver **cómo tomar el IVA sin tocar el costo** (mecanismo pendiente de diseño).

### 6. Posición fiscal al cierre de la sesión (5/8/2026)

| | Julio 2026 | Agosto 2026 |
|---|---|---|
| IVA débito | $60.147.783,57 | $8.722.138,79 |
| IVA crédito | $10.977.150,06 | $0 |
| **IVA a pagar** | **$35.336.059,70** | — |
| Percepciones IIBB | $2.047.894,87 | — |
| Saldo técnico a favor | $0 | — |
| Libre disponibilidad | $0 | — |

Julio ya consumió íntegramente el colchón de apertura de junio (técnico $13.808.267,01 + libre $26.306,80): ambos quedan en **$0**.

### 7. Recordatorio de alcance

**El débito sigue siendo solo ML.** Sin el **B2B de Tango** cargado, la Posición Fiscal **no cuadra contra la DDJJ** por más fino que se cargue el crédito. Cargar crédito mejora el número pero no cierra la brecha de alcance.

---

## Actualización — 7 Agosto 2026 (venta sin comprobante: V1, V2, V3)

Se implementó el circuito de **venta en efectivo sin facturar** (detalle funcional completo en `ADARA-VENTAS-EFECTIVO.md`). Impacto fiscal:

### V1 — Una venta sin comprobante no genera IVA débito

Los ítems de una venta en efectivo se cargan con **`alicuota_iva = 0`** y `precio_unitario_neto` = **lo efectivamente cobrado**. No hay que desagregar nada: sin factura no hay IVA incluido, el bruto **es** el neto. Refuerza (y ahora implementa) la regla dura 2 de este documento, que hasta el 7/8 era solo un enunciado sin código detrás.

### V2 — El filtro `es_gravada` en `v_control_mensual`

`ventas.es_gravada` es una **columna generada** que deriva del tipo de comprobante de la venta. La vista de control mensual pasó a calcular el débito así:

```sql
sum(vi.iva_linea) FILTER (WHERE v.es_gravada)
```

**Por qué hacía falta el filtro y no alcanzaba con la alícuota en 0.** Con la alícuota en 0 el `iva_linea` de esa venta ya da 0, así que hoy el filtro parece redundante. No lo es: es la **defensa estructural**. El día que se cargue una venta no gravada con la alícuota mal puesta —o que otro canal empiece a emitir remitos— el filtro impide que ese IVA se cuele al débito. La condición correcta es "¿esta venta es gravada?", no "¿cuánto IVA quedó en la línea?".

Migración: **`iva_debito_solo_ventas_gravadas`**. `v_control_mensual` es la fuente única de la que `v_posicion_fiscal` **delega** el IVA (ver sección del 10/6), así que el arreglo aplica a las dos pantallas a la vez y no pueden divergir.

### V3 — CORRECCIÓN: no facturar la venta NO genera saldo a favor de IVA

Esto se discutió explícitamente con Sebastián, que esperaba que el crédito de la compra de esa mercadería "quedara como saldo de IVA a favor". **No funciona así, y la diferencia importa.**

Lo que efectivamente pasa:

| Momento | Qué se registra |
|---|---|
| Compra con factura A | El **crédito ya se tomó**, en el período de la compra. No queda esperando a nada. |
| Venta sin facturar | **No suma débito.** El crédito no "se activa" ni se reserva: ya estaba computado. |
| Cierre del período | `a pagar = débito − crédito − colchones`. Menos débito → menos a pagar. |

O sea: el beneficio es **pagar menos IVA ese mes**, no acumular un activo fiscal. **Saldo a favor** solo aparece si el **crédito del período supera al débito del período** — y eso depende de todo el mes, no de esta venta en particular.

**Verificación con datos reales (julio 2026):** débito $60.709.745,42 contra crédito $10.977.150,06. El débito le saca al crédito casi 6 a 1, así que no hay ni cerca de saldo a favor. Julio además **consumió íntegramente** el colchón de apertura de junio (saldo técnico $13.808.267,01 + libre disponibilidad $26.306,80): ambos quedaron en **$0**. Una venta sin facturar en ese contexto reduce el a pagar del mes; no genera nada trasladable.

### Riesgo asumido, explicitado

La operación deja, del lado fiscal, una asimetría deliberada: **el stock sale** (el CMV real se descuenta vía FIFO, el P&L interno queda correcto) y **el crédito de IVA de esa mercadería queda tomado sin débito correlativo**.

ADARA la registra **como es** y la deja **etiquetada** (canal `efectivo`, referencia `EFVO-######`, `es_gravada = false`), para que sea trivialmente identificable. El tratamiento fiscal —regularizar, no regularizar, cómo— es **criterio del contador y queda fuera de ADARA**. Mismo principio que los gastos sin factura de importación: el sistema refleja la realidad económica, no decide la realidad declarable.

### Qué NO cambió

- **`v_resultado_mensual` no se tocó.** El P&L trabaja neto de IVA por diseño, y esta venta ya entra neta. Vendida al costo, aporta margen ~$0, que es lo correcto.
- **El CMV lo fija el costo del lote al consumir**, no el precio de venta. Un error de tipeo en el precio ensucia el margen, nunca el CMV (se comprobó en vivo corrigiendo la venta `EFVO-018262`, ver `ADARA-MOVIMIENTOS.md`).
- **El alcance sigue siendo el mismo:** el débito es solo ML + efectivo. Sin el B2B de Tango, la Posición Fiscal no cuadra contra la DDJJ.

---

## Actualización — 10 Agosto 2026 (IIBB Convenio Multilateral)

Se implementó el módulo de **IIBB Convenio Multilateral**. El **detalle completo** (CM03, coeficientes unificados, alícuotas de las 24 jurisdicciones, mecánica de cálculo, prorrateo y bajada al Resultado) vive en **`ADARA-IIBB-CONVENIO-MULTILATERAL.md`**. Este documento queda con IVA, Ganancias y la posición fiscal general; abajo va solo el resumen.

### El hallazgo

Sebastián preguntó si el Resultado tenía calculados los IIBB a pagar a fin de mes. **No los tenía.** `v_resultado_mensual.impuestos` salía de `ventas_ml.impuestos`, que es la **retención que descuenta ML** (~1,01%–1,11% del ingreso neto, muy estable mes a mes), y se usaba como si fuera el costo de IIBB del mes.

**No lo es.** La retención es un **pago a cuenta**; el costo del período es el **impuesto determinado**: base × coeficiente × alícuota, por cada una de las **24 jurisdicciones**.

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

No aparecía en **ningún lado** del Resultado: **$8.205.589,40** acumulados desde marzo 2026 en la tabla `retenciones`. Ahora se incorpora como costo.

### Regla nueva

- **El gasto de IIBB es el DETERMINADO.** La **retención de ML** y las **percepciones de compra** son **anticipos**: reducen la caja (el saldo a pagar), no el costo del período. Restar las dos cosas sería doble conteo (coherente con la regla del 9/6 sobre pagos a cuenta).
- **Solo la base GRAVADA genera IIBB.** Una venta sin comprobante no se declara y no tributa — mismo criterio que el IVA débito (regla **V2**). Por eso el canal `efectivo` no absorbe IIBB en el prorrateo.

Ambas quedaron como reglas duras 9 y 10 de este documento. El impacto en márgenes (tabla corregida mes a mes) está en `ADARA-PNL.md`.

### Brecha de retenciones

El CM03 de junio computó **$15.471.267,94** de "Valores Restan". ADARA ve **$1,53M** por el settlement de MP y **$4,35M** por la API de ML. Falta casi seguro **SIRCREB** (recaudaciones bancarias del Supervielle), que **no se ingesta**.

**No afecta el margen** (la retención no es gasto), **pero sí el saldo a pagar**: con menos pagos a cuenta reconocidos, ADARA sobreestima lo que queda por depositar.

### Percepciones aduaneras de IIBB

La hoja de **Tucumán** del CM03 muestra **$59.316,88** de percepciones aduaneras de importaciones. ADARA no las tiene porque **los despachos siguen sin cargar** (los 9 borradores del simulador). Ver `ADARA-IMPORTACIONES-SIM.md`.

---

### Análisis — comprar con 50% facturado y 50% en efectivo (10/8/2026)

Un proveedor le ofreció a Sebastián pagar **la mitad con factura y la mitad en efectivo**. El análisis:

**Conclusión: por cada peso pagado sin comprobante se pierden ~45,5 centavos en impuestos.**

| Componente | Tasa |
|---|--:|
| Crédito de IVA que no se toma (mercadería al 10,5%) | 10,5% |
| Costo que no se puede deducir de Ganancias (tasa marginal de sociedades) | 35% |
| **Costo total del peso no documentado** | **45,5%** |

**El supuesto que lo sostiene:** que esa mercadería **se venda facturada**. Es el caso de ADARA — vende casi todo por ML y **no tiene canal para vender en negro a esa escala**: la venta en efectivo de julio fue **$8,1M** contra **$60,7M** de débito mensual.

**Punto de equilibrio:** para que la operación no pierda plata, el descuento tiene que ser del **45,5% sobre la parte en efectivo**, o **22,75% sobre el total de la compra**.

**Si la mercadería es del 21% de IVA**, el costo sube a **56%** de la parte en efectivo.

**Advertencias:**
- El cálculo **NO incluye IIBB**, que lo empeora (el ingreso de esa venta tributa igual, sin costo deducible correlativo).
- Existe además el **régimen de salidas no documentadas**, que hay que **consultar con el contador**.
- La **tasa de Ganancias del 35%** es la **marginal de sociedades** y en el proyecto sigue figurando como **pendiente de confirmar con el contador**. Si la efectiva resulta menor, el 45,5% baja proporcionalmente.
