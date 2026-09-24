# ADARA — Decisiones Consolidadas

Última actualización: 10 Agosto 2026 (**IIBB / Convenio Multilateral — 7 reglas nuevas + 1 de compras**: **IIBB1** el gasto de IIBB es el **impuesto DETERMINADO**; la retención de ML y las percepciones de compra son **anticipos** (usar la retención como gasto subestimaba el costo fiscal en ~4 puntos de margen); **IIBB2** sólo la **base GRAVADA** genera IIBB — mismo criterio que el IVA débito (V2); **IIBB3** la **alícuota efectiva se deriva de un CM03 presentado**, nunca se estima ni se tipea; **IIBB4** `base_referencia` es la base **TOTAL declarada**, no la de ADARA → mientras `base_confirmada = false` la alícuota es un **TECHO**; **IIBB5** **no se inventan líneas de negocio con el remanente**: la facturación que ADARA no ve se expone como **cobertura**, no como ingresos sin costos; **IIBB6** el **coeficiente unificado es de la EMPRESA**, no de la línea — el reparto entre líneas es gestión interna por base gravada; **IIBB7** las **extensiones de vistas son siempre aditivas al final** (`42P16`); **C21** comprar sin comprobante y vender con comprobante cuesta, por cada peso no documentado, **la alícuota de IVA más la tasa de Ganancias**. Seis pedidos nuevos al contador, con la **tasa efectiva de Ganancias** pasando a urgente. Ver "Actualización 10 Agosto 2026" al final + `ADARA-IIBB-CONVENIO-MULTILATERAL.md`.) · 7 Agosto 2026 (**Ventas sin comprobante + compra sin comprobante + token ML + Fase 2 del simulador — 8 reglas nuevas**: **V1** una venta cobrada en efectivo y **no facturada** se registra con `canal='efectivo'` + `tipo_comprobante='sin_comprobante'` → descuenta stock por FIFO y suma a caja, **sin IVA débito**; **V2** el **IVA débito depende del tipo de comprobante, no de la alícuota tipeada** (`v_control_mensual` filtra por `es_gravada`) — doble candado contra el débito fantasma; **V3** el precio de una venta sin comprobante es **lo que se cobra, sin desglosar IVA**; **C20** una compra puede marcarse **sin comprobante** (`compras.sin_comprobante`), que es distinto de "factura pendiente": entra al stock igual pero no genera crédito, y un **trigger** rechaza componentes fiscales; **O15** el circuito de token de ML exige **`offline_access` habilitado en el panel de desarrolladores** — el scope en la URL no alcanza; **IMP-F2-1…8** spec de la Fase 2 del simulador (archivar la sim en vez de borrarla, `sku_id` obligatorio al confirmar, check "viene con factura aparte" por concepto del bolsón, TC de la simulación, pagos con cuenta de origen, proveedor del exterior, no declarado/coima precargados y editables, un proveedor por despacho); **IMP-F2-9** el **check "Declaro distinto" es la fuente de verdad**: un override cargado con el check destildado **no subdeclara** (bug corregido). Ver "Actualización 7 Agosto 2026" al final + `ADARA-VENTAS-EFECTIVO.md` / `ADARA-IMPORTACIONES-SIM.md` / `ADARA-VENTAS-ML.md`.) · 5 Agosto 2026 (**Circuito de compras nacionales cerrado — 10 reglas nuevas**: **C15** los costos accesorios con comprobante propio de un tercero se modelan como **gastos vinculados**, no como componentes de la compra (su crédito fiscal se devenga en la fecha de SU factura); **C16** los componentes se cargan **al valor del comprobante**, nunca recalculados; **C17** conviven **dos tipos de cambio** en una importación; **C18** si el comprobante está **emitido en ARS se carga en ARS**; **C19** **prioridad costo > crédito** cuando falta información; **S10** el **stock entra siempre al llegar la mercadería**; **CF13** el **costo tardío se reparte sobre todas las unidades del lote**; **CB13** el movimiento **lo crea quien tiene la evidencia externa**; **O14** reparto de responsabilidad en la carga diaria; **G13** la **fecha de un gasto o compra es la del comprobante**. Ver "Actualización 5 Agosto 2026" al final.) · 3 Agosto 2026 (**Apertura fiscal de IVA cerrada (FISC-APERTURA) + 2 pendientes fiscales nuevos + 20 SKUs + botón "+ Nuevo SKU"**: corte **30/06/2026**; junio como **total de la DDJJ** en `posicion_fiscal_apertura` y `v_posicion_fiscal` arrastra **dos saldos a favor por separado**. Pendientes nuevos: **FISC-RET-IVA** y **FISC-CRED-ML**. 20 SKUs nuevos → neto julio $214M→$384M. **Importación**: las facturas de costos no se cargan sueltas.) · 22 Julio 2026 (**P15 corregida — flete fuera del ahorro/coima**, caso HOKU.) · 13 Julio 2026 (**O13 — Conciliación mensual con período diferible**, `ventas_ml.conciliacion_periodo`, solo eje plata.) · 7 Julio 2026 (**Simulador Importaciones**: P14, P15, P16, IMP-SEGURO; **ML-BON1**.) · 21 Junio 2026 (**Flex F7 Georef** · **P13/P3 canceladas y devoluciones** · **G12 imputaciones de gastos**) · 17 Junio 2026 (**S8** 3 destinos de stock · **S9** stock/CMV por fecha de VENTA) · 16 Junio 2026 (**O12** · **CANC1/CANC2** · **FISC-APERTURA** · **PAGINACIÓN**: toda query paginada ordena por columna única) · 13 Junio 2026 (**O11** bonificaciones Flex vía shipment; token ML en `workspace_config`) · 10 Junio 2026 (hook de congelamiento; pantalla Posición Fiscal) · 9 Junio 2026 (**P12** · **CF9-CF11** · **P11** · **C11-C13** · **P10** · **TI7** · **G11**) · 8 Junio 2026 (**LN1-LN3** línea=producto/canal=dimensión · **CF8** · **CF5-CF7** · **CF1-CF4**) · 5 Junio 2026 (**O10** devoluciones por `op_id`)

**Este documento es la única fuente de verdad para las reglas duras y decisiones de negocio de ADARA.**

Todo cambio en una regla debe discutirse y registrarse acá ANTES de tocar código o documentación temática. Si una regla cambia, se actualiza acá primero y después se propagan los docs específicos.

---

## Cómo usar este documento

| Audiencia | Para qué |
|-----------|----------|
| Futura conversación con Claude / dev nuevo | Onboarding: leer este doc + el doc del dominio puntual. |
| Sebastián (decisor) | Consulta rápida para confirmar reglas sin abrir 13 docs. |
| Refactors / cambios mayores | Validar que el cambio no viola reglas vigentes. |

**Cada regla referencia el doc temático donde está la justificación y los ejemplos.**

---

## Principios fundacionales

### 1. Conciliación universal
Toda venta y todo gasto debe cerrar contra un movimiento bancario / MP / caja / tarjeta. Lo que no cerró todavía vive en **AR** o **AP**.
→ Ver `ADARA-CONCILIACION-BANCARIA.md`

### 2. Devengado, no percibido
El P&L, los impuestos y el análisis de resultado siempre operan sobre la **fecha del hecho económico**.
→ Ver `ADARA-PNL.md`

### 3. Costos reales por unidad — sin promedios
Cada lote tiene su costo histórico real. Cada venta consume unidades FIFO y registra el CMV exacto.
→ Ver `ADARA-COMPRAS-IMPORTACIONES.md` y `ADARA-STOCK.md`

### 4. Imputación a línea de negocio universal
Cada venta, gasto, sueldo, movimiento, impuesto y reclamo se imputa a una línea. La tesorería es la excepción.
→ Ver `ADARA-LINEAS-NEGOCIO.md`

### 5. Mes cerrado es inmutable
Una vez cerrado un mes, no se modifican ventas, gastos, costos, ledger fiscal ni snapshot patrimonial.

---

## Reglas duras por dominio

### Operación general

| # | Regla | Doc |
|---|-------|-----|
| O1 | Sync ML y Sync Tango son **automáticos**. Botón manual de respaldo. | `ADARA-FLUJO-OPERATIVO.md` |
| O2 | Aprobación de ventas manual en pantalla de tarjetas. **(Nunca se construyó; el modelo actual no la necesita.)** | `ADARA-FLUJO-OPERATIVO.md` |
| O3 | Descuento de stock es **automático**, encadenado al `/ml/sync`. | `ADARA-FLUJO-OPERATIVO.md` |
| O4 | Re-autenticación ML/Tango con flujo guiado desde home cuando expira el token. **Implementado el 7/8/2026 (banner en home) — ver O15.** | `ADARA-VENTAS-ML.md`, `ADARA-TFACTURA.md` |
| O5 | Logística **multi-transportadora** extensible. Asignación al despachar **manual**. | `ADARA-FLUJO-OPERATIVO.md` |
| O6 | Sin login/auth. **⚠️ Revertida por A17.** | (decisión histórica) |
| O7 | **Conciliación de ventas ML (v22):** `ventas_ml` + `vinculos` con `op_tipo='venta_ml'`. Una venta concilia cuando Σ cobros = `por_cobrar` (tol 0,02). **⚠️ El motor del server v21 (`autoConciliarMP` + `movimientos_mp`) está MUERTO.** | `ADARA-VENTAS-ML-V22.md` |
| O8 | **Envío colecta a cargo del comprador → doble liquidación. (RESUELTO.)** Identidad de cierre: **`por_cobrar = cash(producto + envío) + \|IIBB envío\|`**. El puente es `retenciones SETTLEMENT_SHIPPING`. NO tocar `por_cobrar`. | `ADARA-VENTAS-ML-V22.md`, `ADARA-RETENCIONES-IIBB.md` |
| O9 | **Packs NO requieren lógica especial.** Cada producto es una orden con su `mp_payment_id`. | `ADARA-VENTAS-ML-V22.md` |
| O10 | **Devoluciones del AS → match por `op_id`, NUNCA por monto.** Match en capas; se vinculan todas las líneas del bundle con `vinculos.monto` positivo. NO tocar `por_cobrar`. | `ADARA-CANCELACIONES-DEVOLUCIONES.md` |
| O13 | **Conciliación mensual manual con período diferible (13/7/2026).** Tablero bidireccional; `ventas_ml.conciliacion_periodo` es **SOLO el eje plata** (no toca stock/CMV/P&L/fiscal — S9). | `ADARA-VENTAS-ML-V22.md` |
| O14 | **Reparto de responsabilidad en la carga diaria (5/8/2026).** Gastos → **equipo**; compras de mercadería → **Sebastián** (crean lotes; un lote mal cargado contamina el CMV). | `ADARA-FLUJO-OPERATIVO.md` |
| **O15** | **El token de ML exige `offline_access` habilitado en el PANEL de desarrolladores (7/8/2026).** Pedir el scope en la URL de `/ml/auth` **no alcanza**: si la app no lo tiene activado, ML ignora el pedido y devuelve sólo el access token (que dura ~6 h). Sin `refresh_token` el sync se corta cada 6 h y hay que reconectar a mano. Corolarios de implementación: **(a)** un fallo de refresh nunca puede ser silencioso — se registra el motivo y se expone en `/health`/`/ml/status`; **(b)** el callback **nunca pisa con NULL** un `refresh_token` existente; **(c)** el callback **no reporta éxito** si ML no devolvió refresh token; **(d)** el cron refresca **cada hora** con guard de 2 h, no cada 6 (a 6 h el margen de reintento es cero). | `ADARA-VENTAS-ML.md` |

### Ventas sin comprobante (nuevo dominio, 7/8/2026)

| # | Regla | Doc |
|---|-------|-----|
| **V1** | **Una venta cobrada en efectivo y no facturada se registra con `canal='efectivo'` + `tipo_comprobante='sin_comprobante'`.** Descuenta stock por FIFO, suma a `caja_ars` y entra al Resultado, pero **no genera IVA débito** (`es_gravada` es columna generada = false). Se carga por el endpoint atómico `POST /ventas/efectivo` (venta + ítems + movimiento + vínculo + FIFO). **`efectivo` es canal propio y NO se reusa `whatsapp_efectivo`**: ese está reservado para las ventas off-ML que **sí** se emiten en Tango, y mezclarlas las volvería inseparables en el Resultado. | `ADARA-VENTAS-EFECTIVO.md` |
| **V2** | **El IVA débito depende del TIPO DE COMPROBANTE, no de la alícuota cargada en el ítem (7/8/2026).** `v_control_mensual` computa `sum(vi.iva_linea) FILTER (WHERE v.es_gravada)`. Antes sumaba `iva_linea` sin filtrar: una venta no facturada cargada por error con 21 % habría sumado **débito fiscal fantasma** sin que nada avisara. Es un **doble candado** junto con la convención de alícuota 0. Verificado: regresión cero sobre los 8 períodos cargados. | `ADARA-VENTAS-EFECTIVO.md`, `ADARA-IMPUESTOS.md` |
| **V3** | **El precio de una venta sin comprobante es lo que se cobra, sin desglosar IVA.** No hay factura de la cual separarlo: la plata es toda ingreso. **Consecuencia a tener presente:** a igual precio, una venta en efectivo muestra ~21 % más de margen que una venta ML (que entra neta de IVA). Por eso `efectivo` va como canal separado en el Resultado. | `ADARA-VENTAS-EFECTIVO.md`, `ADARA-PNL.md` |

> **Pendiente asociado:** *toda venta off-ML facturada se emite en Tango y entra por sync; ADARA no acepta alta manual de ventas facturadas.* La venta en efectivo sin comprobante es la excepción, precisamente porque no hay comprobante que emitir. Falta construir el **sync Tango de ventas no-ML** — hoy 4 de las 5 líneas no tienen forma de entrar al sistema.

### IIBB / Convenio Multilateral (nuevo dominio, 10/8/2026)

| # | Regla | Doc |
|---|-------|-----|
| **IIBB1** | **El gasto de IIBB del período es el impuesto DETERMINADO.** La retención de ML y las percepciones de compra son **ANTICIPOS**: reducen lo que se paga en caja, no el costo del período. Usar la retención como gasto **subestimaba el costo fiscal en ~4 puntos de margen**. Es la aplicación concreta de P12 al IIBB. | `ADARA-IIBB-CONVENIO-MULTILATERAL.md`, `ADARA-IMPUESTOS.md` |
| **IIBB2** | **Sólo la base GRAVADA genera IIBB.** Una venta sin comprobante no se declara y no tributa. **Mismo criterio que el IVA débito (V2)**: manda el tipo de comprobante, no lo que se tipeó. `v_iibb_base` filtra por `es_gravada`. | `ADARA-IIBB-CONVENIO-MULTILATERAL.md`, `ADARA-VENTAS-EFECTIVO.md` |
| **IIBB3** | **La alícuota efectiva se deriva de un CM03 presentado, nunca se estima ni se tipea a mano.** Sale de `determinado_total / base_referencia` (columna GENERATED en `iibb_parametros`). **Si no hay CM03 del período, se hereda la del último disponible y queda marcado con `alicuota_del_periodo = false`** — el número se sigue mostrando, pero declarado como heredado. | `ADARA-IIBB-CONVENIO-MULTILATERAL.md`, `ADARA-SCHEMA.md` |
| **IIBB4** | **`base_referencia` es la base TOTAL declarada, no la de ADARA.** Mientras `base_confirmada = false`, la alícuota efectiva es un **TECHO** (el denominador está subestimado) y la pantalla lo advierte con un **banner**. Confirmar la base con el contador es lo que convierte el techo en número exacto. | `ADARA-IIBB-CONVENIO-MULTILATERAL.md` |
| **IIBB5** | **No se inventan líneas de negocio con el remanente.** La facturación que ADARA no ve se expone como **COBERTURA** (`cobertura_pct`, `facturacion_fuera_de_adara`), **no como ingresos sin costos**. Fundamento: una línea con ingresos y sin CMV mostraría **margen ~100 %** y mentiría peor que el número que reemplaza. Un hueco declarado es información; un hueco rellenado es un error escondido. | `ADARA-IIBB-CONVENIO-MULTILATERAL.md`, `ADARA-LINEAS-NEGOCIO.md` |
| **IIBB6** | **El coeficiente unificado es de la EMPRESA, no de la línea.** Bajo Convenio Multilateral se determina una sola vez para toda la actividad. **El reparto de IIBB entre líneas es contabilidad de gestión interna** y se hace por **base gravada** (peso dentro del período). No tiene valor fiscal: es la misma lógica que LN8 para la posición de IVA. | `ADARA-IIBB-CONVENIO-MULTILATERAL.md`, `ADARA-LINEAS-NEGOCIO.md` |
| **IIBB7** | **Las extensiones de vistas son SIEMPRE aditivas al final.** `CREATE OR REPLACE VIEW` sólo permite agregar columnas al final: no deja renombrar ni reordenar (error **`42P16`**, confirmado en vivo al intentar intercalar una columna en `v_iibb_determinado`). Además de la limitación de Postgres, las columnas viejas son contrato con el frontend desplegado — por eso `margen_contribucion` y `resultado_operativo` quedaron intactas y las versiones con IIBB adentro son columnas nuevas (`*_real`). | `ADARA-SCHEMA.md` |

### Líneas de negocio

| # | Regla | Doc |
|---|-------|-----|
| LN1 | **6 líneas activas** (8/6/2026): Electrónica, Luminarias, Mochilas Sindicatos, Mochilas Individuos, Repelentes, Vasos Térmicos. | `ADARA-LINEAS-NEGOCIO.md` |
| LN2 | **Línea = producto (familia); canal = dimensión separada.** Asignación automática por familia. Override disponible. | `ADARA-LINEAS-NEGOCIO.md` |
| LN3 | **Familias de SKU** (en singular): `electronica`, `luminaria`, `mochila_sindical`, `mochila_individual`, `repelente`, `vaso_termico`. | `ADARA-LINEAS-NEGOCIO.md` |
| LN4 | **Derogada (21/6/2026).** Reemplazada por G12. | `ADARA-GASTOS.md` |
| LN5 | Sueldos se distribuyen por % por empleado (suma 100%). | `ADARA-LINEAS-NEGOCIO.md` |
| LN6 | Saldo dinámico por línea = saldo inicial + Σ ingresos − Σ egresos. | `ADARA-LINEAS-NEGOCIO.md` |
| LN7 | Movimientos huérfanos van a "Sin asignar" y deben tender a cero antes del cierre. | `ADARA-LINEAS-NEGOCIO.md` |
| LN8 | Posición fiscal por línea es **informativa interna**. La DDJJ es única. | `ADARA-IMPUESTOS.md` |
| LN9 | **Opción A acotada:** `movimientos.linea_id` existe **solo para huérfanos**; los vinculados heredan la línea de la operación. | `ADARA-MOVIMIENTOS.md` |

### P&L (Estado de Resultado)

| # | Regla | Doc |
|---|-------|-----|
| P1 | **Devengado, no percibido.** | `ADARA-PNL.md` |
| P2 | **Sin IVA** en ningún concepto del P&L. | `ADARA-PNL.md` |
| P3 | **Devoluciones en el mes del reembolso.** El COGS se revierte solo si el producto reingresa vendible; `no_disponible` → pérdida; `reacondicionar` → REAC (S8). | `ADARA-PNL.md` |
| P4 | **CMV congelado** al momento de la venta. **Excepción acotada:** recosteo de costo tardío con el mes abierto (CF13). | `ADARA-PNL.md` |
| P5 | **Mes cerrado es inmutable.** | `ADARA-PNL.md` |
| P6 | **Total empresa = suma exacta de líneas.** | `ADARA-PNL.md` |
| P7 | **Pérdidas por mercadería** son **rubro propio**, NO gasto operativo. | `ADARA-PNL.md` |
| P8 | **Resultado financiero** (Tesorería) se imputa **solo a Electrónica**. | `ADARA-INVERSIONES.md` |
| P9 | Vista del mes en curso parcial (diaria) disponible. | `ADARA-PNL.md` |
| P10 | **Resultado operativo por línea** = contribución − CMV − gastos (G12). Los gastos capitalizados quedan fuera (C15). | `ADARA-PNL.md` |
| P11 | **El margen se calcula sobre el ingreso NETO sin IVA.** Los 3 montos de una venta ML: bruto c/IVA → neto s/IVA → por cobrar. | `ADARA-PNL.md` |
| P12 | **Tratamiento de impuestos en el P&L.** IVA fuera del resultado; IIBB determinado y Ganancias **sí restan**; percepciones/retenciones = pagos a cuenta. **Implementada para IIBB el 10/8/2026 — ver IIBB1.** | `ADARA-IMPUESTOS.md` |
| P13 | **Reversión simétrica de canceladas ML** (`fn_reconciliar_ml_canceladas`, hook post-sync: proyectar → reconciliar → consumir → congelar). | `ADARA-CANCELACIONES-DEVOLUCIONES.md` |

### Impuestos

| # | Regla | Doc |
|---|-------|-----|
| I1 | **Devengado** para todos los impuestos. | `ADARA-IMPUESTOS.md` |
| I2 | **Ledger por impuesto.** | `ADARA-IMPUESTOS.md` |
| I3 | **Pagos al fisco no afectan la posición** del período. | `ADARA-IMPUESTOS.md` |
| I4 | **Mes cerrado fiscalmente inmutable.** | `ADARA-IMPUESTOS.md` |
| I5 | **Retenciones y percepciones sufridas** al período en que ocurrieron. | `ADARA-IMPUESTOS.md` |
| I6 | **Ventas con remito (WhatsApp/efectivo)** NO generan IVA débito. **Formalizado y ampliado por V1/V2 (7/8/2026).** **Y por IIBB2 (10/8/2026): tampoco generan IIBB.** | `ADARA-IMPUESTOS.md`, `ADARA-VENTAS-EFECTIVO.md` |
| I7 | **Compras factura B/C o sin factura** no generan crédito fiscal. **Formalizado por C20 (7/8/2026); costeado por C21 (10/8/2026).** | `ADARA-COMPRAS-IMPORTACIONES.md` |
| I8 | **Posición fiscal en vivo en home.** | `ADARA-IMPUESTOS.md` |
| I9 | **Apertura fiscal de IVA con doble saldo a favor (FISC-APERTURA).** Corte **30/06/2026**; arrastra **técnico** y **libre disponibilidad** por separado. | `ADARA-IMPUESTOS.md` |

### Compras e importaciones

| # | Regla | Doc |
|---|-------|-----|
| C1 | **Costo del lote inmutable**, salvo compensación explícita o recosteo acotado (CF13). | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C2 | **Pagos a la compra ≠ compra.** | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C3 | **Componentes fiscales NO suman al costo.** | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C4 | **FIFO por defecto.** | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C5 | **Ajuste de inventario** es delta sin cambio del total invertido. | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C6 | **TC del lote queda fijo** al TC blue del día de la compra. | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C7 | **Gastos sin factura** suman al costo del lote pero **no generan crédito fiscal**. | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C8 | Prorrateo de gastos comunes (flete por peso, seguro por FOB, tributos por CIF). | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C9 | **Snapshot inicial al 31/12/2025.** | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C10 | **NO se cargan importaciones históricas 2025.** | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C11 | **Compra local en USD**: el lote se congela en ARS al TC del día. | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C12 | **Jurisdicción de percepciones normalizada.** | `ADARA-RETENCIONES-IIBB.md` |
| C13 | **N° de factura diferido** (`compras.nro_factura`, nullable). El seed de apertura no se lista ni se anula. | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C14 | **Las facturas de COSTOS de importación NO se cargan sueltas.** Se materializan por la **Fase 2 del simulador**. Solo las facturas de **producto/FOB** son compras de mercadería; los **servicios operativos** van a Gastos. | `ADARA-IMPORTACIONES-SIM.md` |
| C15 | **Los costos accesorios con comprobante propio de un tercero se modelan como gastos vinculados** (`gastos.capitaliza_compra_id`), no como componentes: su crédito fiscal se devenga en la fecha de SU factura. | `ADARA-COMPRAS-IMPORTACIONES.md`, `ADARA-GASTOS.md` |
| C16 | **Los componentes se cargan al valor del comprobante, nunca recalculados.** La diferencia contra el simulador es información sobre la estimación. | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C17 | **En una importación conviven dos tipos de cambio**, deliberadamente: TC oficial (base tributaria) y TC blue (costo del lote, C6). | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C18 | **Si el comprobante está emitido en ARS se carga en ARS**, aunque muestre precios en USD. | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C19 | **Prioridad costo > crédito cuando falta información.** El costo mal cargado es irreversible una vez vendido; la Posición Fiscal se recalcula sola. | `ADARA-COMPRAS-IMPORTACIONES.md` |
| C20 | **Una compra puede marcarse SIN COMPROBANTE (`compras.sin_comprobante`, 7/8/2026), y eso es distinto de "factura pendiente".** Antes las dos se veían igual (`nro_factura IS NULL`) y el destacado de la home iba a contar para siempre una compra que nunca va a tener factura. La mercadería **entra al stock igual** (S10) y el costo del lote no cambia; lo único que desaparece es el crédito fiscal. **Estados excluyentes:** una compra sin comprobante **no puede** tener `nro_factura` (CHECK), y un **trigger** en `compra_componentes` rechaza `iva`/`iibb_percepcion`/`ganancias_percepcion`/`otro_impuesto`. Si la factura aparece después, hay que **desmarcarla primero** para que se cargue con su crédito. Tres barreras (front, backend, base) a propósito: acá lo que está en juego es crédito fiscal. **No confundir con el componente `compra_componentes.tipo='sin_factura'`**, que es la porción de una compra pagada sin comprobante. | `ADARA-COMPRAS-IMPORTACIONES.md`, `ADARA-SCHEMA.md` |
| **C21** | **Comprar sin comprobante y vender con comprobante cuesta, por cada peso no documentado, la alícuota de IVA más la tasa de Ganancias (10/8/2026).** Con mercadería al **10,5 % de IVA** y tasa de Ganancias del **35 %**, el costo es **45,5 %**; con IVA al **21 %**, **56 %**. **No incluye IIBB**, que lo empeora. Consecuencia práctica: una oferta de **"50 % facturado / 50 % en efectivo"** sólo conviene con un **descuento del 45,5 % sobre la parte en efectivo**, que ningún proveedor ofrece. Sólo tendría sentido si esa mercadería **también se vende sin facturar**, y hoy no hay canal para eso a escala: la venta en efectivo de julio fue **$8,1M** contra **$60,7M** de débito mensual de ML. | `ADARA-COMPRAS-IMPORTACIONES.md`, `ADARA-IMPUESTOS.md` |

### Stock

| # | Regla | Doc |
|---|-------|-----|
| S1 | **ADARA es master del stock.** | `ADARA-STOCK.md` |
| S2 | **Consumo de venta queda registrado en `consumo_lote`.** | `ADARA-STOCK.md` |
| S3 | **Devolución vuelve al lote original** cuando se conoce. | `ADARA-STOCK.md` |
| S4 | **Cancelaciones sin entrega no afectan stock.** | `ADARA-STOCK.md` |
| S5 | **Ajustes por inventario** quedan registrados con motivo. | `ADARA-STOCK.md` |
| S6 | **Stock físico vs disponible.** | `ADARA-STOCK.md` |
| S7 | **Stock valorizado al costo histórico de lotes (FIFO).** | `ADARA-STOCK.md` |
| S8 | **Devolución/cancelación tiene 3 destinos:** `ok` / `reacondicionar` (REAC) / `no_disponible`. | `ADARA-STOCK.md` |
| S9 | **El stock se descuenta y el CMV se reconoce en la fecha de la VENTA, no de la entrega.** | `ADARA-STOCK.md` |
| S10 | **El stock entra siempre al llegar la mercadería, aunque falten facturas.** Se posterga el costeo, no el ingreso. | `ADARA-STOCK.md`, `ADARA-PSI.md` |

### Costeo de ventas (CMV)

| # | Regla | Doc |
|---|-------|-----|
| CF1 | **Costeo de ventas ML por proyección + FIFO.** Una venta ML **no se proyecta si su SKU no resuelve** → queda huérfana, fuera del Resultado. | `ADARA-COSTEO-FIFO.md` |
| CF2 | **Modelo neto-driven.** Fuente = `venta_items.precio_unitario_neto`; el resto son columnas generadas. | `ADARA-SCHEMA.md` |
| CF3 | **Seed de stock = foto de hoy desde Tango.** | `ADARA-COSTEO-FIFO.md` |
| CF4 | **Hallazgo (8/6/2026):** hasta esa fecha las ventas ML no consumían lotes. | `ADARA-COSTEO-FIFO.md` |
| CF5 | **Circuito de costeo completo.** `fn_consumir_fifo` + `fn_revertir_devolucion` + hook al `/ml/sync`. **`fn_consumir_fifo` es canal-agnóstica**: toma cualquier venta con `estado in ('aprobada','entregada')`, venga de ML o de una carga manual. **Su `unidades_faltantes` es GLOBAL por rango de fechas**, no por venta: para avisar faltante de una venta puntual hay que consultar `consumo_lote` de sus ítems. | `ADARA-COSTEO-FIFO.md` |
| CF6 | **El costo nace en la compra, se asienta por lote y es inmutable.** Única excepción: seed de apertura. | `ADARA-COSTEO-FIFO.md` |
| CF7 | **Vistas de explotación del costeo.** | `ADARA-COSTEO-FIFO.md` |
| CF8 | **Mapeo SKU ML ↔ catálogo + combos.** La equivalencia vive en la **proyección** (`sku_map`/`combo_map`), no en `ventas_ml` (se pisa en cada sync). | `ADARA-COSTEO-FIFO.md` |
| CF9 | **`skus.costo_referencia`** — fallback del CMV estimado cuando el SKU no tiene lote. El lote real siempre tiene prioridad. | `ADARA-COSTEO-FIFO.md` |
| CF10 | **El CMV, una vez calculado, queda FIJO.** Prioridad: FIFO real → congelado → dinámico residual. | `ADARA-COSTEO-FIFO.md` |
| CF11 | **Lote inicial = fecha de corte.** | `ADARA-COSTEO-FIFO.md` |
| CF12 | **Costos de logística/comisión a terceros capitalizan al lote**, no son AP del proveedor del producto. Matiz: si el tercero emite comprobante propio → gasto capitalizable (C15). | `ADARA-COSTEO-FIFO.md` |
| CF13 | **El costo tardío se reparte sobre TODAS las unidades del lote**, y se recostea lo ya consumido mientras el mes esté abierto. | `ADARA-COSTEO-FIFO.md` |
| **CF14** | **La `fecha_alta` del lote define qué ventas puede costear hacia atrás (7/8/2026).** `fn_consumir_fifo` sólo consume lotes con `fecha_alta <= fecha de la venta`. Si el lote se carga con la fecha de la **factura** en vez de la de **llegada al depósito** (FO5), las ventas anteriores quedan sin costo real aunque hoy haya stock disponible. Caso detectado: 5 SKUs / 59 ítems / $8,0M de CMV estimado con stock en el depósito. | `ADARA-COSTEO-FIFO.md`, `ADARA-STOCK.md` |

### Conciliación bancaria

| # | Regla | Doc |
|---|-------|-----|
| CB1 | **Conciliación universal.** | `ADARA-CONCILIACION-BANCARIA.md` |
| CB2 | **Movimientos del extracto son inmutables.** | `ADARA-CONCILIACION-BANCARIA.md` |
| CB3 | **Conciliado = tolerancia 2 centavos.** | `ADARA-CONCILIACION-BANCARIA.md` |
| CB4 | **Relación N:N** entre operaciones y movimientos. | `ADARA-CONCILIACION-BANCARIA.md` |
| CB5 | **Descartar requiere motivo.** | `ADARA-CONCILIACION-BANCARIA.md` |
| CB6 | **AR/AP se calculan dinámicamente.** | `ADARA-CONCILIACION-BANCARIA.md` |
| CB7 | **Buzón "sin asignar" tiende a cero** antes del cierre. | `ADARA-CONCILIACION-BANCARIA.md` |
| CB8 | **Movimientos internos entre cuentas propias** no suman al P&L. | `ADARA-CONCILIACION-BANCARIA.md` |
| CB9 | **Pago de tarjeta** ofrece desglose en N gastos. | `ADARA-CONCILIACION-BANCARIA.md` |
| CB10 | **3 sub-cuentas Supervielle** como una sola cuenta agregada. | `ADARA-CONCILIACION-BANCARIA.md` |
| CB11 | **Banco y MP solo por importación**; carga manual solo caja. | `ADARA-MOVIMIENTOS.md` |
| CB12 | **Solo se borran movimientos `origen='manual'`.** | `ADARA-MOVIMIENTOS.md` |
| CB13 | **El movimiento lo crea quien tiene la evidencia externa.** Cuentas con extracto → viene del extracto; cajas → lo crea la app. | `ADARA-CONCILIACION-BANCARIA.md` |
| **CB14** | **Un movimiento creado por una operación atómica nace conciliado (7/8/2026).** Cuando la app crea el movimiento **y** la operación en el mismo acto (gasto sin factura en efectivo, venta en efectivo), el vínculo se crea junto: no tiene sentido que caiga en "movimientos sin conciliar" para que alguien lo cruce a mano contra una operación que la propia app acaba de crear. Es corolario de CB13. | `ADARA-MOVIMIENTOS.md`, `ADARA-VENTAS-EFECTIVO.md` |

### Tesorería e inversiones

| # | Regla | Doc |
|---|-------|-----|
| TI1 | **Tesorería General es centralizada.** | `ADARA-INVERSIONES.md` |
| TI2 | **Rendimientos van al P&L de Electrónica.** | `ADARA-INVERSIONES.md` |
| TI3 | **Suscripciones y rescates son movimientos internos.** | `ADARA-INVERSIONES.md` |
| TI4 | **Rendimientos brutos + retención Ganancias por separado.** | `ADARA-INVERSIONES.md` |
| TI5 | **Caja USD valorizada al TC blue del día.** | `ADARA-INVERSIONES.md` |
| TI6 | **Inversión activa no se modifica retroactivo.** | `ADARA-INVERSIONES.md` |
| TI7 | **Trust Wallet (USDT) se modela como caja USD.** | `ADARA-SCHEMA.md` |

### Integración Tango

| # | Regla | Doc |
|---|-------|-----|
| T1 | **ADARA NO escribe en Tango.** | `ADARA-TFACTURA.md` |
| T2 | **Una venta ML no se duplica.** | `ADARA-TFACTURA.md` |
| T3 | **Idempotencia del sync por `MovimientoId`.** | `ADARA-TFACTURA.md` |
| T4 | **`ObtenerInfoAplicaciones: true` obligatorio.** | `ADARA-TFACTURA.md` |
| T5 | **Token de Tango se cachea y renueva.** | `ADARA-TFACTURA.md` |
| T6 | **Comprobantes anulados en Tango → cancelados en ADARA + revierten stock.** | `ADARA-TFACTURA.md` |
| T7 | **Auth endpoint devuelve string URL-encoded.** | `ADARA-TFACTURA.md` |
| T8 | **`UserSecret` = `UserIdentifier`.** | `ADARA-TFACTURA.md` |

### Patrimonial

| # | Regla | Doc |
|---|-------|-----|
| PT1 | **Mes cerrado patrimonial es inmutable.** | `ADARA-PATRIMONIAL.md` |
| PT2 | **Cálculo dinámico** salvo snapshots de cierre. | `ADARA-PATRIMONIAL.md` |
| PT3 | **Patrimonio por línea es informativo.** | `ADARA-PATRIMONIAL.md` |
| PT4 | **Stock valorizado al costo histórico** (FIFO). | `ADARA-PATRIMONIAL.md` |
| PT5 | **AR y AP se calculan dinámicamente.** | `ADARA-PATRIMONIAL.md` |

### Reclamos (a ML y proveedores)

| # | Regla | Doc |
|---|-------|-----|
| R1 | **Reclamo siempre asociado a una venta y un destinatario.** | `ADARA-RECLAMOS.md` |
| R2 | **Destinatario puede ser Mercado Libre o Proveedor.** | `ADARA-RECLAMOS.md` |
| R3 | **No dos reclamos abiertos por la misma venta.** | `ADARA-RECLAMOS.md` |
| R4 | **Pérdida por reclamo no cubierto = rubro propio en P&L.** | `ADARA-RECLAMOS.md` |
| R5 | **Recupero se imputa al mes en que se cobra.** | `ADARA-RECLAMOS.md` |
| R6 | **Producto reemplazo crea lote nuevo con costo original.** | `ADARA-RECLAMOS.md` |
| R7 | **Nota de crédito del proveedor reduce AP.** | `ADARA-RECLAMOS.md` |

### Gastos

| # | Regla | Doc |
|---|-------|-----|
| G1 | **Cada gasto se imputa a UNA categoría y a una o más líneas/canales vía `gasto_imputacion`** (% que suma 100). | `ADARA-GASTOS.md` |
| G2 | **Derogada (21/6/2026).** | `ADARA-GASTOS.md` |
| G3 | **Devengado, no percibido.** | `ADARA-GASTOS.md` |
| G4 | **Gastos sin factura** válidos pero etiquetados; no generan crédito fiscal. | `ADARA-GASTOS.md` |
| G5 | **Crédito IVA solo con factura A.** | `ADARA-GASTOS.md` |
| G6 | **Mes cerrado inmutable.** | `ADARA-GASTOS.md` |
| G7 | **Anular un gasto requiere motivo.** | `ADARA-GASTOS.md` |
| G8 | **Pérdidas por mercadería ≠ gastos.** | `ADARA-GASTOS.md` |
| G9 | **Gasto en USD con `tc IS NULL` traba el cierre del mes.** | `ADARA-GASTOS.md` |
| G10 | **Retenciones y percepciones como renglones en `gasto_fiscal`.** | `ADARA-GASTOS.md` |
| G11 | **El alta de gasto usa un único selector "Pago".** | `ADARA-GASTOS.md` |
| G12 | **Un gasto se reparte por imputaciones** (`gasto_imputacion`, Σ=100). | `ADARA-GASTOS.md` |
| G13 | **La fecha de un gasto o de una compra es la del COMPROBANTE, nunca la de carga.** | `ADARA-GASTOS.md` |

### Simulador de importaciones — Fase 2 (spec, 7/8/2026)

| # | Regla | Doc |
|---|-------|-----|
| **IMP-F2-1** | **Al confirmar, la simulación se archiva, no se borra.** `estado='confirmada'` + `compra_id`, sale de la lista por defecto y queda **bloqueada para edición**. El costo del lote es inmutable (CF6) y la simulación **es su memo de cálculo**: borrarla deja un costo sin respaldo auditable. | `ADARA-IMPORTACIONES-SIM.md` |
| **IMP-F2-2** | **`sku_id` opcional en la carga, obligatorio al confirmar**, vía pantalla de mapeo. Una sim what-if puede ser de un producto que todavía no se trae; pero un lote sin `sku_id` es imposible. **Hallazgo:** los productos del JSONB no tenían ningún vínculo con SKU — sólo `nombre` en texto libre. | `ADARA-IMPORTACIONES-SIM.md` |
| **IMP-F2-3** | **Cada concepto del bolsón lleva un check "viene con factura aparte".** Los tildados **no capitalizan al confirmar**: se descuentan del bolsón y entran después por Gastos con `capitaliza_compra_id` (C15). Evita el doble conteo y **resuelve la decisión pendiente del IVA de los servicios locales**: se toma como crédito en la fecha de SU factura en vez de quedar enterrado en el costo. | `ADARA-IMPORTACIONES-SIM.md` |
| **IMP-F2-4** | **El costo se congela con el TC de la simulación** (`params.tc`) — el mismo con el que se calculó y se validó contra la planilla. | `ADARA-IMPORTACIONES-SIM.md` |
| **IMP-F2-5** | **`pagos[]` lleva cuenta de origen; al confirmar cada pago se materializa como movimiento + vínculo `op_tipo='compra'`.** Engancha con la conciliación existente sin circuito nuevo. | `ADARA-IMPORTACIONES-SIM.md` |
| **IMP-F2-6** | **La compra de importación se asigna al proveedor del exterior** (dado de alta sin CUIT), no a un genérico. Así la cuenta corriente refleja lo que se le debe a cada uno. | `ADARA-IMPORTACIONES-SIM.md` |
| **IMP-F2-7** | **El no declarado y la coima aparecen precargados como pagos, editables**, en el paso de confirmación. Son salida de caja real; precargarlos evita que se olviden, y dejarlos editables evita que la app invente movimientos. | `ADARA-IMPORTACIONES-SIM.md` |
| **IMP-F2-8** | **Un proveedor por despacho, por ahora.** Un consolidado que traiga de varios se asigna al principal. Se puede abrir después sin romper nada. | `ADARA-IMPORTACIONES-SIM.md` |
| **IMP-F2-9** | **El check "Declaro distinto" es la fuente de verdad, no el contenido de los campos (7/8/2026).** Un valor que quedó escrito en un override con el check **destildado** NO subdeclara: el cálculo usa el real. Antes la función `eff()` sólo preguntaba "¿este campo tiene algo?", así que un número olvidado subdeclaraba en silencio, sin ninguna señal en pantalla. Además, al destildar se limpian **los cuatro** overrides — `cantidad_decl` se había olvidado cuando se agregó el eje de cantidad (7/7/2026). | `ADARA-IMPORTACIONES-SIM.md` |

---

## Decisiones de arranque

| # | Decisión | Fecha |
|---|----------|-------|
| A1 | ADARA arranca operación corriente en mayo 2026. Datos retroactivos desde 1/1/2026. | 15/05/2026 |
| A2 | **Snapshot inicial al 31/12/2025**. Los saldos de caja/banco **NO se cargan a mano** — se **derivan** por conciliación. | 15/05/2026 · rev. 28/05/2026 |
| A3 | Conciliación universal. | 15/05/2026 |
| A4 | Costos por SKU exactos (NO promedio), con FIFO. | 15/05/2026 |
| A5 | Compensación entre lotes: ajuste de inventario con delta. | 15/05/2026 |
| A6 | Sin login/auth; RLS off. **⚠️ REVERTIDA por A17.** | 15/05/2026 · **revertida 11/06/2026** |
| A7 | Una sola tarjeta de crédito → no se modela como cuenta. | 19/05/2026 |
| A8 | Inversiones centralizadas en Tesorería General. | 19/05/2026 |
| A9 | Reclamos con dos destinatarios (ML y Proveedor). | 19/05/2026 |
| A10 | Pérdidas por mercadería = rubro propio en P&L. | 19/05/2026 |
| A11 | **ADARA se construye a medida** (Node/JS). | 19/05/2026 |
| A12 | **Alcance acotado** al núcleo financiero/operativo. | 19/05/2026 |
| A13 | **Build limpio sobre `public` sin prefijo.** | 27/05/2026 |
| A14 | **Diseño de DB se pausa después de capa 5 (Gastos).** Capas 6-9 on-demand. | 27/05/2026 |
| A15 | **Frontend HTML modular sin build step.** | 28/05/2026 |
| A16 | **Backend mixto: Supabase REST para lecturas, Express para mutaciones complejas.** | 28/05/2026 |
| A17 | **Login multiusuario con Supabase Auth + RLS (revierte A6).** Toda tabla/vista nueva con RLS + policy `authenticated`. | 11/06/2026 |

---

## Pendientes acumulados (TBD)

### A definir con el contador
- **Tasa de Ganancias por línea (o tasa global). ⚠️ URGENTE desde el 10/8/2026** — ya figuraba como pendiente, pero pasa a bloqueante: **sin la tasa efectiva de Ganancias no se puede evaluar ninguna oferta de compra parcialmente no documentada** (C21). El cálculo del 45,5 % / 56 % usa 35 % como supuesto, no como dato confirmado.
- Alícuotas IIBB por jurisdicción (Convenio Multilateral)
- **Tratamiento fiscal de las operaciones sin comprobante**: gastos, compras y **ventas en efectivo** (V1). Comprar con factura A y vender sin débito corre el ratio crédito/débito hacia un saldo a favor sin sustento en ventas, y deja el stock real por debajo del reconstruible desde comprobantes
- Tratamiento fiscal de bonificaciones Flex
- DDJJ anual 2025 — saldo a favor o en contra
- **Despachos de junio (borradores 1, 5 y 6):** junio ya está declarado (apertura F.2051). Si son despachos reales de junio, su crédito requiere **rectificativa**
- Formato de snapshot mensual exportable

#### Pedidos nuevos del 10/8/2026 (IIBB / Convenio Multilateral)
- **Base imponible TOTAL declarada del CM03 202606.** Un solo número. Convierte la alícuota de **techo** en **exacta** (IIBB4) y además **mide cuánta facturación no ve ADARA** (`cobertura_pct`, IIBB5). Es el pedido de mayor relación valor/esfuerzo de la lista.
- **¿La DDJJ declara una actividad o más de una?** Bajo Convenio Multilateral el **coeficiente es único para la empresa**, pero **la alícuota puede diferir por actividad**. Si "comercio electrónico" y "venta mayorista de iluminación" tributan distinto, `iibb_parametros` necesita **un renglón por actividad** además de por período. **Es un cambio chico si se sabe ahora**, caro después.
- **Determinado negativo en Santa Cruz (−$551.217,97) y Tierra del Fuego (−$15.831,19).** No es normal: hay que entender de dónde sale antes de tomarlo como bueno.
- **Reducción de alícuota de retención en Corrientes, Santa Fe y La Pampa.** Sólo esas tres acumulan **$1,7M de saldos a favor**, sobre **$2.867.481,49** en total. Plata inmovilizada que se puede frenar en origen.
- **¿Hay SIRCREB en el Supervielle?** El CM03 computó **$15.471.267,94** de "Valores Restan" en junio contra **$1,53M** que ve ADARA. La diferencia sugiere retenciones bancarias que no están entrando al sistema.

### Fiscal — pendientes de implementación
- **FISC-RET-IVA:** capturar las **retenciones/percepciones de IVA sufridas** para que crezca la libre disponibilidad. Hoy `ret_mes` está fijado en 0.
- **FISC-CRED-ML:** capturar el **crédito de IVA de las facturas de comisión/envío de ML** (~$18-21M solo en julio) **sin duplicar el costo**, que ya está en las deducciones ML del Resultado.

### Simulador de importaciones — Fase 2 (spec cerrado, en construcción)
Reglas IMP-F2-1…9 arriba. **Primera entrega hecha** (`sku_id` en productos + cuenta en pagos, `datos v:3`, exportadores PDF/Excel). **Falta:** pantalla de mapeo, endpoint atómico de confirmación, archivo/bloqueo de la sim confirmada.
**Bloqueante externo:** Sebastián tiene que confirmar **cuáles de los 9 borradores son despachos reales y con qué fecha de ingreso al depósito** (define `lotes.fecha_alta` → CF14 → qué ventas puede costear hacia atrás, y en qué mes cae el crédito).
Magnitud: **$1.041.161.264,59 de costo** y **$199.339.793,17 de crédito fiscal** sin capturar.

### Ventas — pendiente estructural
- **Sync Tango de ventas no-ML.** Hoy el sync sólo pega facturas a ventas ML por `ExternalID`; **4 de las 5 líneas no tienen forma de entrar al sistema**. Habría que traer también los movimientos sin `ExternalID` y proyectarlos a `ventas`/`venta_items`.

### Compras — pendientes de implementación
- **Tipo `iva_percepcion`** en `compra_componentes`: no existe.
- **Notas de crédito de compra**: sin soporte.
- **Ciclo `abierta`/`cerrada`** de la compra (para importaciones).
- **`meses_cerrados` no existe** → la inmutabilidad del mes cerrado (P5/G6/I4) hoy es solo convención. **Primer guardarriel real construido el 7/8**: `POST /ventas/efectivo` exige confirmación explícita si la fecha cae en un mes anterior.

### Deuda de datos abierta
- **El 86,7 % del CMV de julio es estimado** ($298,1M de $343,9M): 18 SKUs sin ninguna compra cargada, 16 con lotes agotados, 5 con lote de fecha posterior a la venta (CF14).
- **LEANVAL S.A. duplicado** (proveedores 1 y 2, mismo CUIT, ambos en uso).
- **Movimiento 16325**: $2.000.000 cargados en `caja_usd` con descripción "Alquiler deposito". `server.js` no valida que la moneda del monto coincida con la de la cuenta.
- **Las cajas** venían solo con salidas; `caja_ars` se dio vuelta a **+$4.116.312,41** con la primera venta en efectivo. Sigue faltando el circuito de transferencias y el saldo inicial.
- **Compras 1 y 3 con componentes de producto y cero lotes.**
- **Lotes con `costo_unitario = 0`** en la compra inicial #4.
- **Sin unicidad `(proveedor_id, nro_factura)`**: compras 2 y 3 comparten `00006-00000451`.
- ~~**Token de ML caído**~~ → **resuelto el 7/8/2026** (O15). Falta el paso manual de `offline_access` en el panel.
- **(10/8) `iibb_parametros.base_confirmada = false` para 2026-06** — la alícuota efectiva es un **techo** hasta que llegue la base total declarada del CM03.

### A cargar antes del arranque
- Clasificación y costo unitario real al 31/12/2025 de los SKUs
- Distribución % de sueldos por empleado por línea
- Catálogo de proveedores · Logísticas activas

### Operativos a confirmar
- Frecuencia de inventario físico · Política para AR > 90 días · Plazo para promesas de cobro incumplidas · Manejo de notas de crédito

### Integraciones técnicas
- Tango: confirmar `AplicacionNombre` para Tienda Nube; probar factura A
- **Pagos en mediación: `por_cobrar` inflado (DIAGNOSTICADO).** Discriminador `payment.status === 'in_mediation'`.

### Evoluciones futuras (no críticas)
- Plantillas de gastos recurrentes · reclamos en bulk · API de claims ML · sync de stock Tango · reportes al contador · comparativos anuales

---

## Decisiones revertidas / consideradas y descartadas

| Decisión propuesta | Por qué se descartó | Reemplazada por |
|--------------------|---------------------|-----------------|
| Sync ML manual con botón | Demanda atención innecesaria. | Sync automático + botón de respaldo (O1). |
| Vista de Ventas del día como tabla | Incómoda para operar. | Tarjetas + atajos (O2) — que nunca se construyeron; el modelo actual no las necesita. |
| Tarjeta de crédito como cuenta | Complejidad innecesaria. | Desglose en N gastos (CB9). |
| Inversiones asignadas a una línea | Distorsiona el análisis. | Tesorería centralizada (TI1, TI2). |
| Reclamos solo a ML | Excluía garantía con proveedores. | 2 destinatarios (R2). |
| Pérdidas por mercadería = gasto operativo | Distorsiona gastos vs costo. | Rubro propio en P&L (P7). |
| Tablas paralelas con prefijo `r_` | Deuda técnica. | Reset de `public` (A13). |
| Saldo de apertura cargado a mano (A2 original) | No reconstruible; trababa el arranque. | Saldo derivado por conciliación (A2 rev.). |
| Sin login; RLS off (A6) | Seguridad no puede ser solo del backend. | Login + RLS (A17). |
| "Conciliar todas" automático | Sebastián quiere confirmar venta por venta. | Tablero manual bidireccional (O13). |
| **Corte de apertura fiscal el 31/05/2026** | Al implementarlo, la DDJJ de junio ya estaba presentada. | **Corte 30/06/2026** (I9). |
| **Cargar las facturas de costos de importación sueltas** | Duplica el costo o mis-costea el lote con FOB pelado. | **Fase 2 del simulador** (C14). |
| **Cargar el costo accesorio de un tercero como componente** | Su crédito se devengaría en el período equivocado. | **Gasto vinculado + `capitaliza_compra_id`** (C15). |
| **Recalcular los componentes de importación** | Se pierde el cuadre contra el papel. | **Cargar al valor del comprobante** (C16). |
| **Repartir el costo tardío solo sobre el remanente** | Márgenes falsos, hasta negativos, en las últimas unidades. | **Reparto sobre todo el lote** (CF13). |
| **Esperar todas las facturas para dar de alta el stock** | El PSI queda ciego en los días de mayor rotación. | **El stock entra al llegar** (S10). |
| **Reusar el canal `whatsapp_efectivo` para las ventas sin comprobante (7/8/2026)** | Ese canal es para las ventas off-ML que **sí** se emiten en Tango; mezclarlas las volvería inseparables en el Resultado, y el ingreso de una sin comprobante entra al 100 % mientras el de una facturada entra neto de IVA. | **Canal `efectivo` propio** (V1). |
| **Borrar la simulación al confirmar el despacho (7/8/2026)** | El costo del lote es inmutable y la simulación es su memo de cálculo: sin ella no hay forma de reconstruir de dónde salió. | **Archivar** (`estado='confirmada'` + bloqueo de edición, IMP-F2-1). |
| **Botón "= al costo" en la venta en efectivo (7/8/2026)** | Evaluado tras un error de carga real (un dígito de menos generó un margen falso de −$900.000); Sebastián lo descartó por innecesario. | Verificación manual contra el lote. |
| **Tomar la retención de IIBB de ML como gasto del período (10/8/2026)** | Es un **anticipo**, no el impuesto: subestimaba el costo fiscal en **~4 puntos de margen**. | **Impuesto determinado** (IIBB1). |
| **Rellenar el remanente de facturación con una línea de negocio inventada (10/8/2026)** | Una línea con ingresos y sin CMV mostraría **margen ~100 %** y mentiría peor que el número que reemplaza. | **Exponerlo como cobertura** (`cobertura_pct`, `facturacion_fuera_de_adara`, IIBB5). |

---

## Cómo se actualiza este documento

1. **Cuando se decide algo nuevo**: agregar a la sección correspondiente con su número, breve descripción y referencia al doc temático.
2. **Cuando se cambia una regla**: actualizar la entrada vigente Y agregar entrada en "Decisiones revertidas".
3. **Cuando se cierra un pendiente (TBD)**: moverlo de "Pendientes" a la sección que corresponde.
4. **Cuando se agrega un doc temático nuevo**: agregar la sección de dominio y actualizar `ADARA-DOCS-INDEX.md`.

---

## Documentos relacionados

- `ADARA-DOCS-INDEX.md` — índice y navegación
- `ADARA-FLUJO-OPERATIVO.md` · `ADARA-LINEAS-NEGOCIO.md` · `ADARA-PNL.md` · `ADARA-IMPUESTOS.md` · `ADARA-COMPRAS-IMPORTACIONES.md` · `ADARA-STOCK.md` · `ADARA-CONCILIACION-BANCARIA.md` · `ADARA-INVERSIONES.md` · `ADARA-TFACTURA.md` · `ADARA-PATRIMONIAL.md` · `ADARA-RECLAMOS.md` · `ADARA-GASTOS.md` · `ADARA-REFERENCIAS.md` · `ADARA-IMPORTACIONES-SIM.md` · `ADARA-VENTAS-EFECTIVO.md` · `ADARA-IIBB-CONVENIO-MULTILATERAL.md`

---

## Actualización — 11 Junio 2026

### A17 — Seguridad: login multiusuario con Supabase Auth + RLS (revierte A6)
El frontend lee y escribe Supabase **directo**, por lo que la seguridad no puede ser solo del backend. RLS en todas las tablas/vistas con regla única **`authenticated` = todo, `anon` = nada**. Detalle en `ADARA-AUTH.md`.

### CF12 — Costos de logística/comisión a terceros capitalizan al lote
Flete, comisión, despacho y coima a **terceros** capitalizan al costo del lote pero **no son deuda con el proveedor** del producto. **(Matiz 5/8/2026: si el tercero emite comprobante propio, va como gasto capitalizable — C15.)**

### Compras — CUIT de proveedor opcional para informales
CUIT opcional (dedup por nombre normalizado sin CUIT). Compra **sin factura** → **Exento**. `compras.tc_blue` admite el TC del USDT.

---

## Actualización — 13 Junio 2026

### O11 — Bonificaciones de envío Flex vía shipment del payment
El puente está en el **payment crudo de MP** (`transaction_data.reference_id` con `reference_type='shipment'` → `ventas_ml.shipment_id`), no en el settlement report.

### Token ML — persistencia en `workspace_config`
Causa raíz de "ML se desconecta solo": la tabla no existía → token en memoria borrado en cada redeploy. Resuelto. **Pendiente que quedó abierto 2 meses: habilitar `offline_access` en el panel de ML → ver O15 (7/8/2026).**

---

## Actualización — 21 Junio 2026 (Adjuntos + Flex + nav Mercado Libre)

- **AD1** — Gastos y compras llevan su comprobante en Supabase Storage. Ver `ADARA-ADJUNTOS.md`.
- **Flex (F1–F7)** — logística automática, zonas tarifarias, semana lun-sáb, partido por Georef (F7).
- **NAV1** — grupo "Mercado Libre" con sub-pestañas.

---

## Actualización — 7 Julio 2026 (Simulador de Importaciones)

- **P14** — Arancel/tasa SIM capitaliza (vía bolsón) y se paga en el VEP, sin crédito ni base IVA.
- **P15** — Coima sobre el ahorro TOTAL de derechos+estadística (CIF real sombra). **Corrección 22/7 (HOKU):** el ahorro se mide solo contra subdeclaración de FOB/cantidad/posición del producto.
- **P16** — Unidades no declaradas = stock real; divisor del costo unitario = cantidad real.
- **IMP-SEGURO** — Seguro declarado-no-pagado se neutraliza con un fijo negativo.
- **ML-BON1** (hallazgo abierto) — Aporte de ML en promos compartidas no capturado en `importe_bruto`.

---

## Actualización — 13 Julio 2026 (Conciliación mensual bidireccional)

- **O13** — Tablero manual mensual, reusa el motor O7/O8/O10.
- **`ventas_ml.conciliacion_periodo`** — casillero del mes de conciliación. **SOLO eje plata** (S9).

---

## Actualización — 3 Agosto 2026 (Apertura fiscal de IVA + SKUs nuevos)

### I9 — Apertura fiscal de IVA con doble arrastre (FISC-APERTURA, cerrada)
**Corte 30/06/2026**: junio se carga como **total de la DDJJ** (F.2051, tx 1182373081) en `posicion_fiscal_apertura`, y `v_posicion_fiscal` arrastra **dos saldos a favor por separado**: **técnico** ($13.808.267,01) y **libre disponibilidad** ($26.306,80). Meses ≤ junio = `incompleto`; julio+ = `fino`. `v_control_mensual` NO se tocó.

> **Nota 7/8/2026:** el saldo técnico a favor **se consumió íntegro en julio** — quedó en $0,00. No hay arrastre para agosto.

### Pendientes fiscales nuevos: FISC-RET-IVA y FISC-CRED-ML
Ver "Pendientes acumulados → Fiscal".

### 20 SKUs nuevos (huérfanos ML de julio) + costo_referencia
272 órdenes ($187,6M) quedaban fuera del Resultado porque sus 20 SKUs no estaban en el catálogo (CF1). Tras el alta y la re-proyección, el **neto de julio pasó de $214M a $384M**. Se les cargó `costo_referencia` desde las simulaciones (18/20).

### C14 — Las facturas de costos de importación no se cargan sueltas
Solo las facturas de **producto/FOB** son compras de mercadería; los **servicios operativos** van a Gastos; los **costos de importación** se materializan por la Fase 2 del simulador.

---

## Actualización — 5 Agosto 2026 (circuito de compras nacionales: 10 reglas nuevas)

### Modelado del comprobante (C15-C18)
- **C15 — un comprobante de tercero es un gasto, no un componente.** El eje que decide no es "¿esto capitaliza al costo?" sino **de quién es el comprobante y en qué fecha se devenga su crédito fiscal**.
- **C16 — el comprobante manda.** Recalcular rompe la trazabilidad contra el papel.
- **C17 — dos TC conviven.** El oficial (base tributaria) y el blue (costo del lote, C6) miden cosas distintas.
- **C18 — moneda de emisión, no moneda de referencia.** Caso Jukebox: emitidas en pesos con leyenda de TC 1.520 informativa.

### Prioridades operativas (S10, C19, CF13)
- **S10 — el stock entra al llegar.** Se posterga el costeo, no el ingreso.
- **C19 — costo ahora, crédito después.** El costo mal cargado es irreversible una vez vendido; la Posición Fiscal se recalcula sola.
- **CF13 — el costo tardío se reparte sobre todo el lote.** Repartirlo solo sobre el remanente produce márgenes falsos, hasta negativos.

### Responsabilidad y fechas (CB13, O14, G13)
- **CB13 — el movimiento lo crea quien tiene la evidencia externa.**
- **O14 — quién carga qué.** Gastos: el equipo. Compras de mercadería: Sebastián.
- **G13 — fecha del comprobante, no de carga.**

---

## Actualización — 7 Agosto 2026 (ventas y compras sin comprobante · token ML · Fase 2 del simulador)

Sesión larga de implementación. Ocho reglas nuevas (**V1-V3, C20, O15, CF14, CB14** + el bloque **IMP-F2-1…9**), cinco migraciones aplicadas y seis archivos desplegados.

### El patrón que se repitió tres veces: el dato fiscal no puede depender de lo que se tipea

Las tres decisiones más importantes de la sesión son la misma idea aplicada a lugares distintos:

| Dónde | Antes dependía de… | Ahora depende de… |
|---|---|---|
| **IVA débito de una venta** (V2) | la alícuota cargada en el ítem | el **tipo de comprobante** (`es_gravada`, filtro en `v_control_mensual`) |
| **Crédito fiscal de una compra** (C20) | que nadie cargara el componente IVA | la **marca `sin_comprobante`** (CHECK + trigger en la base) |
| **Subdeclaración en el simulador** (IMP-F2-9) | que los campos de override estuvieran vacíos | el **check "Declaro distinto"** |

En los tres casos el estado anterior fallaba **en silencio**: nada rompía, nada avisaba, y el número simplemente salía mal. En los tres se movió la verdad a un campo declarativo y se puso el guardarriel **en la base**, no sólo en el front.

### Ventas sin comprobante (V1-V3)
Ver reglas arriba y `ADARA-VENTAS-EFECTIVO.md`. Hallazgo que lo motivó: al 6/8 la tabla `ventas` tenía 18.259 registros y **el 100 % era `canal='ml'`** — ninguna venta off-ML había entrado nunca. Migraciones: filtro `FILTER (WHERE v.es_gravada)` en `v_control_mensual` (regresión cero verificada) y alta del canal `efectivo`.

### Compra sin comprobante (C20)
Ver regla arriba. Migraciones: `compras.sin_comprobante` + CHECK + trigger `fn_chk_componente_sin_factura` + `v_compras_ap` expone la columna. Se renombró de `sin_factura` a `sin_comprobante` sobre la marcha porque `compra_componentes.tipo` ya tenía un valor `'sin_factura'` con otro significado.

### Token de ML (O15)
Causa raíz: `workspace_config.ml_refresh_token` en **NULL** porque `offline_access` nunca se habilitó en el panel (pendiente abierto desde el 13/6). Tres bugs corregidos que lo hacían invisible. **Resultado: el sync volvió a correr.** Detalle en `ADARA-VENTAS-ML.md`.

### Fase 2 del simulador (IMP-F2-1…9)
Spec cerrado con Sebastián. Hallazgo bloqueante: los productos del JSONB **no tenían `sku_id`** — sólo `nombre` en texto libre, sin puente posible con el catálogo. Primera entrega desplegada.

### Frontend
- **`core/skuPicker.js`** (componente nuevo): buscador de SKUs por código y descripción, sin acentos, con términos sueltos. Con ~190 SKUs el `<select>` era inusable. Patrón de integración: deja un `<input type="hidden">` con la clase que se le pida y dispara un `change` que burbujea, así la lógica delegada existente no cambia. El menú cuelga de `<body>` con `position:fixed` porque `.modal` tiene `overflow-y:auto` y lo recortaba.
- **Banner de token de ML en la home**: primer consumo de `/health` desde el front.
- **Exportadores del simulador**: PDF (print del navegador, sin dependencias) y Excel comparativo declarado vs real (SheetJS lazy desde CDN con fallback a CSV).

### Lecciones
- **Cachear catálogos en variables de módulo rompe en una SPA.** El modal de venta en efectivo cacheaba los SKUs y nunca los refrescaba: un SKU dado de alta después no aparecía jamás. Los catálogos se recargan al abrir.
- **`fn_consumir_fifo` procesa todas las ventas del rango**, no la que se acaba de crear (CF5). Su `unidades_faltantes` es global y no sirve para avisar sobre una venta puntual.
- **Verificar contra la base antes de diagnosticar el código.** El token, el FOB del despacho BISHOP y el margen negativo de julio se resolvieron los tres mirando datos, no leyendo lógica.
- **El entorno de Claude no puede pushear al repo** (proxy de git). Los archivos los sube Sebastián a mano.

---

## Actualización — 10 Agosto 2026 (IIBB / Convenio Multilateral)

Siete reglas nuevas de dominio (**IIBB1-IIBB7**) más una de compras (**C21**), siete migraciones aplicadas y seis pedidos al contador. **El detalle completo del dominio está en `ADARA-IIBB-CONVENIO-MULTILATERAL.md`**; el schema, en `ADARA-SCHEMA.md`.

### El error de fondo: confundir el anticipo con el impuesto

Hasta esta sesión el IIBB entraba al Resultado como **la retención de ML**. Es la plata que efectivamente se descuenta de cada liquidación, así que parecía el número correcto. No lo es: la retención y las percepciones de compra son **pagos a cuenta** (I5, P12). El impuesto del período es **el determinado** del CM03.

**Magnitud del error: ~4 puntos de margen subestimados.** El costo fiscal real de operar era mayor que el que mostraba el Resultado, y la diferencia crecía justamente donde más se retiene.

De ahí salen las tres reglas centrales: **IIBB1** (el gasto es el determinado), **IIBB2** (sólo la base gravada tributa, igual que el IVA débito) y **IIBB3** (la alícuota se deriva de un CM03 presentado, nunca se estima).

### Lo que ADARA no ve: cobertura, no relleno

`base_referencia` es la base **total declarada** en el CM03, que incluye facturación que ADARA no tiene cargada. Eso deja dos huecos y una tentación:

- Mientras la base no esté confirmada, la alícuota efectiva es un **TECHO** (**IIBB4**) — el denominador está subestimado. La pantalla lo dice con un banner en vez de mostrar el número como si fuera exacto.
- La diferencia entre base declarada y base gravada de ADARA se expone como **cobertura** (`cobertura_pct`, `facturacion_fuera_de_adara`), **no** como una línea de negocio inventada (**IIBB5**). Una línea con ingresos y sin CMV mostraría margen ~100 %: mentiría peor que el número que reemplaza.

**Un hueco declarado es información. Un hueco rellenado es un error escondido.** Es el mismo criterio con el que se marca el CMV estimado y con el que `v_posicion_fiscal` distingue `incompleto` de `fino`.

### El coeficiente es de la empresa (IIBB6)

Bajo Convenio Multilateral hay **un solo coeficiente unificado** para toda la actividad. Repartir el IIBB entre líneas es **gestión interna**, no fiscalidad — se hace por **peso de base gravada** dentro del período. Misma naturaleza que LN8: informativo, y la DDJJ es una sola.

Por eso `v_resultado_linea_mensual` conserva `margen_contribucion` y `resultado_operativo` como estaban, y agrega `margen_contribucion_real` y `resultado_operativo_real`: el par viejo sigue siendo el número operativo comparable con los meses anteriores, y el nuevo es el que incluye el costo fiscal repartido.

### IIBB7 — la lección técnica

Un intento de intercalar una columna en el medio de `v_iibb_determinado` falló con **`42P16: cannot change name of view column`**. `CREATE OR REPLACE VIEW` **sólo agrega columnas al final**.

Se eleva a regla del proyecto porque la limitación de Postgres coincide con una regla de producto que ya veníamos aplicando de hecho: **las columnas de una vista son contrato con el frontend desplegado**. Extender siempre al final es lo que permitió meter IIBB en el Resultado sin tocar ninguna pantalla existente.

### C21 — cuánto cuesta comprar en negro

Apareció al evaluar una oferta concreta de proveedor. Por cada peso **no documentado** en la compra que después se **vende con comprobante**, se pierde el crédito de IVA **y** el gasto deducible de Ganancias:

| Alícuota de IVA de la mercadería | Costo por peso no documentado |
|---|---|
| 10,5 % | **45,5 %** |
| 21 % | **56 %** |

Con tasa de Ganancias del 35 % y **sin contar IIBB**, que lo empeora. Conclusión operativa: **"50 % facturado / 50 % en efectivo" sólo conviene con un descuento del 45,5 % sobre la parte en efectivo**, que ningún proveedor ofrece. Sólo cerraría si esa mercadería también se vendiera sin facturar, y **hoy no hay canal para eso a escala**: la venta en efectivo de julio fue **$8,1M** contra **$60,7M** de débito mensual de ML.

**Corolario de agenda:** la **tasa efectiva de Ganancias** deja de ser un pendiente cómodo y pasa a ser urgente — el 35 % de la tabla es un supuesto, y sin el dato real no se puede evaluar ninguna oferta de este tipo.

### Pedidos al contador

Seis, listados en "Pendientes acumulados → A definir con el contador". El de mayor relación valor/esfuerzo es la **base imponible total declarada del CM03 202606**: un solo número que convierte la alícuota de techo en exacta y de paso mide cuánta facturación no ve ADARA.

El más urgente de definir por su impacto en el schema es **si la DDJJ declara una o más actividades**: si "comercio electrónico" y "venta mayorista de iluminación" tributan con alícuotas distintas, `iibb_parametros` necesita un renglón por actividad. **Es un cambio chico si se sabe ahora.**

Y tres hallazgos que salieron de mirar el CM03 renglón por renglón: **determinado negativo** en Santa Cruz (−$551.217,97) y Tierra del Fuego (−$15.831,19), que no es normal; **$1,7M de saldos a favor** concentrados en Corrientes, Santa Fe y La Pampa sobre **$2.867.481,49** totales, donde conviene pedir reducción de alícuota de retención; y una diferencia de **$15.471.267,94 contra $1,53M** entre los "Valores Restan" del CM03 y lo que ve ADARA, que apunta a **SIRCREB en el Supervielle** sin ingestar.
