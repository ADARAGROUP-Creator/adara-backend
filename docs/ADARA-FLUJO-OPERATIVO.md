# ADARA — Flujo Operativo (v22)

Última actualización: 5 Agosto 2026 — **reescrito completo a v22**.

Cadencia operativa del día a día, semana a semana, mes a mes.
**Lectura obligatoria al inicio de cualquier sesión de diseño o desarrollo.**
Define el "para qué" del sistema: la app sirve a esta operatoria, no al revés.

> ⚠️ **Por qué se reescribió.** La versión anterior (15/5/2026) describía un sistema que ya no existe: hablaba de 4 personas sin login, de una pantalla de "aprobar ventas del día" que nunca se construyó, de stock que se descuenta al aprobar, de un "Cerrar mes" que no es ejecutable y de una conciliación automática que fue rediseñada. Cada chat nuevo arrancaba leyendo un mapa viejo. Esta versión describe lo que efectivamente pasa hoy y lo que efectivamente falta.

---

## Principios operativos

1. **~5 personas usan la app, con login.** Desde el 11/6/2026 hay autenticación real (Supabase Auth + RLS, decisión **A17**, que revierte A6). Usuarios al 4/8/2026: `spuccio`, `spugliese`, `adandrea`, `fpuccio`, `sebastianp`. RLS: `authenticated` = todo, `anon` = nada. No hay roles diferenciados todavía: quien entra, ve y toca todo. Ver `ADARA-AUTH.md`.
2. **La app es ambiente compartido en vivo.** Concurrencia: last-write-wins con chequeo de `updated_at` para evitar pisarse.
3. **Sin alertas externas** (email / WhatsApp / push). Todos entran todos los días — la **home** destaca lo que requiere atención al abrir la app.
4. **Conciliación universal**: toda venta (cualquier origen) debe cerrar contra un movimiento bancario o de MP. Lo que no cerró todavía vive en Cuentas por Cobrar.
5. **Stock master en ADARA.** Tango se usa **para emitir comprobantes** (facturas y remitos). ADARA se sincroniza con Tango para traer ventas y números de comprobante. Ninguna venta se carga manualmente en ADARA.
6. **Costos exactos, no estimados.** Cada compra/importación es un lote con su costo real. FIFO por lote. CMV se calcula con el costo real de las unidades consumidas.
7. **Foto de la empresa en tiempo real.** Stock valorizado + saldos bancarios + caja + AR − AP = valor neto del negocio.
8. **Lo que entra por API entra solo; lo que entra en papel hay que cargarlo.** Este es el principio que faltaba. La app quedó muy buena leyendo ML, Tango y MP, y **ciega para todo lo que llega como comprobante**: facturas de compra y de gasto. Si el proceso no reserva un momento para cargarlas, no se cargan — y no se cargaron (ver "Estado real", crédito IVA de gastos = $0 histórico).

---

## Cadencia — vista general

| Bloque | Quién | Cuándo | Tiempo |
|--------|-------|--------|--------|
| Automático | nadie | continuo | — |
| Diario — equipo | equipo operativo | todos los días | ≈5 min |
| Diario — Sebastián | Sebastián | todos los días | ≈5 min |
| Semanal | equipo / Sebastián | 1 vez por semana | — |
| Mensual (cierre) | Sebastián | primeros días del mes siguiente | — |

> **"Diario" significa mirar, no cargar.** Se mira la bandeja todos los días; la mayoría de los días no va a haber nada para cargar. Eso **no es un fallo del proceso**: es el resultado esperado. El objetivo del hábito es que ninguna factura quede más de un día sin entrar, no generar trabajo todos los días.

---

## Bloque automático (corre solo, sin intervención)

| Proceso | Qué hace |
|---------|----------|
| Sync ML | Trae las ventas de Mercado Libre y las upsertea en `ventas_ml` |
| Sync Tango | Cada 3 h. Pega N° de factura + tipo (A/B/C) + total facturado a cada venta ML por `ml_order_id ↔ ExternalID` |
| Proyección a `ventas` | `fn_proyectar_ml` — de `ventas_ml` al modelo costeable (`ventas` / `venta_items`) |
| Consumo FIFO | `fn_consumir_fifo` — descuenta stock del lote correspondiente y registra el CMV real |
| Congelamiento de CMV | Fija el costo de las ventas ya costeadas para que no se mueva hacia atrás |
| Retenciones de IIBB | Se capturan desde el settlement de MP |

**No hay aprobación humana en el medio.** El stock se descuenta solo, encadenado al `/ml/sync`. La pantalla de "aprobar ventas del día" **nunca se construyó** y el modelo actual no la necesita.

> 🔴 **Al 5/8/2026 este bloque NO está corriendo.** El token de ML está caído (`ml_token: false`). Sin sync ML no entran ventas nuevas, y por lo tanto **no se actualizan ni el débito fiscal ni el stock**. Es el primer problema a destrabar de la lista; todo lo demás de esta cadencia depende de él.

---

## Diario — equipo (≈5 min)

| # | Tarea | Pantalla | Notas |
|---|-------|----------|-------|
| 1 | **Revisar la bandeja de facturas del mail** | (fuera de ADARA) | Las facturas llegan por mail. Se mira todos los días |
| 2 | **Cargar los gastos con factura A que hayan llegado** | Gastos | Fecha del comprobante, tipo correcto, línea + categoría, adjuntar el PDF |
| 3 | Procesar devoluciones físicas recibidas | Devol/Canc | OK stock (+1 al lote) / No disponible (inicia reclamo ML) |

## Diario — Sebastián (≈5 min)

| # | Tarea | Pantalla | Notas |
|---|-------|----------|-------|
| 1 | **Cargar las compras de mercadería que hayan llegado** | Compras | Tocan stock, lotes y CMV. Las carga Sebastián, no el equipo |
| 2 | Mirar la tira de Posición Fiscal en la home | Home | IVA a pagar + crédito cargado + días desde la última carga |

### Por qué el reparto es así

Una compra crea **lotes**, y un lote mal cargado contamina el CMV de forma difícil de revertir (el costo nace en la compra y es inmutable — regla CF6; corregir implica ajuste de inventario con delta, no edición directa). Un gasto mal cargado, en cambio, se corrige y listo. Por eso: **gastos → equipo, compras de mercadería → Sebastián**.

---

## Semanal

| Tarea | Pantalla | Notas |
|-------|----------|-------|
| Control de Flex contra el resumen del proveedor | Flex | Envíos del período vs. lo facturado por la logística. Ver `ADARA-FLEX.md` |
| PSI / recompra | PSI | SKUs en riesgo de quiebre → armar OC si corresponde. Ver `ADARA-PSI.md` |

---

## Mensual (cierre, a mes vencido)

Se hace en los primeros días del mes siguiente al que se cierra.

| # | Tarea | Pantalla | Notas |
|---|-------|----------|-------|
| 1 | Tablero de conciliación ML | Ventas ML | Tablero mensual **manual y bidireccional** (rediseñado el 13/7/2026): dos tablas, Ventas del mes ↔ Extracto MP completo, KPIs de anillos, diferir/traer cross-mes. **No** hay vinculación automática por monto. Ver `ADARA-VENTAS-ML-V22.md` |
| 2 | Extracto Banco Supervielle | Conciliación bancaria | Subir el extracto del mes vencido |
| 3 | Extracto Mercado Pago | Conciliación | Subir el AS del mes vencido |
| 4 | **Barrido de las facturas que faltaron** | Gastos / Compras | Red de contención de la carga diaria: lo que no entró en el día, entra acá antes de mirar impuestos |
| 5 | Resultado del mes | Resultado | Coherencia por línea × canal |
| 6 | Posición Fiscal del mes | Posición Fiscal | IVA neto a pagar, crédito cargado, arrastre de saldos a favor |

> ❌ **No hay "Cerrar mes".** `meses_cerrados` no existe: la capa 9 (Patrimonial/Cierres) está en pausa por A14. El cierre mensual hoy es un ritual de revisión, no un bloqueo del período. Mientras no exista, **nada impide modificar un mes ya revisado** — de ahí la importancia del guardarriel de fecha (ver abajo).

---

## Decisiones operativas (5 Agosto 2026)

Tomadas con Sebastián en esta sesión. Son decisiones de proceso, no propuestas.

| # | Decisión | Fundamento |
|---|----------|------------|
| FO1 | **Frecuencia de carga de comprobantes: diaria, a medida que llegan** | Evita el pozo de fin de mes y que el crédito fiscal se pierda por olvido |
| FO2 | **Las facturas llegan por mail** | La bandeja del mail es el disparador del hábito diario |
| FO3 | **Los gastos los carga el equipo; las compras de mercadería las carga Sebastián** | Un lote mal cargado contamina el CMV y es difícil de revertir; un gasto mal cargado se corrige |
| FO4 | **El IVA a pagar se ve todo el tiempo en la home**, no solo al cierre | La decisión de conseguir crédito se toma durante el mes, no después |
| FO5 | **Un despacho de importación se confirma cuando la mercadería llega al depósito** | El hecho que dispara stock y costo es la llegada física, no el pago ni el embarque |

---

## Guardarrieles de la carga delegada (pendientes de construir)

Delegar la carga al equipo baja el riesgo de que no se cargue nada y sube el riesgo de que se cargue mal. Tres frenos mínimos, hoy inexistentes:

| Guardarriel | Qué tiene que hacer | Por qué |
|-------------|---------------------|---------|
| **Fecha = la del comprobante** | La fecha nunca es "hoy": es la del comprobante. Si cae en un mes anterior, la pantalla debe **avisarlo explícitamente** | Sin cierre de mes nada frena una carga retroactiva silenciosa que cambia una posición fiscal ya revisada |
| **Freno a la factura de Mercado Libre** | Bloquear/advertir fuerte al intentar cargar la factura de ML como gasto | Las comisiones y envíos de ML **ya restan vía `ventas_ml`**. Cargar la factura como gasto **duplica el costo**. Es el error más probable de alguien que carga "todo lo que llega" |
| **Selector de tipo de comprobante en criollo** | Etiquetas entendibles, no jerga fiscal, y explicitar qué genera crédito | Es el campo que decide si hay crédito o no. Una **factura A cargada como B es crédito perdido en silencio**: nada falla, nada avisa, el número simplemente nunca aparece |

---

## Flujo de gastos

Tango **no maneja gastos**. Toda la gestión de gastos vive en ADARA, con dos flujos posibles:

| Flujo | Cuándo | Cómo |
|-------|--------|------|
| **A) Proactivo** (el del hábito diario) | Llega la factura al mail | Se carga el gasto en ADARA con la fecha del comprobante y su adjunto. Después aparece el movimiento en banco/MP y se concilia |
| **B) Reactivo** | Aparece un movimiento en el extracto sin gasto asociado | Desde "Mov sin conciliar" → "Pasar a gasto" → línea + categoría |

El reparto por imputaciones (línea + canal + %, Σ=100) y el crédito IVA por línea están en `ADARA-GASTOS.md` (regla G12).

---

## Estados físicos de devoluciones y reclamos

| Estado | Descripción | Acción típica |
|--------|-------------|---------------|
| Reclamo abierto | Claim abierto en ML, esperando respuesta | Esperar / insistir si pasa mucho tiempo |
| En viaje al depósito | Producto vuelve, etiqueta generada por ML | Esperar llegada |
| Recibido — sin revisar | Llegó al depósito, falta abrir y evaluar | Abrir, evaluar estado |
| Recibido — OK stock | Producto en buen estado, vuelve a vender | Sumar al lote correspondiente |
| Recibido — no disponible | Roto, faltante, producto distinto al esperado | Iniciar reclamo a ML |
| En gestión con ML | Reclamo nuestro pendiente de respuesta de ML | Cobrar a ML o dar por perdido |
| Resuelto cubierto | ML pagó el reclamo | Cerrado, registrado como recupero |
| Resuelto perdido | Reclamo fallido | Cerrado, registrado como pérdida en P&L |

La **cuenta corriente con ML** se construye desde acá: "En gestión" + "Resueltas perdidas" es la deuda histórica con ML; lo cubierto es el recupero efectivo. Ver `ADARA-RECLAMOS.md` y `ADARA-CANCELACIONES-DEVOLUCIONES.md`.

---

## Estado real al 5 Agosto 2026

Foto honesta del sistema, para que ninguna sesión nueva asuma de más.

### Pantallas operativas

Home · SKUs · Movimientos · Gastos · Compras · Conciliación · Resultado · Inventario · PSI · Posición Fiscal · Flex · Cuadre · Saldos · Ventas ML · Sim. Importaciones

### Números

| Dato | Valor |
|------|-------|
| Compras cargadas en julio 2026 | 4 compras · **$104.803.138,21** · todas con comprobante adjunto |
| Posición fiscal julio — débito | **$60.147.783,57** |
| Posición fiscal julio — crédito | **$10.977.150,06** |
| Posición fiscal julio — a pagar | **$35.336.059,70** |
| Crédito IVA de gastos | **$0 en todo el histórico** |
| `caja_ars` | **−$4.000.000** (solo salidas, nunca una entrada) |
| `caja_usd` | **−USD 2.000.000** (solo salidas, nunca una entrada) |
| Simulaciones de importación | **9 en borrador, ninguna confirmada**; varias corresponden a despachos que **ya entraron físicamente** |

### Lectura de esos números

- **Crédito IVA de gastos $0** no es un bug de cálculo: es la consecuencia directa de que el proceso nunca pidió cargar facturas de gasto. La carga diaria (FO1–FO3) existe para cerrar exactamente esto.
- **Cajas en negativo** significa que se registraron pagos desde caja pero nunca ingresos ni saldo inicial. No hay circuito de transferencias entre cuentas.
- **9 simulaciones sin confirmar** con mercadería ya en el depósito significa stock y costo real que el sistema no tiene. Se destraba con Fase 2 del simulador (`ADARA-IMPORTACIONES-SIM.md`) + la regla FO5.

---

## Eventos que la home debe destacar al abrir la app

La home muestra arriba de todo, en formato resumido, lo que requiere atención. **No son alertas externas — son destacados internos.**

| Evento | Por qué importa |
|--------|-----------------|
| **Tira de Posición Fiscal con indicador de completitud** | A pagar **+ crédito cargado + días desde la última carga**. Mostrar solo el "a pagar" da un número que asusta y no es accionable: sin saber cuánto crédito hay cargado, no se sabe si el número es real o es un vacío de carga |
| **Compras esperando facturas** | Mercadería que entró sin su comprobante — crédito fiscal y costo pendientes |
| Devoluciones nuevas a procesar | Mercadería física que llegó o reclamo abierto |
| SKUs con stock crítico | Riesgo de quiebre (ver PSI) |
| Cheques que vencen en los próximos 7 días | Esperar acreditación bancaria |
| Cuentas por cobrar vencidas | Cliente B2B atrasado |
| Movimientos sin conciliar (monto y cantidad) | Cierre de mes |
| Reclamos ML sin respuesta hace > 10 días | Insistir con ML |
| Token de ML caído | Sin sync no se actualizan ni ventas ni stock ni débito fiscal |

---

## Pain points

### Resueltos

| # | Dolor | Cómo se resolvió |
|---|-------|------------------|
| 1 | Stock no se descuenta auto al confirmar venta | ✅ `fn_consumir_fifo` encadenada al `/ml/sync`. Sin intervención humana |
| 2 | Vista de Ventas del día como tabla incómoda para operar | ✅ Desapareció el problema: no hay paso de aprobación. La pantalla de tarjetas nunca se construyó y ya no hace falta |
| 3 | Sync ML y Tango manuales | ✅ Automáticos (Tango cada 3 h) |
| 4 | Reclamos ML sin trackear recupero | ✅ Modelo de reclamos + cancelaciones/devoluciones documentado e implementado (P13, GAP de recepción aparte) |
| 5 | Bug del reporte XLSX semanal con canceladas sin fecha de entrega | ✅ Cerrado como **transitorio** (21/6/2026): reporte legacy que se retira; la necesidad está cubierta nativo por **PSI** (unidades por SKU×semana) y **Cuadre** + drill-down de **Resultado**. El discriminador correcto era `estado_envio`, no `fecha_entrega` |
| 6 | Posición IVA no se veía en vivo | ✅ Pantalla Posición Fiscal + `v_posicion_fiscal` con apertura y doble arrastre. Falta llevarla a la home con completitud (ver eventos) |
| 7 | Sin login ni separación de usuarios | ✅ A17 — Supabase Auth + RLS desde el 11/6/2026 |

### Abiertos

| # | Dolor | Frecuencia | Solución prevista |
|---|-------|-----------|-------------------|
| 8 | **Las cargas de compra y de gasto no tenían momento asignado en el proceso** | Permanente | Cadencia diaria FO1–FO3 + guardarrieles. Causa raíz del crédito IVA de gastos en $0 |
| 9 | **Las cajas no reflejan la realidad** (`caja_ars` −$4M, `caja_usd` −USD 2M) | Permanente | Circuito de caja: transferencias entre cuentas + saldo inicial |
| 10 | **Despachos de importación reales sin registrar** (9 borradores, varios ya entraron) | Por importación | Fase 2 del simulador: confirmar → compra importación + lotes + crédito fiscal. Regla FO5 |
| 11 | **Token de ML caído** (`ml_token: false`) | Esporádico, activo hoy | Reautorizar + `offline_access` + auto-refresh proactivo + banner en home |
| 12 | **Sin cierre de mes** (`meses_cerrados` no existe, capa 9 en pausa) | Mensual | Nada bloquea modificar un período ya revisado. Mientras tanto: guardarriel de fecha en la carga |
| 13 | Crédito fiscal de ML sin capturar | Mensual | FISC-CRED-ML: crédito de comisiones/envíos ML sin duplicar el costo |
| 14 | Casos que requieren criterio manual en conciliación | Mensual | Conciliación manual con motivo (tablero bidireccional) |
| 15 | Cross-month: ventas de fin de mes se cobran al siguiente | Mensual | Diferir→ / traer ↰ en el tablero mensual (O13) |
| 16 | Sin foto patrimonial unificada (stock + bancos + caja + AR − AP) | Permanente | Capa 9 — Patrimonial (en pausa por A14) |

---

## Canales de venta — origen de las ventas

| Origen | Cómo entra a ADARA | Factura | Cobro |
|--------|---------------------|---------|-------|
| **ML** (Mercado Libre) | Sync API ML | Tango (vía API ML→Tango) — ADARA importa el N° y tipo de factura | MP → eventualmente banco |
| **Tienda Nube** (web propia) | Sync Tango | Tango | MP → eventualmente banco |
| **B2B** (clientes recurrentes) | Sync Tango | Tango | Transferencia / cheques / efectivo |
| **WhatsApp / efectivo / consumidor final** | Sync Tango | Tango (remito, no factura formal) | Efectivo / transferencia |

---

## Líneas de negocio

Mismo CUIT y mismas condiciones fiscales para todas (IVA RI + IIBB Convenio Multilateral + Ganancias):

| Línea | Productos | Canales típicos |
|-------|-----------|-----------------|
| ML Electrónica | Auriculares, smartwatches, parlantes, etc. | ML |
| Electrónica off-ML | Mismos productos que ML | Tienda Nube + WhatsApp + efectivo |
| Luminarias | Alumbrado público | B2B |
| Mochilas Sindicatos | Mochilas para sindicatos | B2B |
| Mochilas Individuos | Mochilas para personas | ML + Tienda Nube + WhatsApp |

> El modelo evolucionó (8/6/2026): **línea = producto/familia**, **canal = dimensión separada**. El detalle vigente de la desagregación y las reglas de imputación está en `ADARA-LINEAS-NEGOCIO.md`.

---

## Cuentas donde se mueve la plata

| Cuenta | Moneda | Uso | Estado |
|--------|--------|-----|--------|
| Banco Supervielle | ARS | Operativa general (3 sub-cuentas + tarjeta Visa) | Operativa |
| Mercado Pago | ARS | Cobros ML + Tienda Nube | Operativa |
| `caja_ars` | ARS | Cobranzas y pagos en efectivo | ⚠️ −$4.000.000: solo salidas, nunca una entrada ni saldo inicial |
| `caja_usd` | USD | Excedentes para compras al exterior | ⚠️ −USD 2.000.000: mismo problema |
| `santi_financiera` | ARS | Cuenta agregada el 13/6/2026 | Operativa |

Sin cuentas bancarias en USD: las compras en USD salen de caja USD física o se compra USD para transferir.

> **Pendiente estructural — circuito de caja.** Faltan las **transferencias entre cuentas** (la plata que entra a caja sale de otro lado) y el **saldo inicial**. Hasta que existan, los saldos de caja no son leíbles. Ver `ADARA-MOVIMIENTOS.md` y `ADARA-SALDOS`/pantalla Saldos.

---

## Logística — multi-transportadora

### Logísticas activas hoy

| Transportadora | Para qué se usa | Estado |
|----------------|-----------------|--------|
| MEF (Mercado Envíos Flex) | Envíos Flex de ventas ML | Operativa — control semanal en la pantalla Flex |
| Segunda logística | Envíos Flex alternativos | Nombre por confirmar |

### Modelo

- Las logísticas viven en una **tabla configurable**. No están hardcodeadas.
- Botón **"+ Nueva logística"** para sumar transportadoras sin desarrollar.
- **Cada envío se asigna manualmente** a una logística al despachar — no hay auto-asignación por zona / peso / categoría.
- Cada logística se factura por separado y se **audita semanalmente** contra los envíos del período (paso semanal de la cadencia).

Detalle en `ADARA-FLEX.md` (modelo de zonas, asignador partido→zona, reconciliación contra el resumen del proveedor).

---

## Decisiones que cierran este documento

- **Snapshot inicial**: al 31/12/2025 · **corte fiscal de apertura de IVA**: 30/06/2026
- **Operación corriente**: desde mayo 2026
- **Frecuencia de cierre**: mensual, a mes vencido — **sin bloqueo de período** (no hay `meses_cerrados`)
- **Conciliación ML**: tablero mensual **manual y bidireccional** (13/7/2026), no automática por monto
- **Stock**: descuento **automático** por FIFO encadenado al sync, sin aprobación humana
- **Carga de comprobantes**: **diaria**, gastos por el equipo, compras por Sebastián (FO1–FO3)
- **Despacho de importación**: se confirma **cuando la mercadería llega al depósito** (FO5)
- **Compras USD**: TC blue al momento de la compra (criterio histórico)
- **Compensación de costos entre lotes**: ajuste de inventario con delta (no edición directa)

---

## Documentos relacionados

- `ADARA-DECISIONES.md` — reglas de negocio consolidadas (constitución)
- `ADARA-AUTH.md` — login, usuarios y RLS (A17)
- `ADARA-GASTOS.md` — categorías, imputaciones (G12), crédito IVA
- `ADARA-COMPRAS-IMPORTACIONES.md` — lotes, costos reales, prorrateos
- `ADARA-IMPORTACIONES-SIM.md` — simulador what-if + Fase 2 (confirmar despacho)
- `ADARA-COSTEO-FIFO.md` — proyección ML → costeado, FIFO, congelamiento de CMV
- `ADARA-STOCK.md` — lotes, físico vs disponible, ajustes
- `ADARA-IMPUESTOS.md` — posición fiscal, apertura y arrastre de saldos a favor
- `ADARA-VENTAS-ML-V22.md` — tablero de conciliación ML (O13)
- `ADARA-CONCILIACION-BANCARIA.md` — matching universal venta ↔ movimiento
- `ADARA-MOVIMIENTOS.md` — carga manual, cuentas, importación de extractos
- `ADARA-FLEX.md` — control semanal de envíos
- `ADARA-PSI.md` — recompra por SKU
- `ADARA-PNL.md` — estado de resultado mensual por línea
- `ADARA-RECLAMOS.md` / `ADARA-CANCELACIONES-DEVOLUCIONES.md` — devoluciones y recupero
- `ADARA-LINEAS-NEGOCIO.md` — líneas, canales y reglas de imputación
- `ADARA-PATRIMONIAL.md` — valor neto de la empresa (capa 9, en pausa)
