# ADARA — Changelog

Historial de cambios por sesión. Para estado actual ver `ADARA-DOCS-INDEX.md`.

> 📌 Sesiones anteriores al rediseño de mayo 2026 corresponden al sistema **v21** en producción. Las decisiones técnicas vigentes están consolidadas en `ADARA-DECISIONES.md` (rediseño).

---

## 23 Septiembre 2026 — Simulador validado contra despachos oficializados (flete por FOB) · cascada de zona en Flex · doc nuevo de zonificación MEF

Sesión de auditoría y documentación. No se tocó código: se validó el motor del simulador contra los despachos reales de BISHOP agosto y se definió el "cómo" del plan F7 de Flex.

### 1. Por qué la app y la planilla de Sebastián no daban lo mismo

Arrancó con una diferencia en el 30% de derechos + estadística de la sim **BISHOP AGOSTO (id 11)**: la planilla daba USD 4.162,08 y la app USD 3.824,95. Se replicó el cálculo contra el JSONB guardado y se aislaron **dos causas distintas**, ninguna un bug:

1. **Declaración distinta.** La planilla estaba hecha sobre una declaración vieja (tablets USD 22.024 de FOB); la sim guardada tenía tablets por 32.535,50, con ~10.500 movidos de smartwatch (0% de derechos) a tablets (16%). Eso solo son USD 1.682 más de derechos.
2. **Base distinta.** La planilla calcula sobre **FOB** con flete en 1; la app sobre **CIF**. Diferencia: USD 558 de ahorro.

### 2. Los despachos oficializados: el árbitro

Sebastián aportó los dos despachos de la misma factura `ADAR26082601MA`, oficializados el 09/09/2026 en Ezeiza:

| | 26073IC04002688 (real) | 26073IC04002690 (declarado) | Diferencia |
|---|---|---|---|
| Derechos | 19.522,98 | 5.435,85 | 14.087,13 |
| Estadística | 699,84 | 147,40 | 552,44 |
| **Total** | **20.222,82** | **5.583,25** | **14.639,57** |
| **Coima 30%** | | | **USD 4.391,87** |

Ni la planilla ni la app daban ese número, cada una por su motivo. Con los datos del despacho cargados (declaración del 2690 + **flete declarado 5.747,70**, no 4.731,85), la app da **14.631,35 → coima 4.389,41**. El gap de USD 8 es que el despachante escaló los FOB del despacho "real" para que sumen 116.493,00 igual que el modificado (FOB facturado real: 116.443,00).

### 3. Hallazgo — el flete fiscal prorratea por FOB, no por peso

En el despacho 2688 el Valor en Aduana de los tres ítems sale de multiplicar el FOB por el **mismo** factor `1,054586` = `1 + (5.747,70 + 611,20) / 116.493,00`, sin importar el peso (tablets 288,92 kg · smartwatch 116,14 kg · auriculares 94,94 kg).

`ADARA-IMPORTACIONES-SIM.md` decía **"flete por PESO declarado"** en la fórmula del CIF: **estaba mal**. El código (v3) ya lo hacía por FOB. El reparto por peso (`fleteCostoShare`) se usa **solo para el costeo del lote**, no para la base aduanera. Son dos prorrateos que conviven.

Además: estadística 3% sobre 23.328,16 = 699,84, **sin tope** (el `tope_estadistica_usd` de 180 no intervino en este despacho).

### 4. Reglas nuevas del simulador

- **La sim se valida contra el despacho oficializado, no contra planillas de Excel.**
- **El `flete_declarado` se toma del campo *Flete Total* del despacho.**
- **La base del ahorro es CIF, no FOB.** Calcular sobre FOB subestima ~4,5%. Si alguna vez el despachante pactara la coima sobre FOB, haría falta un parámetro `coima_base`; hoy no existe.
- Documentado el schema **`datos v:3`**: `sku_id` por producto, `cuenta` en `pagos[]`, y en `params` `tope_estadistica_usd` (180), `iibb_monto` y `ganancias_monto`. Estaban en uso desde el 7/8 sin figurar en el doc.

### 5. Flex — cómo se resuelve la zona, sin etiquetas

Sebastián pasó el prompt que venía usando aparte para contar etiquetas ZPL y clasificarlas en las 26 zonas de MEF, y preguntó si no alcanzaba con el dato de la venta. **Alcanza.** Se definió la cascada:

```
1. lat/long del envío → Georef → partido        (F7)
2. flex_localidad_partido (caché, ambigua=false)
3. flex_partido_zona: partido → zona
4. sin match → REVISAR (no se fuerza)
```

El punto fino: **el caché no es criterio, es memoria**. La zona se deriva del lat/long una vez y se guarda contra el nombre de la localidad para no repetir la consulta. Eso respeta F7 y resuelve la cola de 287 localidades sin zona.

**Localidades ambiguas que nunca se cachean por nombre:** Villa Adelina (Vicente López/San Isidro), Gerli (Avellaneda/Lanús), Canning (Esteban Echeverría/Ezeiza), Tortuguitas, El Palomar (Morón/3 de Febrero), San Francisco Solano (Quilmes/Alte Brown), Nordelta (urbanización, no partido). Se marcan `ambigua = true` y se resuelven siempre por lat/long; los históricos sin lat/long van a REVISAR.

**Pendiente resuelto:** **La Tablada = MATANZA NORTE (GBA 1)**, porque las etiquetas de MEF la clasifican así y la etiqueta es el criterio de quien factura. Se descarta la auditoría que sugería GBA 2.

**Lo que ningún dato resuelve:** el split Matanza Norte/Sur (zonificación comercial) y las excepciones tipo los 2 envíos de la semana 16/02 que ADARA clasificó CABA y MEF cobró GBA 1.

### 6. Documentación

- **`ADARA-FLEX-ZONAS-MEF.md` (NUEVO)**: las 26 zonas, diccionario localidad→partido con ambiguas marcadas, split Matanza, lectura y normalización de ZPL (`^FO0,660` manda sobre `^FO0,705`), excepciones comerciales.
- **`ADARA-FLEX.md`**: cascada de resolución, tabla `flex_localidad_partido` (a crear), localidades ambiguas, La Tablada resuelta, pendientes actualizados.
- **`ADARA-IMPORTACIONES-SIM.md`**: fórmula del CIF corregida (flete por FOB), caso testigo BISHOP id 11, reglas de validación contra despacho, schema `v:3`.
- **`ADARA-DOCS-INDEX.md`**: doc nuevo #30 + entradas actualizadas.

> 📝 **Actualizados en la sesión del 23/9/2026:** `ADARA-FLEX-ZONAS-MEF.md` (**nuevo**), `ADARA-FLEX.md`, `ADARA-IMPORTACIONES-SIM.md`, `ADARA-DOCS-INDEX.md`, `ADARA-CHANGELOG.md`.

---

## 10 Agosto 2026 — IIBB de Convenio Multilateral en el Resultado · impuesto al cheque · márgenes reales corregidos · fix `iibb_tucuman`

Sesión fiscal. Se implementó el módulo de **IIBB de Convenio Multilateral** de punta a punta y se incorporó el **impuesto al cheque** al Resultado. El efecto es grande y desagradable: **los márgenes que se venían publicando estaban 3 a 5 puntos altos**. Doc nuevo dedicado: **`ADARA-IIBB-CONVENIO-MULTILATERAL.md`**.

### 1. El disparador — el Resultado no tenía calculados los IIBB a pagar

Sebastián preguntó si el Resultado tenía calculados los IIBB a pagar. **No los tenía.** La pantalla restaba la **retención** de Mercado Libre (~**1,05 %** del ingreso neto) en vez del **impuesto determinado** (~**5,29 %**). La retención es un pago a cuenta; el determinado es el impuesto. Consecuencia directa: **todos los márgenes publicados estaban 3 a 5 puntos altos.**

### 2. La fuente — DJ Mensual CM03, anticipo 202606

Se trabajó sobre la **DJ Mensual CM03** del anticipo **202606** (junio 2026) de **ADARA RS SRL**, CUIT **30-71747647-2**, **Sede 901**, **Form. 5866**, N° Verificador **786149**, presentada ante ARCA el **15/07/2026**, tx **1182208938**.

| Concepto | Monto |
|----------|-------|
| **Determinado total** | **$20.765.310,42** |
| Valores restan | $15.471.267,94 |
| Valores suman | $10.094,76 |
| A favor del contribuyente | $2.867.481,49 |
| **Total a pagar** | **$8.171.618,73** |

**24 jurisdicciones** (ADARA está inscripta en todas). Concentración fuerte: **Buenos Aires 40,11 % + CABA 38,79 % = 78,9 %** del impuesto.

### 3. Hallazgo técnico clave — el CM03 ya trae el factor completo

El CM03 **no expone la base imponible ni los coeficientes por separado**, sólo el determinado por jurisdicción. Pero como

```
determinado_j = base × coef_j × alicuota_j
```

el cociente **`determinado_j / base` ya es el factor completo**. **Nunca hizo falta separar coeficiente de alícuota**: con la base y el determinado por jurisdicción alcanza para reproducir la liquidación entera. Esto simplificó el modelo de datos de forma sustancial.

### 4. Implementación

**Tablas nuevas** (ambas con RLS):
- **`iibb_parametros`** — parámetros mensuales, con **`alicuota_efectiva` como columna GENERATED**.
- **`iibb_jurisdiccion`** — los **24 renglones** del CM03.

**Vistas nuevas:** `v_iibb_base`, `v_iibb_determinado`, `v_iibb_jurisdiccion_mensual`, `v_impuesto_cheque_mensual`.

**Vistas extendidas de forma aditiva** (columnas viejas intactas, sin romper nada que las lea): `v_resultado_mensual` y `v_resultado_linea_mensual`.

**Frontend:** `public/js/screens/resultado.js`, entregado a Sebastián para subir.

**Validación:** junio reproduce **$20.765.310,42 exacto** contra el CM03.

### 5. ⚠️ Limitación vigente — la alícuota efectiva de 5,2852 % es un TECHO

Confirmado por Sebastián: **la base declarada del CM03 incluye facturación B2B de luminarias que ADARA no tiene cargada**. Como el módulo reparte el determinado sobre la base que el sistema sí conoce, **hoy la línea ML absorbe también el IIBB del B2B**. El número es conservador (sobreestima el impuesto de ML), no optimista.

**Se arregla cargando la base imponible real declarada: un UPDATE de una línea en `iibb_parametros`.** Es el pedido #1 al contador.

### 6. Márgenes reales corregidos (con IIBB determinado + impuesto al cheque)

| Período | Margen ANTES | **Margen REAL** |
|---------|--------------|-----------------|
| 2026-01 | 11,17 % | **7,00 %** |
| 2026-02 | 14,05 % | **9,83 %** |
| 2026-03 | 11,24 % | **7,06 %** |
| 2026-04 | 8,09 % | **3,45 %** |
| 2026-05 | 7,37 % | **2,37 %** |
| 2026-06 | 6,38 % | **1,59 %** |
| 2026-07 | 8,70 % | **4,16 %** |
| 2026-08 (parcial) | 11,82 % | **7,38 %** |

**Patrón: cuanto más vende, peor margen.** De enero a junio la facturación se **multiplicó por 3,7** y el margen cayó de **7,00 % a 1,59 %**.

### 7. Impuesto al cheque incorporado al Resultado

Antes **no estaba en ningún lado**. **$8.205.589,40** acumulados desde marzo. Ahora entra al Resultado vía `v_impuesto_cheque_mensual`.

### 8. Bug corregido — `iibb_tucuman` en el campo jurisdicción

Las retenciones del **régimen de recaudación de Tucumán** entraban con `tipo='iibb_tucuman'` y **`jurisdiccion` en NULL**. `v_retenciones_iibb` caía al `COALESCE` con `financial_entity` y devolvía la jurisdicción literal **`'iibb_tucuman'`**, partiendo Tucumán en dos renglones distintos.

**Fix:** backfill de **9.980 filas** + la vista ahora **deriva la jurisdicción sin depender de la ingesta**. Tucumán quedó unificado en **$157.443,45**.

### 9. Hallazgo — el sync de Tango descarta la facturación B2B

En `server.js` (~línea **3280**), `syncTangoFacturas()` **ya trae TODAS las facturas** con `ListarMovimientos` y **pide el detalle de cada una**, pero descarta las que no son de Mercado Libre:

```js
if (!(dae && dae.AplicacionNombre === 'Mercado Libre' && dae.ExternalID)) { stats.no_ml++; continue; }
```

**La cañería para conocer la facturación B2B ya existe.** Guardar esos movimientos es el paso 1 del roadmap de IIBB: sin eso no se puede saber la base imponible real.

### 10. Saldos a favor de IIBB — $2.867.481,49 inmovilizados

| Jurisdicción | Saldo a favor |
|--------------|---------------|
| Santa Fe | $886.893,95 |
| Santa Cruz | $725.600,97 |
| Corrientes | $707.678,62 |
| Catamarca | $327.197,08 |
| La Pampa | $143.246,24 |
| Río Negro | $54.113,39 |
| Tierra del Fuego | $22.751,24 |

En **Corrientes retienen ~6x** y en **La Pampa ~10x** lo determinado. Hay pedido de reducción de alícuota al contador.

### 11. Brecha de retenciones — falta SIRCREB

El CM03 computó **$15.471.267,94** de "Valores Restan" en junio. ADARA ve **$1,53M** por el settlement de MP y **$4,35M** por la API de ML. **Falta casi seguro SIRCREB** (recaudaciones bancarias del Supervielle), que hoy **no se ingesta**.

### 12. Verificación del circuito de compra sin comprobante (regla C20, del 7/8)

Verificado contra la base y contra el repo: **está sano**. Columna, CHECK `chk_sin_comprobante_sin_nro`, trigger `trg_componente_sin_factura`, `POST /compras` forzando IVA y percepciones a 0, **409** en `/compras/:id/factura`, y el tilde en `compras.js`. **0 compras cargadas con `sin_comprobante=true` todavía.**

### 13. Hallazgo sobre el CMV congelado

El CMV está **congelado hasta el 8/8/2026** (no hay mes abierto). Pero **`fn_consumir_fifo` es re-ejecutable e idempotente por ítem**, así que **una compra cargada con fecha vieja SÍ puede recuperar el CMV real de ventas pasadas** — pero requiere correr **`fn_consumir_fifo(desde, hasta)` a mano**, porque el hook del `/ml/sync` usa una ventana de **7 días**.

### 14. Bug de datos detectado (NO corregido, pendiente de confirmación)

El movimiento **#16325**, *"Alquiler deposito"* del **1/7/2026** por **−$2.000.000**, está cargado en la **cuenta 4 (Caja USD)** cuando **los tres alquileres anteriores están en Caja ARS**. No se tocó: falta confirmación de Sebastián.

### 15. Análisis — comprar con 50 % facturado y 50 % en efectivo

Un proveedor se lo ofreció a Sebastián como oportunidad. **Conclusión: le sirve al proveedor, no a Sebastián.**

Por cada peso pagado sin comprobante se pierden **~45,5 centavos en impuestos** (alícuota de IVA 10,5 % + tasa de Ganancias 35 %), asumiendo que vende esa mercadería **facturada** — que es su caso, porque vende casi todo por ML. El **punto de equilibrio exigiría un descuento del 45,5 %** sobre la parte en efectivo. Con **IVA al 21 % sube a 56 %**. **No incluye IIBB**, que lo empeora.

### Migraciones aplicadas

1. `iibb_parametros_y_jurisdicciones`
2. `fix_jurisdiccion_iibb_tucuman`
3. `vistas_iibb_determinado`
4. `resultado_con_iibb_determinado`
5. `resultado_linea_con_iibb`
6. `iibb_peso_base_null_safe`
7. `iibb_alicuota_por_periodo_y_cobertura`

### Documentos actualizados

`ADARA-IIBB-CONVENIO-MULTILATERAL.md` (**nuevo**) · `ADARA-DOCS-INDEX.md` · `ADARA-CHANGELOG.md`

---

## 7 Agosto 2026 — Token de ML restaurado, venta en efectivo, buscador de SKU, compra sin comprobante, Fase 2 del simulador

### 1. Token de ML — caído, diagnosticado y restaurado

**Causa raíz (acción del usuario, todavía pendiente):** `offline_access` **no está habilitado en el panel de desarrollador de ML**. Sin eso, ML nunca devuelve `refresh_token`, por más que el scope figure en la URL de autorización. Poner el scope en la URL no alcanza: hay que activarlo en el panel.

**Tres bugs de código que hacían el problema invisible** (los tres corregidos en `server.js`):
1. `refreshML()` fallaba **mudo**: el token moría y no quedaba rastro de por qué. Ahora registra el error en `ML_ERROR` (`setMLError` / `clearMLError`).
2. `/ml/callback` **pisaba el refresh token previo con `null`** cuando la respuesta nueva venía sin uno. Corregido a `data.refresh_token || refreshPrevio`. Además, si no hay refresh token, el callback ahora muestra una **página de advertencia** en vez de festejar una conexión que va a durar 6 horas.
3. El cron corría `'0 */6 * * *'` contra un token de ~6h de vida: ventana de carrera garantizada. Ahora corre `'7 * * * *'` (cada hora) con **guarda de 2h** para no refrescar de más.

**Observabilidad:** `/health` expone `mlEstado()` distinguiendo **tres estados** (conectado / vencido pero recuperable / requiere reconexión manual) más el último error. `home.js` pinta un **banner** con esos estados, y falla en silencio si el backend todavía no tiene el bloque `ml` (compatibilidad hacia atrás).

**Resultado:** el sync retomó. Julio pasó de **2.079 a 2.095 ventas** y el IVA débito de **$60.147.783,57 a $60.709.745,42**.

### 2. Venta en efectivo — circuito nuevo (V1, V2, V3, CB14)

Pedido de Sebastián: poder anotar una venta en efectivo que **descuente stock**, **sume ARS a la caja** y **no genere IVA débito**.

- **`POST /ventas/efectivo`**, atómico: venta + ítems + movimiento de caja + vínculo + FIFO en una sola operación.
- Canal **`efectivo`** nuevo en la tabla `canales`. **Corrección documental importante:** `ventas.canal` **sí tiene FK** a `canales` (en la sesión se había afirmado lo contrario).
- **Sin IVA débito:** ítems con `alicuota_iva = 0`, `es_gravada = false`, y filtro `FILTER (WHERE v.es_gravada)` en `v_control_mensual` (migración `iva_debito_solo_ventas_gravadas`). Ver `ADARA-IMPUESTOS.md`.
- **Primer ingreso de caja del sistema:** hasta ahora `caja_ars` solo tenía egresos. El movimiento nace con **monto positivo**, `origen='venta_efectivo'`. `caja_ars` pasó de **−$4.000.000 a +$4.116.312,41**.
- **CB14 — nace conciliado:** el movimiento se crea ya vinculado (`vinculos` con `op_tipo='venta'`). No pasa por Conciliación: no hay nada que conciliar, la plata y la venta son el mismo hecho.
- **Guarda de mes anterior:** si la fecha cae en un período ya cerrado, responde **409 `fecha_mes_anterior`** y exige `confirmar_mes_anterior`.
- Referencia legible **`EFVO-######`**.
- **El FIFO corre último y en su propio `try`**: si falla, no tumba la venta ya registrada. Se recostea después.

**Error corregido en producción:** la venta `EFVO-018262` se cargó con **un dígito de menos** ($4.000.000 en lugar de $4.900.000) y produjo un margen falso de **−$900.000**. Corregida por SQL. **El CMV no se tocó**: lo fija el costo del lote al momento de consumir, no el precio de venta.

### 3. Buscador de SKU — `public/js/core/skuPicker.js` (componente nuevo)

Con ~200 SKUs el `<select>` era inusable. Componente reusable con búsqueda **por código y por descripción**, sin acentos, multi-término (`"jbl 520"` encuentra `JBL520BT — Auricular JBL 520 BT`), con orden por relevancia y navegación por teclado.

- **Patrón de integración sin refactor:** mantiene un `<input type="hidden">` con la clase que se le pida y dispara un `change` **que burbujea** → los listeners delegados que escuchaban al `<select>` siguen funcionando sin tocarlos. El diff en `compras.js` fueron **15 líneas**.
- El menú **cuelga de `<body>` con `position:fixed`** porque `.modal` tiene `overflow-y:auto` y un dropdown absoluto se recortaba justo en la última fila de ítems. Obliga a llamar `destroy()` al re-renderizar.
- Integrado en el modal de venta en efectivo y en la carga de ítems de compra.

**Bug corregido:** los SKUs recién creados **nunca** aparecían en el modal. Causa: `SKUS_CACHE` en una variable de módulo, dentro de una SPA que no recarga la página → el caché no expiraba jamás. Ahora se traen en cada apertura. Convención **12** en `ADARA-FRONTEND.md`.

### 4. Compra sin comprobante — C20

Columna `compras.sin_comprobante` + CHECK + trigger `fn_chk_componente_sin_factura` + manejo en `POST /compras` y `POST /compras/:id/factura` + tilde y badge en el frontend. Detalle completo en `ADARA-COMPRAS-IMPORTACIONES.md`.

**Renombrada a mitad de camino** de `sin_factura` a `sin_comprobante`: colisionaba con `compra_componentes.tipo='sin_factura'`, que significa otra cosa (un renglón informal dentro de una compra sí facturada).

### 5. Simulador de importaciones — Fase 2, primera entrega (IMP-F2-1 … IMP-F2-9)

Especificación completa del circuito de confirmación en `ADARA-IMPORTACIONES-SIM.md`. Entregado en esta sesión:

- **`sku_id` por producto** y **`cuenta` por pago**; `datos` versionado a `v:3`.
- La fila de SKU usa `<input list>` + `<datalist>` nativo, **no** `skuPicker`: el archivo re-renderiza entero y el componente tiene bind único. Documentado como "cuándo NO usarlo".

**Bug corregido — `declaro_distinto` se ignoraba.** Reportado por Sebastián: "no tenía seleccionada la casilla pero me tomaba como que sí". La función `eff()` no miraba el flag, así que valores tipeados y después destildados **seguían pesando** en el cálculo. Además, destildar no limpiaba los overrides. Corregido: `eff()` recibe el producto y respeta `declaro_distinto`; destildar limpia los **cuatro** overrides, incluida `cantidad_decl`.

**Reconciliación del despacho BISHOP:** tras el fix, la única diferencia real eran **10 unidades × US$178,00 = US$1.780,00**. Con la corrección de Sebastián el FOB quedó en **US$101.426,80 con 1.928 unidades**, exacto contra la proforma.

**Exportadores nuevos:**
- **PDF** de la tabla de precios finales (botón arriba de la tabla).
- **Excel comparativo**: declarado vs real en dos tablas lado a lado, con cantidades reales y modificadas y el **% de diferencia de precio**. SheetJS con lazy-load desde cdnjs y **fallback a CSV**.
- Verificados extrayendo el motor de cálculo y reproduciendo al centavo `costo_total_ars` **$170.524.295,63** y `credito_total_ars` **$31.004.354,10**.

**Bloqueante de la segunda entrega:** falta que Sebastián confirme **cuáles de los 9 borradores son despachos reales** y con qué **fecha de ingreso al depósito**.

### 6. Auditoría de CMV de julio

**86,7% del CMV de julio es estimado:** $298.099.423,47 sobre $343.862.983,22. Se separó en **tres bolsas diagnosticables (A/B/C)** con una query reutilizable — ver `ADARA-COSTEO-FIFO.md`.

### 7. Hallazgos cuantificados

Los **9 borradores del simulador sin confirmar** acumulan **$1.041.161.264,59 de costo** y **$199.339.793,17 de crédito fiscal sin capturar**, contra un IVA a pagar de julio de $27.036.258,42. Confirmar esos despachos es, de lejos, la palanca fiscal más grande disponible.

### 8. Limitación de entorno + pendiente de seguridad

- **El entorno de Claude no puede pushear al repo.** El git proxy responde `access denied by the git proxy: ADARAGROUP-Creator/adara-backend is not in this session's authorized repository set`. **No se intentó rodear la restricción.** Flujo vigente: Claude entrega los archivos, Sebastián los sube. Documentado en `ADARA-FRONTEND.md`.
- ⚠️ **Pendiente de seguridad:** **revocar el PAT de GitHub `adara-claude`**, que quedó pegado en el chat. Nunca llegó a usarse (el proxy bloqueó el push), pero está expuesto. github.com/settings/personal-access-tokens

### Migraciones aplicadas

1. `iva_debito_solo_ventas_gravadas`
2. `canal_efectivo_sin_comprobante`
3. `compras_sin_factura`
4. `compras_sin_comprobante_rename`
5. `v_compras_ap_expone_sin_comprobante`

### Documentos actualizados

`ADARA-DECISIONES.md` · `ADARA-VENTAS-EFECTIVO.md` (**nuevo**) · `ADARA-IMPORTACIONES-SIM.md` · `ADARA-VENTAS-ML.md` · `ADARA-DOCS-INDEX.md` · `ADARA-FRONTEND.md` · `ADARA-MOVIMIENTOS.md` · `ADARA-COSTEO-FIFO.md` · `ADARA-SCHEMA.md` · `ADARA-COMPRAS-IMPORTACIONES.md` · `ADARA-IMPUESTOS.md` · `ADARA-CHANGELOG.md`

---

## Sesión 5 agosto 2026 — Circuito de compras nacionales cerrado

Sesión de implementación + carga real: se destrabó el bug que impedía guardar compras con gastos prorrateables, se cerró el circuito de **compras nacionales** de punta a punta (factura del proveedor, cargos en la propia factura, costos de terceros con factura vinculados como gastos, pagos sin comprobante) y se cargaron las tres compras que faltaban de julio. Reglas de negocio nuevas (C15-C19, S10, CF13, CB13, O14, G13) en `ADARA-DECISIONES.md`; migraciones y hallazgos de schema en `ADARA-SCHEMA.md`.

**Lo nuevo**
- **Fix del bug que impedía guardar compras con gastos prorrateables.** `compra_componentes.clase` es una columna **GENERATED** y se estaba mandando en el insert → PostgREST devolvía **`428C9`**. Se sacó del payload.
- **CHECK de `compra_componentes.tipo` ampliado** con `gasto_prorrateable` y `extra_directo` (migración `ampliar_tipos_compra_componentes`).
- **Gastos prorrateables con alícuota de IVA propia** y tilde **"de esta factura"** que decide entre `flete` (suma al AP del proveedor) y `gasto_prorrateable` (no suma).
- **Soporte de N alícuotas de IVA** en un mismo comprobante.
- **IVA de SKU pasó de input de texto libre a select** (Exento / 10,5% / 21%) en los tres lugares donde se editaba: modal de `skus.js`, modal dentro de `compras.js` y celda inline de la tabla. Un valor fuera de lista se muestra como tal en vez de pisarse al redibujar.
- **Gasto capitalizable:** columna `gastos.capitaliza_compra_id`, con **exclusión del P&L** (`v_gastos_mensual` y `v_gastos_categoria_mensual` filtran `capitaliza_compra_id IS NULL`; `v_gastos_ap` no, la deuda con el proveedor existe igual), **reparto del neto entre los lotes**, endpoint de preview **`GET /compras/:id/impacto-capitalizacion`**, **re-costeo de `consumo_lote`** y **confirmación obligatoria cuando la compra ya tuvo ventas**.
- **Pantalla de compras:** detalle expandible al hacer click en la fila (productos, otros costos, impuestos, lotes con barra de consumo, gastos capitalizados, comprobante), **badge PDF** visible en lugar del clip, y **KPIs que respetan el filtro de período**.

**Datos cargados**
- Proveedor **Jukebox S.A.** (id 7), SKUs `NT004`, `TA014G`, `AI001N` y los de **Stylus**.
- Compras **#9 ($6.431.377,79)**, **#10 ($8.937.538,74)** y **#11 ($27.756.411,74)**. **Julio quedó con 4 compras por $104.803.138,21, todas con comprobante.**
- La **#9** requirió corrección manual por SQL (el fix todavía no estaba); la **#10** fue la primera en entrar completa y sola, **validando el prorrateo automático que nunca se había ejecutado**.

**Lecciones**
- Una columna **GENERATED nunca debe viajar en el payload** de PostgREST; el error **`428C9`** es específico y fácil de diagnosticar si se conoce.
- **`CREATE OR REPLACE VIEW` no deja reordenar ni renombrar columnas**: las nuevas van al final (error **`42P16`**).
- **Una feature documentada como implementada puede no existir en la base.** Los gastos prorrateables figuraban como andando desde el 11/6 con fórmula y todo, pero el CHECK no los admitía y había **0 filas**. Conviene verificar contra `information_schema` **y contra la existencia de filas reales** antes de darlos por hechos.
- El **repo real es `ADARAGROUP-Creator/adara-backend`** (público). El `adara-app-v21.html` que vive en el proyecto Claude es **legacy** y no corresponde a la app en producción: es tema oscuro, no tiene compras, ni SKUs, ni menciona `compra_componentes`.

**Estado al cerrar**
- **Compras nacionales operativo de punta a punta:** factura del proveedor, cargos en la propia factura, costos de terceros con factura vinculados como gastos, y pagos sin comprobante vía `sin_factura`.
- **Posición fiscal de julio:** débito **$60.147.783,57** · crédito **$10.977.150,06** · **a pagar $35.336.059,70**. Crédito de gastos sigue en **$0**.
- **Próximo:** gastos de julio, circuito de caja (transferencias y saldo inicial), crédito de ML, Fase 2 del simulador de importaciones.

**Pendientes abiertos**
- Tipo `iva_percepcion` en `compra_componentes` (no existe).
- **LEANVAL duplicado** (proveedores 1 y 2, mismo CUIT, ambos en uso).
- **Movimiento de $2.000.000 en `caja_usd`** (pesos en una caja de dólares, origen `sin_factura_auto` sin validación de moneda).
- **Las cajas en negativo** (`caja_ars` −$4.000.000, `caja_usd` −USD 2.000.000: solo salidas, nunca una entrada).
- **Compras 1 y 3 sin lotes** (mercadería que nunca entró al stock).
- **Lotes con costo 0** (compra inicial #4, SKU 6 y 19, con stock activo).
- **Token de ML caído.**
- **`meses_cerrados` inexistente.**
- **Notas de crédito de compra sin soporte.**

**Documentación.** Actualizados `ADARA-SCHEMA.md` (4 migraciones + schema real verificado + deuda de datos), `ADARA-DECISIONES.md` (C15-C19, S10, CF13, CB13, O14, G13 + bloque "Actualización 5 Agosto 2026") y este `ADARA-CHANGELOG.md`.

---

## 4 Agosto 2026 — Integración Tango Factura IMPLEMENTADA (N° de factura + tipo + total en cada venta ML) · auth destrabado · causa raíz de la lentitud/errores · columnas fiscales en `ventas_ml` · pantalla Ventas ML con Factura/Facturado

Sesión larga de debugging + implementación de la integración con Tango Factura, de punta a punta: destrabar credenciales, encontrar por qué el listado de facturas era lento y se caía, y pegar a cada venta ML su **N° de factura, tipo (A/B/C) y total facturado**. Detalle completo en `ADARA-TFACTURA.md` (reescrita) y `ADARA-ML-BONIFICACIONES.md` (§12).

**Auth destrabado (dos causas).**
- **Variables de entorno = `TF_*`, no `TANGO_*`.** El `server.js` lee `TF_APP_KEY`/`TF_USERNAME`/`TF_PASSWORD`/`TF_USER_ID`. La doc (index + TFACTURA) decía `TANGO_*` → estaba mal; se corrigió. Los valores son los de la app ADARA en el portal de Tango (app ya autorizada, no hizo falta recrearla; Tango no deja borrarla porque está "configurada para otro usuario", no molesta).
- **Endpoint + parseo del token.** El código viejo usaba `/Services/Autorizacion/GetToken` con parseo `data.Data.Token`. Lo correcto (y validado): `POST /Provisioning/GetAuthToken` con `{ UserName, Password, UserSecret }` (UserSecret = UserIdentifier), respuesta = **string URL-encoded suelto** → `decodeURIComponent(await r.json())`. Se reescribió `getTFToken` para probar la variante documentada primero y caer a la vieja, con parseo tolerante. `/health` pasó a `tango:true` (token de 228 chars, arranca `OWJ6TXFu…` igual que el ejemplo de la doc).

**Causa raíz de la lentitud y los errores de `ListarMovimientos` (el bug que costó medio día).** El código mandaba a `ListarMovimientos` los parámetros **`FechaComprobante` / `FechaServicioHasta`** (que son de OTRO endpoint). Los reales de `ListarMovimientos` son **`Desde` / `Hasta` / `Tope`** (confirmado en la doc oficial `tangofactura.com/Help/DocApi?resName=Factura`). Con los nombres equivocados, Tango **ignoraba el filtro de fechas y corría una consulta sin límite** → tardaba y devolvía "An error occurred while executing the command definition" de forma intermitente (dependía de si su base aguantaba la consulta gigante). El falso patrón "5 días sí, 7 no" se desarmó al ver que el mismo rango de 5 días a veces andaba y a veces no. **Fix: usar `Desde`/`Hasta`/`Tope`** → la consulta pasó a ser rápida y estable (30 días = 2.116 comprobantes en ~30s, sin caídas).

**Endpoints de Tango — qué sirve y qué no.**
- `ListarMovimientos` (`Desde/Hasta/Tope`): el listado. Lento (~30s) pero estable con los params correctos. Corre en segundo plano, con reintento + backoff, sin ráfagas (Tango throttlea).
- `ObtenerInfoMovimiento` (`MovimientoId`, `ObtenerInfoAplicaciones:true`): el detalle. **Rápido (~0,5s) y confiable.** Trae `Letra`, `Numero`, `Total` y `DatosAplicacionExterna.ExternalID` (= `ml_order_id`). El vínculo con ML SOLO viene con `ObtenerInfoAplicaciones:true`.
- `ObtenerInfoMovimientosPorNroFactura`: **inservible** (>55s, se cuelga). Descartado. Era el que usaba el `/tango/sync` viejo → por eso moría siempre.

**Sync reescrito (`syncTangoFacturas`).** `ListarMovimientos(Desde,Hasta,Tope)` → por cada movimiento `ObtenerInfoMovimiento(...ObtenerInfoAplicaciones:true)` → si `AplicacionNombre=='Mercado Libre'` y hay `ExternalID`, `UPDATE ventas_ml SET nro_factura, tipo_factura(=Letra), total_facturado_tango(=Total), tango_movimiento_id WHERE ml_order_id = ExternalID`. Corre en **segundo plano** (`/tango/sync?desde&hasta` → `{status:'started'}` → volver a pegarle da `progress`/`done`). `/tango/facturas` arreglado (leía la tabla inexistente `facturas_tango` → 500; ahora lee `ventas_ml`). Cron cada 3 h (ventana 3 días, guarda anti-solape). Endpoints de debug (`/debug/tango`, `/debug/tango-raw`) usados para validar y **retirados**.

**DB (Supabase MCP, `dlazhftkwrcordsdbeah`).**
- La tabla **`facturas_tango` NO existe** (estaba en docs viejos, nunca se recreó en el rediseño). El dato fiscal de ML vive en `ventas_ml`.
- Migración `ventas_ml_datos_factura_tango`: columnas `nro_factura`, `tipo_factura`, `tango_movimiento_id`.
- Migración `ventas_ml_total_facturado_tango`: columna `total_facturado_tango` (total real de Tango). **Decisión:** al principio se reusó `importe_facturado`, pero **el sync de ML ya la escribe** (= `importe_bruto − aporte_ml` = neto del comprador) → los dos syncs se pisaban. Se separó: Tango va a `total_facturado_tango`, `importe_facturado` queda para ML. Se restauró `importe_facturado = importe_bruto − aporte_ml` por SQL. Verificado: **ningún P&L/vista/función usa `importe_facturado`** al momento del cambio.

**Frontend (`ventas-ml.js`).** Primera versión metió el dato como renglones chiquitos bajo # Venta y Bruto → ilegible. Rediseño (a pedido de Sebastián, "todo en el mismo renglón"): **dos columnas nuevas** al lado del Bruto — **Factura** (`tipo` + `nro_factura`, filtrable) y **Facturado** (`total_facturado_tango` con semáforo: **✓ verde** si = neto del comprador (`importe_facturado`), **⚠ ámbar con el descuadre** si no). Se tocaron encabezado, fila de filtros, `filaHTML`, `filaTotales` y `COLV` de forma coordinada (mismo nº de celdas). El detalle de la venta también muestra la factura.

**Hallazgo de negocio — el aporte de ML y los descuadres.** La diferencia entre `importe_bruto` y `total_facturado_tango` es, en la gran mayoría, **exactamente el aporte de ML** (`importe_bruto − importe_facturado = aporte_ml`) — el aporte no se factura, es esperado, NO es descuadre. El descuadre real es `total_facturado_tango ≠ importe_facturado` (neto del comprador): se detectaron ~pocos por período, en 3 patrones (Tango facturó menos sin aporte que lo explique / ML tiene aporte pero Tango facturó el bruto completo / aporte y bonificación no coinciden, resto redondo). Con `total_facturado_tango` en la base, el **aporte y el delta de IVA quedan calculables en SQL** (destraba `ADARA-ML-BONIFICACIONES.md` §10-§11).

**Operativa del deploy.** Cada cambio de código lo sube Sebastián a GitHub (root = `server.js`; `public/js/screens/` = `ventas-ml.js`), Railway auto-deploya. **Ojo:** un redeploy **reinicia el proceso** y corta el backfill en curso (el sync es en memoria) → se retoma solo (cron) o re-disparando. Los cambios de env var también disparan redeploy.

**Pendientes que quedan.**
- **`ml_token:false`** en `/health` (reautorizar el token de Mercado Libre) — afecta el sync de ventas ML, NO el de Tango. Tema aparte.
- **Backfill histórico** más allá de ~30 días (correr `/tango/sync` por ventanas; el cron cubre lo nuevo).
- **Revisar los descuadres reales** (patrones 1-3) — se ofreció export a Excel.
- **Comprobantes no-ML** (B2B/efectivo/Tienda Nube): crear venta + FIFO (etapa 3, futura).
- **Regenerar credenciales de Tango** (se pegaron en el chat) y recargar las `TF_*` nuevas.

**Documentación.** Reescrita `ADARA-TFACTURA.md` (verdades duras: `TF_*`, `Desde/Hasta/Tope`, endpoints, esquema de columnas, `facturas_tango` inexistente, flujo de sync, frontend); actualizada `ADARA-ML-BONIFICACIONES.md` (§12: medición del aporte/IVA ahora en SQL + patrones de descuadre); corregido `ADARA-DOCS-INDEX.md` (bloque de variables `TANGO_*`→`TF_*`, doc #17 marcado implementado, estado); y este `ADARA-CHANGELOG.md`. **Pendiente de nota:** `ADARA-SCHEMA.md` (columnas nuevas de `ventas_ml`) y `ADARA-FRONTEND.md` (columnas Factura/Facturado en la pantalla).

**Seguridad (recordatorio vigente).** Nunca guardar credenciales vivas en los `.md` del proyecto; entregar archivos completos, nunca diffs/snippets.

---

## 3 Agosto 2026 — Apertura fiscal de IVA (junio como total DDJJ + arrastre) · 20 SKUs faltantes + re-proyección julio · costo de referencia desde el simulador · botón "+ Nuevo SKU" en Compras · tratamiento costos importación / crédito ML

Sesión larga de puesta a punto fiscal y de catálogo, arrancando la carga real de IVA débito/crédito (junio como monto total de la DDJJ, julio en adelante factura por factura).

**Apertura fiscal de IVA — junio congelado + arrastre de dos saldos.**
- **Tabla nueva `posicion_fiscal_apertura`** (migración `create_posicion_fiscal_apertura`): `periodo text PK, iva_debito, iva_credito, saldo_favor_previo, saldo_favor (técnico, arrastra), iva_a_pagar, libre_disponibilidad (arrastra), fuente text, creado_en`. RLS on + política `authenticated` (A17). **Junio 2026 sembrado** desde la DDJJ F.2051 (tx 1182373081): débito 70.183.307,66 / crédito 50.415.389,17 / saldo_favor_previo 33.576.185,50 / **saldo_favor (técnico) 13.808.267,01** / iva_a_pagar 0 / **libre_disponibilidad 26.306,80**.
- **Vista `v_posicion_fiscal` recreada** (migración `v_posicion_fiscal_arrastre_apertura`): mantiene las **7 columnas originales en su orden** (restricción de `create or replace view`) y **agrega al final** `saldo_tecnico_favor`, `libre_disponibilidad`, `estado_fiscal` (`apertura`/`fino`/`incompleto`). Usa un **`WITH RECURSIVE run`** sembrado desde la apertura de junio; fórmula del mes: `a_pagar = máx(0, máx(0, débito − crédito − técnico_previo) − libre_previa − ret_mes)`, con `ret_mes = 0` como **placeholder** (pendiente FISC-RET-IVA). Arrastra **dos saldos independientes**: el **técnico a favor** (crédito>débito, solo compensa débito futuro) y la **libre disponibilidad** (retenciones/percepciones de IVA; aplica a a-pagar/otros impuestos/devolución). **`v_control_mensual` NO se tocó** (el panel de Resultado sigue igual). Fallos resueltos en el camino: `relation "run" does not exist` (faltaba `WITH RECURSIVE`) y `cannot change name of view column "iibb_retenido"` (las columnas nuevas debían ir al final).
- **Regla nueva I9** (apertura fiscal de IVA): en vez de reconstruir el histórico de compras, se congela el mes de corte (junio) con el total de la DDJJ del contador y se arrastra el saldo; de ahí en adelante todo fino. Análoga a A2.

**Frontend Posición Fiscal (`posicion-fiscal.js`).** Reescrita para mostrar las columnas/KPIs nuevos (saldo técnico a favor, libre disponibilidad) y el **chip de estado** apertura/fino/incompleto. Corrección de texto: "saldo a favor vigente" → **"saldo a favor de apertura (jun)"** + nota de que el arrastre es automático. Validada `node --check` (copia `.mjs`), entregada por SendUserFile (segunda versión corregida).

**Diagnóstico julio (neto vs por cobrar) → 20 SKUs faltantes + re-proyección.** Los números de julio venían bajos porque **272 órdenes pagadas no proyectaban**: 20 SKUs no estaban catalogados, así que `fn_proyectar_ml` no los tomaba. Se dieron de alta los **20 SKUs** en `skus` (IVA **preguntado, no asumido** — regla del usuario):
- **10,5%**: NT001/NT002/NT003 (notebooks), TA003G/TA004A/TA005G/TA006G/TA007G/TA008G/TA010G/TA011G (tablets), 177V, **TA009V (Kindle)**, IM005 (impresora **de función única**).
- **21%**: AU007N/AU007B (auriculares), BT001/BT002 (baterías).
- Regla del usuario: auriculares 21%, todo lo demás del lote 10,5%; impresora 10,5% **porque no es multifunción** (las multifunción impresora+scáner van a 21%); Kindle 10,5%.
- `select fn_proyectar_ml('2026-07-01','2026-07-31')` devolvió **272**; **neto de julio pasó de ~$214M a ~$384M**.

**Costo de referencia cargado desde el simulador de importaciones (18 de 20).** `skus.costo_referencia` (neto s/IVA) cargado para 18 SKUs desde la pantalla "Costo final por producto" del simulador: **177V = 281.912,89** (Paraguay/en negro), TA005G 465.172,24, SW001N/SW001S 115.272,41 c/u, TA004A 193.476,17, TA006G 372.512,78, TA007G 280.126,90, TA010G 474.038,09, TA011G 470.528,87, TA009V 166.123,82, AU007N/AU007B 95.000, IM005 192.921,83, NT001 786.112,67, NT002 686.673,81, NT003 926.692,82, TA003G 463.268,19, TA008G 327.667,00. **Pendiente: BT001/BT002 sin costo.**

**Botón "+ Nuevo SKU" en Compras (`compras.js`).** Para dar de alta un SKU sin salir de la carga de la factura: import ampliado a `sbGet, sbPost`, constante `FAMILIAS`, botón `id="c-new-sku"` en el header del bloque Productos, y `openNuevoSku()` (mini-modal que hace `sbPost('skus', {codigo, descripcion, familia, alicuota_iva, activo:true})`, empuja a `SKUS`/`SKU_BY_ID` y auto-selecciona el SKU en la primera fila de ítems vacía). Validado `node --check`, entregado.

**Clasificación de la primera compra (LEANVAL) + verificación de costo.** Se cargó la primera factura de compra de productos (LEANVAL, compra #6): SKU 302, lote a **$247.106,61**, con el IVA crédito fluyendo a `v_control_mensual` ($10.378.477,69). Verificado que el costo del lote pisa al estimativo previo (FIFO real > `costo_referencia`).

**FIFO por lote (aclaración de negocio).** Se confirmó el modelo: **cada lote conserva su propio precio** (no promedio manual), el consumo es FIFO y de ahí salen las rentabilidades; `v_costo_sku_actual = COALESCE(promedio ponderado de lotes con costo>0, skus.costo_referencia)`.

**Clasificación del Excel de AFIP ("Mis Comprobantes Recibidos") de julio.** Se armó una lista de trabajo (deliverables xlsx v1/v2). Categorías de v2: **1·ML/marketplace** $18,03M crédito · **2·Productos→Compras** $15,72M (LEANVAL/JUKEBOX/STYLUS) · **3·Importación (costos)→Simulador/Fase 2** $15,90M · **4·Servicios→Gastos** $0,22M · **5·Factura C sin crédito** $2,84M. **Total crédito julio $49.869.679,98 > débito $45,45M** → julio cierra con saldo a favor una vez cargado todo.
- **Regla nueva C14 (costos de importación no se cargan sueltos).** La mayoría de las facturas de "mercadería" son en realidad **costos de importación** (fletes internacionales, depósitos de aduana, logística interna, comisiones del despachante) que **ya están en las simulaciones de importación** — NO se cargan sueltas como compra/gasto para no duplicar. Solo LEANVAL, JUKEBOX y STYLUS son compras de producto reales.
- **Crédito de ML / MELI LOG (FISC-CRED-ML, pendiente).** De las facturas de Mercado Libre se debe capturar **solo el IVA crédito de la comisión**, sin volver a cargar el costo (ya está en los cargos de la venta) para no duplicarlo.

**Documentación.** Sesión dedicada además a `actualiza todos los md`: se actualizaron `ADARA-IMPUESTOS.md` (apertura + arrastre de los dos saldos), `ADARA-DOCS-INDEX.md` (header + anotaciones), `ADARA-DECISIONES.md` (I9 apertura, C14 costos de importación, pendientes FISC-RET-IVA y FISC-CRED-ML, bloque "Actualización 3 Agosto 2026"), `ADARA-SCHEMA.md` (tabla `posicion_fiscal_apertura` + vista `v_posicion_fiscal` con doble arrastre + sección fiscal, restaurando el detalle por capa tras un resumen inicial que se corrigió), `ADARA-FRONTEND.md` (botón +Nuevo SKU + Posición Fiscal apertura/arrastre/chips) y este `ADARA-CHANGELOG.md`.

**Pendientes abiertos (negocio/fiscal):** FISC-RET-IVA (capturar retenciones/percepciones de IVA del mes → hoy `ret_mes=0`); FISC-CRED-ML (crédito de comisión ML sin doble conteo); **Fase 2 del simulador de importaciones** (materializar los costos de importación); cargar costo de **BT001/BT002**; seguir cargando **JUKEBOX/STYLUS** + servicios vía Gastos; **re-chequear AFIP** por más facturas de julio ahora que cerró el mes.

**Seguridad (recordatorio vigente):** nunca guardar credenciales vivas en los `.md` del proyecto (no son privados); entregar archivos completos, nunca diffs/snippets.

---

## 22 Julio 2026 — Simulador Importaciones: fix flete fuera del ahorro/coima (caso HOKU)

**Bug de modelado en `calc()` de `importaciones-sim.js` (entregado a GitHub por Sebastián).**

Cargando el despacho consolidado **HOKU (id 8)** —sin subdeclaración de producto (todos los overrides en null)— el simulador pedía pagar coima. Diagnóstico: el motor prorrateaba el **CIF real sombra** con el **flete real** (8.388) mientras el CIF declarado usaba el **flete declarado** (3.800). Como el real era más alto (incluye gastos en origen, no es subfacturación), el sombra daba derechos+estadística "reales" mayores → **ahorro fantasma USD 843** → coima 50% USD 421. Falso positivo estructural.

- **Fix (quirúrgico, CIF sombra líneas ~93-100):** el sombra pasa a prorratear **flete y seguro DECLARADOS** (los mismos del CIF declarado). Difiere del declarado **solo** por FOB y cantidad reales → el ahorro nace exclusivamente de los tres ejes de subdeclaración del **producto** (FOB, cantidad, posición arancelaria). El flete queda fuera del ahorro. La variable `seguroReal` se eliminó; `fleteReal` sobrevive solo para `fleteDiff` (→ bolsón, intacto).
- **Costo por producto: invariante.** La diferencia de flete sigue capitalizando vía bolsón exactamente igual. Solo cambia el ahorro/coima.
- **HOKU validado:** ahorro y coima → 0. `node --check` OK, sin duplicados.
- **Xiaomi (id 6):** tiene subdeclaración real de producto (cantidad/FOB) → su coima es genuina y > 0. El número validado el 7/7 (ahorro USD 6.869,05 / coima USD 2.060,71) incluía el efecto flete y quedó obsoleto; pendiente re-validar contra planilla.

**Docs actualizados:** `ADARA-DECISIONES.md` (P15 corregida + header), `ADARA-IMPORTACIONES-SIM.md` (fórmula CIF sombra + sección coima + caso HOKU + header), `ADARA-COMPRAS-IMPORTACIONES.md` (hallazgo "DOS fletes" matizado), este `ADARA-CHANGELOG.md`, `ADARA-DOCS-INDEX.md` (header).

---

## 13 Julio 2026 — Conciliación de ventas ML: tablero mensual manual bidireccional + limpieza de docs

**Rediseño de la conciliación de Mercado Libre (`public/js/screens/ventas-ml.js`, ~1.822 líneas, entregado a GitHub por Sebastián).**

Se reemplazó el flujo anterior (botón "Conciliar" por venta + "Conciliar todas" en lote) por un **tablero mensual de dos tablas, manual y bidireccional**, sin vinculación automática. Requisito de Sebastián: ver las dos puntas y conciliar a mano venta por venta, con la IA solo sugiriendo la contraparte.

- **Estado nuevo del módulo:** `VISTA='control'|'conciliar'`, `CONC_SEL` (selección activa para sugerir contraparte), `CONC_HIDE_DONE` (declutter), `MOV_MP` (todos los movimientos MP del mes). Import pasa a `import { sbGet, sbPatch } from '../core/sb.js';`. En `loadVentasML()` se resetea el estado y se cargan los movimientos: `MOV_MP = await sbGet('movimientos', 'origen=eq.mp_account_statement&order=fecha.asc,id.asc');`. `render()` bifurca: `if (VISTA==='conciliar') return renderConciliar();`.
- **`renderConciliar()` — tablero mensual:** filtro por **mes** (`efPeriodo`); **KPIs con anillos de progreso** (donut SVG `ring(pct,color)`, paleta ADARA verde `#0F6E56`/ámbar `#D97706`, spec skill dataviz) — ventas conciliadas % + movimientos vinculados %.
  - **Tabla izquierda (Ventas del mes):** `#Venta`, **Entrega** (nueva), **SKU** (nueva), Producto, Objetivo, Estado, Acción. Incluye canceladas y devoluciones **excepto** las canceladas sin envío (no mueven stock). Botones (`ventaRow`): Conciliar / Conciliar mov. / Deshacer / **Diferir→** / **↩**.
  - **Tabla derecha (Extracto MP completo):** `Fecha`, `N°op`, `Descripción`, `Monto`, `Estado`. Trae **TODOS** los movimientos MP del mes (no solo cobros: liquidaciones, retenciones, envíos), porque una venta puede tener más de un movimiento.
  - **Selección bidireccional:** clickear una venta resalta el/los movimiento(s) sugeridos (`selMovs`) y viceversa (`data-venta`/`data-traer`). El motor de matching **no** se reescribió: reusa `matchCobros`, `objetivoDe`, `retEnvioDe`, `shipDe`, `conciliable`, `asignarBonifs`, `buscarBonif`, `conciliar`, `conciliarMovimientos`, `desvincular`, `estaConciliada`.
  - **Declutter:** `pendV`/`doneV` + `movSug`/`movPend`/`movDone`; con `CONC_HIDE_DONE` lo conciliado se oculta.
- **Cross-mes (diferir/traer):** helper `nextPeriodo(ym)`; `async function diferirVenta(ventaId, periodo, select=false)` → `sbPatch('ventas_ml', ..., { conciliacion_periodo })`. **Diferir→** manda la venta al mes siguiente (desaparece del mes actual → permite cerrar el mes al 100%); **traer ↰** trae una venta de mes previo al actual desde su movimiento; **↩** deshace. La tabla izquierda muestra: ventas del mes con `conciliacion_periodo` nulo/igual + traídas − diferidas.
- **Alcance:** es **solo el eje plata** (dinero MP ↔ venta). **No toca stock, CMV, P&L ni el mes fiscal** — eso se reconoce por **fecha de venta** (S9). Regla nueva **O13**.
- **Removido:** botón "Conciliar todas" + funciones `conciliarTodas`/`armarLoteConciliacion`.

**DB (Supabase MCP, proyecto ADARA APP - CLAUDE `dlazhftkwrcordsdbeah`).**
- Columna nueva **`ventas_ml.conciliacion_periodo` (text, nullable)** — mes al que se difiere una venta. Migración `migracion_conciliacion_periodo` aplicada. Política vigente `ALL:authenticated` → el `sbPatch` del front funciona.

**Diagnóstico de datos (no bug):** en la prueba de julio todas las ventas figuraban "sin cobro" — es un tema de **datos** (los cobros cargados eran de abril; julio aún no liquidado en MP), no del código. Abril funciona (854 cierran).

**Limpieza de documentación + protocolo de mantenimiento (misma fecha).** Se resolvieron los 4 documentos **duplicados** del proyecto (`ADARA-DOCS-INDEX.md`, `ADARA-DECISIONES.md`, `ADARA-FLEX.md`, `ADARA-VENTAS-ML.md` — se conservó la versión vigente de cada uno, se eliminó la copia vieja "fantasma"); se **renumeró** la tabla del rediseño (tenía un `#17` duplicado); se agregó `ADARA-VENTAS-ML-V22.md` a la lista; se sincronizó el conteo (**27 rediseño + 6 v21 + 1 proceso = 34 .md**); se documentó que **A6 quedó revertida por A17** (login+RLS); y se creó **`ADARA-MANTENIMIENTO-DOCS.md`** con el protocolo de mantenimiento entre chats (onboarding + handoff + regla anti-duplicados). Sin pérdida de contenido técnico.

**Seguridad (registrado).** Se evaluó automatizar el push a GitHub. Sebastián propuso guardar un token de GitHub en un `.md`; se **rechazó**: los `.md` del proyecto **no son privados** (se comparten con los 4 usuarios del equipo, persisten entre sesiones y alimentan las respuestas/RAG) y un token con permiso de escritura = llave para pushear a producción vía Railway. **Regla: nunca guardar credenciales vivas en docs del proyecto.** El path de escritura sigue siendo manual (Sebastián sube los archivos) o, a futuro, Claude Code GitHub Actions con API key de pago. Por ahora se sigue a mano.

**Docs actualizados:** `ADARA-VENTAS-ML-V22.md` (sección conciliación bidireccional), `ADARA-SCHEMA.md` (columna `conciliacion_periodo`), `ADARA-DECISIONES.md` (O13 + S9 aclarada + "Conciliar todas" a Decisiones revertidas), `ADARA-FRONTEND.md` (sección 13/7 + fila Ventas ML/Flex en pantallas), `ADARA-DOCS-INDEX.md` (header + doc #14), este `ADARA-CHANGELOG.md`. Doc nuevo de proceso: `ADARA-MANTENIMIENTO-DOCS.md`.

---

## 7 Julio 2026 — Simulador Importaciones (SIM + cantidad declarada + coima) · hallazgo bonificaciones ML · auditoría margen junio

**Simulador (`importaciones-sim.js` ~1064 líneas, desplegado):**
- **Arancel SIM** (param `arancel_sim_usd`, default 10): capitaliza vía bolsón + suma a tributos del despacho, sin base IVA ni crédito (P14).
- **Cantidad declarada** por producto (`cantidad_decl`): separa cantidad real (costo/stock/divisor del unitario) de la declarada (aduana). Soporta `cant_decl > real`. El divisor del costo unitario pasa a ser la cantidad real (P16).
- **Coima sobre ahorro total**: motor en dos pasadas — CIF declarado + **CIF real sombra** para medir el ahorro de derechos+estadística declarando todo real (FOB+cantidad+alícuota); coima global = `coima_pct` × ahorro, va al bolsón y se reparte; **tablota por producto** reemplaza la tarjeta resumen (P15).
- Reparto de fijos **a mano en modo $** confirmado funcional (cargar más a un producto y menos a otro).
- Validado con despacho Xiaomi (id 6): ahorro total USD 6.869,05; coima 30% USD 2.060,71. `node --check` OK, sin duplicados.

**Hallazgos:**
- **Bonificaciones ML (ML-BON1):** en promos compartidas ("X% Mercado Libre") el aporte de ML no se captura en `importe_bruto` (= `total_amount`, ya descontado; el coupon se saltea en server.js 753-755). Evidencia SKU 88B: cluster $42.999 (precio comprador) vs seller-effective $46.198. Subestima ingreso/débito/margen. **Gap PROBABLE, no confirmado** — confirmar con 1 `ml_order_id` o por conciliación. Doc nuevo dedicado `ADARA-ML-BONIFICACIONES.md`.
- **Auditoría margen junio:** 6,5% (vs 7,4% mayo, 8,1% abril) es **mix-driven**, NO error de costeo. Junio vendió menos unidades al doble de ticket ($192.824/u vs $95.025); CMV ~66,8% genuino (tablet SKU 178 = 36% del neto a 63,6%; TVs Enova ~70%). Buckets consistentes: FIFO real 67,6% / estimado 66,2%. El cartel "crédito incompleto" del panel IVA es sobre el IVA a pagar, no sobre el CMV.

**Data-fixes pendientes:**
- Kindle `TA002V` sin costo (CMV 0, 10 ventas junio) — **BLOQUEADO** por alícuota (`skus` = 21% vs Sebastián dice 10,5%; resolver antes de cargar, mueve débito fiscal). Costo dado: $162.803,51 (confirmar neto/bruto).
- Disco `DS001N` CMV 95,6% por **lote del seed sobrecargado** → costo real $106.814,09 neto → CMV ~69%, libera ~$7,4M de margen en junio. Corregir `lotes.costo_unitario` (excepción seed CF6, patrón Targus) **+ re-snapshot** de las ventas de junio.

**Pendientes abiertos:** IVA al día (diseño listo — apertura fiscal corte 31/05; falta saldo a favor de la DDJJ de mayo); conciliación may/jun (no corrida; confirma el gap de bonificaciones); confirmación + fix del gap ML-BON1.

**Docs actualizados:** `ADARA-IMPORTACIONES-SIM.md` (SIM/cantidad/coima), `ADARA-DECISIONES.md` (P14/P15/P16/IMP-SEGURO/ML-BON1), `ADARA-ML-BONIFICACIONES.md` (nuevo), `ADARA-DOCS-INDEX.md` (índice). Pendientes de nota: `ADARA-COSTEO-FIFO.md` (Kindle/disco), `ADARA-IMPUESTOS.md` (alícuota Kindle), `ADARA-PSI.md` (auditoría junio), `ADARA-VENTAS-ML.md` (link a bonificaciones).

---

## 21 Junio 2026 (parte 4) — Flex: carga de la grilla partido→zona + decisión de resolver el partido por Georef (F7)

**Contexto.** Se retomó Flex para cargar el mapeo partido→zona (foto de zonas de MEF aportada por Sebastián). Estado previo: 4 zonas + 4 precios cargados, solo 2 partidos mapeados (CABA, La Matanza Norte); 3.478 envíos sin zona.

**Carga (aplicada).** 28 partidos nuevos en `flex_partido_zona` (los 25 de la grilla MEF + variantes de alto volumen: Quilmes Oeste, Jose Clemente Paz, Lanús Oeste) → 30 en total. Zonas por precio: CABA 3450 / GBA1 4750 (amarillo en la foto) / GBA2 5350; GBA3 7050 cargado pero no activo. `ON CONFLICT (partido) DO NOTHING`. Cobertura: **3.478 → 2.055** envíos sin zona (~1.423 mapeados, 41%).

**Falso problema descartado.** La pantalla "no cambiaba" → era la semana por defecto (la más reciente, casi vacía) + caché; verificado que `v_flex_envios` (incluso como rol `authenticated`) devuelve la zona en vivo. No es bug de datos ni de RLS.

**Decisión de fondo (F7) — el partido sale del dato, no se infiere.** El asignador mostraba **localidades** (Castelar, Temperley…) porque ML manda en `city.name` a veces la localidad y no el partido. Mapear localidad→partido a mano es suponer y se rompe (variantes: "Lanús"/"lanus oeste"/"Lanuseste"). Verificado con `/debug-order` (orden de Castelar): ML manda `municipality: null`, pero **sí** `zip_code: "1712"` y `lat/long`. Y ML **no publica** una grilla de zonas (su tarifa Flex es por distancia; la grilla CABA/GBA1/GBA2/GBA3 es comercial de MEF, la transportadora). Decisión: resolver el partido con **Georef** (gob, `apis.datos.gob.ar/georef/api/ubicacion?lat&lon` → departamento = partido oficial en PBA, fuente ARBA), geométrico y determinístico. Pipeline: `ML lat/long → Georef partido → flex_partido_zona zona`.

**Pendiente Flex (no urgente, Flex no está activo):** backfill por nombre con Georef de las 287 localidades sin zona; capturar `lat/long`+`cp` en `ventas_ml` y resolver el partido en el sync (server.js usa hoy `city.name`, ~líneas 660–682); precio de `caba_tardia`; afinar regla CABA tardía; definir resumen del 08/06; Flex Fase 3 (gasto semanal).

**Docs:** `ADARA-FLEX.md` reescrito a v22 (la referencia v21 queda al final); `ADARA-DECISIONES.md` F7 nueva + F3 ajustada.

---

## 21 Junio 2026 (parte 3) — Canceladas/devoluciones en el Resultado: fix del ingreso fantasma + línea de devoluciones (P13)

**Hallazgo (diagnóstico con datos reales).** `v_resultado_mensual` filtra `estado in ('aprobada','entregada')`, pero las **14.833** ventas ML están todas en `aprobada` → el filtro era **no-op**. Como `fn_proyectar_ml` es **insert-only**, una orden que pasa a `cancelled` después de proyectada quedaba `aprobada` con ingreso + CMV intactos. Se colaron al resultado **21 canceladas** (16 entregado $3.202.322,79 + 3 no_preparado $536.590,32 + 2 despachado $375.090,75 ≈ **$4,11M de ingreso fantasma**). Las **3 `no_preparado` habían consumido CMV ($384.062,20)** de mercadería que nunca salió → violaba S4 (stock subvaluado). Además, 7 `partially_refunded` cuentan el ingreso entero sin netear el reembolso.

**Criterio (Sebastián).** Cancelación (`cancelled`) = **no es venta** → sale del resultado; si volvió al stock o nunca salió, no inventar ventas. Devolución (reembolso de venta real) = la venta queda en su mes, el reembolso es **saldo negativo en el mes en que se concilia el dinero** (ej. venta junio, reembolso julio → negativo en julio). Regla P3.

**Fix DB (aplicado y verificado).**
- `fn_reconciliar_ml_canceladas()` (regla nueva **P13**): pasa las `cancelled` proyectadas a `estado='cancelada'` (salen del resultado con su CMV) y revierte el CMV de las que nunca salieron (`no_preparado`/`preparado`, reusa `fn_revertir_devolucion`). `despachado`/`entregado` no revierten CMV (depende de recepción, GAP). Idempotente. **Backfill:** 21 flipeadas, 3 reversos (3u al stock), 0 canceladas quedan en el resultado.
- `v_resultado_mensual`: columna **`devoluciones`** (append) desde `v_cc_devoluciones` por `mes_imputacion` (mes del reembolso), atribución por venta proyectada, signo real, **guard anti doble conteo** (no netea ventas ya excluidas por cancelación). Hoy solo abril tiene conciliado (1 caso atribuible +$17.130,57; 8 huérfanas −$712.201,34 quedan fuera hasta resolver O10).

**Fix código.**
- `server.js`: hook post-sync ahora `proyectar → reconciliar → consumir → congelar` (reconciliar antes de consumir para que el FIFO no re-consuma; full-scan, idempotente).
- `public/js/screens/resultado.js`: columna **Devoluciones** + renglón "± Devoluciones del mes" en el panel; cascada `contribución − CMV − gastos + devoluciones`.

**GAP que queda (diferido).** Las 16 `entregado` + 2 `despachado` canceladas tienen el resultado correcto pero el **stock pendiente** de registrar recepción. El flujo existe (solo en solapa Devueltas); falta rutear las canceladas. Es el pendiente prioritario que sigue.

**Archivos:** `server.js`, `public/js/screens/resultado.js`. Migraciones: `fn_reconciliar_ml_canceladas` (+ fix ambigüedad), `v_resultado_mensual_add_devoluciones` (+ fix atribución por venta + guard canceladas). Docs: `ADARA-DECISIONES.md` (P13 + P3), `ADARA-PNL.md`, `ADARA-CANCELACIONES-DEVOLUCIONES.md`, `ADARA-COSTEO-FIFO.md`, `ADARA-FLUJO-OPERATIVO.md` (#4 transitorio).

**Cierre — reporte XLSX semanal (pain point #4).** Diagnosticado: es un reporte externo/legacy que clasifica canceladas por presencia de `fecha_entrega` (proxy de "¿se entregó?") y confunde "no entregada" con "nunca salió" → las `despachado` canceladas (salieron, sin `fecha_entrega`) desaparecen de las hojas por SKU. Sebastián lo usa para control **mientras la app no tiene toda la data real**; se **retira** después, no se reemplaza (la necesidad ya está en PSI + Cuadre/Resultado). Sacado de la lista de pendientes.

---

## 21 Junio 2026 — Adjuntos (Storage) + dominio Flex (zonas + pantalla) + reorg nav Mercado Libre

**Adjuntos / comprobantes (nuevo, reutilizable).** Bucket privado `comprobantes` + tabla polimórfica `adjuntos` (`op_tipo`+`op_id`) + 3 endpoints (`POST /adjuntos`, `GET /adjuntos/:id/url` link firmado 1h, `DELETE`). UI enganchada en **Gastos** y **Compras**: campo de archivo en el alta (se sube tras crear la operación, con el id devuelto) y 📎 en la lista. Diseño genérico → despachos/importaciones después solo necesitan front. Doc nuevo `ADARA-ADJUNTOS.md`.

**Flex — dominio nuevo (reemplaza el v21 borrado en el reset).** 4 tablas (`flex_logistica`, `flex_zona`, `flex_precio`, `flex_partido_zona`) + 3 vistas (`v_flex_envios`, `v_flex_semanas`, `v_flex_partidos_sin_zona`). Clasificación automática: logística (CABA 12–16h → CABA tardía; resto MEF), zona (mapeo partido→zona; CABA automático), precio (logística×zona, editable). Precios MEF validados contra el resumen real: CABA 3450 / GBA1 4750 / GBA2 5350 / GBA3 7050. Pantalla `#flex` (sub-pestaña de Mercado Libre): control semanal estilo resumen, cuadre contra el total del proveedor, asignador self-service por volumen, precios editables. Doc `ADARA-FLEX.md` reescrito a v22.

**Reconciliación Flex (hallazgo).** ADARA y el resumen MEF no cuadran al envío (ej. 01–06/06: ADARA 197 despachados vs MEF 156). Es esperado; la pantalla lo expone para investigar (corte de fechas, sábado→lunes, devoluciones, franja CABA tardía que sobre-captura). No se forzó el cuadre. Pendiente: definir cuál resumen del 08/06 vale ($495.500 vs $421.450).

**Nav — grupo "Mercado Libre".** "Ventas ML" + "Flex" unificados en un solo item con sub-pestañas (componente compartido `core/mlTabs.js`). `NAV_ALIAS` en `main.js` para el resaltado; cache-buster `ventas-ml.js?v=dev6`.

**Archivos:** `server.js` (endpoints adjuntos), `public/js/screens/{gastos,compras,flex,ventas-ml}.js`, `public/js/core/mlTabs.js` (nuevo), `public/js/main.js`, `public/index.html`. Migraciones: `create_adjuntos_y_bucket`, `create_flex_modelo_zonas`, `create_v_flex_envios`, `create_v_flex_semanas`, `create_v_flex_partidos_sin_zona`.

## 17 Junio 2026 — Reacondicionar: depósito REAC + flujo de recepción de 3 destinos; modelo de reconocimiento confirmado por fecha de venta

**Modelo de reconocimiento — explorado y resuelto**
- Se evaluó mover el consumo de stock/CMV al momento de la **entrega** (modelo "consumo por entrega"). Diagnóstico con datos reales: 14.313 entregadas pagadas, **735 (~5%) con mes-orden ≠ mes-entrega**; el shift de ingreso por mes era material (hasta ±15M). El CMV real solo existe desde junio (siembra 8/6), así que el reproceso hubiera sido chico.
- Se construyó y **probó en ROLLBACK** una función `fn_consumir_fifo_entrega` para validar el efecto.
- **DECISIÓN (Sebastián):** la rentabilidad se reconoce por **fecha de VENTA**, no de entrega — una venta del 31 a las 23:59 va a ese mes. La `fecha_entrega` la pone el correo (días después), no nosotros. El modelo por entrega mandaría ventas de fin de mes al mes siguiente → **descartado**. `fn_consumir_fifo_entrega` **borrada**. Queda como estaba (consumo a fecha de venta). Ver `ADARA-DECISIONES.md` S9.

**Depósito REAC (reacondicionar) — construido y validado**
- Modelo cerrado: una devolución/cancelación que vuelve tiene 3 destinos → **sano** (vuelve a vendible), **reacondicionar** (a depósito `REAC`, recuperable), **no volvió** (pérdida).
- DB (Supabase): `fn_devolucion_a_reacondicionar(ml_order_id, fecha)` — **Caso A** (consumió FIFO): revierte el CMV y mueve la unidad del lote original a un lote `deposito='REAC'` (hereda `compra_id`/costo); **Caso B** (cancelada despachada sin consumo): mueve la unidad de stock vendible (FIFO) a `REAC`. `fn_reacondicionado_a_venta(lote_id, unidades, deposito, fecha)` — botón "Pasar a venta" (default `DEP`). Transferencias con ajustes `compensacion` auditables.
- **Probadas en ROLLBACK contra datos reales:** Caso A (order 2000016926104580): CMV→0, 2u a REAC, lote original sin recuperar la unidad vendible; round-trip REAC→venta deja REAC en 0. Caso B (order 2000015608559562): vendible 2→1, REAC 0→1. Nada persistido.
- Backend (`server.js`): `/ml/recepcion` acepta `condicion='reacondicionar'`; nuevo `POST /reacondicionar/a-venta`.
- Frontend (`ventas-ml.js`): botón **🔧 Reacondicionar** en el modal de canceladas (entre OK stock y No disp.), prompt de nota opcional, badge "🔧 reacondicionar" en la fila.
- **Regla:** el reacondicionado conserva su **costo original** (sin desvalorización automática); el depósito `REAC` se **excluye** de disponible/PSI.

**Pendiente del workstream**
- Inventario: mostrar el depósito `REAC` aparte con su botón "↪ Pasar a venta".
- PSI/disponible: excluir `REAC` del stock vendible (regla definida; falta el ajuste en la pantalla).

---

## 16 Junio 2026 — Bonificaciones abril, backfill shipment_id, filtros/totales en pantallas, bug de paginación, apertura fiscal, cancelaciones

**Bonificaciones de envío Flex (abril) — cerrado**
- Backfill de `ventas_ml.shipment_id` vía `POST /ml/sync {desde,hasta}` por mes: **ene/feb/mar** (antes en 0). Ene/feb quedaron 100%, mar 3.707/3.998.
- Se recuperaron **158 de las 167** bonificaciones pendientes de abril (`POST /mp/conciliar-bonificaciones write:true`). `cobro_venta` sin conciliar: 500 → 342.
- **9 residuales** ($58.059,54): bonis de envíos de **órdenes 2025** (fuera de la ventana sincronizada). Payment ids: 153734718000, 153739465000, 153730307680, 153736240120, 153737614790, 153745654980, 153747429342, 153751904726, 152985287243. No se recuperan re-sincronizando 2026.
- **Guardarraíl validado**: re-sync de un mes incompleto es idempotente en costeo (FIFO no consume si no hay lote ≤ fecha; CMV congelado `ON CONFLICT DO NOTHING`) pero **aditivo en ventas** — mar incorporó **+189 ventas reales** que faltaban (corrección, no corrupción). Ene/feb intactos (3.754 proyectadas=congeladas, FIFO 0). Snapshot de agregados antes/después como control.

**Operatoria**: el endpoint `/mp/conciliar-bonificaciones` se dispara desde la **consola del navegador** (`fetch('/mp/conciliar-bonificaciones', {method:'POST', body: JSON.stringify({desde,hasta,write})})`), dry primero, luego write.

**Pantalla Conciliación** (`conciliacion.js`)
- **Filtros por columna estilo Excel** (selects con valores presentes + inputs de texto), repintado parcial del `tbody` para no perder foco, contador "Mostrando X de Y" + Limpiar filtros.
- **Selector de período (mes)** + toggle **Movimientos / Ventas / Ambas**. La tabla de ventas lee `ventas_ml` del mes con semáforo propio (Conciliada/Parcial/Sin cobro/Cancelada) derivado de `vinculos` (`ventas_ml.estado_conciliacion/conciliado/balance_conciliacion` están **sin poblar**, no usar).
- **Decisión de arquitectura**: el cruce venta↔cobro lo hace mejor **Ventas ML**; Conciliación es para movimientos que NO son venta (gastos/compras/transferencias/impuestos); Cuadre será tablero multi-mes. La tabla de ventas en Conciliación quedó como duplicado a revisar más adelante (anotado, "quizás algún dato sirva").

**Pantalla Ventas ML** (`ventas-ml.js`)
- **Filtros por columna** + **fila de totales sticky** (suma Cant/Bruto/Comisión/Envío/Impuestos/Financiero/Por cobrar sobre lo filtrado). Se agregó y luego se **reemplazó** la columna F. cobro (vacía en canceladas) por **Estado envío** (chip no_preparado/despachado/entregado, con filtro). **Nro de operación** (`mp_source_id`) agregado en cada línea de retención del modal de detalle.
- **BUG RESUELTO (importante): paginación no determinística.** `sbGet` pagina por Range de a 1.000; varias queries ordenaban por columnas con empates (`fecha`), y en el borde de página Postgres reordenaba → **filas salteadas al azar**. Síntoma: un cobro que existía y cerraba exacto aparecía como "sin cobro" (Cobradas=0). Fix: desempate único `id` en todas las cargas >1.000 (`ventas_ml`, cobros, vínculos, retenciones, devoluciones). **Regla nueva: toda query paginada debe ordenar por una columna única.** Pendiente: auditar todas las `sbGet` del proyecto.

**Backend** (`server.js`)
- `GET /venta/:id/detalle`: antes solo mostraba movimientos **ya vinculados**; ahora también trae los matcheados por **`mp_payment_id`** (cobro + devoluciones/cancelación), unidos y deduplicados. Así el modal muestra el cuadro completo aunque la venta no esté conciliada.

**Diagnóstico abril (ventas sin conciliar)**
- 1.416 ventas / $102M sin vínculo venta↔cobro. **No falta plata**: los cobros están en el AS (93% conciliados del lado movimiento). De las pendientes, **866 tienen `fecha_cobro` en mayo** (esperan el AS de mayo) y **1 sola** era conciliable ya (cobro presente, sin clic). **`periodo_cobro` resultó NO confiable** (queda desfasado por la regla de exclusión del upsert); **el campo confiable es `fecha_cobro`**.
- **Canceladas (166 abril)**: `fecha_cobro` NULL en todas (esperado). Pero **147 movieron plata** (cobro) y **139 se devolvieron**; ~8 cobradas con **reintegro pendiente** (vigilar contra AS de mayo), 19 sin cobro.
- **Hallazgo (regla nueva): una venta cancelada NO es neutra.** ML **no reintegra el envío** si ya se despachó (ej. orden 2000015789022460: ML muestra Total operación $8.000 por el envío no anulado). Pero ese costo **NO está en el AS de MP** (los 3 movimientos del AS netean $0) — vive en la **facturación de ML**, fuente aún no integrada. Pendiente: cargar facturación de ML para capturar el costo de envío de canceladas y cargos ML.
- **Hallazgo (clasificación): `cancelled` ≠ producto no salió.** Por `estado_envio`, las 166 canceladas de abril: **106 no_preparado** (no salió), **12 despachado**, **48 entregado** → **60 despacharon el producto**. ML mezcla cancelaciones reales con devoluciones de entregadas. Criterio ADARA: clasificar por `estado_envio` (¿salió?) + `recepcion_*` (¿volvió?), no por la etiqueta de ML. Riesgo: una cancelada despachada/entregada que no vuelve = **stock sobrestimado**.
- **Flujo de recepción / stock (ya existe, gap identificado)**: `POST /ml/recepcion` con `condicion='ok'` → reverso FIFO real (`fn_revertir_devolucion`) → la unidad vuelve al lote original = **vendible**; `condicion='no_disponible'` → sin reverso = **pérdida** (reclamable a ML/proveedor). Campos `recepcion_condicion/fecha/nota`. **GAP**: hoy los botones 📦 OK stock / ❌ No disp. solo aparecen en la solapa **Devueltas** (por `claim_status`), NO en **Canceladas** → una cancelada despachada/entregada no tiene acceso a la recepción. **Conciliar una cancelada**: no hay botón "conciliar" como en las normales (es cobro+devolución neto 0, se concilia del lado movimientos); lo que la cierra es la **recepción**. Pendiente: rutear canceladas con `estado_envio` despachado/entregado al flujo de recepción.

**Fiscal**
- Se discutió y confirmó: **IVA fuera del P&L** es correcto (pass-through). El flujo a pagar/a favor vive en **Posición Fiscal** (`v_posicion_fiscal`: `iva_debito/iva_credito/iva_a_pagar`). Hoy el **IVA crédito está en ~0** porque faltan cargar las compras → el "a pagar" está sobrestimado.
- **Decisión nueva (apertura fiscal, análoga a A2)**: en vez de reconstruir el histórico de compras, cargar el **saldo a favor de IVA al mes de corte** (de la DDJJ del contador) y de ahí en adelante cargar todo fino. `v_posicion_fiscal` debe **arrastrar** ese saldo a favor mes a mes. Pendiente: definir mes de corte + saldo a favor (ene/feb/mar quedan "fiscal incompleto").

**Pendientes para próximas sesiones**
- Cargar **compras** + UI con prorrateo (cuenta corriente de compra: flete/aduana/etc. → `compra_componentes.costo_adicional`; alimenta IVA crédito).
- **Apertura fiscal**: mes de corte + saldo a favor IVA → filita `posicion_fiscal_apertura` + ajuste de `v_posicion_fiscal`.
- Renglón **Ganancias** en Resultado (falta la tasa del contador).
- **Cuadre multi-mes** (tablero de cierre por mes).
- **Facturación de ML** como fuente de costos (envío de canceladas, cargos).
- Revisar a fondo **cancelaciones y devoluciones** (workstream del usuario).
- Auditar todas las `sbGet` por orden único.
- Token ML `offline_access`; ancla MP (dinero disponible) en Saldos; 326 liquidaciones residuales ($20,8M).

---

## Sesión 12-13 junio 2026 — Saldos por cuenta · pantalla Cuadre · fix token ML (workspace_config) · puente bonificaciones Flex · PSI Fase 2

### PSI — Fase 2
- Parámetros globales **Lead time** (15) y **Colchón** (7); punto de reorden = lead+colchón días; flag **🛒 Recomprar YA** (`recompra>0 && díasStock ≤ lead+colchón`) con KPI propio y filas al tope; export respeta el filtro de búsqueda (sufijo `_filtrado`) + columna "Recomprar YA"; botón ✕ limpiar búsqueda. Archivo: `public/js/screens/psi.js`.

### Saldos por cuenta (nueva pantalla `#saldos`)
- Arranque de fondos **desde fecha de corte**: saldo = ancla(al corte) + Σ movs posteriores. `saldos_iniciales.linea_id` → **NULLABLE** (ancla a nivel cuenta) + índice único parcial; vista **`v_saldo_cuenta`**. MP carga solo dinero **disponible** (no "por cobrar"). Cuenta nueva `santi_financiera` (USD). Cargados: Supervielle $16.148.413,34, Caja ARS $1.689.400, Caja USD US$90, Trust Wallet US$2.053,04. **Pendiente:** cargar ancla de MP. Archivos: `saldos.js`, `main.js`, `index.html`; migración `saldos_ancla_a_nivel_cuenta`.

### Cuadre (nueva pantalla `#cuadre`)
- Tablero de control de conciliación: semáforo % conciliado por monto por cuenta + cola accionable (✨ sugerencia / link a `#conciliacion`) + bloque "espera motor". Reusa criterios de `conciliacion.js`. Archivos: `cuadre.js`, `main.js`, `index.html`.

### Router serializado (fix race)
- `main.js`: cola `_routing` que serializa `renderScreen`. Bug: saltar entre pantallas pesadas (Conciliación/Cuadre, ~5.500 movs) pintaba el DOM viejo encima del nuevo. Resuelto.

### Fix token ML "se desconecta solo" (causa raíz)
- La tabla **`workspace_config`** (persistencia del token) **no existía** → `saveMLToken` fallaba en silencio → token solo en memoria → cada redeploy lo borraba. Creada (RLS on, solo backend). Además: `refreshML` **single-flight** + reintento 401 en `mlGet` (la pelea entre llamadas paralelas del sync invalidaba el refresh single-use); `scope=offline_access` en `/ml/auth`. **Pendiente:** habilitar `offline_access` en el panel de developers de ML para el `refresh_token` (hoy el access vence cada 6h, hay que reconectar a mano). Archivo: `server.js`; migración `crear_workspace_config`.

### Bonificaciones de envío Flex — puente hallado y aplicado
- Las bonificaciones (CASHBACK Flex) **no** se ligan a la venta por el settlement report (vienen sin order/shipping/pack — verificado sobre 572 filas). El puente está en el **payment crudo**: `GET /v1/payments/{SOURCE_ID}` → `point_of_interaction.transaction_data.reference_id` (`reference_type='shipment'`) → `ventas_ml.shipment_id` → venta. Se agregó **`ventas_ml.shipment_id`** (poblado en el sync desde `o.shipping.id`, + índice) y el endpoint **`POST /mp/conciliar-bonificaciones`** (`desde/hasta/limit?/write?/inspect?`, dry-run por default, idempotente, concurrencia 8).
- **Abril:** 1.375 bonif; **519 vinculadas en esta sesión** ($3,67M), 1.208/1.375 total, **167 pendientes** (envíos de marzo: `shipment_id` solo en abr/may/jun). A verificar: 689 ya estaban vinculadas de antes. Archivo: `server.js`; migración `ventas_ml_shipment_id`.

### Entregas
- **GitHub:** `server.js`, `public/js/screens/psi.js`, `public/js/screens/saldos.js`, `public/js/screens/cuadre.js`, `public/js/main.js`, `public/index.html`.
- **DB (Supabase MCP):** `saldos_ancla_a_nivel_cuenta` (linea_id nullable + `v_saldo_cuenta`), `crear_workspace_config`, `ventas_ml_shipment_id`, cuenta `santi_financiera`.
- **Docs:** actualizados `ADARA-PSI.md`, `ADARA-CONCILIACION-BANCARIA.md`, `ADARA-DECISIONES.md`, `ADARA-SCHEMA.md`, `ADARA-FRONTEND.md`, `ADARA-CHANGELOG.md`, `ADARA-DOCS-INDEX.md`.

### Próximos pasos
- Backfill `shipment_id` de marzo (y ene/feb) → re-correr `write` de bonificaciones (recupera las 167; verificar que el re-sync no altere CMV congelado). Habilitar `offline_access` en panel ML. Cargar ancla MP. Revisar las 689 bonif ya vinculadas. 326 liquidaciones residuales ($20,8M).

---


### Costeo
- Al verificar el deploy del 10/6 se detectó que el `server.js` del repo **no tenía** el hook de congelamiento (hacía solo `proyectar → consumir`); los 13.914 congelados habían salido de una corrida **manual**. Se agregó `fn_congelar_cmv_estimado()` al `/ml/sync` (orden `proyectar → consumir → congelar`, idempotente, devuelve `costeo.cmv_congelados`). Cobertura verificada: **14.047 venta_items = 13.914 congelados + 133 FIFO real** (0 sin costo). Nota: SKUs `68/93/153` del resumen previo son **códigos**, no ids (68=id103 Redmi, 93=id93 JBL, 153=id66 Targus). Archivo: `server.js`.

### Compras
- **Proveedor sin CUIT:** `POST /proveedores` con CUIT **opcional** (valida 11 díg si se carga); dedup con CUIT por CUIT, sin CUIT por nombre normalizado. Para informales. Archivos: `server.js`, `compras.js`.
- **Gastos al costo del lote (implementado):** extra directo por producto + prorrateables compartidos (criterio costo/unidades); flete/comisión/coima a terceros = costo del lote, **no** AP del proveedor (tipos `extra_directo`/`gasto_prorrateable` en `compra_componentes`; `v_compras_ap` recreada para excluirlos). Costo lote = `base + extra/u + prorrateo/u`, congelado a ARS al TC. Compra sin factura → **Exento**; `tc_blue` admite USDT; lote a stock al instante. Archivos: `server.js` (`POST /compras`), `compras.js`; migración `v_compras_ap`. *(Nota 5/8/2026: los tipos `extra_directo`/`gasto_prorrateable` **nunca llegaron a la base** — el CHECK no los admitía y había 0 filas. Se aplicaron recién el 5/8.)*
- SKU nuevo `TA002V` (Kindle Paperwhite 16GB 2024 Jade, electronica, IVA 21%).

### Login multiusuario (Supabase Auth + RLS) — doc nuevo `ADARA-AUTH.md`
- 4 usuarios `@adara.local` (`spuccio/spugliese/adandrea/fpuccio`, temporal `ADARA123%`, cambio obligado al 1er ingreso) en `auth.users`. `sb.js` con login/refresh/logout + token de usuario en cada request + parche de fetch; `main.js` con gate + pantallas; `server.js` con middleware que valida el JWT (HS256). **RLS aplicado a 28 tablas + 18 vistas** (`authenticated`=todo, `anon`=nada; `service_role` intacto). **Revierte A6.**
- **Gotchas:** usuarios creados por SQL necesitan las columnas de token en `''` (no NULL) o el login da "Database error querying schema" (corregido). Re-login por rotación de refresh tokens entre pestañas y por 401 del backend → `sb.js` endurecido (single-flight, re-lee localStorage, no desloguea por 401 de backend ni por error de red, sync entre pestañas).
- **`SUPABASE_JWT_SECRET` cargado y luego REMOVIDO:** al cargarlo, sincronizar dio "No autorizado" (caso **claves asimétricas**: el backend verifica HS256 y el proyecto firma distinto). Se removió de Railway → backend **fail-open** (endpoints sin candado). **La data sigue protegida por RLS.** Reactivar el candado del backend requiere verificación asimétrica (JWKS) — Fase 2, opcional.
- `skus.js`: columna **"Costo actual"** (`v_costo_sku_actual`).

### PSI (recompra) — doc nuevo `ADARA-PSI.md`
- **Velocidad por mediana de semanas con venta:** se cambió el cálculo para que **ignore las semanas en cero (quiebres)** y use la **mediana** de las semanas con venta, con toggle "Ignorar semanas sin venta (quiebres)" (default ON). Resuelve que el número dependía de cuántas semanas se tomaran (caso SKU 178: 206 con 2 sem vs 106 con rango largo por quiebres). Cuidado: baja rotación → destildar.
- **Buscador** por código/nombre (filtra al instante, sin recargar).
- **Columnas fijas** SKU + Producto con **scroll horizontal**; **nombre completo** (sin recorte).
- Se documentó la guía conceptual: cobertura objetivo = ciclo + lead time + colchón; recompra prospectiva; cantidad vs. momento; dependencia del stock cargado. Archivo: `public/js/screens/psi.js`.

### Entregas
- **GitHub:** `server.js`, `public/js/core/sb.js`, `public/js/main.js`, `public/js/screens/compras.js`, `public/js/screens/skus.js`, `public/js/screens/psi.js`.
- **DB (Supabase MCP):** SKU `TA002V`; `v_compras_ap` recreada; usuarios `auth.users`/`auth.identities`; RLS (`rls_on_tablas`, `rls_on_vistas`).
- **Docs:** nuevos `ADARA-AUTH.md`, `ADARA-PSI.md`; actualizados `ADARA-CHANGELOG.md`, `ADARA-DOCS-INDEX.md`, y (tanda previa de la sesión) `ADARA-COMPRAS-IMPORTACIONES.md`, `ADARA-DECISIONES.md`, `ADARA-COSTEO-FIFO.md`, `ADARA-IMPUESTOS.md`, `ADARA-SCHEMA.md`, `ADARA-FRONTEND.md`.

---

## Sesión 10 junio 2026 — Hook de congelamiento + 3 costos faltantes + Targus + Posición Fiscal

- **Hook de congelamiento enganchado al `/ml/sync` (cierra el pendiente de CF10).** Se agregó `fn_congelar_cmv_estimado()` al final del bloque de costeo del `/ml/sync` en `server.js`, dentro del mismo `try` que no bloquea el sync. Orden garantizado: **`fn_proyectar_ml` → `fn_consumir_fifo` → `fn_congelar_cmv_estimado`**. Idempotente (`ON CONFLICT DO NOTHING`), corre en cada sync sin duplicar ni mover meses cerrados. La respuesta trae `costeo.cmv_congelados`. Las ventas nuevas sin lote se congelan solas. Archivo: `server.js` (subido por GitHub).
- **3 costos faltantes cargados (cierra CF9) → cobertura CMV 100%.** SKUs `68` Redmi Buds 6 Pro = 71.621,43304 · `93` JBL Flip 7 = 164.304,91 · `153` Targus = 20.955,84 (s/IVA, sobre `skus.costo_referencia`). Se corrió `fn_congelar_cmv_estimado()` → **27 ítems congelados** (23+2+2), origen `costo_referencia`. No quedan SKUs vendidos sin costo.
- **Targus (SKU 153) — lote del seed con costo 0, corregido.** Tenía 2 lotes del seed (1u DEP + 26u MFUL) en `costo_unitario=0`. Las ventas históricas (pre-seed) ya se congelaron bien con `costo_referencia`, pero las futuras iban a costear a $0 (FIFO real pisa al congelado). Se actualizó `lotes.costo_unitario = 20.955,84` en los 2 lotes (excepción de seed CF6). Ahora `v_costo_sku_actual` lo toma del lote; FIFO futuro y valorización correctos.
- **Pantalla Posición Fiscal (`#posicion_fiscal`, nivel 1).** `screens/posicion-fiscal.js` (nuevo) + `main.js` + `index.html`. Tres secciones (IVA / IIBB / Otros) + KPIs. Lee la vista nueva **`v_posicion_fiscal`** (mensual): **IVA tomado de `v_control_mensual`** (misma fuente que el panel de Resultado, no pueden divergir); **`iibb_retenido` desde `ventas_ml.impuestos`** (fuente canónica del monto, completo desde enero, venta por venta); percepciones (`compra_componentes`) e impuesto al cheque (`retenciones`). Determinado de IIBB y Ganancias marcados *pendiente* (alícuotas/tasa del contador). Avisos honestos: IVA crédito incompleto → "a pagar" alto hasta cargar compras/gastos.
- **Aclaración sobre la cobertura de `retenciones` (duda recurrente, documentada).** La tabla arranca el 10/3; ene/feb vacíos. Causa: el auto-sync (`correrRetenciones` en `/ml/sync`) pide solo *mes anterior → hoy* y MP solo expone ~3 meses → ene/feb irrecuperables. **No es trabajo manual**: se mantiene solo de junio en adelante. Los 2 CSV "manual" fueron carga de arranque. El **monto** de IIBB retenido está completo en `ventas_ml.impuestos`; `retenciones` solo abre por jurisdicción. Documentado en `ADARA-RETENCIONES-IIBB.md`.
- **Decisiones que quedan (contador):** alícuotas IIBB por jurisdicción → determinado + sumarlo al Resultado; tasa de Ganancias → renglón final; tratamiento del `impuesto_cheque` (costo financiero vs pago a cuenta de Ganancias).
- **Archivos:** `server.js`, `public/js/screens/posicion-fiscal.js` (nuevo), `public/js/main.js`, `public/index.html` (subidos por GitHub). DB (Supabase MCP): `skus.costo_referencia` ×3, `lotes` Targus ×2, congelamiento, vista `v_posicion_fiscal`. Docs: `ADARA-COSTEO-FIFO.md`, `ADARA-IMPUESTOS.md`, `ADARA-SCHEMA.md`, `ADARA-FRONTEND.md`, `ADARA-DECISIONES.md` (P12), `ADARA-RETENCIONES-IIBB.md`, `ADARA-DOCS-INDEX.md`.

---

## Sesión 9 junio 2026 (parte 3) — CMV estimado a costo actual + costo de referencia + congelamiento del CMV + lote inicial

- **CMV estimado a costo actual (workstream previo, ahora documentado).** El histórico sin FIFO (ventas anteriores al seed del 8/6, ~13.487 ítems) se costea con `cantidad × v_costo_sku_actual.costo_unit`, etiquetado **estimado**; las ventas nuevas siguen FIFO real. `v_resultado_mensual`/`v_resultado_linea_mensual` recreadas para exponer `cmv` combinado + `cmv_real`/`cmv_estimado`.
- **Margen operativo real + 3 montos.** El % de la pantalla Resultado pasó de "contribución antes de CMV" a **margen operativo** (resultado op / ingreso neto). Aclarados los 3 montos de la venta ML (bruto c/IVA $356,1M = total ML; neto s/IVA $295,5M = base resultado; por cobrar $265,1M = liquida ML), validados al peso para mayo. IVA débito mayo $38,5M validado (alícuotas 10,5%/21% confirmadas por Sebastián).
- **Drill-down de control mensual.** Vista **`v_control_mensual`** (por período: ML crudo, ventas válidas, excluidas/canceladas, IVA débito/crédito/a pagar). Panel desplegable en Resultado con 4 tarjetas (cuadre con ML / cascada / IVA / cobranza). `resultado.js` actualizado.
- **`skus.costo_referencia` (CF9).** 427 ítems sin CMV = 21 SKUs vendidos **sin lote** (nunca entraron al inventario). Columna nueva `costo_referencia` (neto s/IVA) + `v_costo_sku_actual` recreada con `COALESCE(promedio de lotes, costo_referencia)`. Cargados **18 SKUs** desde la hoja CMV (columna *CMV s/IVA*); faltan 3 (68 Buds 6 Pro, 93 Flip 7, 153 Targus). Cobertura pasó de 97% a 99,8%.
- **Congelamiento del CMV (CF10) — el CMV una vez calculado queda FIJO.** Tabla **`cmv_estimado_congelado`** + función **`fn_congelar_cmv_estimado()`** (idempotente, `ON CONFLICT DO NOTHING`, nunca re-pisa). `v_resultado_mensual` recreada: FIFO real → congelado → dinámico residual. Corrida inicial: **13.887 ítems congelados**. Validado: mayo idéntico antes/después (se congeló con el costo de hoy). Un mes cerrado ya no se mueve aunque cambien costos o se carguen compras nuevas. **Pendiente:** hook al `/ml/sync`.
- **Decisión lote inicial (CF11).** Los 18 SKUs recibirán su lote inicial formal (costo = `costo_referencia`, cantidad = stock físico recontado, `fecha_alta` = fecha de corte) cuando Sebastián recuente el depósito. La fecha de corte va posterior a las ventas históricas para no recostear lo congelado. Plan: antes del arranque 100% funcional, recontar y cargar TODO el stock físico.
- **Todo fue base de datos** (Supabase MCP): columna, tabla, función y 3 vistas recreadas. No hubo deploy de archivos. Docs actualizados: `ADARA-COSTEO-FIFO.md`, `ADARA-SCHEMA.md`, `ADARA-DECISIONES.md` (CF9-CF11, P11), `ADARA-PNL.md`.
- **Aclaración de fuentes de doc:** en la copia del proyecto faltaban `ADARA-SCHEMA.md` y `ADARA-DECISIONES.md`; se tomaron de base las versiones de la sesión 9/6 (parte 2) y se editaron sobre ésas.
- **Tratamiento de impuestos en el P&L (decisión conceptual, P12).** El **IVA no va al resultado** (se trabaja todo neto de IVA; es flujo financiero → Posición Fiscal). El **IIBB determinado** del período y el **Impuesto a las Ganancias** del ejercicio **sí restan** a la rentabilidad (Ganancias como renglón final). Las **percepciones / retenciones / anticipos** (IVA, IIBB, Ganancias, incl. importación) son **pagos a cuenta, no costo**: deducirlas además del impuesto determinado sería doble conteo. En importación se distingue costo capitalizable (derechos / tasa / flete / seguro / nacionalización → CMV) de las percepciones (crédito fiscal, ya separadas en `compra_componentes`). El "ahorro" por comprar facturas es efecto de caja/fiscal, **no rentabilidad** → no se modela en el P&L (rompería la confiabilidad del número). Documentado en `ADARA-IMPUESTOS.md`, `ADARA-DECISIONES.md` (P12), `ADARA-PNL.md`.
- **Workstream futuro definido: pantalla de Posición Fiscal** (IVA a pagar débito−crédito; IIBB determinado vs anticipos + saldo a favor; anticipos de Ganancias vs impuesto del ejercicio). En paralelo, sumar al Resultado el **IIBB determinado** y la **línea de Ganancias**. Validar con contador la determinación de IIBB y la base de Ganancias.

---

## Sesión 8 junio 2026 (parte 3) — Huérfanos resueltos + pantalla Resultado + Costeo→Inventario + modelo de líneas

- **Huérfanos de `ventas_ml` resueltos.** 4 grupos (13 ventas + 1 combo = $614.708,20) no proyectaban por mismatch de código ML↔catálogo; ninguno era producto nuevo. Mapeos: `97→98`, `95→PA001N`, foco sin código→`401` (por título), combo `86+ac001`→ reloj `86` + film `AC001`. **Mecanismo (CF8):** tablas **`sku_map`** (equivalencia 1:1 por `codigo`/`titulo`) y **`combo_map`** (un código = N SKUs, con `neto_factor`/`es_principal`); **`fn_proyectar_ml` reescrita** para resolver combo → directo → alias código → alias título (idempotente). Durable: el sync pisa `ventas_ml.sku` (merge-duplicates), por eso la equivalencia vive en la proyección, no en la tabla. Combo: el reloj cobra todo ($71.492 neto, IVA 10,5%); el film va a $0 venta pero descuenta stock. Las 4 son pre-seed → no consumen FIFO (sólo suman revenue).
- **Pantalla "Resultado" (nueva, `#resultado`, `resultado.js`).** Estado de resultado mensual por línea: ventas netas − comisión − envío − financiero − IIBB = **margen de contribución (antes de CMV)**. Una fila por mes + Total. **Selector de línea + selector de canal.** Lee la vista nueva **`v_resultado_mensual`** (período×línea×canal; join `ventas`↔`ventas_ml` por `ml_order_id`). CMV se muestra aparte, marcado pendiente (sólo 19 ventas costeadas). Aviso visible: es contribución antes de CMV/gastos; el % no es el margen real.
- **Pantalla "Costeo" renombrada a "Inventario"** (hash sigue `#costeo`). Se le quitaron CMV mensual y Margen bruto (van a Resultado). Quedó: Valorización de stock + SKUs sin costo. Nombres legibles de familia/depósito (mapas `FAM_NOMBRE`/`DEPO_NOMBRE`), electrónica agrupada en fila desplegable por depósito, **columna Valorizado USD** (`TC_USD=1465` hardcode, futuro BCRA), totales por tabla.
- **Cambio de fondo en el modelo de líneas (LN1-LN3).** Línea = producto (familia); canal = dimensión separada. Se **unificó electrónica** ("ML Electrónica" + "Electrónica off-ML") en una sola línea **"Electrónica"** (id 1, renombrada; id 2 borrada, 0 referencias). Regla simplificada a `familia → línea` (`lineas_negocio_reglas` repuntada). Quedan 6 líneas. Las 13.932 ventas ya estaban en línea 1 → sin remapeo.
- **Deuda detectada:** `.num` y `.psi-aviso` **no existen** en `base.css` (varias pantallas las usan) → definir o scopear. Familias `repelente`/`vaso_termico` están en **singular** en el catálogo (el doc LINEAS las mencionaba en plural).
- Archivos subidos por GitHub: `resultado.js` (nuevo), `costeo.js` (Inventario), `main.js`, `index.html`.

---

## Sesión 8 junio 2026 (parte 2) — Seed ejecutado + circuito CMV completo (Fases 2-4) + costos de apertura + vistas + pantalla Costeo

- **Seed de stock EJECUTADO** desde el export de Tango. **Match de stock por la columna `SKU` del export = `skus.codigo`** (la otra columna, `Código` interno de Tango, **colisiona** con `skus.codigo` de productos distintos → NO usar). Casos: `TMP-<n>` para luminarias/mochilas sin SKU; ceros a la izquierda (`55→055`). Se agregó columna **`lotes.deposito text`** (un lote por SKU × depósito; 4 depósitos DEP/DJ/MFUL/MENV). Resultado: **137 lotes / 89 SKUs / 102.534 u / $60.836.417,17** (coincide exacto con Tango). Compra `tipo='inicial'`, `linea_id=NULL`.
- **Corrección CF3:** `compras.linea_id` **es nullable** (el handoff/COSTEO-FIFO que decían NOT NULL estaban mal).
- **Líneas/familias nuevas** (huérfanos con stock sin SKU): líneas `repelentes` y `vasos_termicos`; SKUs `REP001` (repelente, 80.000u), `VT001`/`VT002` (vaso_termico — "Vasos Térmicos" incluye el mate Voox), `TA001G` (electrónica, tablet ML). Falta definir reglas familia×canal de repelentes/vasos.
- **Fase 2 FIFO (consumo) desplegada y validada:** `fn_consumir_fifo(p_desde,p_hasta)` — consume lotes del mismo `sku_id` con `cantidad_actual>0 AND fecha_alta<=venta.fecha` orden `fecha_alta,id`, snapshot del costo en `costo_unitario_al_consumir`, idempotente por unidades ya consumidas. Corrida 8/6: **19 ítems / 19 consumos / 0 faltantes** (sólo ventas ≥ seed consumen; las históricas quedan sin CMV por diseño = CMV diferido a capa 6). Drift `v_stock_check`=0.
- **Fase 4 (hook al sync) desplegada:** helper `sbRpc(fn,params)` en `server.js`; en `POST /ml/sync` (tras `kickRetenciones`) corre `fn_proyectar_ml → fn_consumir_fifo` (orden obligatorio), no bloqueante, devuelve bloque `costeo`. **PSI quedó "vivo"** con el seed (sin tocar código): velocidad ← `ventas_ml`, stock ← `Σ lotes.cantidad_actual`.
- **Fase 3 (reversas de devolución) desplegada:** `fn_revertir_devolucion(p_ml_order_id,p_fecha)` — devuelve al **lote original** (S3) las unidades consumidas con el **costo snapshot** (reversión de CMV exacta), idempotente por neto. `/ml/recepcion` **reemplaza el `+1 catalogo_skus` muerto (v21)** por el reverso FIFO real cuando `condicion='ok'`; fechado a `recepcion_fecha`; `no_disponible` → sin reverso → **pérdida** (P3). Validado sobre el lote 106 (30→31, reverso −1, drift 0, idempotente) y prueba deshecha. **Regla temporal P3:** el reverso de stock/CMV se imputa al mes del evento (recepción), no al de la venta; la plata va aparte al mes del movimiento MP (O10).
- **Costos de apertura cargados (corrección del seed, por SQL):** desde la planilla "Costos x canal / CMV", criterio **CMV s/IVA** (neto; el IVA es crédito fiscal). 1ª tanda **56 SKUs / 95 lotes**; re-snapshot de los 19 consumos del día (mismo día, nada cerrado) → CMV junio $2.122.782. 2ª tanda **5 SKUs** (`TA001G` 307.500, `73B` 11.387, `AC001` 1.118, `200` 113.458, `201` 69.580). Valorización: $60,84M → $287,94M → **$313,11M**. **Pendientes: 13 SKUs en $0** (`153,83R,27,76,79,162,68N` + `REP001,VT001,TMP-164,TMP-109,TMP-110,VT002`).
- **Regla de negocio fijada — el costo no se edita a mano:** nace en la **compra** (factura del proveedor), se asienta por **lote** en `lotes.costo_unitario`, y es **inmutable**. No hay ni habrá UI de edición de costo. Única excepción: el **seed de apertura** (compra `tipo='inicial'`), corregido por SQL. (Se descartó armar un editor/endpoint de carga manual.)
- **Vistas de explotación del costeo (nuevas, `grant select to anon`):**
  - `v_cmv_mensual` — COGS devengado por `periodo` × familia (`cmv_consumo`/`cmv_reverso`/`cmv_neto`).
  - `v_valorizacion_stock` — inventario a costo por familia × depósito, con `unidades_sin_costo`.
  - `v_margen_ventas` — a nivel `venta_item`: `ingreso_neto − cmv = margen_bruto`, con flag **`costeada`** (filtrar `=true` para márgenes reales; las pre-8/6 dan `false`).
  - `v_skus_sin_costo` — SKUs con lotes a costo 0 y stock > 0 (lista de pendientes de apertura).
- **Pantalla Costeo** (`public/js/screens/costeo.js`, hash `#costeo`, registrada en `main.js` + ítem en nav de `index.html`). Lee las 4 vistas por REST anon (`sbGet`). KPIs (valorización, uds sin costo, ingreso/CMV/margen costeado, % margen) + tablas Valorización, **SKUs sin costo (read-only, drill-down)**, CMV mensual y Margen bruto (sólo `costeada`). Aviso al pie: margen bruto NO incluye comisión ML/IIBB/envío.
- **Documentación:** actualizados `ADARA-COSTEO-FIFO.md`, `ADARA-DECISIONES.md`, `ADARA-SCHEMA.md`, `ADARA-FRONTEND.md`, `ADARA-PNL.md`, `ADARA-STOCK.md`, `ADARA-LINEAS-NEGOCIO.md`, `ADARA-CANCELACIONES-DEVOLUCIONES.md`, `ADARA-VENTAS-ML-V22.md`, `ADARA-DOCS-INDEX.md`.
- **Pendientes que quedan:** cargar los 13 costos de apertura faltantes (por SQL, corrección de seed); CMV histórico ene–hoy (capa 6, sync Tango); huérfanos ML restantes `97`/`95`/combo `86+ac001` + re-correr `fn_proyectar_ml`; reglas familia×canal de repelentes/vasos; refinamiento PSI (excluir depósito MFUL de recompra); P&L como pantalla (bloqueado por Gastos + `linea_id`); confirmar alícuota IVA de TA001G (0,1050).

---

## Sesión 8 junio 2026 — PSI Recompra (v22) + costeo de ventas ML (proyección Fase 1) + seed de stock decidido

- **Pantalla PSI Recompra reimplementada (v22)** (`public/js/screens/psi.js` + ruta `#psi` + ítem de menú). Porta la lógica del v21 a fuentes v22: velocidad ← `ventas_ml`, stock ← `Σ lotes.cantidad_actual` mapeado por `sku_id→codigo`. Sin columna "en tránsito" (v22 no modela OC en viaje). Escala 5 colores de `ADARA-STOCK.md`, ventana/cobertura configurables. Export Excel vía SheetJS lazy desde CDN. Read-only, sin cambios en `server.js`.
- **Hallazgo crítico (CF4):** las ventas ML **no consumían lotes** — `consumo_lote=0`, `ventas`/`venta_items` vacíos, `cantidad_actual` congelado (SKU 300: inicial=actual=9 con 17 ventas). Cualquier "stock a hoy" era falso.
- **Decisión de arquitectura (CF1):** costear ML proyectando `ventas_ml → ventas/venta_items` (1:1 por `ml_order_id`) + FIFO `consumo_lote`. Camino prolijo, desbloquea CMV/P&L.
- **Fase 1 (proyección revenue) implementada y corrida:** índice `ux_ventas_canal_refext` + función `fn_proyectar_ml(desde,hasta)` (set-based, idempotente). **13.918 ventas 2026** proyectadas (neto $1.133.687.557,72 · IVA débito $148.661.718,41 · bruto $1.282.349.276,13), todas ML Electrónica. 14 excepciones reconciliadas (2 sin SKU + 12 código fuera de catálogo: `97`, `95`, `TA001G`, combo `86+ac001`).
- **Constraints descubiertos (CF2):** en `venta_items`/`ventas` hay **columnas generadas** (`periodo`, `es_gravada`, `iva_unitario`, `precio_unitario_bruto`, `neto_linea`, `iva_linea`, `bruto_linea`) — no insertar. Fuente de verdad = `precio_unitario_neto` (sin redondear → bruto regenera ≈ `importe_bruto`, ±1 centavo aceptado). CHECKs: `ventas.estado/tipo_comprobante`, `consumo_lote.tipo`, `lotes.cantidad_actual>=0`, `compras.tipo` incluye `inicial`; `canal` FK a `canales` (ml existe); alícuota fracción [0,1].
- **Seed de stock decidido (CF3):** foto de hoy desde **export de Tango** (stock actual valorizado) → lotes fechados hoy (compra `tipo='inicial'`). FIFO de hoy en adelante; CMV histórico ene–hoy diferido al sync de ventas Tango (capa 6). Tango no es fuente de stock vía API (su API es solo ventas). Pendiente: recibir el export.
- **Documentación:** **nuevo `ADARA-COSTEO-FIFO.md`** (doc de dominio); actualizados `ADARA-DECISIONES.md` (CF1-CF4), `ADARA-DOCS-INDEX.md`, `ADARA-STOCK.md`, `ADARA-SCHEMA.md`, `ADARA-PNL.md`, `ADARA-TFACTURA.md`, `ADARA-FRONTEND.md`.
- **Pendientes que quedan:** import del export Tango → lotes; alta de SKUs huérfanos + re-correr `fn_proyectar_ml`; Fase 2 FIFO (consumo + validación CMV); Fases 3 (reversas) y 4 (hook sync + PSI con stock vivo).

---

## Sesión 5 junio 2026 — Herramienta de devoluciones por `op_id` (construida) + detalle por venta + UI ML unificada

- **Diagnóstico de los "17 residuales" (RESUELTO).** No eran ventas sin sincronizar: el `op_id` del bundle de devolución es el **`SOURCE_ID` de la liquidación**, distinto del `mp_payment_id` de la venta. `retenciones` es el puente (guarda `mp_source_id` + `venta_id`/`order_id`). Validado contra los 213 `op_id` reales: **capa 1** `op_id==mp_payment_id` → 189; **capa 2** vía `retenciones` (`venta_id`, y si null por `order_id`) → 16; **capa 3** agregados ML (op_id corto) → 7; **capa 4** revisión → 1. **212/213.** Match **siempre por `op_id`, nunca por monto**.
- **Corrección de guardrail `vinculos.monto`.** El handoff decía "monto con signo real" — **era falso y rompía el endpoint**. `vinculos.monto` va en **magnitud positiva** (`abs`); `/vincular[-lote]` rechazan ≤0; el signo real vive en `movimientos.monto`. (SCHEMA ya lo documentaba bien.)
- **Backend (`server.js`), 3 endpoints nuevos:** `GET /devoluciones/resolver` (agrupa por `op_id`, clasifica bundles), `POST /devoluciones/vincular {op_id, venta_id}` (vincula todas las líneas, `monto=abs`, idempotente; marca `devuelta` solo si neto<0; no toca `por_cobrar`), `GET /venta/:id/detalle` (cobro + retenciones + devoluciones de una venta).
- **`v_cc_devoluciones` recreada** (`DROP VIEW + CREATE`): de `min(fecha)` por venta → **una fila por (venta, mes)** con signo real e imputación por línea (regla dura 2 del P&L). Suma `linea_negocio_id`.
- **UI unificada (decisión de producto):** se **eliminó la solapa Devoluciones**; todo lo de ML vive en la pantalla **Ventas ML**. El chip **Devueltas** abre la herramienta de devoluciones por bundle, con el **mismo navegador Día/Mes**. **Nuevo: clic en una fila de venta → modal de detalle** que junta Cobro / Impuestos / Devoluciones.
- **Fix de caché (RESUELTO).** Causa raíz de "los cambios no se ven": módulos ES cacheados. Solución: `Cache-Control: no-cache` en `express.static` + versionado `?v=devN` en el import de `ventas-ml.js` (bump por deploy). Ya alcanza un F5.
- **Bug y fix en el camino:** la app quedó en "inicializando" por una **función `detalleHTML` declarada dos veces** (la mía colisionó con la del toggle de N° de operación). `node --check` no lo agarra (parsea como script, no como módulo); el browser sí. Renombrada a `ventaDetalleHTML`. **Lección:** al agregar funciones, `grep` de nombres duplicados.
- **Documentación:** actualizados `ADARA-CANCELACIONES-DEVOLUCIONES.md`, `ADARA-DECISIONES.md` (O10), `ADARA-SCHEMA.md` (vista), `ADARA-FRONTEND.md` (pantalla + caché resuelta), `ADARA-RETENCIONES-IIBB.md` (puente + backfill). `ADARA-HANDOFF-DEVOLUCIONES.md` queda obsoleto → borrar.
- **Pendientes que quedan:** backfill `retenciones.venta_id` (7 filas por `order_id`); vincular en masa las ~204 devoluciones de abril (operativo); pago en mediación `2000016031933056` (`por_cobrar` inflado, residual); matching de envío colecta (2 liquidaciones AS); saldos iniciales 31/12/2025; P&L como pantalla; ~~bug XLSX (cancelados sin `fecha_entrega`)~~ **(cerrado 21/6: reporte externo transitorio, se retira — ver FLUJO-OPERATIVO #4)**; v2 del detalle ("Ver liquidación en MP" en vivo); limpiar endpoints muertos `/mp/*`.

---

## Sesión 4 junio 2026 — Fix colecta envío IMPLEMENTADO + RLS de retenciones + pendiente mediación

- **Fix O8 (colecta con envío a cargo del comprador) implementado en el front** (`public/js/screens/ventas-ml.js`) y desplegado. Helpers `shipDe`/`retEnvioDe`/`objetivoDe`; índice `SHIP_BY_VENTA` desde `retenciones SETTLEMENT_SHIPPING` con prioridad `venta_id → ml_order_id → pack_id` (una sola clave por venta → no cuenta dos veces en un pack); `matchCobros` suma el cobro del envío; los 4 puntos de cierre (`conciliable`, `asignarBonifs`, `armarLoteConciliacion`, `conciliar`) usan `objetivo = por_cobrar + retEnvio`. Validado con `node` y con el testigo `2000015791986818` (cierra al centavo: 26.287,93 + 5.218,77 + 10,77 = 31.517,47). Cero cambios en `server.js` ni en `por_cobrar`.
- **Debug clave — RLS en `retenciones`.** Tras desplegar, ninguna colecta-envío enganchaba. Causa: `retenciones` tenía **RLS on**, así que el front (anon key) recibía `200` con `[]` (sin error en consola) → `SHIP_BY_VENTA` vacío. Síntoma en Network: el request `retenciones?...` pesaba 0,8 kB vs cientos de kB de `movimientos`. Fix: `alter table retenciones disable row level security; grant select on retenciones to anon;`. Reforzada **A6** (toda tabla nueva nace con RLS off + grant a `anon`).
- **Nuevo pendiente — pagos en mediación.** La venta `2000016031933056` ("Cobro en mediación") tiene `por_cobrar` inflado en 12.128,67 (= envío a tu cargo 7.470 + impuestos 4.658,67) porque el sync no capturó impuestos ni envío para ese pago. **No es O8** (una sola liquidación, ya neta). Out of scope (toca `server.js`/`por_cobrar`). Decisión: dejarla como **residual**, **no editar `por_cobrar` a mano** (el re-sync lo pisa). Detalle: `ADARA-VENTAS-ML-V22.md` (Pendiente 5).
- **Verificación de despliegue:** `main` del repo idéntico byte a byte al archivo entregado; código nuevo confirmado en el `.js` servido (`SHIP_BY_VENTA`) y en Network. Recordatorio: el módulo ES puede quedar cacheado — usar **"Empty Cache and Hard Reload"** con DevTools abierto.

---

## Sesión 3 junio 2026 — Conciliación colecta envío RESUELTA + motor v21 muerto + limpieza de docs

- **Colecta con envío a cargo del comprador — RESUELTO.** El residual que no cerraba (caso testigo $10,77) es la **retención de IIBB sobre la liquidación del envío** (`SETTLEMENT_SHIPPING` en `retenciones`), que NO está dentro de `por_cobrar`. Identidad de cierre: `por_cobrar = cash(producto + envío) + |IIBB envío|`. Validado a escala: 58 ventas con retención de envío → 46 cierran exacto, 3 sin cash aún, ~9 residuales acotados. Fix (en el front `ventas-ml.js`): puente vía `retenciones SETTLEMENT_SHIPPING` (por `venta_id`/`ml_order_id`/`pack_id`), `matchCobros` suma la liquidación del envío y `conciliable` usa `por_cobrar + retEnvio`. Detalle: `ADARA-VENTAS-ML-V22.md`, regla `ADARA-DECISIONES.md` O8.
- **Hallazgo de arquitectura:** el motor de conciliación del server v21 (`autoConciliarMP`, tabla `movimientos_mp`, columnas `balance_conciliacion`/`estado_conciliacion`/`conciliado`) está **MUERTO**: `movimientos_mp` se borró en el reset del 27/05 y no se recreó. La conciliación viva es exclusivamente el front (`vinculos` vía `/vincular` y `/vincular-lote`). Endpoints `/mp/conciliar|recalcular-balance|vincular|desvincular|descartar` → deuda técnica a limpiar.
- **Limpieza de documentación:** eliminados `ADARA-CURRENT-STATE.md` (v21 desactualizado) y los 2 handoffs (consolidados en docs permanentes). Banners "congelado v21" en los docs de referencia v21. 30 → 27 docs.

---

## Sesión 3 junio 2026 — Retenciones IIBB por settlement de MP

Objetivo: capturar las retenciones de IIBB por jurisdicción (y la base para devoluciones de envío) desde el **settlement de MP bajado por API**, sin subir archivos, integrado al flujo y en segundo plano. Detalle completo en `ADARA-RETENCIONES-IIBB.md`.

### Hallazgos sobre el settlement de la API (verificado con abril real)
- El reporte de la **API** es **CSV separado por `;`** con headers **en inglés** (`SOURCE_ID`, `TAXES_DISAGGREGATED`, `MONEY_RELEASE_DATE`, etc.) — NO el XLSX en español de la web. `TAXES_DISAGGREGATED`/`METADATA` traen `,` adentro, por eso el separador es `;`.
- MP descarga el settlement **por `file_name`** (del listado), **no por `id`** numérico (el GET por id da 403). No existe `download_url` ni `status:'ready'` — el estado terminal es `processed`.
- **Clasificación por `detail` + `financial_entity`**: `tax_withholding_sirtac`+prov → `iibb_provincial` (SIRTAC); `tax_withholding`+prov → `iibb_provincial` (**régimen propio**, Santa Fe y Corrientes, montos grandes); `tax_withholding_collector`+`iibb_tucuman` → `iibb_tucuman`; `tax_withholding_collector`+`debitos_creditos` → `impuesto_cheque` (costo); `tax_withholding_payout` → `payout`. Se **ignora** `tax_withholding_payer*` (comprador).
- Se **excluyen** filas `CASHBACK`/`CASHBACK_CANCEL` (bonificaciones Flex: solo imp. al cheque, **sin IIBB provincial**). Confirmado con el `taxmap` del archivo entero (24 jurisdicciones).
- Transaction types relevantes: `SETTLEMENT`, `REFUND`/`DISPUTE` (reversos, IIBB positiva, mismo `SOURCE_ID`), `SETTLEMENT_SHIPPING`/`DISPUTE_SHIPPING` (envío normal/full/colecta — **sí** traen IIBB por provincia, con `SOURCE_ID` propio, atado por `pack_id`/`order_id`), `PAYOUTS`.

### Backend (`server.js`, repo `adara-backend/main`)
- **Fix `GET /debug/settlement`** (read-only): descarga por `file_name`, separador `;`; nuevos modos `?file=`, `?types=1` (conteo por transaction_type) y `?taxmap=1` (mapa `transaction_type → detail|financial_entity → conteo/suma`).
- **Ingesta `GET /mp/retenciones-sync`** (paso 2a): parsea, clasifica, resuelve `venta_id` (por `mp_payment_id`/`mp_payment_ids`, y por `pack_id` en envío) y hace **upsert idempotente** en `retenciones`. Por defecto **dry-run**; escribe con `&write=1`. **No toca `por_cobrar` ni la conciliación.**
- **Background 2b**: `kickRetenciones()` enganchado al final de `/ml/sync` (sin botón, throttle 6 h, ventana móvil mes anterior→hoy). Reporte **pendiente resumible** (poll ~12 min; si MP tarda, se reanuda en la próxima sync sin regenerar). Endpoints `GET /mp/retenciones-estado` (indicador al_dia/actualizando/error) y `GET /mp/retenciones-refresh` (forzar/reanudar).

### DB
- **Tabla `retenciones`** creada (ver `ADARA-SCHEMA.md`). Idempotencia `UNIQUE (mp_source_id, transaction_type, detail, financial_entity)`. Devengado al **release** (`fecha = MONEY_RELEASE_DATE`, `periodo` generado).
- **Vista `v_retenciones_iibb`** (punto 4): acumulado IIBB por `periodo` × `jurisdiccion` × `detail` (SIRTAC vs propio vs Tucumán). Solo IIBB.
- Carga real: abril (manual, 10.857 filas) + mayo/junio (11.541). Acumulado neto IIBB ≈ −2,44 M repartido por período de release (2026-03 a 2026-07).

### Decisión clave (paso 3 — no se tocó la conciliación)
- Verificado con 30 ventas reales: **`por_cobrar` ya incluye la retención** (`ventas_ml.impuestos` ≈ suma del settlement; nunca menos). Sumar `retenciones` al `esperado` **doble-contaría** y rompería conciliaciones. Por eso **NO se modificó la fórmula** (`esperado = por_cobrar + sumDev` sigue igual).
- Además, en el estado actual de la base **ninguna venta está conciliada** (13.575 con `balance_conciliacion = null`), así que no hay residual que diagnosticar.
- **3b queda parado** con disparador claro: si al cargar el AS y correr la conciliación quedan ventas sin cerrar, recién ahí se revisa el residual (retención faltante vs bug colecta vs cheque de Flex). Ver `ADARA-RETENCIONES-IIBB.md`. *(Nota jun 2026: resuelto — la conciliación viva es el front; handoff eliminado.)*

### Pendientes que salieron
- Re-correr la conciliación (cargar AS) para validar que cierra a 0 con `por_cobrar` actual.
- Bug colecta de envío (doble liquidación): el `SETTLEMENT_SHIPPING` confirma la fila aparte con `SOURCE_ID` propio, atable por `pack_id`/`order_id`. Ver `ADARA-VENTAS-ML-V22.md`. *(Nota jun 2026: resuelto; handoff eliminado.)*

---

## Sesión 1 junio 2026 — Conciliación de Ventas ML (v22) + layout

### Pantalla Ventas ML (nueva)
- `public/js/screens/ventas-ml.js`: control diario/mensual (toggle Día/Mes), filtros por estado (Todas/Por cobrar/Cobradas/Conciliadas/Canceladas/Devueltas), link directo a la venta en ML, detalle de cargos. Cruce venta↔cobro por `mp_payment_id`.
- Conciliación con el modelo `vinculos`: nuevo `op_tipo='venta_ml'` (`op_id=ventas_ml.id`), agregado al CHECK y a `tiposOk` de `/vincular`. Botón Conciliar por venta + "Conciliar todas" (endpoint nuevo `POST /vincular-lote`, upsert idempotente).
- Una venta concilia cuando la suma de sus cobros (sola o + bonificación de envío) iguala `por_cobrar` (tol 0,02). Soporta **varios cobros** (split payment, dos medios de pago) y **bonificación de envío** (movimiento aparte, neto −0,6%, intercambiable y de uso único). Regla v21 "primero el pago, después la bonificación".
- **Fix zona horaria** en el sync: helper `fechaHoraARG()` normaliza `date_created`/`money_release_date` a America/Argentina/Buenos_Aires. La franja 00:00 ya no se corre de día.

### Layout (toda la app)
- Menú horizontal arriba (`nav.topnav`) en lugar del sidebar lateral (`index.html` + `base.css`). `<main>` a ancho completo.
- Tablas con scroll horizontal de respaldo (`.table-wrap{overflow-x:auto}`). Tabla Ventas ML responsive con fuente/padding fluidos (`clamp`), montos `nowrap`.

### Conciliación abril 2026 + diagnóstico de "cobradas sin conciliar"
- **Abril conciliado**: 2.012 de 3.134 ventas vía "Conciliar todas" (Modo Mes). Resto: 911 por cobrar (liquidan en mayo, AS no cargado), 46 cobradas sin conciliar, 165 canceladas.
- **Packs: descartado como problema.** Verificado al centavo: cada orden del pack tiene su `mp_payment_id` y concilia igual que una venta suelta (colecta 1:1, flex con bonificación). El código no los aparta. Tasa packs 766/1.172 (65%) ≈ no-packs 1.246/1.962 (64%). El síntoma era abril sin conciliar, no un bug. El caso "cecilia thiene" era un flex con bonificación grande, no un reparto.
- **Envío colecta — doble liquidación (nuevo pendiente diagnosticado).** Las colecta con envío a cargo del comprador generan en el extracto **dos liquidaciones con ids distintos**: producto (con el `mp_payment_id` de la venta) + pago del envío del comprador (otro id, no asociado a la venta en MP). `matchCobros` solo cruza por el id de la venta → no suma la del envío → "monto ≠". El `por_cobrar` está bien (= Total MP). Caso testigo `2000015791986818`: 26.287,93 (producto) + 5.218,77 (envío), con retención ~$10,77 a resolver. Ver regla O8 en `ADARA-DECISIONES.md` y `ADARA-VENTAS-ML-V22.md`. *(Nota jun 2026: resuelto — el $10,77 es IIBB del envío; handoff eliminado.)*
- `sbGet` confirmado que pagina (Range en loop); el cap de 1000 de Supabase no afecta esta pantalla.
- Detectado `tipo_envio='cross_docking'` sin mapeo explícito en el sync (packs de mayo) → pendiente confirmar, anotado en `ADARA-VENTAS-ML.md`.

### Pendiente
- **Envío colecta (doble liquidación)**: capturar/asociar la liquidación del envío + resolver la retención ~$10,77 para cerrar al centavo. Toca sync y/o matching → diseñar y acordar antes de codear.
- **Packs**: ~~no resuelto~~ → **resuelto, no había problema** (concilian solos).

Detalle completo: `ADARA-VENTAS-ML-V22.md`. *(Handoff eliminado en jun 2026; consolidado.)*

---

## Sesión 30 mayo 2026 — Módulo Compras + Padrón ARCA + fixes

### Compras locales (capa 2 — pantalla nueva)
- **Pantalla Compras** (`public/js/screens/compras.js`): 2 pestañas — **Facturas** (lista desde `v_compras_ap` + KPIs + alta + anular) y **Cuenta corriente** (saldo por proveedor uniendo compras + gastos). Registrada en `main.js` + sidebar.
- **Alta `POST /compras`** (atómico): cabecera (`tipo='local'`, `moneda='ARS'`, N° factura → `compras.notas`) + componentes producto (generan lotes con `costo_unitario` neto) + componentes fiscales (`iva`/`iibb_percepcion`/`ganancias_percepcion`). Rollback best-effort.
- **IVA automático por alícuota de SKU**: cada producto toma `skus.alicuota_iva`; selector 21/10,5/27/Exento editable por línea; el IVA se carga como crédito fiscal, no al costo del lote.
- **Anular compra `POST /compras/:id/anular`**: soft-delete con reversa de stock — `estado='anulada'` + motivo, borra lotes + componentes; **bloquea (409)** si hay stock consumido o pagos vinculados. La lista/KPIs/cuenta corriente filtran `estado_compra='anulada'`. Botón "Anular" por fila.
- Detalle en `ADARA-COMPRAS-IMPORTACIONES.md` › sección **Implementación v22**.

### Padrón ARCA (integración nueva)
- **Nuevo doc** `ADARA-ARCA-PADRON.md` (índice). Endpoint `GET /padron/:cuit` → razón social/tipo/estado desde el WS oficial de ARCA: WSAA (firma CMS del TRA con `node-forge`, servicio `ws_sr_constancia_inscripcion`) → `personaServiceA5 getPersona_v2`.
- Caché del Ticket de Acceso en memoria + tabla **`arca_ta`** (graceful). Normalizador de PEM (Railway aplasta saltos de línea). Endpoint diag `GET /padron-diag`. `node-forge ^1.4.0`.
- Variables Railway: `ARCA_CERT`, `ARCA_KEY`, `ARCA_CUIT=30717476472`. Certificado alias **ADARA-APP** (CUIT 30-71747647-2, vence **29/05/2028**), servicio "Consulta de constancia de inscripción" autorizado.
- **Autocompletado de proveedores**: en el alta rápida de Compras y Gastos, CUIT primero → al `blur` trae la razón social con cartelito `✓ NOMBRE · ESTADO`.

### Proveedores
- **`POST /proveedores` get-or-create por CUIT**: normaliza a 11 dígitos y reutiliza si ya existe (no duplica).
- **Anti-olvido**: al guardar una compra/gasto, si quedó un CUIT en el alta rápida sin tocar "Crear", el proveedor se crea/vincula solo (antes se guardaba sin proveedor).

### Fixes
- **Modal con scroll** (`public/css/base.css`): `.modal` con `max-height: calc(100vh - 40px)` + `overflow-y:auto`. Antes, modales más altas que la pantalla (Gastos) no dejaban llegar al botón. Afecta todas las modales.
- **`gastos.cuenta_origen_intencion` migrada de `bigint` a `text`** (error 22P02 al elegir cuenta origen). Migración con drop/recreate de `v_gastos_ap` (vía `pg_get_viewdef`) + re-grant. Detalle en `ADARA-GASTOS.md` › historial de errores.

### Pendiente de fondo
- **Importaciones multi-factura/multi-proveedor por despacho**: cómo capitalizar costos de varios proveedores en un mismo despacho. A resolver antes de construir importaciones. Documentado en `ADARA-COMPRAS-IMPORTACIONES.md`.

---

## Sesión 29 mayo 2026 (tarde) — Conciliación (Parte 1 + 2) + importadores MP/KPIs

### Conciliación (capa 4 — pantalla nueva)
- **Nuevo doc** `ADARA-CONCILIACION-BANCARIA.md` (índice #9, antes era placeholder): modelo, estados, pantalla, endpoints, transferencias internas, multi-vínculo, guardrails e historial de errores.
- **Pantalla Conciliación** (`public/js/screens/conciliacion.js` → `loadConciliacion()`): muestra **todos** los movimientos de la cuenta (no solo los accionables) con su estado, pills (Por conciliar / Conciliados / Auto / Todos), filtro de cuenta y KPIs. Registrada en `main.js` + sidebar.
- **Acciones por fila**: ✨ aceptar-sugerencia (match exacto de saldo gasto/compra) · **Vincular** (picker gastos+compras ordenados por cercanía) · **Transf. interna** · ✕ desvincular. Lo atado a venta se marca **"espera venta"** (lo concilia el futuro sync de ML).
- **Endpoints**: `POST /vincular`, `DELETE /vincular/:id`, `POST /transferencia-interna`.
- **Multi-vínculo (CB4)**: un pago que cubre varios gastos se resuelve incremental (parcial → cierra), sin UI especial.
- **Transferencias internas (CB8)**: par de vínculos cruzados `op_tipo='transferencia'` (o vínculo a sí mismo si no hay contrapartida). No tocan P&L/AR/AP. Se descartó la tabla agrupadora `transferencias` planeada.

### Importadores + KPIs (movimientos)
- **Importador Mercado Pago** (`POST /mp/import`, botón "Importar" con selector Supervielle/MP): parsea el Account Statement (preámbulo de resumen + cabecera real), oldest-first, idempotente por `referencia_externa = fecha|refid|monto|saldo`. Probado abril: 5.405 movimientos, 0 saltos de saldo. Solo Rendimientos e "Impuesto por extracción" quedan auto; el resto sin conciliar.
- **Fix filtro de KPIs por cuenta** en Movimientos: los KPIs y pills ahora derivan del set filtrado por cuenta + búsqueda; el estado solo filtra la lista.

### Decisión / fix crítico
- **Convención de signo de `vinculos.monto` = magnitud positiva**, y `v_movimientos_estado` recreada con `abs(monto) − Σ`. La vista original (signed) entraba en conflicto con `v_gastos_ap`/`v_compras_ap`/`v_ventas_ar` que imputan en positivo. Sin datos a migrar (`vinculos` estaba vacía). Documentado en `ADARA-SCHEMA.md` y en el historial de errores de `ADARA-CONCILIACION-BANCARIA.md`.

---

## Sesión 29 mayo 2026 — Pantalla Gastos v1 + Importador Supervielle

### Gastos (capa 5 — pantalla + fiscal)
- **Pantalla Gastos v1** (`public/js/screens/gastos.js` → `loadGastos()`): lista desde `v_gastos_ap` + KPIs (gastos del período, pendiente AP, USD sin TC, crédito IVA) + filtros (período / categoría / línea / estado / búsqueda) + modal de alta + anular con motivo. Registrada en `main.js` y en el sidebar de `index.html`.
- **Tabla `gasto_fiscal`** (nueva): renglones de retenciones/percepciones del gasto (`tipo` ∈ ret_ganancias/ret_iva/ret_iibb/ret_suss/perc_iva/perc_iibb/otro_ret/otro_perc, `clase` generated, `monto`). 
- **Vista `v_gastos_ap` recreada**: ahora calcula `a_pagar = bruto + percepciones − retenciones` y expone `total_factura_origen`, `a_pagar_origen`, `a_pagar_ars`, `percepciones_origen`, `retenciones_origen` además de lo previo (`bruto`, `vinculado_ars`, `saldo_pendiente_ars`, `estado_pago`).
- **Endpoint `POST /gastos`** (Express, A16): alta **atómica** — inserta el gasto + sus renglones `gasto_fiscal` + (si `sin_factura` + efectivo) un movimiento `origen='sin_factura_auto'` en caja con vínculo `op_tipo='gasto'`. Rollback best-effort.

### Movimientos (importador Supervielle + borrado)
- **Importador de extracto Supervielle** (`POST /supervielle/import`, botón "Importar Supervielle"): parsea el export de movimientos (.xlsx o .csv) → tabla `movimientos`, auto-clasifica el ruido (impuestos, comisiones, intereses, FCI → `conciliado_auto`) y deja los reales como pendientes. Idempotente por `referencia_externa = fecha|hora|monto|saldo`. Probado con abril: 99 movimientos, 0 saltos de saldo, 78 auto / 21 pendientes.
- **Chequeo de continuidad de saldo** como red de seguridad: si el saldo no encadena, avisa que faltan movimientos (confirmado que el export NO está topado en 100 — trae el mes completo).
- **Borrado de movimientos manuales**: ✕ en filas `origen='manual'`; `DELETE /movimientos/:id` con guarda en backend (rechaza no-manuales, limpia `vinculos`).
- Carga manual renombrada a **"Movimiento de caja"** y limitada a cuentas de caja.

### Decisiones del día
- **CB11**: banco/MP solo por importación de extracto; carga manual solo a caja (anti doble-cargado).
- **CB12**: solo se borran movimientos `origen='manual'`; los importados son inmutables y se restauran al re-importar.
- **G10**: retenciones/percepciones como renglones en `gasto_fiscal`; `a pagar = bruto + percepciones − retenciones`; alta atómica vía `POST /gastos`.

### Bug resuelto
- **Import CSV Supervielle**: acentos exportados como `?` (`D?bito`) rompían la detección de columnas, y `cellDates` invertía DD/MM. Fix: CSV en `latin1`+`raw:true`; detección de columnas y clasificador con regex tolerantes a `?`. XLSX y CSV dan idéntico.

---

## Sesión 28 mayo 2026 (2) — Pantalla Movimientos v1 + A2 revertida

### Lo nuevo
- **Pantalla Movimientos v1** (`public/js/screens/movimientos.js` → `loadMovimientos()`): lista del extracto unificado (tabla `movimientos`, capa 4) + filtros (cuenta / estado / búsqueda) + KPIs (movimientos, pendientes, neto ARS) + **carga manual** por modal. Registrada en `main.js` y en el sidebar de `index.html`.
- Carga manual: toggle **Entrada/Salida** (se ingresa el monto positivo, la app pone el signo), caja USD en **USD nativo**, todos los campos obligatorios.
- **Nuevo doc**: `ADARA-MOVIMIENTOS.md` (índice #18) — pantalla + reglas de carga + columnas reales de `movimientos`.

### Decisiones del día
- **A2 revertida**: el saldo de apertura al 31/12/2025 **no se carga a mano** desde planilla del contador. Se **deriva** por conciliación (`saldo real conocido − Σ movimientos cargados`) y se afina al cargar la historia 2026. Motivo: el saldo exacto al 31/12 no es reconstruible con precisión y pedirlo trababa el arranque. P&L / conciliación / AR/AP no dependen de él. Carga manual puntual queda como opción.
- **Línea de negocio en movimientos — opción A**: NO se almacena `linea_id` en el movimiento; se hereda de la operación vinculada al conciliar (CB6). Opción B (`linea_id` opcional para huérfanos) evaluada y descartada por ahora.
- **Carga manual de movimientos**: todos los campos obligatorios (cuenta/fecha/tipo/monto/categoría/descripción), cada uno valida con toast. Dedup: `referencia_externa` surrogate única (`manual-<uuid>`), **sin** dedup por contenido (dos movimientos iguales pueden ser reales); aviso al guardar si hay uno igual (cuenta+fecha+monto); lista pasiva "ya cargados ese día" (máx 6).

### Aprendizajes técnicos
- **`movimientos.referencia_externa` y `movimientos.categoria` son NOT NULL** (no estaban en la doc; se descubrió cargando, error `23502`). Fix: surrogate ref + categoría obligatoria. Lección reforzada: verificar `information_schema.columns` antes de codear inserts — el `details` del error de PostgREST ("Failing row contains") muestra el orden y los valores de todas las columnas.
- **Toast de error ilegible**: `.toast.error` usaba texto blanco sobre `var(--red)` (rojo claro de la paleta) y se lavaba. Fix en `base.css`: `background:var(--red-strong,#B42318)` con texto blanco.
- **Trampa GitHub web**: los archivos subidos caen en la carpeta donde estás parado. `movimientos.js` terminó en `public/js/` (debía ir en `public/js/screens/`) e `index.html` en `public/js/` (debía ir en `public/`). El router mostraba Home en `#movimientos` hasta moverlos a su lugar.
- Convención de entrega reforzada: **archivos completos siempre** (código y .md), nunca snippets ni diffs.

### Docs actualizados
- `ADARA-DECISIONES.md` — A2 reescrita + fila en "Decisiones revertidas".
- **Nuevo**: `ADARA-MOVIMIENTOS.md`.
- `ADARA-SCHEMA.md` — columnas reales de `movimientos` (NOT NULL), `saldos_iniciales` con A2, gotcha #12 (verificar NOT NULL), TBD del parser con ordinal para duplicados.
- `ADARA-FRONTEND.md` — pantalla Movimientos en implementadas, nota del toast/estilo propio, trampa de rutas GitHub.
- `ADARA-DOCS-INDEX.md` — registra `ADARA-MOVIMIENTOS.md` (#18), estado actual y A2.

### Estado al cerrar
- Frontend v22: **Home + SKUs + Movimientos** operativas. Carga manual de movimientos andando y validada (incluido el caso de duplicado).
- DB: 5 capas, sin cambios. RLS off.
- Datos: 145 SKUs clasificados + algunos movimientos de prueba en caja. Ventas/compras/gastos en cero.
- Próximo paso: pantalla **Gastos** (capa 5 ya en DB: `gastos` + `v_gastos_ap`).

---

## Sesión 28 mayo 2026 — Frontend v22 + catálogo cargado + pantalla SKUs

### Lo nuevo
- **Frontend v22 desplegado y andando**: estructura modular `public/{css,js/core,js/screens}`, sin build step, sin Node local. Se sirve desde `server.js` con `express.static('public')`. URL: `adara-backend-production.up.railway.app`.
- **Endpoint `/config`** en el backend expone `SUPABASE_URL` + `SUPABASE_ANON_KEY` al front (anon key separada de service_role).
- **145 SKUs cargados** desde "Control de stock Tango" (PDF de 158 productos): 138 con SKU + 7 del core sin SKU con código `TMP-<código_tango>` + 13 descartados (juguetería/servicios discontinuados).
- **Catálogo clasificado**: 138 electrónica, 2 luminaria, 4 mochila sindical, 1 mochila individual. 0 sin clasificar.
- **Pantalla SKUs** operativa: lista + buscador + pills de filtro + edición inline (familia/IVA/activo) + modal de alta. Guardado en vivo con toast.
- **Diseño claro aplicado** sobre el dark/amber del v21: fondo `#FAFAF9`, tipografía Manrope, KPI a 36px, base 15px, acento ámbar suave `#D97706`.
- **Router básico** Home ↔ SKUs por `#hash`.

### Decisiones del día
- **A15** — Frontend HTML modular sin build step.
- **A16** — Backend mixto (Supabase REST + Express).
- **Diseño claro confirmado** por el usuario.
- **Criterio de carga del catálogo**: SKUs del core sin código Tango → `TMP-<código>`. Productos no-core/descontinuados → no se importan.
- **RLS off** en todas las tablas de `public` (alineado con A6). Sin esto la app no podía leer aunque los datos estuvieran.

### Aprendizajes técnicos
- Supabase REST corta en **1000 filas sin avisar** → paginación obligatoria en el helper `SB.get` (ya estaba como aprendizaje del v21, ahora implementado).
- `Success. No rows returned` en el SQL Editor es el mensaje normal de INSERT sin RETURNING — NO significa que falló.
- Al subir archivos a GitHub web, si se arrastra la carpeta wrapper (con el nombre del ZIP), GitHub crea un nivel extra de jerarquía. Hay que entrar y arrastrar el contenido.
- La tabla `skus` real tenía dos columnas no documentadas (`activo`, `creado_en`). Antes de generar SQL contra tablas, verificar el Table Editor.

### Pendientes registrados
- **Reparto de costo de lote entre líneas** (caso 100 mochilas → 50 sindical + 50 individual): anotado en `ADARA-COMPRAS-IMPORTACIONES.md` para resolver al construir esa pantalla.
- **Caché de scripts en producción**: tras un deploy, conviene versionar `<script src="?v=...">` o agregar `Cache-Control: no-cache` para `/js/` en `server.js`.
- **Hashchange no refresca pantallas previas**: si SKUs creó un SKU nuevo y se vuelve al Home, el KPI no se actualiza. Pendiente de invalidación.

### Docs actualizados
- `ADARA-DECISIONES.md` — A15, A16.
- `ADARA-SCHEMA.md` — columnas reales de `skus`.
- `ADARA-COMPRAS-IMPORTACIONES.md` — pendiente reparto de costo entre líneas.
- **Nuevo**: `ADARA-FRONTEND.md` — arquitectura, convenciones, componentes, cómo sumar pantallas.
- `ADARA-DOCS-INDEX.md` — entrada para ADARA-FRONTEND.md.

### Estado al cerrar
- Backend: 5 capas en Supabase, sin cambios. RLS off.
- Frontend v22: Home + SKUs operativas. Router activo. Estilo claro.
- Datos cargados: 145 SKUs clasificados. Resto en cero (ventas, compras, gastos, movimientos).
- Próximo paso: pantalla **Saldos iniciales al 31/12/2025** (tabla `saldos_iniciales`).

---

## Sesión 27 mayo 2026 (3) — Capa 5 (Gastos) + pivote a frontend

### Capa 5 — Gastos (implementada)
- Tabla `gastos`: **atómica** (sin items — el gasto operativo es plano). Montos en moneda origen (`monto_neto` + `monto_iva` libres), `monto_bruto` generated. `genera_credito_iva` generated (`factura_a AND monto_iva > 0`).
- **Moneda USD soportada** con `tc` nullable al alta, completado al primer pago (regla operativa B1).
- **Intención de pago** persistida (`cuenta_origen_intencion`, `forma_pago`) pero NO fuente de verdad — el pago real vive en `vinculos` + `movimientos`.
- Vista `v_gastos_ap`: AP dinámico con conversión USD→ARS bajo `tc`, `estado_pago` derivado ∈ {`usd_sin_tc`, `pendiente`, `parcial`, `pagado`}.
- **Sin ALTER en capa 4**: `vinculos.op_tipo` ya aceptaba `'gasto'` desde la sesión anterior.

### Decisiones del día
- **G9**: gasto USD con `tc IS NULL` traba el cierre del mes (capa 9 lo enforcea cuando exista; mientras tanto, app-level).
- **A14**: diseño de DB se pausa después de capa 5. Capas 6-9 (Fiscal, Reclamos, Tesorería, Cierres) se construyen on-demand. **Próximo foco: frontend**.
- **Montos en moneda origen** libres (no alícuota fija) por dos motivos: redondeos del proveedor y no hay SKU que ancle la tasa en gastos operativos.
- **TC al primer pago** (Opción B1) por preferencia operativa: simpleza > pureza devengado. La trampa del cross-month se mitiga con G9.

### Aprendizajes técnicos
- Convención nueva: **"intención vs fuente de verdad"** como patrón de schema. Aplicable a futuros campos que asisten UI/matching pero no son la verdad final.
- El patrón de `compras` con `tc_blue` fijo (C6) se relaja en gastos: `tc` puede ser NULL al alta. Justifica una columna nueva en `gastos` que NO existe en `compras`.

### Docs actualizados
- `ADARA-DECISIONES.md` — G9 y A14 agregadas.
- `ADARA-SCHEMA.md` — sección "Capa 5 — Gastos" con tabla, vista, invariantes. Mapa de capas: 5 a ✅, 6-9 a ⏸ (pausa por A14). TBD actualizado.
- `ADARA-GASTOS.md` — nota técnica con detalle de implementación + G9 agregado a reglas duras.
- `ADARA-DOCS-INDEX.md` — "Estado actual" actualizado (5 capas hechas, 6-9 en pausa, próximo = frontend).
- Esta entrada de changelog.

### TBD registrados
- **Mov automático para `sin_factura` en efectivo**: lógica de aplicación pendiente cuando se construyan endpoints.
- **Validación FK app-level** de `vinculos.op_id` cuando `op_tipo='gasto'` (vínculo polimórfico sin FK declarativa, como el resto).
- **Trigger o validación** de cierre de mes con gastos USD sin TC (capa 9 futura).

### Estado al cerrar
Capas 1-5 corriendo limpio. Capas 6-9 documentadas y en pausa por A14. La DB ya cubre el ciclo operativo completo: venta → conciliación, compra → conciliación, gasto → conciliación, AR/AP dinámico para los 3.

**Próximo chat**: arquitectura del frontend.
- Decisión 1: stack (HTML monolítico estilo v21 vs framework moderno).
- Decisión 2: backend (endpoints Express vs Supabase REST directo).
- Decisión 3: home + módulo prioritario para arrancar (recomendado: conciliación, el corazón visual).
- Datos: empezar a cargar seeds operativos en paralelo (132 SKUs, saldos iniciales).

---

## Sesión 27 mayo 2026 (2) — Capas 3 (Ventas+CMV) y 4 (Movimientos+Conciliación)

### Capa 3 — Ventas + CMV (implementada)
- `ventas` (cabecera): canal, línea (con override), tipo_comprobante, `es_gravada` generada, estado (`pendiente`/`aprobada`/`entregada`/`cancelada`), cliente como campos libres. **No guarda totales** — viven en items.
- `venta_items`: cantidad, `precio_unitario_neto`, **`alicuota_iva` snapshot** desde `skus`. 5 totales por línea como generated columns con `round()` (zero-centavo accuracy).
- `consumo_lote`: traza FIFO con **snapshot del costo del lote** al momento de la venta (S2). Soporta reversos vía `unidades` negativas + `tipo` ∈ {`consumo`, `reverso_cancelacion`, `reverso_devolucion`}.
- Vistas: `v_ventas_totales` (agregado bruto/iva/neto por venta), `v_stock_check` (detecta drift entre `lotes.cantidad_actual` y la traza).
- **ALTER capa 1**: se agregó `alicuota_iva numeric(5,4) default 0.2100` a `skus`. La tasa de IVA es propiedad del producto (Argentina: 21%, 10.5%, 27%, exento).

### Cambio de diseño durante la capa 3
- Diseño original: IVA agregado en cabecera (`ventas.bruto`, `ventas.iva`, `ventas.neto` con CHECK).
- Cambio: IVA por ítem con `alicuota_iva` snapshotada desde `skus`. Motivo: en Argentina un mismo carrito puede tener productos a 21% y 10.5% simultáneamente. La cabecera ya no guarda totales — la vista `v_ventas_totales` los expone agregando desde items. Bonus: la DDJJ de IVA queda en una query trivial agrupando por `alicuota_iva`.

### Capa 4 — Movimientos + Conciliación universal (implementada)
- `movimientos`: extracto unificado de todas las cuentas. Monto **signed** (+/−). `origen` + `referencia_externa` UNIQUE para idempotencia. `categoria` con convención (cobro_venta, pago_proveedor, comision_*, transferencia_interna, etc., documentada en `ADARA-SCHEMA.md`). `conciliado_auto` para movs autoexplicativos sin vínculo (ej. comisión bancaria).
- `vinculos`: N:N **polimórfico**. `op_tipo` ∈ {`venta`, `compra`, `gasto`, `reclamo`, `transferencia`, `ajuste`} + `op_id` **sin FK formal**. Extensible sin ALTER. Soporta vínculos parciales (`monto` por vínculo).
- Vistas: `v_movimientos_estado` (estado derivado por mov: `auto`/`conciliado`/`parcial`/`pendiente`), `v_ventas_ar` (AR en vivo por venta), `v_compras_ap` (AP en vivo por compra, con conversión USD→ARS vía `tc_blue`).

### Decisiones del día
- **IVA por ítem con snapshot** desde `skus.alicuota_iva` en `venta_items` (no cabecera).
- **Vínculos polimórficos** (op_tipo + op_id sin FK) por sobre FKs explícitas por operación. Pro: extensible. Con: integridad por app.
- **Monto signed** en movimientos: una sola columna `monto` (+/−) en vez de `tipo` separado.
- **AR/AP 100% derivado**: ninguna columna almacenada de saldo, ni en ventas ni en compras ni en movimientos.

### Aprendizajes técnicos (sumados a `ADARA-SCHEMA.md`)
- `round(numeric, integer)` SÍ es IMMUTABLE — funciona en generated columns.
- CHECK no puede mirar otra tabla (ej. "si venta es remito → items.alicuota=0"); va al app o a trigger.
- Vínculos polimórficos = compromiso consciente: sin FK declarativa, integridad por app.

### Docs actualizados
- `ADARA-SCHEMA.md` — capas 3 y 4 documentadas (tablas + vistas + invariantes + nuevos gotchas).
- `ADARA-DOCS-INDEX.md` — "Estado actual" reescrito: 4 capas hechas, 5 a 9 por arrancar.
- Esta entrada de changelog.

### TBD registrados
- Mini-capa ML (deducciones de marketplace) — hasta entonces conciliación ML incompleta.
- Generación de `referencia_externa` para parser Supervielle (componer hash desde fecha+monto+descripcion).
- Transferencias internas (tabla agrupadora chica en capa Tesorería).
- Multi-currency en pagos (`v_compras_ap` asume vínculos en ARS).
- Posible 5ta cuenta `supervielle_usd` si la sub-cuenta operativa USD necesita tracking separado.

### Estado al cerrar
Capas 1-4 corriendo limpio. Próximo: **capa 5 — Gastos** (`gastos` + vinculación a movimientos vía la capa 4 que ya soporta `op_tipo='gasto'`). Punto natural para migrar a chat nuevo: se cerró una unidad coherente (movimientos+conciliación) con docs al día.

---

## Sesión 27 mayo 2026 — Reset de `public` + arranque del rediseño (capas 1-2)

### Decisión de arranque del rediseño
Se descartó el enfoque de tablas paralelas con prefijo `r_` (Decisión 0) y se hizo **reset completo** del schema `public` de Supabase. Justificación: la app v21 nunca estuvo realmente operativa en producción; conservar las tablas no aportaba valor y el prefijo dejaba deuda técnica (rename masivo futuro). Decisión registrada como **A13** en `ADARA-DECISIONES.md` + fila en "Decisiones revertidas".

### Capa 1 — Maestros / Dimensiones (implementada)
11 tablas + seeds básicos:
- `familias` (PK text, 4 seeds), `canales` (PK text, 4 seeds), `categorias_gasto` (PK text, 17 seeds)
- `lineas_negocio` (5 seeds), `lineas_negocio_reglas` (15 reglas familia×canal sembradas)
- `cuentas` (4 seeds: supervielle_ars, mp_ars, caja_ars, caja_usd)
- `skus` (catálogo vacío; sin costo ni stock — derivan de lotes)
- `proveedores`, `empleados`, `empleado_linea_pct` (reemplaza el viejo `sueldos_lineas`)
- `saldos_iniciales` (estructura lista, datos pendientes del contador)

### Capa 2 — Compras / Stock (implementada)
4 tablas:
- `compras` (cabecera con `tipo ∈ {local, importacion, inicial}`, USD obliga `tc_blue`)
- `compra_componentes` (14 tipos enumerados; columna `clase` generada en {costo, fiscal, sin_factura})
- `lotes` (running state `cantidad_actual` + `costo_unitario`; sin `linea_id` — stock por familia)
- `ajustes_inventario` (faltante/sobrante/compensacion/ajuste_costo; con `lote_contrapartida_id` para pares)

`consumo_lote` y la vista de verificación `v_stock_check` quedan diferidos a la capa 3 (Ventas), porque referencian `ventas(id)`.

### Convenciones de schema establecidas
- Schema `public` sin prefijo. RLS off (A6).
- PK por defecto `bigint generated always as identity`; lookups puros con PK text-código.
- `periodo` como **columna generada inmutable** vía `extract`+`lpad`. **Gotcha:** `to_char` está marcada STABLE en Postgres, no IMMUTABLE → rompe los generated columns. Patrón válido documentado en `ADARA-SCHEMA.md`.
- Columnas derivadas (ej. `clase`) vía generated columns para evitar drift.
- AR/AP jamás persistidos — siempre derivados (CB6/PT5).

### Docs actualizados
- **`ADARA-SCHEMA.md` (nuevo)** — schema técnico del rediseño: convenciones, mapa de capas, tablas implementadas, gotchas.
- `ADARA-DECISIONES.md` — A13 + fila en "Decisiones revertidas".
- `ADARA-DOCS-INDEX.md` — fila #16, advertencia v21 = referencia, "Estado actual" reescrito.
- `ADARA-LINEAS-NEGOCIO.md` — actualizada referencia `sueldos_lineas` → `empleado_linea_pct`.
- Esta entrada de changelog.

### Estado al cerrar
Capas 1-2 corriendo limpio. Próximo: **capa 3 — Ventas + CMV** (`ventas`, `venta_items`, `consumo_lote`, `v_stock_check`).
El código v21 (`server.js`, `adara-app-v21.html`) se conserva como referencia para reimplementar integraciones (ML, MP, Tango, Flex). Acciones de seguridad pendientes (rotar credenciales Tango, eliminar `/test/tango`) siguen abiertas. *(Nota 5/8/2026: `adara-app-v21.html` es legacy y NO corresponde a la app en producción — el repo real es `ADARAGROUP-Creator/adara-backend`.)*

---

## Sesión 19 mayo 2026 — Rediseño documental completo + integración Tango Factura

### Documentación del rediseño (14 docs nuevos)

Auditoría completa y refactor del modelo del proyecto. Se crearon 14 documentos del rediseño que cubren la próxima generación del sistema:

- **`ADARA-DOCS-INDEX.md`**: índice maestro con navegación
- **`ADARA-DECISIONES.md`**: "constitución" del proyecto con 78 reglas duras + 5 principios fundacionales + decisiones de arranque A1-A10
- `ADARA-FLUJO-OPERATIVO.md`: cadencia diaria + mensual, multi-logística
- `ADARA-LINEAS-NEGOCIO.md`: 5 líneas con asignación automática por familia × canal
- `ADARA-PNL.md`: estado de resultado con CMV exacto, pérdidas por mercadería como rubro propio
- `ADARA-IMPUESTOS.md`: ledger fiscal por impuesto, posición en vivo
- `ADARA-COMPRAS-IMPORTACIONES.md`: lotes con costos reales, gastos sin factura, FIFO
- `ADARA-STOCK.md`: lotes, consumo_lote, FIFO al consumir, ajustes
- `ADARA-CONCILIACION-BANCARIA.md`: conciliación universal + parser Supervielle (18 patrones documentados)
- `ADARA-INVERSIONES.md`: Tesorería General centralizada, rendimientos al P&L de ML Electrónica
- `ADARA-TFACTURA.md`: integración API con Tango Factura (Axoft)
- `ADARA-PATRIMONIAL.md`: Activo − Pasivo = Patrimonio Neto, snapshots mensuales
- `ADARA-RECLAMOS.md`: cuenta corriente con ML + Proveedores, 4 formas de recupero
- `ADARA-GASTOS.md`: 17 categorías, flujo proactivo/reactivo, gastos sin factura

### Docs eliminados (reemplazados)

- `ADARA-PNL.md` viejo (77 líneas) → nuevo (284 líneas)
- `ADARA-GASTOS.md` viejo (41 líneas) → nuevo (364 líneas)
- `ADARA-IMPUESTOS.md` viejo (86 líneas) → nuevo (256 líneas)
- `ADARA-IMPORTACIONES.md` viejo (26 líneas) → `ADARA-COMPRAS-IMPORTACIONES.md` (377 líneas)

### Integración API Tango Factura validada

Se confirmó técnicamente la integración con Tango Factura (Axoft) a través del endpoint de prueba `/test/tango` en Railway:

- **Auth**: `POST /Provisioning/GetAuthToken` con `{UserName, Password, UserSecret}` donde **UserSecret = UserIdentifier** (no la PublicKey). Response es **string URL-encoded directo**, no objeto `{Data}`. Hay que `decodeURIComponent`.
- **ListarMovimientos**: trae lista de facturas en rango de fechas con `Tope` (límite).
- **ObtenerInfoMovimiento**: trae detalle completo con `ObtenerInfoAplicaciones: true` (flag obligatoria).

**Hallazgo clave**: el campo `DatosAplicacionExterna` en el detalle del movimiento contiene:
- `AplicacionNombre`: "Mercado Libre" (o "Tienda Nube" TBD)
- `ExternalID`: **= `ml_order_id`** — vincula la factura Tango con la venta ML en ADARA
- `DatosComprador`: JSON con info del comprador (DNI, nickname)
- `DatosPago`: JSON con info del pago en MP (incluyendo `mp_payment_id`, installments, payment_method)

Esto resuelve el modelo de sync Tango ↔ ADARA sin necesidad de tabla de equivalencias manual. **(Nota 4/8/2026: implementado en producción — ver la entrada del 4/8 arriba. Ojo: los parámetros reales de `ListarMovimientos` son `Desde`/`Hasta`/`Tope`, no `FechaComprobante`; y las variables de Railway son `TF_*`, no `TANGO_*`.)**

### Docs del v21 actualizados (esta sesión)

Para reflejar coexistencia con el rediseño:
- ADARA-SYSTEM.md: header v21, referencias al rediseño, mención de cambios futuros en DB
- ADARA-CURRENT-STATE.md: agregada sección "Rediseño — estado de la documentación"
- ADARA-CHANGELOG.md: esta entrada
- ADARA-VENTAS-ML.md, ADARA-FLEX.md, ADARA-CANCELACIONES-DEVOLUCIONES.md, ADARA-MOV-SIN-CONCILIAR.md: headers v21

### Estado al cerrar

Sistema v21 sigue operativo. Rediseño completamente documentado, pendiente de implementación. Próximos pasos: carga inicial al 31/12/2025, reunión con contador, diseño DB del rediseño.

### Acciones de seguridad pendientes para Sebastián

- Eliminar app "ADARA Integración" actual en Tango (credenciales fueron pegadas en chat varias veces)
- Crear nueva app, autorizarla, actualizar 4 variables en Railway (las **`TF_*`**: TF_USERNAME, TF_PASSWORD, TF_APP_KEY, TF_USER_ID)
- Borrar endpoint `/test/tango` de `server.js`
- Confirmar que `.env` (si existe local) está en `.gitignore`

---

## Sesión 31 marzo 2026 — PSI Recompra + Órdenes de Compra + Carga Stock

### Nuevas pantallas

1. **PSI Recompra** — Planificación de recompra por SKU.
   - Rango de fechas personalizable, días objetivo configurable
   - Tabla: SKU, columnas por semana, Prom/sem, Stock, 🚚 Viaje (en tránsito), Días stock, Recompra, Alerta
   - KPIs: SKUs activos, Uds vendidas, Crítico (<7d), Bajo (<14d), Sin stock
   - Fórmula recompra: `max(0, ceil(vel_diaria × dias_objetivo) - stock - en_transito)`
   - Ordenamiento: rojos primero → naranjas → verdes
   - Exportación Excel con SheetJS
   - Leyenda explicativa al pie de la tabla
   - Integrada con Órdenes de Compra (columna En tránsito)
   - Ubicación: OPERACIONES, después de Catálogo SKUs

2. **Órdenes de Compra** — Tracking de stock en tránsito.
   - Tabs: En viaje / Recibidas / Todas
   - KPIs: órdenes en viaje, uds en tránsito, recibidas
   - Formulario: proveedor (con creación rápida "+ Nuevo"), tipo (import/local), medio envío (✈/🚢/🚚), fecha llegada, monto, moneda (USD/ARS), notas
   - SKUs dinámicos (dropdown catálogo + cantidad + costo unitario)
   - Fila expandible con detalle de SKUs
   - Botón "📦 Recibir" → suma stock automáticamente a catalogo_skus
   - Botón cancelar orden
   - Tablas: ordenes_compra + orden_compra_skus
   - Ubicación: OPERACIONES, después de PSI

### Nuevas tablas (DB)

```sql
ordenes_compra (id, fecha, proveedor_id, tipo, medio_envio, fecha_estimada_llegada, fecha_recepcion, estado, monto_total, moneda, notas, created_at)
orden_compra_skus (id, orden_compra_id, sku, cantidad, costo_unitario)
```

### Carga de stock desde Tango Factura

- PDF "Control de stock" (31/03/2026) exportado de Tango Factura
- 132 productos procesados, campo "Disponible" usado como stock
- INSERT masivo en catalogo_skus con ON CONFLICT DO UPDATE
- 7 items sin SKU en Tango → SKUs generados (IGN150, IGN200, JBL125BT, VOOX750, MOCHMETRO, VOOXVASO, REPEL)
- 2 SKUs nuevos creados: 65 (Redmi Buds 8 Active Black), 65B (Redmi Buds 8 Active White)
- Nuevo proveedor: AODELI-KAVEH (tipo exterior)
- Primera orden de compra registrada: 19 SKUs, importación aérea, USD

### Frontend

- Pantalla PSI con leyenda explicativa
- Pantalla Órdenes de Compra con tabs y formulario completo
- Quick-add proveedor desde formulario de OC (sin salir del modal)
- Sidebar: 2 items nuevos en OPERACIONES (PSI Recompra, Órdenes de Compra)

### Archivos modificados

- adara-app-v21.html: 2 pantallas nuevas, PSI con integración OC, sidebar, nav handlers
- ADARA-CURRENT-STATE.md, ADARA-CHANGELOG.md, ADARA-STOCK.md, ADARA-SYSTEM.md

### Estado al cerrar

PSI y Órdenes de Compra funcionales. Stock cargado desde Tango. Primera OC en viaje.
Pendiente: deployar server.js (fix balance devoluciones de sesión 24/03), recalcular balance Enero, descuento automático de stock en sync ML.

---

## Sesión 24 marzo 2026 (2) — Fix balance devoluciones + Guardrails + Auto-descarte

### Bugs resueltos
1. **Balance devoluciones incorrecto** — `esperado = por_cobrar` no funciona para devueltas (sum_movs ≈ 0 pero esperado > 0 → balance negativo). Fix: fórmula dinámica `esperado = por_cobrar + sumDev` donde sumDev se calcula desde movimientos vinculados con categoria IN (devolucion, venta_cancelada, cargo_envio_devolucion).
2. **Descartadas corrompidas por recalcular-balance** — endpoint no excluía descartadas → las pisaba a parcial. Fix: `if (estado_conciliacion === 'descartada') continue`.
3. **Tolerancia inconsistente** — vincular/desvincular usaban `balance === 0`, resto usaba `< 0.02`. Fix: unificado a `Math.abs(balance) < 0.02` en los 4 puntos.
4. **Cancelaciones sin entrega en Conciliación** — sync no auto-descartaba + queries no excluían descartadas. Fix: auto-descarte post-sync + `estado_conciliacion=neq.descartada` en tab Todas, KPI y ct-todas.

### Error de fix v1 (revertido)
Primer intento usó campos almacenados (monto_reembolso, cargo_envio_devolucion) para calcular esperado. Esos campos estaban desincronizados con los movimientos reales → rompió 63 ventas. Revertido con fix v2 (dinámico).

### Guardrails técnicos
Sección "⚠ Guardrails técnicos" agregada a ADARA-CONCILIACION.md con invariantes, errores conocidos e historial. Principio genérico agregado a ADARA-SYSTEM.md.

### Frontend
Botón "🔄 Recalcular balance" en toolbar de conciliación.
Tab Todas + KPI + ct-todas ahora excluyen descartadas.

### SQL ejecutado
```sql
UPDATE ventas_ml 
SET estado_conciliacion = 'descartada', conciliado = false, balance_conciliacion = 0
WHERE ml_status = 'cancelled' 
  AND estado_envio IN ('no_preparado', 'preparado')
  AND estado_conciliacion != 'descartada';
```

### Archivos modificados
- server.js: fórmula balance dinámica (4 puntos), descartadas excluidas, tolerancia unificada, auto-descarte post-sync
- adara-app-v21.html: botón recalcular balance, exclusión descartadas en queries frontend
- ADARA-CONCILIACION.md: sección Guardrails, auto-descarte, endpoint recalcular-balance
- ADARA-SYSTEM.md: principio guardrails por dominio
- ADARA-DECISIONES.md, ADARA-CURRENT-STATE.md, ADARA-DOCS-INDEX.md, ADARA-CHANGELOG.md, ADARA-CANCELACIONES-DEVOLUCIONES.md

### Estado al cerrar
Deployar server.js + frontend → sync cualquier mes → verificar descartadas no aparecen → recalcular balance Enero → verificar conciliación.

---

## Sesión 24 marzo 2026 — Reestructuración docs + Devol/Canc completo

### Reestructuración documentación
Auditoría completa de 16 archivos .md. Reglas duplicadas consolidadas en DECISIONES.md como fuente única de verdad. Total líneas: 2977 → 1435 (-52%). Eliminado ADARA-AI-NAVIGATION.md.

### Bugs resueltos
1. **periodo_cobro null persistente** — `allKeys.delete('periodo_cobro')` eliminaba el campo del upsert → ventas nuevas no lo recibían. Fix: quitar exclusión, periodo_cobro se incluye en upsert.
2. **Tab Todas mostraba descartadas** — cancelaciones descartadas aparecían en Conciliación. Fix: query excluye `estado_conciliacion=descartada`.

### Devol/Canc — reescritura completa
1. **Tabs unificados**: 3 tabs (Devol/Canc/Resueltas) → 2 tabs (Pendientes/Resueltas). Todo en una tabla.
2. **Clasificación por tipo**: Cancelación / Entrega fallida / Devol. comprador / Devolución (claim).
3. **Claims API ampliada**: opened + búsqueda por resource_id para huérfanas.
4. **Substatus shipment**: sync captura substatus como motivo_cancelacion para entregas fallidas.
5. **Estado en_gestion**: botón 📞 con nota para ventas perdidas/en reclamo. Después → ✅ Aprobar.
6. **aprobarParaConciliar**: usa v.periodo (mes original), no topbar. Venta se arrastra con → Pasar.
7. **Botón Nota 📝**: disponible en todas las filas. Se muestra en Devol/Canc y en Conciliación.
8. **Auto-load**: al navegar a Devol/Canc ejecuta fetchDevolucionesML automáticamente.
9. **UI mejorada**: fondo expandible legible (var(--card) + borde accent), detalle adaptativo sin campos vacíos.

### SQL ejecutado
```sql
UPDATE ventas_ml SET periodo_cobro = periodo WHERE periodo_cobro IS NULL;
```

### Archivos modificados
- server.js: periodo_cobro upsert, Claims API resource_id, substatus shipment, debug/claim endpoint
- adara-app-v21.html: tabs unificados, tipoBadge, motivoCol, buildDetailRow, en_gestion, aprobarParaConciliar, nota, tab Todas excluye descartadas
- 15 archivos .md reestructurados

### Estado al cerrar
Enero: 1510 conciliadas + 213 pendientes (devoluciones con balance incorrecto).
Febrero/Marzo: sincronizados parcialmente.
Próximo: fix balance devoluciones → conciliar Enero → Febrero → Marzo.

---

## Sesión 23 marzo 2026 — Debugging Febrero + Fixes críticos

### Bugs resueltos
1. **periodo_cobro null en sync** — Fix: periodo_cobro = periodo en sync + excluido del upsert.
2. **Bonificaciones cross-month** — Fix: filtrar desde movs (batch actual).
3. **Colisión posiciones entre meses** — Fix: solo indexar batch actual.
4. **autoConciliarMP con rango fechas** — nuevo parámetro fechaDesde/fechaHasta.
5. **Límite 3 meses en → Pasar** — validación agregada.

### Reset completo DB
DELETE FROM movimientos_mp + ventas_ml. Resultado: Enero 1510/1671 conciliadas.

---

## Sesión 21 marzo 2026 — Fixes Flex + multi-mes

### Bugs resueltos
1. **Botón → Pasar silencioso** — Fix: data-* attributes.
2. **cargo_envio Flex incorrecto** — Fix: + shipping_amount del comprador.
3. **Tab Todas no mostraba arrastradas** — Fix: OR(periodo, periodo_cobro).

---

## Sesión 20 marzo 2026 — Cancel/Devol + v21

POST /ml/recepcion, stock_devoluciones, bonificacion_cancelada, cancelaciones sin filtro período, descarte masivo, botón editar venta, sticky headers, bump v21.

---

## Sesión 19 marzo 2026 — Bonificaciones posición + periodo_cobro

Matching posición+monto, financing_add_on_fee vs financing_fee, bug impuestos payer, periodo_cobro + → Pasar.

---

## Sesiones anteriores (resumen)

### 13 marzo — Paginación Supabase
De 125 a 1338 conciliadas. Matching sin filtro categoría. Bonificaciones por monto neto.

### 12 marzo — Conciliación V2
Modelo datos V2, filtro aprobada=true, tabs, endpoints manuales.

### 11 marzo — Devol/Canc UI + Filtros Excel

### 9 marzo — P&L, Gastos, Líneas, Sync fixes
Decisiones técnicas base establecidas.
