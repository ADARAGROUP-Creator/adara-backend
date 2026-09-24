# ADARA — Índice de Documentación

Última actualización: 23 Septiembre 2026 (**Simulador de importaciones validado contra despachos oficializados + zonificación MEF de Flex separada en doc propio.** (1) **Prorrateo del flete corregido**: los despachos **26073IC04002688** (real) y **26073IC04002690** (declarado) de BISHOP agosto prueban que Aduana prorratea **flete y seguro por FOB, no por peso** — factor idéntico 1,054586 en los tres ítems. `ADARA-IMPORTACIONES-SIM.md` decía "por peso" y estaba mal; el código ya lo hacía bien. El reparto por peso (`fleteCostoShare`) se usa **solo para el costeo del lote**. (2) **Caso testigo BISHOP id 11**: ahorro oficial derechos+estadística **USD 14.639,57** → coima 30% **USD 4.391,87**; la app con los datos del despacho da 14.631,35 (gap de USD 8 por el FOB escalado del despachante). **Regla nueva: la sim se valida contra el despacho oficializado, no contra planillas** — la sim tenía flete 4.731,85 contra 5.747,70 real y una declaración distinta de la oficializada. (3) **La base del ahorro es CIF, no FOB**: calcular sobre FOB subestima ~4,5%. (4) Documentado el schema **`datos v:3`** del simulador (`sku_id`, `tope_estadistica_usd`, `iibb_monto`, `ganancias_monto`, `cuenta` en pagos). (5) **Flex — cascada de resolución de zona**: lat/long → Georef → partido, con caché **`flex_localidad_partido`** (tabla a crear); el nombre de la localidad nunca decide solo. Lista de **localidades ambiguas** (Villa Adelina, Gerli, Canning, Tortuguitas, El Palomar, San Francisco Solano, Nordelta) que se resuelven siempre por lat/long. (6) **La Tablada = MATANZA NORTE (GBA 1)**, pendiente resuelto por etiqueta MEF. (7) Doc nuevo **`ADARA-FLEX-ZONAS-MEF.md`** con las 26 zonas, el diccionario localidad→partido, el split Matanza Norte/Sur, las normalizaciones ZPL y las excepciones comerciales.) · 10 Agosto 2026 (**Módulo de IIBB de Convenio Multilateral implementado + impuesto al cheque en el Resultado → los márgenes reales quedaron 3 a 5 puntos abajo de lo que se venía publicando.** (1) **El Resultado no tenía el IIBB a pagar**: restaba la **retención** de ML (~1,05 % del ingreso neto) en vez del **impuesto determinado** (~5,29 %). (2) **Fuente**: DJ Mensual **CM03**, anticipo **202606**, determinado total **$20.765.310,42** sobre **24 jurisdicciones** (Buenos Aires 40,11 % + CABA 38,79 % = **78,9 %** del impuesto). **Hallazgo técnico clave**: el CM03 no expone base ni coeficientes por separado, pero como `determinado_j = base × coef_j × alicuota_j`, el cociente `determinado_j / base` **ya es el factor completo** — nunca hizo falta separar coeficiente de alícuota. (3) **Implementación**: tablas `iibb_parametros` (mensual, con `alicuota_efectiva` GENERATED) e `iibb_jurisdiccion` (24 renglones del CM03), ambas con RLS; vistas `v_iibb_base`, `v_iibb_determinado`, `v_iibb_jurisdiccion_mensual`, `v_impuesto_cheque_mensual`; `v_resultado_mensual` y `v_resultado_linea_mensual` **extendidas de forma aditiva**. Junio reproduce **$20.765.310,42 exacto** contra el CM03. (4) **Márgenes reales corregidos**: enero 11,17 %→**7,00 %**, febrero 14,05 %→**9,83 %**, marzo 11,24 %→**7,06 %**, abril 8,09 %→**3,45 %**, mayo 7,37 %→**2,37 %**, junio 6,38 %→**1,59 %**, julio 8,70 %→**4,16 %**, agosto parcial 11,82 %→**7,38 %**. Patrón: **cuanto más vende, peor margen** — de enero a junio la facturación se multiplicó por **3,7** y el margen cayó de 7,00 % a 1,59 %. (5) **Impuesto al cheque incorporado al Resultado**: antes no estaba en ningún lado, **$8.205.589,40** acumulados desde marzo. (6) **Bug corregido — `iibb_tucuman` en el campo jurisdicción**: las retenciones del régimen de recaudación de Tucumán entraban con `tipo='iibb_tucuman'` y `jurisdiccion` NULL, `v_retenciones_iibb` caía al COALESCE con `financial_entity` y partía Tucumán en dos. Backfill de **9.980 filas** + la vista ahora deriva la jurisdicción sin depender de la ingesta. Tucumán unificado en **$157.443,45**. (7) **LIMITACIÓN VIGENTE**: la alícuota efectiva de **5,2852 %** es un **techo** — la base declarada del CM03 incluye facturación **B2B de luminarias** que ADARA no tiene cargada, así que hoy la línea ML absorbe también el IIBB del B2B. Se destraba cargando la base imponible real (un UPDATE de una línea). (8) **Hallazgo**: el sync de Tango **ya trae todas las facturas** con `ListarMovimientos` y pide el detalle de cada una, pero **descarta** las que no son de Mercado Libre — la cañería para conocer la facturación B2B ya existe. (9) **$2.867.481,49 de saldos a favor de IIBB inmovilizados** (Santa Fe, Santa Cruz, Corrientes, Catamarca, La Pampa, Río Negro, Tierra del Fuego). (10) **Brecha de retenciones**: el CM03 computó **$15.471.267,94** de "Valores Restan" en junio y ADARA ve $1,53M (settlement MP) + $4,35M (API de ML) — falta casi seguro **SIRCREB**, que no se ingesta. (11) Verificado el circuito de **compra sin comprobante** (C20, del 7/8): está sano de punta a punta. (12) Análisis: **comprar con 50 % facturado y 50 % en efectivo NO conviene** — por cada peso pagado sin comprobante se pierden ~45,5 centavos en impuestos. Doc nuevo: **`ADARA-IIBB-CONVENIO-MULTILATERAL.md`**.) · 7 Agosto 2026 (**Sesión larga de implementación: token ML reparado · venta en efectivo · buscador de SKUs · compra sin comprobante · Fase 2 del simulador especificada + exportadores.** (1) **Token de ML — causa raíz encontrada y arreglada.** No era el sync: `workspace_config.ml_refresh_token` estaba en **NULL**, así que el access token moría cada ~6 h y `refreshML()` hacía un `return` mudo. Tres bugs corregidos: el `return` silencioso, `/ml/callback` que pisaba con NULL el refresh existente, y la pantalla de éxito que decía "conectado correctamente" aunque ML no hubiera devuelto refresh token. Cron de 6 h → 1 h con guard de 2 h; `/ml/status` con estado real; `/health` con bloque `ml`; **banner en la home**. Paso manual que sólo puede hacer Sebastián: habilitar **`offline_access`** en el panel de desarrolladores de ML. **Resultado: el sync volvió a correr.** (2) **Venta en efectivo sin comprobante** — doc nuevo `ADARA-VENTAS-EFECTIVO.md`, canal `efectivo`, endpoint atómico `POST /ventas/efectivo`, botón en Movimientos, y **filtro `FILTER (WHERE v.es_gravada)` en `v_control_mensual`** para que el IVA débito dependa del tipo de comprobante y no de la alícuota tipeada. Reglas **V1–V3**. (3) **Buscador de SKUs** (`core/skuPicker.js`, componente nuevo) en la venta en efectivo y en los ítems de compra: con ~190 SKUs el `<select>` era inusable. (4) **Compra sin comprobante**: columna `compras.sin_comprobante` + CHECK + **trigger** que rechaza componentes fiscales, check en el alta, badge propio en la lista. Distingue "sin comprobante" de "factura pendiente", que antes eran indistinguibles. (5) **Fase 2 del simulador especificada** (IMP-F2-1…8) con el hallazgo bloqueante: los productos del JSONB **no tienen `sku_id`**. Primera entrega hecha (`sku_id` + cuenta en pagos, `datos v:3`). (6) **Fix del `declaro_distinto`**: el motor usaba los overrides aunque el check estuviera destildado → subdeclaraba en silencio; además no limpiaba `cantidad_decl`. (7) **Exportadores** PDF y Excel comparativo declarado vs real en el simulador. Hallazgo de datos: **el 86,7 % del CMV de julio es estimado** ($298,1M de $343,9M) — 18 SKUs sin ninguna compra cargada + 16 con lotes agotados.) · 5 Agosto 2026 (**Cadencia operativa redefinida + `ADARA-FLUJO-OPERATIVO.md` reescrito a v22**: se detectó que el proceso **nunca tuvo un momento asignado para cargar facturas de compra y de gasto** — de ahí que el crédito IVA de gastos esté en **$0 en todo el histórico**. La app quedó muy buena leyendo lo que entra solo por API (ML, Tango, MP) y ciega para todo lo que entra en papel. Se definió con Sebastián una cadencia nueva: **automático** (sync ML, sync Tango cada 3 h, proyección, FIFO, congelamiento de CMV, retenciones IIBB) · **diario equipo ≈5 min** (bandeja de mail → cargar gastos con factura A; devoluciones físicas) · **diario Sebastián ≈5 min** (compras de mercadería; tira de Posición Fiscal) · **semanal** (Flex vs. resumen del proveedor; PSI) · **mensual** (tablero de conciliación ML, extractos Supervielle + MP, barrido de facturas faltantes, Resultado y Posición Fiscal). Decisiones operativas nuevas **FO1–FO5**. Guardarrieles pendientes: fecha = la del comprobante con aviso si cae en mes anterior, freno para no cargar la factura de ML como gasto, selector de tipo de comprobante en criollo. Docs actualizados: COMPRAS-IMPORTACIONES, GASTOS, COSTEO-FIFO, SCHEMA, DECISIONES, CHANGELOG, IMPUESTOS, STOCK, MOVIMIENTOS, FRONTEND, FLUJO-OPERATIVO.) · 4 Agosto 2026 (**Integración Tango Factura IMPLEMENTADA Y VALIDADA**: auth destrabado con variables **`TF_*`** y endpoint `/Provisioning/GetAuthToken`; causa raíz de la lentitud — `ListarMovimientos` recibía los parámetros equivocados en vez de **`Desde/Hasta/Tope`**. Sync que pega **N° de factura + tipo (A/B/C) + total facturado** a cada venta ML por `ml_order_id ↔ ExternalID`. Ver `ADARA-TFACTURA.md`.) · 3 Agosto 2026 (**Apertura fiscal de IVA IMPLEMENTADA + carga de compras/gastos de julio + 20 SKUs nuevos**: tabla `posicion_fiscal_apertura` + `v_posicion_fiscal` con **doble arrastre**. Corte 30/06/2026; junio como total de la DDJJ (F.2051): saldo técnico a favor **$13.808.267,01**, libre disponibilidad **$26.306,80**. 20 SKUs nuevos → neto julio $214M→$384M.) · 22 Julio 2026 (**Simulador — fix flete fuera del ahorro/coima**, caso HOKU. P15 corregida.) · 13 Julio 2026 (**Conciliación manual bidireccional de ventas ML — tablero mensual**, regla O13 · **🧹 Limpieza de documentación** + `ADARA-MANTENIMIENTO-DOCS.md`.) · 7 Julio 2026 (**Simulador de Importaciones**: P14, P15, P16, IMP-SEGURO; **ML-BON1**.) · 21 Junio 2026 (**Flex** partido→zona por Georef F7 · **Canceladas/devoluciones en el Resultado** P13 · **Adjuntos** + dominio Flex · **Gastos** reparto por imputaciones G12) · 13 Junio 2026 (**Saldos** · **Cuadre** · fix token ML `workspace_config` · bonificaciones Flex · **PSI Fase 2**) · 11 Junio 2026 (**login multiusuario** A17 · compras con prorrateo · PSI)

> 📂 **Dónde viven estos documentos (desde el 24/9/2026).** La documentación está versionada en el repo **`ADARAGROUP-Creator/adara-backend`**, carpeta **`docs/`**. Esa es la fuente de verdad: cada cambio de regla queda con su commit, y los dos agentes que trabajan el proyecto (Claude Code y Codex) la leen desde ahí. En la raíz del repo están **`CLAUDE.md`** y **`AGENTS.md`**, gemelos entre sí, con las reglas duras y el protocolo de trabajo; apuntan a este índice. El prefijo `claude/` que tenían tres documentos **se eliminó**: todos los `.md` viven planos en `docs/`.

Punto de entrada al proyecto ADARA. Si sos una conversación nueva con Claude (o un developer/colaborador que recién se suma), **leé este doc primero**.

---

## Bienvenida

ADARA es la aplicación de gestión integral del negocio de Sebastián: ecommerce argentino con 5 líneas de negocio (electrónica en ML, electrónica off-ML, luminarias, mochilas sindicatos, mochilas individuos). Único CUIT, IVA RI, Convenio Multilateral, Ganancias inscripto.

La app cubre: sync de ventas (ML + Tango Factura), conciliación universal (banco + MP + caja), stock con costos reales FIFO, P&L mensual por línea, posición fiscal en vivo, estado patrimonial, reclamos a ML y proveedores, gastos, e integración con tesorería e inversiones.

**Lo que vivís leyendo estos docs es un rediseño completo en curso** (iniciado en mayo 2026). El sistema actual (versión v21) funciona pero tiene limitaciones que estamos resolviendo. El rediseño se está documentando primero y se implementa en paralelo (ya hay frontend v22 andando).

---

## Lectura recomendada (orden sugerido)

### Para entender de qué se trata el proyecto

1. **Este índice** (ya estás acá)
2. **`ADARA-MANTENIMIENTO-DOCS.md`** — protocolo de mantenimiento de la documentación (cómo leer y actualizar los docs entre chats)
3. **`ADARA-DECISIONES.md`** — la "constitución": reglas duras + 5 principios fundacionales + decisiones de arranque (A1–A17)
4. **`ADARA-FLUJO-OPERATIVO.md`** — operatoria diaria, qué pasa cada día / mes. **Reescrito a v22 el 5/8/2026: vuelve a ser confiable como lectura inicial.**

### Para entender el modelo de negocio

5. **`ADARA-LINEAS-NEGOCIO.md`** — cómo se desagrega todo en 5 líneas

### Para entender la mecánica financiera

6. **`ADARA-PNL.md`** — estado de resultado mensual
7. **`ADARA-PATRIMONIAL.md`** — valor de la empresa (foto)
8. **`ADARA-CONCILIACION-BANCARIA.md`** — conciliación universal

### Para construir frontend

9. **`ADARA-SCHEMA.md`** — schema real implementado (capas 1-5)
10. **`ADARA-FRONTEND.md`** — arquitectura del frontend v22
11. **`ADARA-MOVIMIENTOS.md`** — pantalla Movimientos + reglas de carga manual

### Para temas específicos

Los demás docs cubren dominios puntuales (ver lista completa abajo).

---

## Lista completa de documentos

> 📝 **Actualizados en la sesión del 7/8/2026:** `ADARA-VENTAS-EFECTIVO.md` (**nuevo**), `ADARA-IMPORTACIONES-SIM.md` (spec Fase 2), `ADARA-DOCS-INDEX.md`.
> ⏳ **Pendientes de propagar de esa misma sesión:** `ADARA-DECISIONES.md` (V1–V3, C20, IMP-F2-1…8), `ADARA-SCHEMA.md` (canal `efectivo`, FK `ventas.canal→canales`, `compras.sin_comprobante` + trigger, filtro de `v_control_mensual`, `v_compras_ap`), `ADARA-VENTAS-ML.md` (circuito de token), `ADARA-FRONTEND.md` (skuPicker, banner de home, exportadores), `ADARA-COMPRAS-IMPORTACIONES.md` (compra sin comprobante), `ADARA-CHANGELOG.md`, `ADARA-COSTEO-FIFO.md`, `ADARA-IMPUESTOS.md`, `ADARA-MOVIMIENTOS.md`.

### Documentos del rediseño (la nueva versión que se va a implementar)

| # | Doc | Propósito |
|---|-----|-----------|
| 1 | `ADARA-DOCS-INDEX.md` | Este índice — entrada y navegación |
| 2 | **`ADARA-DECISIONES.md`** | **Reglas duras + decisiones consolidadas** (constitución del proyecto) |
| 3 | `ADARA-FLUJO-OPERATIVO.md` | **v22 (5/8/2026)** — cadencia automática / diaria / semanal / mensual, decisiones operativas FO1–FO5, guardarrieles de la carga delegada, estado real, pain points, canales, líneas, cuentas, multi-logística |
| 4 | `ADARA-LINEAS-NEGOCIO.md` | 5 líneas + reglas de imputación familia × canal + saldo por línea |
| 5 | `ADARA-PNL.md` | Estado de resultado: ventas netas, CMV exacto, margen, gastos, resultado por línea |
| 6 | `ADARA-IMPUESTOS.md` | Ledger fiscal: IVA, IIBB, Ganancias, posición en vivo, retenciones/percepciones. **Apertura fiscal implementada (corte 30/06/2026 + doble arrastre).** |
| 7 | `ADARA-RETENCIONES-IIBB.md` | **(Implementado)** Retenciones IIBB por jurisdicción desde el settlement de MP; tabla `retenciones` + vista `v_retenciones_iibb` |
| 8 | `ADARA-COMPRAS-IMPORTACIONES.md` | Modelo de lotes con costos reales, gastos sin factura, prorrateos, FIFO. **Falta documentar `sin_comprobante` (7/8).** |
| 9 | **`ADARA-IMPORTACIONES-SIM.md`** | **Simulador what-if de importaciones** (Fase 1 ✅). **Fase 2 especificada el 7/8/2026** (IMP-F2-1…8): confirmar → compra importación + lotes + crédito fiscal + pagos. Hallazgo bloqueante: los productos del JSONB **no tenían `sku_id`**. Reglas P14/P15/P16/IMP-SEGURO. **Validado contra despachos oficializados (23/9/2026): flete y seguro prorratean por FOB, el ahorro se mide sobre CIF, schema `datos v:3`** |
| 10 | `ADARA-STOCK.md` | Lotes activos, físico vs disponible, FIFO al consumir, ajustes de inventario |
| 11 | `ADARA-COSTEO-FIFO.md` | **Circuito de costeo CMV completo (✅).** Proyección + FIFO + reversas + hook al `/ml/sync` + seed + 4 vistas + pantalla Inventario. Reglas CF1-CF11 |
| 12 | `ADARA-CONCILIACION-BANCARIA.md` | Matching universal venta↔movimiento, regla N:N, parser Supervielle |
| 13 | `ADARA-MOVIMIENTOS.md` | Pantalla Movimientos (capa 4): columnas, carga manual, dedup, N° de operación MP, importación CSV. **+ botón "Venta en efectivo" (7/8)** |
| 14 | `ADARA-VENTAS-ML-V22.md` | **Conciliación de ventas de Mercado Libre (v22):** O8, O9, O10, O11, **tablero mensual manual bidireccional (O13)** |
| 15 | **`ADARA-ML-BONIFICACIONES.md`** | **Promos y bonificaciones ML en el P&L.** Gap de ingreso CERRADO; punto abierto = fiscal. §12: con Tango el aporte y el delta de IVA son calculables en SQL |
| 16 | `ADARA-INVERSIONES.md` | Tesorería General centralizada, rendimientos al P&L de ML Electrónica |
| 17 | `ADARA-TFACTURA.md` | **Integración Tango Factura IMPLEMENTADA (4/8/2026).** Verdades duras: vars **`TF_*`**, auth `/Provisioning/GetAuthToken`, `ListarMovimientos` usa **`Desde/Hasta/Tope`** |
| 18 | `ADARA-PATRIMONIAL.md` | Activo − Pasivo = Patrimonio Neto, foto al día, evolución mensual |
| 19 | `ADARA-RECLAMOS.md` | Cuenta corriente con ML y proveedores, 4 formas de recupero |
| 20 | `ADARA-GASTOS.md` | 17 categorías, **reparto por imputaciones** (G12), flujo proactivo/reactivo, gastos sin factura, crédito IVA por línea |
| 21 | `ADARA-REFERENCIAS.md` | Recursos open-source de referencia + decisión de arquitectura |
| 22 | `ADARA-SCHEMA.md` | Schema de DB del rediseño: convenciones, mapa de capas, tablas y gotchas |
| 23 | `ADARA-ADJUNTOS.md` | Comprobantes en Supabase Storage: bucket + tabla polimórfica, endpoints, UI |
| 24 | `ADARA-FRONTEND.md` | Arquitectura del frontend v22: estructura `public/`, convenciones, router, componentes |
| 25 | `ADARA-ARCA-PADRON.md` | Integración padrón ARCA: endpoint `/padron`, certificado, autocompletado por CUIT |
| 26 | **`ADARA-PSI.md`** | **Planificación de recompra por SKU** |
| 27 | **`ADARA-AUTH.md`** | **Login multiusuario: Supabase Auth + RLS** (A17) |
| 28 | **`ADARA-VENTAS-EFECTIVO.md`** | **(NUEVO 7/8/2026)** Ventas cobradas en efectivo **sin facturar**: canal `efectivo`, `tipo_comprobante='sin_comprobante'` → `es_gravada=false`, alícuota 0. Descuentan stock por FIFO y suman a caja, **sin IVA débito**. Doble candado (convención + filtro por `es_gravada` en `v_control_mensual`). Endpoint atómico `POST /ventas/efectivo` con guardarriel de fecha. Caso testigo EFVO-018262. **Pendiente estructural: 4 de las 5 líneas siguen sin forma de entrar (falta el sync Tango de ventas no-ML)** |
| 29 | **`ADARA-IIBB-CONVENIO-MULTILATERAL.md`** | **(NUEVO 10/8/2026)** **IIBB de Convenio Multilateral: del CM03 al Resultado.** Cómo se derivan las alícuotas efectivas por jurisdicción desde la DJ Mensual CM03 (`determinado_j / base` = factor completo, sin separar coeficiente de alícuota), tablas `iibb_parametros` + `iibb_jurisdiccion`, vistas `v_iibb_base` / `v_iibb_determinado` / `v_iibb_jurisdiccion_mensual` / `v_impuesto_cheque_mensual`, extensión aditiva de `v_resultado_mensual` y `v_resultado_linea_mensual`, márgenes reales 2026, saldos a favor por jurisdicción, brecha de retenciones (SIRCREB) y la **limitación vigente: la alícuota efectiva de 5,2852 % es un techo hasta cargar la base imponible declarada** |

| 30 | **`ADARA-FLEX-ZONAS-MEF.md`** | **(NUEVO 23/9/2026)** **Zonificación de MEF para Flex.** Las 26 zonas del cuadro de control, diccionario localidad→partido con las **ambiguas marcadas** (nunca se resuelven por nombre), split **La Matanza Norte/Sur** (comercial, no geográfico — La Tablada = Norte/GBA 1), lectura y normalización de etiquetas ZPL (`^FO0,660` manda sobre `^FO0,705`), y excepciones donde MEF zonifica distinto a la geografía. **No es criterio de clasificación**: el partido sale del dato vía Georef (F7) |

Total rediseño: **30 documentos**.

### Documentos del sistema v21 (referencia técnica del código actual en producción)

> ⚠️ A partir del **27/05/2026** estos docs son **referencia técnica únicamente**. Las tablas del v21 fueron borradas en el reset; la DB ahora vive bajo el schema del rediseño (ver `ADARA-SCHEMA.md`).

| Doc | Propósito |
|-----|-----------|
| `ADARA-SYSTEM.md` | ⚠️ **LEGACY.** Stack, módulos y tablas del v21. Referencia histórica únicamente |
| `ADARA-CHANGELOG.md` | Historial de cambios por sesión (**vigente**) |
| `ADARA-VENTAS-ML.md` | Implementación del sync ML: endpoints, flujo, campos en `ventas_ml`, charges_details. **Falta documentar el circuito de token reparado el 7/8** |
| `ADARA-FLEX.md` | **v22**: modelo de zonas en DB, pantalla `#flex`, asignador partido→zona, reconciliación. **Cascada de resolución de zona + caché `flex_localidad_partido` (23/9)** |
| `ADARA-CANCELACIONES-DEVOLUCIONES.md` | Clasificación claims API: motivos PDD, estados claim_status, campos exactos |
| `ADARA-MOV-SIN-CONCILIAR.md` | ⚠️ MUERTO: opera sobre `movimientos_mp` (tabla inexistente). Solo referencia histórica |

Total v21: **6 documentos**.

> 🚨 **`ADARA-SYSTEM.md` y `adara-app-v21.html` son LEGACY.** El HTML del proyecto **no corresponde a la app en producción**. **El código real está en el repo GitHub `ADARAGROUP-Creator/adara-backend`** (`main`): front en `public/`, backend en `server.js`. Ante cualquier duda sobre qué hace la app, se lee el repo.

### Documento de proceso / gobernanza

| Doc | Propósito |
|-----|-----------|
| `ADARA-MANTENIMIENTO-DOCS.md` | **Protocolo de mantenimiento de la documentación**: onboarding, qué bajar a `.md`, handoff, regla anti-duplicados, concurrencia, checklist de cierre |

Total proceso: **1 documento**.

> **Conteo total:** 29 rediseño + 6 v21 + 1 proceso = **36 documentos `.md`** (+ `adara-app-v21.html`, legacy).

> ⚠️ **Motor de conciliación:** el del server v21 está MUERTO (`movimientos_mp` no existe). La conciliación viva es el front (`ventas-ml.js` + `vinculos`). Ver `ADARA-DECISIONES.md` O7.

---

## Mapa por dominio / pregunta

| Si necesitás trabajar en... | Empezá leyendo |
|------------------------------|----------------|
| Mantener/actualizar la documentación entre chats | `ADARA-MANTENIMIENTO-DOCS.md` |
| Cómo opera el sistema día a día · quién carga qué y cuándo | `ADARA-FLUJO-OPERATIVO.md` |
| Modelar una nueva línea de negocio | `ADARA-LINEAS-NEGOCIO.md` |
| Modificar el cálculo de margen / P&L | `ADARA-PNL.md` |
| Calcular IVA, IIBB o Ganancias · apertura fiscal / arrastre | `ADARA-IMPUESTOS.md` |
| **Calcular o liquidar IIBB · Convenio Multilateral** | **`ADARA-IIBB-CONVENIO-MULTILATERAL.md`** |
| Retenciones IIBB por jurisdicción (MP) | `ADARA-RETENCIONES-IIBB.md` |
| Cargar una nueva compra o importación · **compra sin comprobante** | `ADARA-COMPRAS-IMPORTACIONES.md` |
| Simular el costo de una importación · **confirmar despacho (Fase 2)** | `ADARA-IMPORTACIONES-SIM.md` |
| Trabajar con stock o lotes | `ADARA-STOCK.md` |
| Costear ventas / CMV / FIFO | `ADARA-COSTEO-FIFO.md` |
| Conciliar movimientos bancarios | `ADARA-CONCILIACION-BANCARIA.md` |
| Cargar o ver movimientos (extracto, caja) | `ADARA-MOVIMIENTOS.md` |
| **Registrar una venta en efectivo sin factura** | **`ADARA-VENTAS-EFECTIVO.md`** |
| Modelar una inversión nueva | `ADARA-INVERSIONES.md` |
| Integrar con Tango Factura | `ADARA-TFACTURA.md` |
| Calcular el valor de la empresa | `ADARA-PATRIMONIAL.md` |
| Procesar un reclamo a ML o a proveedor | `ADARA-RECLAMOS.md` |
| Cargar un gasto operativo | `ADARA-GASTOS.md` |
| Adjuntar un comprobante a gasto/compra | `ADARA-ADJUNTOS.md` |
| Autocompletar/validar un proveedor por CUIT | `ADARA-ARCA-PADRON.md` |
| Planificar recompra de SKUs (PSI) | `ADARA-PSI.md` |
| Login, usuarios y RLS | `ADARA-AUTH.md` |
| Construir o tocar el frontend · **buscador de SKUs** | `ADARA-FRONTEND.md` |
| Conciliar ventas de Mercado Libre (v22) | `ADARA-VENTAS-ML-V22.md` |
| **Token de ML caído / reconexión** | `ADARA-VENTAS-ML.md` |
| Bonificaciones / promos ML en el P&L | `ADARA-ML-BONIFICACIONES.md` |
| Control semanal de envíos Flex | `ADARA-FLEX.md` |
| **Zonificación de MEF · localidades · Matanza Norte/Sur · etiquetas ZPL** | **`ADARA-FLEX-ZONAS-MEF.md`** |
| Confirmar una regla de negocio existente | **`ADARA-DECISIONES.md`** |

---

## Glosario

| Término | Definición |
|---------|------------|
| **AR** / **AP** | Cuentas por cobrar / por pagar |
| **AS** (Account Statement) | Extracto de Mercado Pago |
| **CAE** | Código de Autorización Electrónica (AFIP) |
| **CMV** | Costo de Mercadería Vendida |
| **DDJJ** | Declaración Jurada (IVA, IIBB, Ganancias) |
| **Devengado** | Criterio temporal: cuenta al hecho económico (no al cobro/pago) |
| **Familia de SKU** | Clasificación intermedia que conecta SKU con línea de negocio |
| **FIFO** | First In First Out — descuento de stock |
| **Línea de negocio** | Una de las 5 unidades del negocio |
| **Lote** | Unidades del mismo SKU con mismo costo real, generadas en una compra |
| **MEF** | Mercado Envíos Flex |
| **N:N (regla)** | Un mov puede cubrir N ops; una op puede tener N movs |
| **Percibido** | Criterio opuesto a devengado: cuenta al cobro/pago real |
| **Saldo técnico a favor** | Saldo de IVA que nace de crédito > débito; solo compensa débito futuro (arrastra) |
| **Libre disponibilidad** | Saldo de IVA de retenciones/percepciones sufridas; usable o pedir devolución (arrastra) |
| **SIRCREB** | Régimen de retención IIBB sobre acreditaciones bancarias |
| **SKU** | Stock Keeping Unit |
| **Snapshot inicial** | Foto de stock y posición fiscal al 31/12/2025 |
| **Tesorería General** | Inversiones centralizadas; rendimientos al P&L de ML Electrónica |
| **TC blue** | Tipo de cambio blue, referencia histórica para compras USD |
| **Tope** | Parámetro de la API de Tango — límite de resultados |
| **Override** | Modificación manual de una asignación automática |
| **Sin comprobante** | Operación sin factura: **no genera crédito ni débito fiscal**, pero sí mueve stock y caja. Aplica a `ventas.tipo_comprobante='sin_comprobante'` y a `compras.sin_comprobante` |
| **CM03** | Formulario de la DJ Mensual de IIBB Convenio Multilateral (anticipo mensual por jurisdicción) |
| **Alícuota efectiva** | `determinado_j / base` — el factor completo por jurisdicción (coeficiente unificado × alícuota), derivado del CM03 |

---

## Stack técnico

### Frontend v22 (en producción)

- **Frontend**: HTML/CSS/JS modular en `public/` (sin build step, A15). Pantallas en `js/screens/`, helper `SB` en `js/core/sb.js`, componentes compartidos en `js/core/`, router por `#hash` en `js/main.js`.
- **Backend**: Node.js / Express en Railway, auto-deploy desde GitHub.
- **DB**: Supabase (PostgreSQL). Front lee/escribe Supabase directo con el **token del usuario logueado**; **RLS ON** en todas las tablas/vistas. Ver `ADARA-AUTH.md`.
- **APIs externas**: Mercado Libre, Mercado Pago, Tango Factura (Axoft).

> 📦 **Código real = repo GitHub** `ADARAGROUP-Creator/adara-backend` (`main`). Antes de analizar o modificar lógica, traer el archivo del repo (`git clone --depth 1 …` o `raw.githubusercontent.com/.../main/<ruta>`).
>
> ⚠️ **El entorno de Claude no puede pushear al repo** (proxy de git: sólo repos autorizados de la sesión). Sebastián sube los archivos a mano por la web de GitHub. Para archivos nuevos: **Add file → Create new file**, escribiendo la ruta completa en el nombre.

### Variables de entorno en Railway

```
ML_CLIENT_ID
ML_CLIENT_SECRET
ML_REDIRECT_URI
SUPABASE_URL
SUPABASE_KEY              ← service_role, solo backend
SUPABASE_ANON_KEY        ← anon, se expone al front vía /config (inofensiva con RLS)
# SUPABASE_JWT_SECRET    ← (removida) reactivar solo con verificación asimétrica (JWKS)
TF_USERNAME              ← Tango Factura. ⚠ el código lee TF_*, NO TANGO_*
TF_PASSWORD
TF_APP_KEY
TF_USER_ID
```

> ⚠️ **Mercado Libre:** además de las variables, la aplicación debe tener **`offline_access` habilitado en el panel de desarrolladores**. Sin eso ML no devuelve `refresh_token` y el access token muere cada ~6 h. Ver `ADARA-VENTAS-ML.md`.

### Servicios externos del negocio

| Servicio | Cuenta | Uso |
|----------|--------|-----|
| Banco Supervielle | CUIT 30-71747647-2 | 3 sub-cuentas + tarjeta Visa |
| Mercado Pago | Cuenta vendedor | Cobros ML + AS mensual |
| Mercado Libre | Cuenta vendedor | Ventas de electrónica y mochilas |
| Tienda Nube | Cuenta tienda | Ventas off-ML |
| Tango Factura (Axoft) | Cuenta empresa | Facturación electrónica + remitos |
| AFIP | CUIT propio | DDJJ IVA mensual, IIBB Convenio Multilateral, Ganancias anual |

---

## Estado actual del proyecto (7 Agosto 2026)

**Etapa**: rediseño implementado y en uso. El foco está en **cerrar los agujeros de carga de datos** que dejan números incompletos.

### ✅ Operativo

**15 pantallas en producción:** Home · SKUs · Movimientos · Gastos · Compras · Conciliación · Resultado · Inventario · PSI · Posición Fiscal · Flex · Cuadre · Saldos · Ventas ML · Sim. Importaciones.

- Login multiusuario (Supabase Auth + RLS, A17).
- Circuito automático: sync ML, sync Tango (cada 3 h), proyección, FIFO, congelamiento de CMV, retenciones IIBB.
- **Sync de ML funcionando de nuevo** desde el 7/8 (ver el fix del token).
- Integración Tango Factura implementada y validada.
- Apertura fiscal de IVA con doble arrastre (corte 30/06/2026).
- **Venta en efectivo sin comprobante** operativa (canal `efectivo`).
- **Compra sin comprobante** operativa (`compras.sin_comprobante` + trigger).
- **Buscador de SKUs** en compras y en venta en efectivo.
- **IIBB determinado + impuesto al cheque en el Resultado** (10/8), con las alícuotas efectivas derivadas del CM03.

### 📊 Números al 7/8/2026

| Dato | Valor |
|------|-------|
| Posición fiscal julio — débito | **$60.709.745,42** |
| Posición fiscal julio — crédito | **$19.838.913,19** |
| Posición fiscal julio — a pagar | **$27.036.258,42** |
| Posición fiscal agosto — a pagar | **$16.014.132,69** (crédito $0: nada cargado aún) |
| Saldo técnico a favor | **$0,00** — se consumió íntegro en julio |
| `caja_ars` | **+$4.116.312,41** (la venta en efectivo la dio vuelta) |
| Crédito IVA de gastos | **$0 en todo el histórico** |
| **CMV de julio estimado** | **$298.099.423,47 de $343.862.983,22 = 86,7 %** |
| **Márgenes reales 2026** *(actualizado 10/8, con IIBB determinado + impuesto al cheque)* | **de 1,59 % (junio) a 9,83 % (febrero)** — muy por debajo de lo que se venía publicando (la pantalla mostraba 6,38 % y 14,05 % respectivamente) |
| **IIBB determinado junio (CM03 202606)** | **$20.765.310,42** · a pagar $8.171.618,73 |
| **Impuesto al cheque acumulado desde marzo** | **$8.205.589,40** |

### 🔴 Agujeros conocidos (los números que hoy no cierran)

| Hallazgo | Detalle |
|----------|---------|
| **El 86,7 % del CMV de julio es estimado** | 1.273 de 2.095 ventas ML sin costo real. Tres causas: **18 SKUs sin ninguna compra cargada** ($133,8M — son los despachos sin confirmar), **16 SKUs con lotes agotados** ($156,3M — falta reposición; el peor: SKU `178` con 320 u sin costear), y **5 SKUs con stock pero lote de fecha posterior a la venta** ($8,0M — `fn_consumir_fifo` sólo consume lotes con `fecha_alta <= fecha de venta`) |
| **Crédito IVA de gastos = $0 en todo el histórico** | El proceso nunca pidió cargar facturas de gasto. Se ataca con la cadencia diaria FO1–FO3 |
| **Despachos reales sin registrar** | **9 simulaciones en borrador**: **$1.041.161.264,59 de costo** y **$199.339.793,17 de crédito fiscal** sin capturar. Un solo despacho (BISHOP, $32,5M) cubre todo el a pagar de julio |
| **Cajas irreales** | Falta el circuito de transferencias entre cuentas y el saldo inicial |
| **4 de las 5 líneas no tienen forma de entrar al sistema** | Las ventas off-ML facturadas (Tienda Nube, B2B, WhatsApp con remito) no tienen camino: el sync de Tango sólo pega facturas a ventas ML |
| **Sin cierre de mes** | `meses_cerrados` no existe. El único freno hoy es el guardarriel de fecha del endpoint de venta en efectivo |
| **IIBB determinado fuera del Resultado** | **RESUELTO el 10/8** (antes se restaba la retención de ML, ~1,05 %, en vez del determinado, ~5,29 %). **Queda una limitación**: la alícuota efectiva de **5,2852 %** está en **modo techo** — la base declarada del CM03 incluye facturación **B2B de luminarias** que ADARA no tiene cargada, así que hoy la línea ML absorbe también el IIBB del B2B. Se destraba cargando la base imponible declarada (un UPDATE de una línea) |
| **Retenciones de IIBB sin capturar** | Brecha de **~$13,9M** entre lo que el CM03 computó como "Valores Restan" en junio (**$15.471.267,94**) y lo que ADARA ve ($1,53M del settlement de MP + $4,35M de la API de ML). Falta casi seguro **SIRCREB** (recaudaciones bancarias del Supervielle), que hoy no se ingesta |
| **Saldos a favor de IIBB inmovilizados** | **$2.867.481,49**: Santa Fe $886.893,95 · Santa Cruz $725.600,97 · Corrientes $707.678,62 · Catamarca $327.197,08 · La Pampa $143.246,24 · Río Negro $54.113,39 · Tierra del Fuego $22.751,24. En Corrientes retienen ~6x y en La Pampa ~10x lo determinado |

### ⏸ En pausa (capas 6-9, por A14 — on-demand)

Capa 6 — Fiscal (parcialmente cubierta) · Capa 7 — Reclamos · Capa 8 — Tesorería/Inversiones · Capa 9 — Patrimonial/Cierres.

---

## Pendientes / próximos pasos (7 Agosto 2026)

Ordenados por lo que más destraba.

| # | Pendiente | Por qué importa |
|---|-----------|-----------------|
| 1 | **Fase 2 del simulador — confirmar despachos** | **$199,3M de crédito fiscal** + **$1.041M de costo** sin capturar. Arregla a la vez la posición fiscal y el 86,7 % de CMV estimado. Spec cerrado (IMP-F2-1…8); primera entrega hecha. **Bloqueante: Sebastián tiene que confirmar cuáles borradores son despachos reales y con qué fecha de ingreso al depósito** |
| 2 | **Habilitar `offline_access` en el panel de ML** | Sin eso el token sigue muriendo cada ~6 h. Es lo único del fix que Claude no puede hacer |
| 3 | **Gastos de julio** | Crédito IVA de gastos en $0. **La DDJJ de julio vence a mediados de agosto** |
| 4 | **Compras de reposición faltantes** | 16 SKUs vendieron más de lo que el sistema tiene cargado |
| 5 | **Circuito de caja** | Transferencias entre cuentas + saldo inicial |
| 6 | **Crédito fiscal de ML** (FISC-CRED-ML) | ~$21M potenciales solo en julio, sin duplicar el costo |
| 7 | **Sync Tango de ventas no-ML** | 4 de las 5 líneas no pueden entrar al sistema |
| 8 | **Tipo `iva_percepcion`** | Falta el tipo para percepciones de IVA sufridas (con FISC-RET-IVA) |
| 9 | **Notas de crédito de compra** | No hay circuito para devoluciones/ajustes de compra |
| 10 | **`meses_cerrados`** | Cierre de mes que bloquee el período (capa 9) |
| 11 | **Guardarrieles de la carga delegada** | Freno a la factura de ML como gasto · selector de tipo de comprobante en criollo. (El de fecha ya existe en venta en efectivo) |
| 12 | **Cargar la base imponible declarada del CM03** (10/8) | Es lo que **destraba que la alícuota efectiva deje de ser un techo**. Hoy la base del CM03 incluye la facturación B2B de luminarias que ADARA no tiene cargada, así que la línea ML absorbe el IIBB del B2B. Es un UPDATE de una línea en `iibb_parametros` |
| 13 | **Paso 1 del roadmap de IIBB: guardar los movimientos no-ML de Tango** (10/8) | `syncTangoFacturas()` (`server.js` ~línea 3280) **ya trae todas las facturas** y pide el detalle de cada una, pero descarta las que no son de Mercado Libre. Guardarlas es el primer paso para conocer la facturación B2B real y, con ella, la base imponible verdadera |

### Pedidos al contador

- Tasa de Ganancias por línea (o tasa global)
- Alícuotas IIBB por jurisdicción (Convenio Multilateral)
- DDJJ anual 2025 — saldo a favor o en contra
- **Política sobre operaciones sin comprobante** (gastos, compras y **ventas en efectivo**) y bonificaciones Flex
- **¿El aporte de ML en promos compartidas es base imponible de IVA?**
- **Despachos de junio**: los borradores 1, 5 y 6 son de un período **ya declarado** (apertura F.2051). Si son despachos reales de junio, su crédito requiere rectificativa
- **Base imponible total declarada en el CM03 202606** — es el dato que saca a la alícuota efectiva del modo techo
- **¿La DDJJ declara una actividad o más de una?**
- **Determinado negativo** en Santa Cruz (**−$551.217,97**) y Tierra del Fuego (**−$15.831,19**): ¿a qué responde?
- **Reducción de alícuota de retención** en Corrientes, Santa Fe y La Pampa: en esas jurisdicciones nos retienen muy por encima de lo determinado (Corrientes ~6x, La Pampa ~10x)
- **¿Hay SIRCREB en el Supervielle?** Es el candidato principal para explicar la brecha de ~$13,9M entre las retenciones del CM03 y las que ve ADARA

### Carga inicial por Sebastián

- Costo unitario real al 31/12/2025 de los SKUs
- Distribución % de sueldos por empleado por línea
- Datos de proveedores con políticas de garantía
- Confirmación de la segunda logística

### Acciones de seguridad

- Regenerar la app de Tango y rotar credenciales (las `TF_*` se pegaron en chat) → recargar las nuevas en Railway
- **Revocar el token de GitHub** generado el 7/8 (`adara-claude`): quedó escrito en el chat y no llegó a usarse (el entorno de Claude no puede pushear)
- Confirmar que `.env` (local) esté en `.gitignore`

---

## Cómo se contribuye / actualiza

- **Protocolo completo de mantenimiento entre chats:** `ADARA-MANTENIMIENTO-DOCS.md`.
- Cambios menores a un doc temático → editar directo el doc **en el lugar** + nota en `ADARA-DECISIONES.md` si modifica una regla.
- Reglas nuevas o modificadas → actualizar `ADARA-DECISIONES.md` primero, después propagar.
- Doc temático nuevo → crear + agregar entrada en este índice (con conteo) + referencia en `ADARA-DECISIONES.md`.
- **No re-subir un `.md` con nombre existente desde la interfaz** (genera duplicados): pedir que Claude lo edite en el lugar.
- Decisiones controvertidas → discutir con Sebastián ANTES de implementar ("charlemos antes de hacer").
- **Entrega de archivos de código**: siempre el archivo completo, listo para reemplazar (nunca snippets ni diffs). **Un archivo por mensaje** si el cliente falla al descargar varios juntos.

---

## Contacto y contexto adicional

- **Sebastián**: dueño/operador del negocio, decisor único. No programa. **Carga personalmente las compras de mercadería** (FO3).
- **Equipo operativo**: usuarios con login (`@adara.local`). **Cargan los gastos con factura A** (FO3).
- **Idioma**: castellano argentino.
- **Estilo de trabajo**: discusión y acuerdo antes de implementar. Documentación primero, código después.
