# ADARA — Referencias y Decisión de Arquitectura

Última actualización: 19 Mayo 2026

Recursos open-source de referencia para el desarrollo de ADARA, y la decisión documentada de **construir ADARA a medida** en vez de partir de una plataforma all-in-one existente.

---

## Decisión estratégica: ADARA a medida (no partir de Odoo/ERPNext)

### La pregunta

¿Conviene construir ADARA desde cero o partir de una plataforma all-in-one existente (Odoo, ERPNext) y agregarle la lógica específica?

### El análisis

| Parte del sistema | ¿Existe en Odoo/ERPNext? | ¿Existe en ADARA? |
|--------------------|--------------------------|---------------------|
| Contabilidad doble entrada, CRM, ecommerce genérico, website, SEO | ✅ Sí, hecho | ❌ No |
| Integración ML (orders, shipments, claims, Flex) | ❌ No | ✅ Funcionando (v21) |
| Integración MP (Account Statement, payments, conciliación) | ❌ No | ✅ Funcionando (v21) |
| Integración Tango Factura + AFIP/CAE | ❌ No | ✅ Validada técnicamente |
| Conciliación Supervielle + lógica bancaria argentina | ❌ No | ✅ Diseñada (rediseño) |
| Retenciones ML, bonificaciones Flex netas, IIBB, costos reales FIFO | ❌ No | ✅ Resuelto / diseñado |

### El insight clave

La parte **genérica** (contabilidad, CRM, ecommerce) que ofrece Odoo es la **fácil**. La parte **específica argentina** (ML + MP + Tango + AFIP + Supervielle + Flex + retenciones) **no existe en ninguna plataforma del mundo** y hay que construirla igual — y ADARA **ya la tiene medio resuelta** en el v21.

Si se tomara Odoo de base, habría que construir toda la integración argentina **dentro del modelo de datos de Odoo** (en Python, adaptándose a su forma de hacer las cosas), lo cual probablemente sea **más difícil** que construirla a medida.

### Decisión

**ADARA se construye a medida.** Razones:

1. La lógica difícil y específica ya está en ADARA — es la ventaja real del proyecto.
2. El stack es Node/JS; Odoo/ERPNext son Python/Frappe — adoptarlos significa tirar lo hecho y aprender un ecosistema nuevo.
3. Odoo/ERPNext cargan muchísima complejidad que ADARA no necesita (manufactura, HR, multi-company).
4. Para los módulos genéricos que falten, se **toman patrones de código** de proyectos open-source (ver abajo) sin adoptar toda la plataforma.

### Nota sobre el alcance (scope)

ADARA hoy resuelve **gestión financiera y operativa de ecommerce** con una profundidad (costos reales, conciliación argentina) que las suites genéricas no tienen. Ese es su diferencial.

Ambiciones como CRM completo, SEO, gestión de publicidad y redes sociales son el alcance de suites con años de desarrollo y equipos grandes. **La estrategia es clavar el núcleo financiero/operativo primero** (lo documentado en el rediseño), y recién después evaluar sumar esos módulos o integrarse vía API con herramientas dedicadas.

---

## Recursos de referencia por categoría

### APIs que ADARA ya usa — SDKs oficiales

| Recurso | Repo | Para qué |
|---------|------|----------|
| Mercado Pago SDK Node.js | `github.com/mercadopago/sdk-nodejs` | SDK oficial v2+. Maneja auth, payments, reintentos. Evaluar migrar desde `fetch` directo |
| Mercado Libre SDK Node.js | `github.com/mercadolibre/nodejs-sdk` | SDK oficial. OAuth 2.0, items, orders |

Hoy ADARA hace las llamadas con `fetch` directo. Migrar a las SDKs daría manejo de OAuth, refresh de tokens y reintentos ya resueltos. A evaluar en el rediseño.

### Contabilidad / inventario / ledger — patrones de código

| Recurso | Repo | Para qué en ADARA | Licencia |
|---------|------|---------------------|----------|
| **Bigcapital** ⭐ | `github.com/bigcapitalhq/bigcapital` | **La referencia estrella.** Node.js + TypeScript + React + PostgreSQL (stack casi idéntico). Inventario **FIFO + COGS** (= tu CMV), double-entry, facturación, gastos, reportes. Tiene demo en `demo.bigcapitalhq.com` | AGPL-3.0 |
| Midaz | `github.com/LerianStudio/midaz` | Ledger con transacciones **N:N double-entry** — justo tu regla N:N de conciliación. Estudiar el modelo de datos | — |
| LedgerSMB | `github.com/ledgersmb/LedgerSMB` | ERP contable maduro en PostgreSQL, miles de transacciones/semana. Referencia de plan de cuentas serio | GPL |

> ⚠ **Sobre la licencia AGPL de Bigcapital**: se puede leer y aprender del código libremente. Para ADARA (uso interno, no se distribuye el software) no genera obligaciones. Solo aplicaría si se distribuyera ADARA como producto a terceros.

### Apps de finanzas open-source — inspiración visual

Productos terminados con buena UI para mirar cómo resuelven dashboards, tablas de transacciones, conciliación y reportes:

| App | Repo / demo | Por qué mirarla |
|-----|-------------|-----------------|
| Firefly III | `github.com/firefly-iii/firefly-iii` | UI moderna, REST API. Mejor referencia para **conciliación y categorización de transacciones**, budgets y reportes con gráficos |
| Bigcapital | `demo.bigcapitalhq.com` | Pantallas de inventario, facturas, reportes. Stack igual al de ADARA |
| Frappe Books | `github.com/frappe/books` | Interfaz limpia, dashboard en tiempo real, POS integrado |
| Akaunting | `github.com/akaunting/akaunting` | UI limpia PyME, multi-moneda (relevante para USD/ARS) |
| Maybe / "Sure" | buscar "maybe-finance" en GitHub | App React, una de las UIs más pulidas del rubro |

### Templates de dashboard — para usar en el código

Stack de ADARA es HTML/CSS/JS. Estos sirven para construir el frontend del rediseño visual:

| Template | Detalle | Encaje |
|----------|---------|--------|
| **TailAdmin** ⭐ | `tailadmin.com` — open-source Tailwind, versión HTML pura + React/Vue. 400+ componentes, gráficos ApexCharts, tablas, dark mode | El más directo — versión HTML, tablas + gráficos listos |
| Tremor | Componentes de gráficos para Tailwind | Gráficos de P&L, patrimonial, evolución mensual |
| Windmill | Template Tailwind minimalista con dark mode | Dark mode (como el mockup actual) |
| Flowbite | Componentes Tailwind (modales, tablas, dropdowns) | Componentes sueltos |

### Plataformas all-in-one — NO se adoptan, solo referencia

| Plataforma | Nota |
|------------|------|
| Odoo | All-in-one (CRM, ecommerce, contabilidad, conciliación, marketing, website, SEO). Módulos clave en versión Enterprise (paga). Python. **Referencia de UX de CRM/marketing** para cuando llegue el momento, no base |
| ERPNext | 100% libre (AGPL-3.0), sin gating. Contabilidad + inventario + CRM. Sin ecommerce nativo. Python/Frappe. Referencia de modelo contable |

---

## Mapeo: módulo de ADARA → mejor recurso a estudiar

| Módulo de ADARA | Recurso de referencia |
|------------------|------------------------|
| CMV / lotes FIFO (`ADARA-STOCK.md`, `ADARA-COMPRAS-IMPORTACIONES.md`) | **Bigcapital** (inventario FIFO + COGS) |
| Conciliación universal N:N (`ADARA-CONCILIACION-BANCARIA.md`) | **Midaz** (N:N) + **Firefly III** (UI de conciliación) |
| Ledger fiscal por impuesto (`ADARA-IMPUESTOS.md`) | **Bigcapital** / **LedgerSMB** (plan de cuentas) |
| P&L y patrimonial (`ADARA-PNL.md`, `ADARA-PATRIMONIAL.md`) | **Bigcapital** (balance + income statement) |
| Sync ML / MP / Tango (`ADARA-VENTAS-ML.md`, `ADARA-TFACTURA.md`) | SDKs oficiales de **ML** y **MP** |
| Rediseño visual / frontend | **TailAdmin** (base de componentes) + **Firefly III** y **Bigcapital** (inspiración) |

---

## Plan recomendado para el rediseño

1. **Núcleo primero**: implementar lo documentado (lotes FIFO, conciliación universal, P&L, patrimonial, Tango). Es el diferencial real de ADARA.
2. **Inspiración antes de codear**: explorar el código de **Bigcapital** (FIFO/COGS, double-entry) y los demos de **Firefly III** y **Bigcapital** (UI).
3. **Frontend**: arrancar el rediseño visual con **TailAdmin HTML** + gráficos **Tremor/ApexCharts**, evolucionando el mockup `adara-mockup-conciliacion-v1.html`.
4. **SDKs**: evaluar migrar el sync de `fetch` directo a las SDKs oficiales de ML y MP.
5. **Después del núcleo**: recién ahí evaluar CRM/marketing/SEO (a medida o vía integración con herramientas dedicadas).

---

## Documentos relacionados

- `ADARA-DOCS-INDEX.md` — índice general
- `ADARA-DECISIONES.md` — la decisión "ADARA a medida" registrada como decisión de arquitectura
- `ADARA-TFACTURA.md` — integración con Tango (parte de la lógica específica que justifica build a medida)
- `ADARA-STOCK.md`, `ADARA-COMPRAS-IMPORTACIONES.md` — CMV/FIFO (referencia: Bigcapital)
- `ADARA-CONCILIACION-BANCARIA.md` — conciliación N:N (referencia: Midaz)
