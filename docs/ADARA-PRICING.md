# ADARA — Pricing, publicaciones y logística

Última actualización: 24/9/2026. Relevamiento de `pricing-adara-online` para migración: no se modificaron datos ni código de la aplicación origen.

## Propósito y límites

Pricing es una app Next.js/Vercel interna para catálogo, precio por canal, publicaciones Mercado Libre, promociones, Tienda Nube y logística. Usa una base Supabase separada.

- Catálogo: 132 productos, identificados por `products.sku` UNIQUE; EAN principal y secundarios.
- Costo: `cost_without_vat` es costo vigente editable; no hay compras, lotes ni FIFO. El histórico ayuda a análisis, pero no es costo contable.
- Stock: `products.stock` y `meli_stock` son informativos. No se encontró escritura de stock físico a ML.
- Tienda Nube: administra catálogo/precio/contenido; no importa órdenes ni factura ventas.
- Logística: administra lotes operativos de despacho; no descuenta stock contable.

En ADARA APP el backend será dueño de producto, costo FIFO y stock. El primer match será `products.sku = skus.codigo`, pero el vínculo persistente destino debe guardar `skus.id`. `86G+AC001` se excluye y no debe crear un combo backend.

## Pantallas

| Ruta | Propósito | Acciones |
|---|---|---|
| `/productos` | Catálogo | Alta/edición/pausa, SKU, nombre, EAN, categoría, costo, IVA y medidas. |
| `/precios` | Matriz de precios | Calcula por margen/utilidad/PVP manual; publica precio ML. |
| `/mercadolibre` | Configuración de canales | Impuestos, cuotas, comisiones, margen por producto/canal y sync de publicaciones. |
| `/promociones-meli` | Promociones | Ve oportunidades; activa/restaura campañas y controla B2B. |
| `/metricas-meli`, `/rotacion-sku`, `/rentabilidad-meli`, `/monitor-ventas`, `/analisis-mercado` | Análisis ML | Publicaciones, ventas, rotación, margen histórico y competencia. |
| `/preguntas` | Preguntas ML | Lista y responde preguntas. |
| `/logistica` | Despacho ML | Imprime ZPL, crea lote, preparación por escáner, embalaje, PDFs y archivo Drive. |
| `/tienda-nube` | Catálogo web | Conecta/sincroniza/crea productos, cambia precios y administra banners. |
| `/impuestos`, `/simulador`, `/calculadora-movil` | Cálculo | Tasas de pricing, escenarios guardados y calculadora móvil. |

Usuarios: equipo ADARA autenticado. No hay roles de negocio propios; RLS es para `authenticated` y las rutas admin usan service role.

## Fórmula de precio

Implementación: `lib/pricing.ts`, `calculatePriceSummary`.

- `N = P / (1 + IVA venta)`; la venta lleva el IVA del producto si el canal lo habilita.
- `costoParaRentabilidad = costoNeto + costoNeto × cost_vat_rate`.
- Comisión ML/cuotas se carga sobre precio bruto y se vuelve equivalente neto: `((1+IVA venta)/1,21) × (comisión+cuota)`.
- IIBB e IDC se aplican a venta neta; Ganancias sólo a ganancia bruta positiva.
- Fijo/envío ML se netean por 1,21. Estructura es importe fijo; canal directo permite envío y comisión manual.

Con utilidad neta objetivo `U`:

`N = (costoParaRentabilidad + fijos + U/(1-Ganancias)) / (1 - feeNeto - IIBB - IDC - comisiónDirecta)`.

Con margen objetivo `m` se reemplaza `U/(1-Ganancias)` por la porción `m/(1-Ganancias)` en el denominador. Después se aplica IVA y redondeo (`nearest`, `up`, `down`). Un `manual_sale_price` no se recalcula: expone margen real.

| Código | Significado |
|---|---|
| `EF` | Efectivo/directo; sin fee/envío ML y normalmente sin IVA de venta. |
| `MC` | Mercado Libre Clásica / un pago. |
| `MP3`, `MP6`, `MP9`, `MP12` | ML con 3/6/9/12 cuotas. |
| `TN`, `TN6` | Tienda Nube contado / 6 cuotas. |
| `TR` | Transferencia. |

`product_channel_margins` configura por producto+canal: margen/utilidad, PVP manual, estructura, envío, comisión, IVA de venta/costo y descuento de promoción.

Ejemplo reproducible SKU `101`, MC al 24/9: costo neto $342.000, IVA 10,5%, fee Smartwatches 12,5%, IIBB 5%, envío bruto $7.290 = $6.024,79 neto, utilidad objetivo $30.756. Fee neto = 11,4153%. `N=(342.000+6.024,79+30.756)/(1-11,4153%-5%)=$453.169,95`; bruto $500.752,80; redondeado a $100: **$500.800**.

## Schema real (Supabase, 24/9/2026)

19 tablas `public`, todas con RLS activo. Filas estimadas: products 132; product_cost_history 179; product_eans 30; product_channel_margins 927; tax_settings 1; ML accounts 1; category fees 31; installment fees 7; shipping/publications 1.293; shipping logs 4; promotion opportunities 5.849; B2B guard 48; order items 8.757; Flex rates 4; logistics batches 3; simulations 116; TN accounts 1; TN publications 156; TN banners 8. Hay una vista `mercadolibre_product_profitability`.

| Tabla | PK/FK/constraints | Columnas |
|---|---|---|
| `products` | PK `id`; UNIQUE `sku`; costo >=0, stock >=0, IVA 21/10,5, status active/paused/discontinued | `id uuid`, `sku/ean/name/description/brand/model/category text`, `cost_without_vat/vat_rate numeric`, `cost_with_vat numeric GENERATED`, medidas numeric, `stock integer`, proveedor/garantía, autores UUID, timestamps. |
| `product_cost_history` | PK id; FK product CASCADE | Producto/SKU, costo e IVA anterior/nuevo, actor y fecha. |
| `product_eans` | PK `ean`; FK `sku→products.sku`; EAN 8–14 dígitos | EAN, SKU, autor, fecha. |
| `product_channel_margins` | PK id; FK product CASCADE; UNIQUE producto+canal | SKU, canal, margen/utilidad/PVP/fijos/comisiones/IVA/descuento numeric, flags, notas, timestamps. |
| `tax_settings` | PK id; UNIQUE code | code/key, IIBB/IDC/IIGG/estructura, active, notas, timestamps. |
| `mercadolibre_accounts` | PK id; UNIQUE meli_user_id | usuario/nickname, access/refresh token, scope, expiración, timestamps. |
| `mercadolibre_category_fees` | PK id; UNIQUE categoría | categoría, fee viejo/actual, categorías ML JSONB, fuente/sync, active/notas/timestamps. |
| `mercadolibre_installment_fees` | PK id; UNIQUE code; round check | código/nombre/cuotas, fee/margen/redondeo, tipo canal y flags `applies_*`, active/notas/timestamps. |
| `mercadolibre_shipping_costs` | PK id; FK product CASCADE | producto/SKU, fijo/envío, free shipping, datos completos de MLA (título, precio, promo, fees, cuotas, stock, envío, catálogo, tags/terms/promotion JSONB), estado/fuente/sync/timestamps. |
| `mercadolibre_shipping_sync_logs` | PK id | SKU/MLA, costo previo/nuevo, estado/mensaje/fecha; se purga a 7 días. |
| `mercadolibre_promotion_opportunities` | PK id | MLA, promoción/oferta, estado, precios, aporte ML/vendedor, vigencias, raw JSONB, timestamps. |
| `mercadolibre_b2b_margin_guard` | PK `(meli_item_id,minimum_purchase_unit)` | MLA, cantidad >1, SKU, margen/aporte requeridos, active, pausa/razón/timestamps/id. |
| `mercadolibre_order_items` | PK id; FK product SET NULL; UNIQUE pedido+MLA+variación+SKU | pedido, fecha, estado, MLA/variación/SKU, producto, cantidad/precios/fees, pago/envío/raw y columnas `real_*`/`normalized_*` de rentabilidad. |
| `flex_shipping_rates` | PK id; UNIQUE zone; importe >=0 | zona, importe, IVA incluido, active, `effective_from`, notas, timestamps. |
| `logistics_batches` | PK id; FK creador auth.users | modo cross_docking/self_service, fecha, estado printed/collecting/packing/completed, shipments array JSONB 1–50, staged/packed JSONB, timestamps. |
| `simulator_saved_simulations` | PK id; payer seller/customer | nombre/categoría/costo/margen/precio/IVA/envío/fijo, URL/proveedor, timestamps. |
| `tiendanube_accounts` | PK id; UNIQUE store_id | tienda, token/scope, timestamps. |
| `tiendanube_publications` | PK id; FK product SET NULL; UNIQUE tienda+variante | IDs TN, producto/SKU, título/variante/URLs, precio/promo/stock, flags, categorías/raw JSONB, sync/timestamps. |
| `tiendanube_web_banners` | PK id; placement check | ubicación, orden, textos, URLs/imágenes, color/opacidad, vigencias, autores/timestamps. |

Funciones: `product_cost_at(product,sku,fecha)` resuelve costo histórico; `log_product_cost_change` lo registra; `recalculate_meli_order_items_profitability(days_back)` recalcula órdenes; `replace_meli_promotion_snapshot` reemplaza snapshot por MLA bajo lock. Triggers actualizan `updated_at` de producto, orden, Flex y TN; uno registra cambio de costo. No copiar RLS sin rediseño de roles.

## Integraciones y procesos

### Mercado Libre

`lib/mercadolibre.ts` hace OAuth, refresh bajo demanda cuando vence en menos de cinco minutos y persiste tokens en `mercadolibre_accounts`; `meliFetch` tiene timeout 30s. Esto choca con la futura integración única del backend: no migrar tokens ni dejar dos refreshers.

- `sync-shipping`: busca publicaciones, detalle, categorías, pricing, envío, cuotas, catálogo y promociones; actualiza snapshots, comisiones y logs.
- `sync-sales`: lee orders, shipment y costs; upsert de órdenes y rentabilidad con costo histórico.
- `update-sku-price`: PUT de precio ML; `update-b2b-prices` modifica precios por cantidad; B2B guard revisa margen/aporte.
- Promociones activan/restauran campañas y PVP lista; preguntas hacen GET/POST `/answers`; competencia consulta catálogo/ofertas/vendedores.
- Logística consume órdenes, shipment, SLA/history y etiquetas ZPL.

Rutas internas actuales: `GET /api/mercadolibre/connect|callback|status|catalog-competition|logistics-board|logistics-summary|questions`; `POST /api/mercadolibre/sync-shipping|sync-sales|auto-sync|update-sku-price|b2b-pricing|update-b2b-prices|activate-promotion|refresh-adara-promotion|questions/answer|shipment-labels|logistics-batches|logistics-batches/documentation`; `GET/POST /api/mercadolibre/import-products|import-categories|notifications`. `logistics-batches` también tiene GET.

### Tienda Nube

OAuth y requests con timeout 30s. `sync-products` lee productos; `create-product` crea producto y sube imágenes; `update-price` modifica variante; banners administra contenido/script. No hay ordenes TN, webhook de ventas ni facturación.

Rutas internas: `GET /api/tiendanube/connect|callback|status|privacy|web-banners|web-banners/public|web-banners/script.js|install-web-script`; `POST /api/tiendanube/sync-products|create-product|update-price|privacy|web-banners|web-banners/upload|install-web-script`; `PUT/DELETE /api/tiendanube/web-banners`.

### Cron, deploy, variables

Vercel ejecuta cada 5 minutos `/api/cron/mercadolibre-live`, autenticado con `CRON_SECRET`; llama sync incremental de ventas de hoy con solapamiento de 5 minutos. `auto-sync` existe, protegido, pero no está programado. Deploy: GitHub → Vercel.

Variables por nombre: `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`, `MELI_CLIENT_ID`, `MELI_CLIENT_SECRET`, `MELI_REDIRECT_URI`, `CRON_SECRET`, `MELI_AUTO_SYNC_TOKEN`, `TIENDANUBE_APP_ID`/`TIENDANUBE_CLIENT_ID`, `TIENDANUBE_CLIENT_SECRET`, `TIENDANUBE_USER_AGENT`, `TIENDANUBE_WEB_BANNER_SCRIPT_ID`/`TIENDANUBE_SCRIPT_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON`, `GOOGLE_DRIVE_ROOT_FOLDER_ID`, `NEXT_PUBLIC_PRICING_FIXTURES`.

## Migración: qué llevar y conflictos

Migrar tras validación: productos/EAN, historia de costos como dato analítico, márgenes/canales, tasas/cuotas, banners y posiblemente documentación logística. Se regeneran por sync publicaciones ML, competencia, oportunidades y logs. No copiar tokens. Órdenes ML requieren plan de deduplicación contra ventas backend.

Conflictos: costo de reposición vs FIFO; snapshots de stock vs propietario único backend; combos; token ML único; y Flex. Tarifas vigentes pricing: CABA $3.850, GBA1 $5.350, GBA2 $5.950, GBA3 $7.850 IVA incluido. La fecha de vigencia sigue pendiente: no actualizar la tabla backend actual, porque su vista recalcula historia. Tienda Nube se integrará luego al backend con órdenes, fiscal y FIFO; no automatizarla todavía.

## Estado de la migración en ADARA APP

**25/9/2026 — Paso 2a: motor de precios portado.** `public/js/core/pricing.js` es un port 1:1 de `lib/pricing.ts` de pricing (commit `8b56bcd`): `calculatePriceSummary`, `calculateB2bPriceSummary`, `normalizeOption`, `roundPrice`, `promoListPrice`, `priceForMercadoLibreUpload`. Es un módulo **puro**: no lee ni escribe base ni llama a ML. Conserva los nombres de campo de pricing (`cost_without_vat`, `vat_rate`, `iibb_rate`…) para que la migración de datos mapee directo; todas las tasas en porcentaje.

- Tests: `tests/pricing.test.mjs` (`npm test`): el ejemplo del SKU 101 ($500.800), PVP manual, Ganancias sólo sobre ganancia positiva, canales directos, casos inválidos, redondeo, y el port de los tests B2B de pricing (exención del fijo bajo $33.000).
- Todavía **no hay pantalla** ni datos: el módulo espera los datos de canales, tasas y márgenes que se migran en el paso 2b.
- **Resuelto 25/9/2026 (PRC1, PRC2):** pricing es simulación. Calcula con **su propio costo editable y sus propias tasas**; no tiene que coincidir con FIFO ni con el CM03. Al migrar, esos datos van a **tablas propias de pricing** y nunca se escriben en `skus`, `lotes` ni `iibb_parametros`. La pantalla puede mostrar el costo FIFO al lado, como referencia.

**Plan por pasos** (de menor a mayor riesgo): 1. catálogo (match `products.sku` → `skus.id`, EAN, categoría, medidas) · 2a. motor de precios ✅ · 2b. canales, tasas, cuotas, comisiones y márgenes + pantalla de Precios sólo lectura · 3. escrituras en ML (precio, B2B, promociones, preguntas), recién con la conexión de ML unificada · 4. análisis ML sobre `ventas_ml` · 5. logística y Tienda Nube.
