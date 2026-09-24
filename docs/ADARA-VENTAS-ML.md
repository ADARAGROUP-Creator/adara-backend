# ADARA — Ventas Mercado Libre (sistema v21)

Última actualización: 7 Agosto 2026 (**circuito de token OAuth reparado — sección nueva al final; es contenido VIGENTE, no v21**) · 8 Junio 2026 (nota: `ventas_ml.sku` = `seller_sku||seller_custom_field`, se pisa en cada sync → equivalencias en la proyección, CF8) · 19 Mayo 2026

> ⚠️ **REFERENCIA v21 — CONGELADO. No es el estado actual.** Las tablas del v21 se borraron en el reset del 27/05/2026; la DB vive bajo el schema del rediseño (`ADARA-SCHEMA.md`). Este doc se conserva solo como referencia técnica de las integraciones. Estado vigente: `ADARA-DOCS-INDEX.md`.
>
> 🔴 **Excepción:** la sección **"Circuito de token OAuth"** (al final, 7/8/2026) describe código **vigente en producción**. Es lo primero a leer si el sync de ML deja de traer ventas.

> 📌 **Doc del sistema v21 en producción.** Documenta la implementación técnica actual del sync de ventas de ML. Las reglas del rediseño viven en `ADARA-DECISIONES.md`.

---

## Sync ML

Endpoint: POST /ml/sync
Parámetros: desde, hasta, dias

Botones frontend: **Sync día** (fecha seleccionada), **Sync mes** (mes completo con confirmación).

---

## Flujo

1. Obtener seller_id
2. Cargar feriados del rango
3. Dividir rango en chunks de 7 días (límite de ML API)
4. Por cada chunk: obtener orders (paginado de a 50)
5. Por cada order: obtener shipment + TODOS los payments aprobados en paralelo (batches de 10)
6. Acumular charges_details de todos los payments (split payments — ver regla más abajo)
7. Calcular cargos, partido, fecha_despacho_flex
8. Upsert ventas_ml (on_conflict: ml_order_id)

---

## Fuentes de datos

| API | Datos que trae |
|-----|----------------|
| Orders API | fecha, producto, sku, bruto, status, hora_venta, payments |
| Shipments API | tipo_envio, partido, ciudad_destino, fecha_entrega |
| Payments API | cargo_venta, cargo_envio, costo_financiero, impuestos, shipping_amount |

---

## Campos en ventas_ml

```
fecha, hora_venta, periodo, periodo_cobro
titulo, sku, cantidad
importe_bruto, ml_status
cargo_venta, cargo_envio, costo_financiero, impuestos, por_cobrar
tipo_envio, partido, ciudad_destino
fecha_entrega, fecha_despacho_flex
mp_payment_id, mp_payment_ids, pagos_cantidad, pack_id
aprobada, conciliado, fecha_cobro
enviado, descartada
estado_conciliacion, balance_conciliacion, fecha_devolucion
estado_envio, estado_cancelacion
claim_id, claim_status, motivo_devolucion
devuelta, monto_reembolso, cargo_envio_devolucion
recepcion_condicion, recepcion_fecha, recepcion_nota
nro_factura, tipo_factura, total_facturado_tango, tango_movimiento_id   ← Tango (4/8/2026)
conciliacion_periodo                                                    ← tablero mensual (13/7/2026)
shipment_id                                                             ← bonificaciones Flex (13/6/2026)
```

> **`sku`** = `seller_sku || seller_custom_field` del item de la orden. **Se sobrescribe en cada sync** (`sbUpsert` con `Prefer: merge-duplicates`). Por eso, para resolver equivalencias de código con el catálogo (`skus.codigo`) o combos, **NO se remapea esta tabla** (se pisaría): la equivalencia vive en la **proyección** (`fn_proyectar_ml` + tablas `sku_map`/`combo_map`, regla CF8). Ver `ADARA-COSTEO-FIFO.md`.

---

## Payment API (charges_details)

| type | name | Uso en ADARA |
|------|------|--------------|
| fee | (general) | `cargo_venta` — comisión de ML |
| fee | `financing` / `interest` / `add_on` | `costo_financiero` — ver regla abajo |
| shipping | (Colecta) | `cargo_envio` — negativo en colecta, ya descontado por ML |
| shipping | (Flex) | `cargo_envio` = `base_cost − list_cost` (bonificación Flex) |
| tax | (collector) | `impuestos` — retenciones ML a cuenta de IIBB |
| tax | (payer) | **IGNORAR** — son retenciones del comprador |
| coupon | (cualquier) | **IGNORAR** — buyer-side |

Campo `shipping_amount` del payment = contribución del comprador al envío (suma a `por_cobrar`, NO a `cargo_envio`).

### Reglas duras de sync ML (v21)

1. **`por_cobrar` incluye `financing_add_on_fee`** pero **excluye `financing_fee`**. Comparación exacta con `===`, no `includes()`.
2. **`cargo_envio` para Flex = `flexBonificacion`** (`base_cost − list_cost`).
3. **Costos de envío Colecta** los deduce ML directamente y NO aparecen en el AS.
4. **Bonificaciones Flex en el AS son montos netos** (después de 0,6 % de retención). Matching: `neto = cargo_envio − round(cargo_envio × 0.006, 2)`.
5. **`aprobarParaConciliar` usa `v.periodo`** (mes original de la venta).
6. **Ventas `cancelled` con `estado_envio IN (no_preparado, preparado)`** se auto-descartan post-sync.
7. **`periodo_cobro` se setea al construir la row** y se EXCLUYE del upsert.
8. **Matching de bonificaciones**: "primero el pago, después el envío".
9. **Claims API**: `resource_id={order_id}` es más confiable que la búsqueda global.

---

## Shipments API — clasificación de tipo_envio

| tipo_envio del API | tipo en ADARA |
|---------------------|----------------|
| `self_service` | **flex** |
| `fulfillment` | **fulfillment** (Full) |
| `xd_drop_off` | **colecta** |
| `cross_docking` | ⚠️ **pendiente confirmar mapeo** |

Clasificación del partido: ver `ADARA-FLEX.md`.

---

## Re-sync y campos protegidos

- `claim_status` (si fue marcado como `reingresado`, `perdida` o `en_gestion`)
- `recepcion_condicion`, `recepcion_fecha`, `recepcion_nota`
- `periodo_cobro`
- `aprobada`, `descartada`
- `estado_conciliacion = 'descartada'`

---

# Circuito de token OAuth (VIGENTE — 7 Agosto 2026)

> Esta sección **no es v21**: describe el código en producción. Es lo primero a leer si el sync deja de traer ventas.

## Cómo funciona

El token de ML se persiste en **`workspace_config`** (una sola fila): `ml_access_token`, `ml_refresh_token`, `ml_token_expires`.

- El **access token dura ~6 h**.
- `mlGet()` refresca on-demand si faltan menos de 60 s para el vencimiento.
- Un **cron horario** (minuto 7) refresca de forma proactiva, pero **sólo si faltan menos de 2 h** para el vencimiento — así no rota el `refresh_token` (que en ML es de un solo uso) más veces de las necesarias.
- `refreshML()` es **single-flight**: el sync dispara hasta 10 llamadas en paralelo y sin eso todas refrescarían con el mismo `refresh_token`, invalidándose entre sí. Ése fue el bug original de "ML se desconecta solo" (13/6/2026).

## 🔑 `offline_access` — el paso que no es código

**ML sólo devuelve `refresh_token` si la aplicación tiene `offline_access` habilitado en el panel de desarrolladores.** Pedir el scope en la URL de `/ml/auth` **no alcanza**: si la app no lo tiene activado, ML ignora el pedido y devuelve sólo el access token.

Sin `refresh_token`, el token muere cada ~6 h y hay que reconectar a mano cada vez.

**Cómo se habilita:** `developers.mercadolibre.com.ar` → la aplicación → Editar → tipo de autorización / scopes → **offline_access** → guardar → volver a entrar a `/ml/auth` y autorizar.

**Cómo se verifica:** `/ml/status` debe devolver `tiene_refresh: true`. En la base: `select ml_refresh_token is not null from workspace_config`.

## Diagnóstico del 7/8/2026 (causa raíz + 3 bugs)

Síntoma: `/health` daba `ml_token:false` y no entraban ventas nuevas.

**No era el sync.** En la base, `ml_refresh_token` estaba en **NULL** y el access había vencido a las 15:34. Había 537 ventas de agosto, así que el sync venía corriendo bien hasta que el token murió.

Tres bugs que lo hacían invisible, todos corregidos:

| # | Bug | Efecto |
|---|-----|--------|
| 1 | `refreshML()` arrancaba con `if (!ML.refresh) return;` — **un `return` mudo** | El cron corría, no hacía nada y no dejaba rastro en los logs. El token moría cada 6 h en silencio |
| 2 | `/ml/callback` guardaba `refresh: data.refresh_token` **sin fallback** (a diferencia de `refreshML()`, que usa `|| ML.refresh`) | Cada reconexión manual **pisaba con NULL** un refresh token que existiera |
| 3 | La pantalla de éxito decía **"✅ Mercado Libre conectado correctamente"** aunque ML no hubiera devuelto refresh token | Parecía resuelto y se volvía a caer en 6 h. Es exactamente lo que venía pasando |

## Qué se construyó

- **`refreshML()`**: registra el motivo del fallo en `ML_ERROR` (`sin_refresh_token` / `invalid_grant` / `respuesta_inesperada` / `error_red`) y loguea. Distingue el refresh revocado del error transitorio.
- **`/ml/callback`**: conserva el refresh previo si ML no manda uno nuevo, y si **no hay ninguno** muestra una pantalla de **advertencia con el instructivo del panel**, no un "conectado correctamente".
- **`/ml/status`**: estado real — `conectado`, `vencido`, `tiene_refresh`, `requiere_reconexion`, `expira_en_min`, `error`. Antes devolvía `conectado: !!ML.access`, o sea "conectado" con el token vencido hacía horas.
- **`/health`**: suma el bloque `ml` con ese mismo detalle. `checks` queda igual (no rompe consumidores).
- **Cron**: de 6 h a **1 h**, con guard de 2 h. A 6 h el margen era cero — un fallo transitorio y el siguiente intento llegaba **después** del vencimiento.
- **Banner en la home** (`home.js`): lee `/health` y distingue tres casos — sin refresh token (rojo, con el instructivo), refresh revocado (rojo), vencido pero renovable (naranja). Con link a `/ml/auth`. Falla en silencio si el backend todavía no tiene el bloque `ml`, así que los archivos se pueden deployar en cualquier orden.

Es el **primer consumo de `/health` desde el front**: el endpoint existía y calculaba bien `ml_token`, pero nadie lo miraba — por eso la caída se detectaba días después, al notar que faltaban ventas.

## Checklist cuando el sync no trae ventas

1. `/health` → mirar el bloque `ml`.
2. Si `tiene_refresh: false` → **habilitar `offline_access` en el panel** y reconectar. Es la causa estructural.
3. Si `requiere_reconexion: true` con `error.motivo = 'invalid_grant'` → el refresh fue revocado; reconectar en `/ml/auth`.
4. Si `vencido: true` pero `tiene_refresh: true` → esperar al cron (máximo 1 h) o forzar una llamada; si persiste, mirar logs por `✗ ML refresh`.
5. Recordar: **un redeploy de Railway reinicia el proceso** y corta cualquier sync en curso (es en memoria).

---

## Relación con el rediseño

El rediseño NO modifica el sync ML actual. Lo que cambia es:

1. Las ventas ML se vinculan con las facturas Tango vía `DatosAplicacionExterna.ExternalID = ml_order_id` (ver `ADARA-TFACTURA.md`) — **implementado el 4/8/2026**
2. El descuento de stock pasa al modelo de lotes FIFO con `consumo_lote` (ver `ADARA-STOCK.md`) — **implementado**
3. La línea de negocio se asigna automáticamente por familia + canal — **implementado**
4. Las devoluciones no vendibles pueden disparar reclamos (ver `ADARA-RECLAMOS.md`)

---

## Actualización — 7 Julio 2026 (promos con aporte de ML — gap probable)

En promos **compartidas** ("X% Mercado Libre") el aporte de ML **no se captura**: `importe_bruto = total_amount` viene con el descuento de ML aplicado (= precio del comprador) y el aporte llega como `coupon`, que el sync **saltea**. Resultado: ingreso/débito/margen subestimados. Evidencia SKU 88B: cluster $42.999 (comprador) vs seller-effective $46.198. Doc dedicado: **`ADARA-ML-BONIFICACIONES.md`** (regla ML-BON1).

> **Nota 4/8/2026:** con `total_facturado_tango` en la base, el aporte y el delta de IVA quedaron **calculables en SQL**. Ver `ADARA-ML-BONIFICACIONES.md` §12.
