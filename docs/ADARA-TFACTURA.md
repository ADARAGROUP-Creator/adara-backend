# ADARA — Integración Tango Factura

Última actualización: **10 Agosto 2026 — HALLAZGO: el sync descarta toda la facturación que NO es de Mercado Libre.** `ListarMovimientos` ya trae TODAS las facturas del período y el código pide el detalle de cada una, pero las no-ML se cuentan en `stats.no_ml` y se tiran. Es la razón por la que la facturación B2B de luminarias nunca entra a ADARA. Ver §"Hallazgo 10/8/2026". · **4 Agosto 2026 — INTEGRACIÓN IMPLEMENTADA Y VALIDADA EN PRODUCCIÓN.** Se destrabó el auth (credenciales + endpoint real), se descubrió que el código mandaba parámetros equivocados a `ListarMovimientos` (causa raíz de la lentitud/errores), y se implementó el sync que pega a cada venta ML su **N° de factura, tipo (A/B/C) y monto facturado**. Ver §"Implementación real". · Historial: 8 Junio 2026 (stock de apertura por export único) · 19 Mayo 2026.

Cómo se integra ADARA con **Tango Factura** (Axoft S.A.) — el software de facturación electrónica que es la fuente fiscal oficial del negocio.

> Nota: el portal se muestra como "Tfactura.io" pero el dominio real y la API son `tangofactura.com`. Es el mismo producto. Doc oficial de la API: `https://www.tangofactura.com/Help/DocApi?resName=Factura`.

ADARA es **reactivo**: consume datos de Tango (lectura), no le manda facturas (no las emite).

---

## ⚠ Verdades duras aprendidas el 4/8/2026 (leer antes de tocar el sync)

1. **Variables de entorno en Railway = `TF_*`, NO `TANGO_*`.** El `server.js` lee:
   ```
   TF_APP_KEY    (= ApplicationPublicKey / Clave pública)
   TF_USERNAME   (= Username)
   TF_PASSWORD   (= Password)
   TF_USER_ID    (= UserIdentifier / Identificador de usuario)
   ```
   La doc vieja decía `TANGO_*` — **era incorrecto**. Si `/health` da `tango:false`, lo primero a chequear es que existan las `TF_*` con esos valores exactos (los tokens largos de la app, no el usuario/clave humano; ojo con el `=` final y los espacios).

2. **Endpoint de auth que FUNCIONA:** `POST https://www.tangofactura.com/Provisioning/GetAuthToken` con body `{ UserName, Password, UserSecret }` (UserSecret = UserIdentifier, **no** la PublicKey). La respuesta es un **string URL-encoded suelto** → `token = decodeURIComponent(await r.json())`. (El código viejo usaba `/Services/Autorizacion/GetToken` con parseo `data.Data.Token` → estaba mal, se corrigió.)

3. **`ListarMovimientos` usa `Desde` / `Hasta` / `Tope`** — NO `FechaComprobante` / `FechaServicioHasta` (esos son de otro endpoint). Mandar los nombres equivocados hacía que Tango **ignorara el filtro de fechas y corriera una consulta sin límite** → lentísima y con error de base de datos ("An error occurred while executing the command definition"). Con los parámetros correctos la consulta es chica, rápida y estable. **Este fue el bug que nos volvió locos medio día.**

4. **`ObtenerInfoMovimientosPorNroFactura` es inservible** (>55s, se cuelga). No usar. El listado se hace con `ListarMovimientos`.

5. **`ListarMovimientos` es lento igual** (~20-35s por llamada) y no soporta ráfagas (throttling / errores transitorios de Tango). Por eso: correr en **segundo plano**, con **reintento + backoff**, sin martillar. El **detalle** (`ObtenerInfoMovimiento`) en cambio es rápido (~0,5s) y confiable.

6. **La tabla `facturas_tango` NO existe** en la base del rediseño (estaba en docs viejos, nunca se recreó). El dato fiscal de las ventas ML vive **en `ventas_ml`** (columnas nuevas, ver §"Modelo de datos"). No hay que crear `facturas_tango` salvo que se decida guardar el universo completo de comprobantes (incluidos los no-ML) para conciliación — pendiente.

---

## Autenticación con la API

### Endpoint de auth (validado)

```
POST https://www.tangofactura.com/Provisioning/GetAuthToken
Body: { "UserName": "<TF_USERNAME>", "Password": "<TF_PASSWORD>", "UserSecret": "<TF_USER_ID>" }
```

Respuesta = **string URL-encoded** (no un objeto `{Data:...}`):
```javascript
const raw = await r.json();               // string suelto, ej "OWJ6TXFu...%2F..."
const token = decodeURIComponent(raw);    // token final
```

El **Token vive ~horas**; el código lo cachea 18 min y lo renueva. Las 4 credenciales se sacan del portal `https://www.tangofactura.com/PGR/Aplicaciones` (crear app → Autorizar).

---

## Endpoints usados por ADARA

Todos POST en `https://www.tangofactura.com/Services/Facturacion/`. `tfPost` agrega al body `ApplicationPublicKey`, `UserIdentifier` y `Token`.

| Endpoint | Parámetros clave | Uso | Estado |
|----------|------------------|-----|--------|
| `ListarMovimientos` | **`Desde`, `Hasta`, `Tope`** | Lista de comprobantes de venta en rango. Vista resumida. | ✅ En uso (lento ~30s) |
| `ObtenerInfoMovimiento` | `MovimientoId`, `ObtenerInfoAplicaciones:true` | Detalle completo de UNA factura: CAE, `Numero`, `Letra`, `Total`, y `DatosAplicacionExterna.ExternalID` (= ml_order_id). | ✅ En uso (rápido ~0,5s) |
| `ObtenerInfoMovimientosPorNroFactura` | — | — | ❌ Descartado (se cuelga >55s) |

**Importante:** `DatosAplicacionExterna` (el vínculo con ML) **solo viene si se setea `ObtenerInfoAplicaciones:true`** en el detalle. `ListarMovimientos` NO lo trae.

---

## Lo que trae cada factura

### `ListarMovimientos` (resumen) — campos por movimiento
`MovimientoId`, `MovimientoFecha` (formato .NET `/Date(ms)/`), `MovimientoLetra` (A/B/C), `MovimientoDescripcion` (ej. `"Factura de venta stock B 00003-00039519"` — el N° va embebido), `ClienteNombre`, `Total`, `EstadoId` (3 = emitido OK, a confirmar), `MovimientoCuotas[]`, `Renglones[]` (con `ProductoCodigo`=SKU, `Cantidad`, `Precio`, `Bonificacion`, alícuotas IVA).

### `ObtenerInfoMovimiento` (detalle) — agrega, entre otros
`CAE`, `VencimientoCAE`, `UrlPDF`, `Letra`/`Numero` (ej. `B` / `0003-00039519`), `Subtotal`, `TotalIVA`, `Total`, `ClienteTipoDocumento`/`ClienteNumeroDocumento`, `ClienteRazonSocial`, `ClientePerfilImpositivo`, `FormaPago`, `Cuotas[]`, `Renglones[]`, y **`DatosAplicacionExterna`**:

```json
"DatosAplicacionExterna": {
  "AplicacionNombre": "Mercado Libre",
  "ExternalID": "2000016212537666",   // = ml_order_id
  "DatosComprador": "{...}", "DatosPago": "[...]"
}
```

`ExternalID` = **`ml_order_id`** → la llave para vincular con `ventas_ml`.

---

## Implementación real (server.js + Supabase)

### Sync (`syncTangoFacturas`)
```
1. ListarMovimientos({ Desde, Hasta, Tope:5000 })  → lista de MovimientoId  (con retry+backoff)
2. Por cada movimiento:
   a. ObtenerInfoMovimiento({ MovimientoId, ObtenerInfoAplicaciones:true })
   b. Si DatosAplicacionExterna.AplicacionNombre == "Mercado Libre" && ExternalID:
        UPDATE ventas_ml
          SET nro_factura, tipo_factura (=Letra), total_facturado_tango (=Total), tango_movimiento_id
          WHERE ml_order_id = ExternalID
   c. Si no matchea ninguna venta_ml → cuenta como "sin_venta" (huérfana, reintenta en próximo sync)
   d. Si no es de ML → se ignora (por ahora; B2B/efectivo = etapa futura)
```

### Endpoints del backend
- **`GET /tango/sync?desde=YYYY-MM-DD&hasta=YYYY-MM-DD`** — corre en **segundo plano** (devuelve `{status:'started'}`; volver a pegarle da `{status:'running', progress}` o `{status:'done', ...stats}`). `?reset=1` reinicia. Rango recomendado ≤ ~30 días por corrida.
- **`GET /tango/facturas`** — lista las ventas ML que ya tienen factura vinculada (lee de `ventas_ml`).
- **`GET /health`** — incluye `tango:true/false` (hace un login de prueba).
- Cron: cada 3 h (`30 */3 * * *`), ventana de 3 días, con guarda anti-solapamiento.

> Endpoints temporales de debug (`/debug/tango`, `/debug/tango-raw`) se usaron para validar y se retiraron. Si reaparece un problema, recrearlos es la vía para ver el error crudo de Tango.

---

## Hallazgo 10/8/2026 — el sync descarta toda la facturación que no es de Mercado Libre

En `server.js`, dentro de `syncTangoFacturas()` (~línea 3280):

```js
if (!(dae && dae.AplicacionNombre === 'Mercado Libre' && dae.ExternalID)) { stats.no_ml++; continue; }
```

`ListarMovimientos` ya trae **TODAS** las facturas del período (con `Desde` / `Hasta` / `Tope: 5000`), y el código **ya pide el detalle de cada una** con `ObtenerInfoMovimiento`. Las que no son de ML se cuentan en **`stats.no_ml`** y **se tiran**.

### Por qué importa

La **facturación B2B de luminarias de Sebastián está en Tango** y **nunca entra a ADARA**. Consecuencias:

- **(a)** La **base imponible de IIBB** que calcula ADARA está **incompleta**.
- **(b)** **4 de las 5 líneas de negocio no tienen P&L.**

**La cañería ya existe y ya transporta esas facturas: le falta un `else`.**

### Dato accionable (medición sin escribir código)

Pegarle a **`/tango/sync`** y mirar el campo **`no_ml`** de la respuesta **mide hoy mismo cuántas facturas B2B hay en el período**, sin tocar una línea de código.

### Distinción importante: base imponible ≠ P&L

| Objetivo | Qué hace falta | Esfuerzo |
|---|---|---|
| **IIBB correcto** (base imponible) | Solo el **monto facturado** (+ fecha y jurisdicción del cliente) | Bajo — el dato ya viene en el detalle |
| **Margen de luminarias** (P&L de la línea) | **SKU y costo** por renglón, con lotes/FIFO | Alto — implica dar de alta compras y stock de esa línea |

Son dos esfuerzos muy distintos y **el primero es mucho más barato**. Se puede cerrar el IIBB sin resolver el P&L.

Esto es el **"paso 1"** del roadmap de `ADARA-IIBB-CONVENIO-MULTILATERAL.md`.

---

## Modelo de datos (columnas en `ventas_ml`)

| Columna | Origen | Significado |
|---------|--------|-------------|
| `ml_order_id` | ML | Llave de vínculo (= `ExternalID` de Tango) |
| `importe_bruto` | ML sync | `total_amount` de ML (incluye el aporte de ML embebido) |
| `aporte_ml` | ML sync | Aporte/subsidio de ML a la promo (ver `ADARA-ML-BONIFICACIONES.md`) |
| `importe_facturado` | ML sync | **Neto del comprador** = `importe_bruto − aporte_ml` (lo que pagó el comprador). **NO es el total de Tango.** |
| `nro_factura` | Tango sync | N° de factura (ej. `0003-00039519`) |
| `tipo_factura` | Tango sync | Letra A / B / C |
| `total_facturado_tango` | Tango sync | Total real facturado en Tango (`ObtenerInfoMovimiento.Total`) |
| `tango_movimiento_id` | Tango sync | `MovimientoId` (idempotencia/trazabilidad) |

**Comparación clave (control de descuadres):** `total_facturado_tango` **vs** `importe_facturado` (neto comprador). Si coinciden → la factura es correcta (✓). Si difieren → **descuadre real a revisar** (⚠). La diferencia contra el **bruto** NO es descuadre: es el aporte de ML, que no se factura.

> Decisión de diseño (4/8): `importe_facturado` es del ML sync y el total de Tango va en columna propia (`total_facturado_tango`). Antes se reusó `importe_facturado` y los dos syncs se pisaban → se separó. Ningún P&L/vista/función usaba `importe_facturado` al momento del cambio (verificado).

---

## Frontend (pantalla Ventas ML)

La tabla muestra, en la **misma fila**, dos columnas al lado del Bruto:
- **Factura**: `tipo` + `nro_factura` (ej. `B 0003-00039595`), filtrable por número.
- **Facturado**: `total_facturado_tango` con semáforo — **✓ verde** si = neto comprador, **⚠ ámbar** con el descuadre si no.
El detalle de la venta (clic) también muestra la factura. Ver `ADARA-FRONTEND.md`.

---

## Vínculo con Mercado Libre y otros canales

Cuando hay venta ML, Tango la factura automáticamente. La misma venta queda con dos representaciones vinculadas por `ml_order_id ↔ ExternalID`: `ventas_ml` (operativo, API ML) y la factura de Tango (fiscal).

| Canal | En Tango | En ADARA |
|-------|----------|----------|
| Mercado Libre | `DatosAplicacionExterna.AplicacionNombre = "Mercado Libre"` | Vincula a `ventas_ml` existente (implementado ✅) |
| Tienda Nube (TBD) | Probablemente `DatosAplicacionExterna` con otro nombre | A confirmar |
| B2B / WhatsApp / efectivo | Factura/remito **sin** `DatosAplicacionExterna` | Etapa futura: crear venta nueva + descuento FIFO. **Hoy se descartan en `stats.no_ml`** (ver §"Hallazgo 10/8/2026") |

---

## Reconciliación mensual (pendiente de UI)

Comparar comprobantes de Tango vs ventas de ADARA por mes, listar huérfanos. Con `total_facturado_tango` en la base, buena parte ya es calculable en SQL.

---

## Reglas duras

1. **ADARA NO escribe en Tango.** Solo lee.
2. Una venta ML no se duplica: se vincula por `ml_order_id`, no se crea otra.
3. Idempotencia por `MovimientoId` / vínculo por `ml_order_id`.
4. `ObtenerInfoAplicaciones:true` es obligatorio para traer el vínculo ML.
5. Token de Tango se cachea (18 min) y se renueva.
6. `ListarMovimientos` en segundo plano, con retry, sin ráfagas.

---

## Pendientes / TBD

- **Capturar los comprobantes no-ML (`stats.no_ml`) — paso 1 del IIBB por jurisdicción.** El filtro de `syncTangoFacturas()` necesita un `else` que persista esas facturas. Ver §"Hallazgo 10/8/2026" y `ADARA-IIBB-CONVENIO-MULTILATERAL.md`.
- **Backfill histórico completo** más allá de ~30 días (correr `/tango/sync` por ventanas o extender el rango; el cron cubre lo nuevo).
- **Descuadres reales** (`total_facturado_tango ≠ importe_facturado`): revisar caso por caso (3 patrones detectados el 4/8 — ver `ADARA-ML-BONIFICACIONES.md` / export de descuadres).
- **Comprobantes no-ML** (B2B/efectivo/Tienda Nube): crear venta + FIFO (etapa 3).
- **`EstadoId`**: confirmar mapeo (3 = emitido OK, aparente).
- **Notas de crédito / anulaciones**: reflejar como cancelación + reversión de stock.
- **Reautorización del token ML** (`ml_token:false` en `/health` al 4/8) — afecta el sync de ventas ML, no el de Tango. Tema aparte.

---

## Documentos relacionados
`ADARA-IIBB-CONVENIO-MULTILATERAL.md` (roadmap del IIBB por jurisdicción — este doc es el paso 1), `ADARA-ML-BONIFICACIONES.md` (aporte ML, descuadres), `ADARA-VENTAS-ML.md`, `ADARA-FRONTEND.md`, `ADARA-SCHEMA.md`, `ADARA-CONCILIACION-BANCARIA.md`, `ADARA-PNL.md`, `ADARA-IMPUESTOS.md`, `ADARA-DECISIONES.md`, `ADARA-CHANGELOG.md`.
