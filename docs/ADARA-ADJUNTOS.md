# ADARA — Adjuntos (comprobantes)

Última actualización: 21 Junio 2026 (creación del dominio)

Sistema reutilizable para guardar archivos (facturas, comprobantes, despachos) y
asociarlos a cualquier operación del sistema. Implementado sobre **Supabase
Storage** + una tabla polimórfica. Hoy enganchado en **Gastos** y **Compras**;
diseñado para sumar **despachos / importaciones** sin tocar el backend.

---

## Objetivo

Que un gasto o una compra pueda llevar su **factura/comprobante en PDF o imagen**
guardado de verdad (no solo el número de factura), visible y descargable cuando
haga falta. El mismo mecanismo sirve para cualquier operación futura.

Encaja con el flujo de Gastos: un gasto se carga como factura A (neto + IVA
crédito), forma de pago transferencia (se concilia con el movimiento bancario vía
`vinculos`), y **la factura va adjunta al gasto**. El adjunto es el comprobante
que respalda el gasto; el pago es un movimiento aparte que se le vincula.

---

## Modelo

Dos piezas:

1. **Bucket privado `comprobantes`** en Supabase Storage.
   - `public = false`, límite **10 MB** por archivo.
   - MIME permitidos: `application/pdf`, `image/jpeg`, `image/png`, `image/webp`.
   - `storage.objects` queda **sin policies** → solo `service_role` (el backend)
     accede. El frontend NUNCA toca Storage directo: todo pasa por el backend.

2. **Tabla polimórfica `adjuntos`** (un registro por archivo).

```
adjuntos
  id          bigint IDENTITY PK
  op_tipo     text NOT NULL        -- 'gasto' | 'compra' | 'despacho' | ...
  op_id       bigint NOT NULL      -- id de la operación a la que pertenece
  bucket      text NOT NULL DEFAULT 'comprobantes'
  path        text NOT NULL        -- ruta dentro del bucket
  nombre      text NOT NULL        -- nombre original del archivo
  mime        text
  tamano      bigint               -- bytes
  subido_en   timestamptz NOT NULL DEFAULT now()
  subido_por  text
  INDEX ix_adjuntos_op (op_tipo, op_id)
```

Patrón post-A17: RLS ON + policy `auth_all` para `authenticated` + GRANT a
`authenticated, service_role`, sin `anon`. El front lee `adjuntos` directo por
REST (para mostrar el 📎); el archivo en sí solo lo sirve el backend.

**Esquema de `path`:** `${op_tipo}/${op_id}/${timestamp}-${nombre_saneado}`
(ej: `gasto/42/1718900000000-factura_edenor.pdf`). El nombre se sanea (sin
tildes, solo `[a-zA-Z0-9._-]`, 80 chars máx).

---

## Endpoints (backend)

Todos en `server.js`. Usan `multer` (memoryStorage → `req.file.buffer`) y suben a
Storage vía REST con la `service_role` key (`SUPABASE_KEY`).

| Método | Ruta | Qué hace |
|---|---|---|
| `POST` | `/adjuntos` | Multipart: campos `op_tipo`, `op_id` + `file`. Sube a Storage e inserta la fila. Devuelve `{ ok, id, path }`. |
| `GET` | `/adjuntos/:id/url` | Genera un **link firmado temporal** (1 h) para ver/descargar. Devuelve `{ url, nombre }`. |
| `DELETE` | `/adjuntos/:id` | Borra el archivo de Storage + la fila. |

Helpers internos: `ADJ_BUCKET='comprobantes'`, `STORAGE_H` (headers
service_role), `_safeName()`.

---

## Integración frontend

Patrón aplicado en `gastos.js` y `compras.js`:

1. **Alta:** un campo `<input type="file" accept="application/pdf,image/*">` en el
   modal.
2. **Subida diferida:** primero se crea la operación (POST /gastos o /compras),
   se toma el **id devuelto** (`data.id` en gastos, `data.compra_id` en compras),
   y recién ahí se sube el archivo a `/adjuntos` con `op_tipo` + `op_id`.
3. **Tolerante a fallo:** si la operación se guarda pero la subida falla, se avisa
   con un toast y NO se pierde la operación.
4. **Lista:** se carga el mapa de adjuntos (`sbGet('adjuntos','op_tipo=eq.<tipo>...')`)
   y las filas con adjunto muestran un **📎** que, al clickear, pide el link
   firmado (`GET /adjuntos/:id/url`) y abre el archivo.

---

## Cómo engancharlo en una pantalla nueva (ej: Despachos)

El backend ya está listo y es genérico. Para sumar adjuntos a otra pantalla solo
hace falta tocar **el frontend** de esa pantalla:

1. Agregar el `<input type="file">` en su modal de alta.
2. Tras crear la operación, subir con `op_tipo='<nuevo_tipo>'` y `op_id=<id>`.
3. Cargar `sbGet('adjuntos','op_tipo=eq.<nuevo_tipo>&select=id,op_id,nombre')` y
   pintar el 📎 que llama a `/adjuntos/:id/url`.

No hay que crear tablas, buckets ni endpoints nuevos.

---

## ⚠ Guardrails técnicos

- El frontend **nunca** sube ni lee de Storage directo: siempre por el backend
  (service_role). El bucket es privado a propósito.
- La factura adjunta (📎) es distinta del **número de factura** (texto). En
  Compras conviven: el 📎 es el archivo, "Nº factura" es el string `A 0001-…`.
- El adjunto se sube **después** de crear la operación (necesita el `op_id`). Si
  se invierte el orden quedaría un archivo huérfano.
- Borrar la operación no borra automáticamente sus adjuntos (no hay FK con
  cascade; `op_id` es polimórfico). Si más adelante importa, limpiar con el
  `DELETE /adjuntos/:id` o un job por `op_tipo+op_id`.

---

## Pendientes

- Enganchar adjuntos en **Despachos / Importaciones** (solo frontend; backend listo).
- Mostrar/permitir **múltiples** adjuntos por operación en la UI (hoy el 📎 abre
  el primero; la tabla ya soporta N).
- Poblar `subido_por` con el usuario logueado (hoy queda NULL; el backend es
  fail-open en auth).

---

## Documentos relacionados

- `ADARA-GASTOS.md` — la factura del gasto va como adjunto.
- `ADARA-COMPRAS-IMPORTACIONES.md` — comprobante de compra / despacho.
- `ADARA-SCHEMA.md` — definición de la tabla `adjuntos` y el bucket.
- `ADARA-FRONTEND.md` — patrón de UI (campo de archivo + 📎).
