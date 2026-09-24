# ADARA — Integración Padrón ARCA (constancia de inscripción)

Última actualización: 30 Mayo 2026

Integración con el web service oficial de ARCA (ex AFIP) para, dado un **CUIT**, traer **razón social, tipo de persona y estado**. Se usa para autocompletar el alta de proveedores en Compras y Gastos.

Es gratis: usa los WS oficiales (WSAA + constancia de inscripción), no un servicio pago de terceros.

---

## Qué resuelve

- En el alta rápida de proveedor (Compras y Gastos), tipeás el CUIT y se completa sola la razón social, con verificación contra el padrón real.
- Evita errores de tipeo y proveedores mal nombrados.

Endpoint expuesto por el backend:

```
GET /padron/:cuit  →  { cuit, nombre, tipoPersona, estado }
```

Ejemplo real:
```
GET /padron/30717476472
→ {"cuit":"30717476472","nombre":"ADARA RS","tipoPersona":"JURIDICA","estado":"ACTIVO"}
```

El `:cuit` se normaliza a 11 dígitos (acepta guiones) y se valida el dígito verificador antes de consultar.

---

## Arquitectura (dos pasos)

ARCA exige autenticarse primero (WSAA) y después consultar el padrón con ese ticket.

### 1) WSAA — obtener Ticket de Acceso (token + sign)

1. Se arma un **TRA** (Ticket de Requerimiento de Acceso): un XML con `uniqueId`, `generationTime`, `expirationTime` y `service = ws_sr_constancia_inscripcion`.
2. Se **firma el TRA como CMS (PKCS#7)** con el certificado + clave privada, usando **node-forge**.
3. Se hace `loginCms` (SOAP) contra `https://wsaa.afip.gov.ar/ws/services/LoginCms`.
4. ARCA devuelve un **token** y un **sign** con validez de ~12 horas.

### 2) Consulta al padrón

Con token + sign se consulta el servicio de constancia de inscripción:

- `https://aws.afip.gov.ar/sr-padron/webservices/personaServiceA5` (SOAP `getPersona_v2`).
- Parámetros: `cuitRepresentada = ARCA_CUIT` (el CUIT de ADARA, dueño del certificado), `idPersona = ` el CUIT consultado.
- Devuelve razón social / apellido y nombre, tipo de persona (FISICA/JURIDICA) y estado (ACTIVO, etc.). El backend normaliza la respuesta al JSON simple de arriba.

---

## Caché del Ticket de Acceso (TA)

El TA vale ~12 h. Para no pedir uno nuevo en cada consulta:

- Se guarda **en memoria** (mientras viva el proceso).
- Se **persiste en Supabase** en la tabla `arca_ta` (sobrevive a redeploys de Railway).
- En cada `/padron`, si hay un TA vigente (memoria o tabla) se reutiliza; si venció, se pide uno nuevo y se guarda.

Tabla:

```sql
CREATE TABLE arca_ta (
  id          int PRIMARY KEY,        -- siempre 1 (una sola fila)
  token       text,
  sign        text,
  expiration  timestamptz
);
```

La persistencia es **graceful**: si la tabla no existe, sigue funcionando solo con la caché en memoria.

---

## Configuración (variables de entorno en Railway)

| Variable | Contenido |
|----------|-----------|
| `ARCA_CERT` | El certificado público (`.crt`) que emite ARCA, en texto PEM completo. |
| `ARCA_KEY`  | La **clave privada** (`.key`) con la que se generó el CSR, en texto PEM. **No es el CSR.** |
| `ARCA_CUIT` | CUIT de ADARA dueño del certificado: `30717476472`. Va como `cuitRepresentada`. |

### Normalizador de PEM (importante)

Railway suele **aplastar los saltos de línea** al pegar las variables, y el PEM deja de ser válido. El backend incluye un normalizador que **reconstruye** el formato:
- Detecta los headers `-----BEGIN ...-----` / `-----END ...-----`.
- Reagrupa el cuerpo en líneas de 64 caracteres.
- Así funciona aunque la variable venga en una sola línea.

---

## El certificado

- Alias en ARCA: **ADARA-APP**.
- CUIT titular: **30-71747647-2** (ADARA RS).
- Validez: **30/05/2026 → 29/05/2028**.
- Servicio autorizado al alias: **"Consulta de constancia de inscripción"** (Administrador de Relaciones de Clave Fiscal → adherir servicio → asociar al certificado/alias).

### Cómo se generó (openssl)

```bash
# 1) clave privada (2048 bits)
openssl genrsa -out adara_privada.key 2048

# 2) CSR (pedido de certificado)
openssl req -new -key adara_privada.key -subj "/C=AR/O=ADARA RS/CN=adara/serialNumber=CUIT 30717476472" -out adara_pedido.csr
```

- El `.csr` se sube a ARCA (Administración de Certificados Digitales → crear certificado) y ARCA devuelve el `.crt`.
- `ARCA_KEY` = contenido de `adara_privada.key`. `ARCA_CERT` = contenido del `.crt` de ARCA.
- El módulo del `.crt` debe coincidir con el de la `.key` (si no, no validan juntos).

---

## Diagnóstico

```
GET /padron-diag
```

Devuelve el estado de la config (si están `ARCA_CERT` / `ARCA_KEY` / `ARCA_CUIT`, si el PEM parsea, si hay TA vigente) **sin exponer el contenido del secreto**. Útil cuando algo falla.

---

## Uso en el frontend

En el alta rápida de proveedor de **Compras** (`compras.js`) y **Gastos** (`gastos.js`):

- El campo **CUIT va primero**.
- Al salir del campo (`blur`), `buscarPadronProv()` consulta `/padron/:cuit` y **autocompleta la razón social**, con un cartelito al lado: `✓ NOMBRE · ESTADO` si lo encontró, o un aviso si el CUIT está mal / no existe.
- El nombre queda editable por si hace falta ajustarlo.
- Ver `ADARA-COMPRAS-IMPORTACIONES.md` (sección implementación) para el get-or-create de proveedores por CUIT.

---

## Troubleshooting (historial)

| Síntoma | Causa | Solución |
|---------|-------|----------|
| WSAA rechaza la firma | En `ARCA_KEY` estaba pegado el **CSR** en vez de la clave privada | Pegar el contenido de `adara_privada.key` (no el `.csr`) |
| PEM inválido / no parsea | Railway aplastó los saltos de línea | Normalizador de PEM (ya incluido en el backend) |
| `cert y key no coinciden` | Cert y clave de pares distintos | Regenerar el par o usar el `.crt` correcto para esa `.key` |

---

## Pendientes / mantenimiento

- **Renovar el certificado antes del 29/05/2028.** Generar nuevo CSR (misma clave o nueva), subir a ARCA, reemplazar `ARCA_CERT` (y `ARCA_KEY` si se cambió la clave).
- Posible uso futuro: traer condición frente a IVA / actividad del padrón para automatizar tratamiento fiscal de proveedores (hoy solo razón social + estado).

---

## Dependencias

- `node-forge` `^1.4.0` (firma CMS del TRA) — en `package.json`.

---

## Documentos relacionados

- `ADARA-COMPRAS-IMPORTACIONES.md` — dónde se usa (alta de proveedores en Compras).
- `ADARA-GASTOS.md` — alta de proveedores en Gastos.
- `ADARA-SCHEMA.md` — endpoint `/padron`, `/padron-diag` y tabla `arca_ta`.
- `ADARA-SYSTEM.md` — infraestructura (Railway, variables de entorno).
