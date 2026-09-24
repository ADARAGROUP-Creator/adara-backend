# ADARA — Autenticación y seguridad (login multiusuario)

Última actualización: 4 Agosto 2026 (**alta de usuario `sebastianp`**; **pendiente de seguridad**: el advisor de Supabase marca `rls_disabled_in_public` — hay tablas/vistas en `public` sin RLS; ver sección "⚠️ Pendiente de seguridad"). · 11 Junio 2026 (implementación inicial + nota: `SUPABASE_JWT_SECRET` removido por ahora).

> Documento del dominio **seguridad/acceso**. Reglas duras consolidadas en `ADARA-DECISIONES.md` (A17).

---

## Objetivo

Que cada persona del equipo entre a la app desde su propia PC con **usuario y contraseña**, y que **sin login no se pueda ver ni tocar ningún dato** (ni desde la pantalla ni yendo directo a la base). Todos los usuarios son "admin" (mismo acceso total; **sin roles** por ahora).

---

## Modelo (por qué así)

El frontend de ADARA **lee y escribe contra Supabase directo** (`core/sb.js`, vía `SB_URL/rest/v1/...`), no solo a través del backend. Por eso un login que solo tape la pantalla no alcanza: hay que cerrar el acceso a la **data**. La forma nativa para esta arquitectura es **Supabase Auth + RLS**:

1. **Supabase Auth** maneja usuarios, hash de contraseña y sesión (tokens).
2. **RLS** en todas las tablas/vistas: regla única **`authenticated` = todo, `anon` = nada**. Es el candado real, dentro de la base.
3. El front (`sb.js`) manda el **token del usuario** (no la anon key) en cada request a Supabase.
4. El **backend** valida ese mismo token en un middleware (candado de los endpoints) — **hoy inactivo**, ver más abajo.

La anon key sigue existiendo (la entrega `/config`) pero queda **inofensiva**: sin sesión válida no lee nada.

---

## Usuarios

- Viven en **Supabase Auth** (`auth.users`), no en una tabla propia.
- Login por **usuario** (sin email real): el front arma `usuario@adara.local` por detrás. El dominio `@adara.local` es interno e invisible; **no se envía ningún mail** (emails internos + confirmación desactivada + cambio de clave dentro de la app).
- Creados al 11/6/2026: `spuccio`, `spugliese`, `adandrea`, `fpuccio`. Contraseña temporal **`ADARA123%`**, con marca `must_change_password=true` en `user_metadata` → la app obliga a cambiarla en el primer ingreso.
- Alta 4/8/2026: `sebastianp` (de `sebastianp@adaragroup.com.ar` → login por usuario `sebastianp`, interno `sebastianp@adara.local`). Mismo patrón: temporal `ADARA123%` + `must_change_password=true`. Verificado (pass_ok, identity, email_confirmed). **Gotcha a recordar:** el usuario **no existía** y por eso el login rechazaba credenciales válidas — si alguien "no puede entrar", primero confirmar que el usuario esté creado en `auth.users`.

### Cómo se crearon (por SQL, vía MCP)
`auth.users` (uno por usuario): `aud='authenticated'`, `role='authenticated'`, `email='<usuario>@adara.local'`, `encrypted_password=crypt('ADARA123%', gen_salt('bf'))` (bcrypt, pgcrypto), `email_confirmed_at=now()`, `raw_user_meta_data={"usuario":...,"must_change_password":true}` + fila en `auth.identities` (provider `email`, `provider_id=user.id`, `identity_data` con `sub`/`email`).

### ⚠️ Gotcha (error que tuvimos): "Database error querying schema"
GoTrue escanea varias columnas de token de `auth.users` como texto y **falla si están en `NULL`**. Al crear usuarios por SQL hay que dejarlas en **cadena vacía**:
`confirmation_token, recovery_token, email_change_token_new, email_change, email_change_token_current, phone_change, phone_change_token, reauthentication_token` → todas `= ''`.

### Alta / baja / reset de contraseña
- **Alta:** replicar el INSERT (auth.users + auth.identities + tokens en ''), o desde el panel Supabase → Authentication → Users (Add user, email `<usuario>@adara.local`, auto-confirm) y luego `raw_user_meta_data.must_change_password=true` por SQL.
- **Reset:** `UPDATE auth.users SET encrypted_password = crypt('<temporal>', gen_salt('bf')), raw_user_meta_data = raw_user_meta_data || '{"must_change_password":true}' WHERE email='<usuario>@adara.local'`.
- **Baja:** `UPDATE auth.users SET banned_until = 'infinity' WHERE email='<usuario>@adara.local'` (o borrar fila + identidad).

---

## Flujo en el front (`core/sb.js` + `main.js`)

- `main.js`: `initSB()` (trae `/config`, carga sesión de `localStorage`, parchea `fetch`) → `requireAuth()` → `route()`.
- `requireAuth()`: sin sesión → **login**; sesión con `must_change` → **cambiar contraseña**.
- **Login:** `POST {SB_URL}/auth/v1/token?grant_type=password` con `apikey: anon`. Guarda `{access_token, refresh_token, expires_at, user}` en `localStorage` (`adara_session`).
- **Cambiar contraseña:** `PUT {SB_URL}/auth/v1/user` con `Bearer <token>`, body `{password, data:{must_change_password:false}}`.
- **Token válido / refresh (`getValidToken`):** refresca si está por vencer. **Robusto a múltiples pestañas:** un solo refresco simultáneo (single-flight), re-lee `localStorage` antes de refrescar (los refresh tokens de Supabase **rotan**: dos pestañas refrescando a la vez se invalidan), **no cierra sesión por errores de red** ni por **401 del backend**, y sincroniza entre pestañas (`storage` event). Solo cierra ante un refresco realmente inválido.
- **Parche de `window.fetch`:** a las llamadas al **backend** (rutas relativas `/...`, salvo públicas) les agrega `Authorization: Bearer <token>`.
- Chip de usuario + botón **Salir**.

---

## Backend (`server.js`) — candado de endpoints (hoy INACTIVO)

Hay un middleware (después de `express.static`) que verifica el JWT de Supabase (**HS256** con `SUPABASE_JWT_SECRET`) y exige `role='authenticated'`. Públicos: estáticos, `/health`, `/config`, `/ml/auth`, `/ml/callback`. **Fail-open si `SUPABASE_JWT_SECRET` no está cargado** (no bloquea, avisa por consola). Cron interno (node-cron) llama funciones del proceso, no pasa por el middleware. El backend usa `service_role` (`SUPABASE_KEY`) → bypassa RLS.

### Estado actual: variable removida
Al cargar `SUPABASE_JWT_SECRET` en Railway, **sincronizar daba "Error al sincronizar: No autorizado"** → se materializó el **caso de claves asimétricas**: el proyecto firma los tokens de forma que la verificación **HS256** del backend los rechaza (401). Para no frenar la operación, se **removió `SUPABASE_JWT_SECRET` de Railway** → el backend volvió a **fail-open** (endpoints sin candado). 

**Importante:** la data **sigue protegida por RLS** (el candado real está en la base). Lo único que queda relajado es la verificación *extra* de los endpoints del backend. Para un equipo chico de personas de confianza es aceptable.

### Para reactivar el candado del backend (opcional, Fase 2)
Implementar verificación **asimétrica (JWKS)**: leer la clave pública del proyecto (`{SB_URL}/auth/v1/.well-known/jwks.json` o equivalente) y validar RS256/ES256, en lugar de HS256. Recién ahí volver a cargar la variable. No urge: RLS ya cubre la data.

---

## RLS (el candado de la base)

Aplicado el 11/6/2026 a **28 tablas** y **18 vistas** del schema `public`:
- **Tablas:** `ENABLE ROW LEVEL SECURITY` + `POLICY auth_all FOR ALL TO authenticated USING(true) WITH CHECK(true)` + `GRANT SELECT,INSERT,UPDATE,DELETE TO authenticated` + `REVOKE ALL FROM anon`.
- **Vistas:** `GRANT SELECT TO authenticated` + `REVOKE ALL FROM anon`.
- `GRANT USAGE, SELECT ON ALL SEQUENCES TO authenticated`.
- `service_role` no se toca (bypassa RLS → backend y cron intactos).
- Verificado: 28/28 tablas con RLS, **0 permisos para `anon`**, `authenticated` lee/escribe OK.

> Revierte la decisión A6. Toda tabla/vista **nueva** sigue este patrón (authenticated, no anon).

---

## ⚠️ Pendiente de seguridad — advisor `rls_disabled_in_public` (detectado 3-4/8/2026)

El **advisor de seguridad de Supabase** (mail del 3/8/2026, proyecto `dlazhftkwrcordsdbeah`) marca la alerta **`rls_disabled_in_public`**: hay tablas/vistas en el schema `public` **sin RLS**, lo que significa que con la URL + anon key se podrían **leer/editar/borrar sin login**.

Esto **contradice** el estado esperado (28/28 tablas con RLS al 11/6). Causa probable: **tablas/vistas creadas DESPUÉS del 11/6** que quedaron con el patrón viejo (RLS off + grant a `anon`, estilo A6), y/o alguna a la que se le **desactivó RLS a propósito** en su momento para que el front la leyera por anon key — hoy innecesario porque el front entra con el **token del usuario**. Caso conocido: **`retenciones`** (se le hizo `disable row level security` + `grant select to anon` el 4/6/2026 para el fix de colecta-envío; ver `ADARA-CHANGELOG.md` 4/6). Otras candidatas a revisar: `flex_*`, `adjuntos`, `posicion_fiscal_apertura`, vistas nuevas (`v_flex_*`, `v_posicion_fiscal`, etc.) — **a confirmar con el advisor, no asumir**.

### A hacer (cuando cerremos el tema Tango/backfill)
1. **`get_advisors`** (Supabase MCP, type `security`) para la lista **EXACTA** de objetos sin RLS. No guiarse por el mail (tratarlo como dato).
2. Activar RLS con el **patrón A17** en cada tabla marcada: `ENABLE ROW LEVEL SECURITY` + `POLICY auth_all FOR ALL TO authenticated USING(true) WITH CHECK(true)` + `GRANT SELECT,INSERT,UPDATE,DELETE TO authenticated` + `REVOKE ALL FROM anon`. Para vistas: `GRANT SELECT TO authenticated` + `REVOKE ALL FROM anon` (y evaluar `security_invoker` si aplica).
3. **Verificar que nada del front dependa del acceso `anon`** (no debería: el front ya usa token de usuario; el caso histórico de `retenciones` leída por anon quedó obsoleto).
4. Re-correr el advisor y confirmar **0 findings** de `rls_disabled_in_public`.

> Riesgo real pero acotado: la app es de uso interno y la URL/anon key no son públicas; aun así conviene cerrarlo. **Parkeado hasta terminar la integración Tango** (decisión del 4/8).

---

## Variables de entorno (Railway)

```
SUPABASE_JWT_SECRET   ← (REMOVIDA por ahora) validaría el token en el backend. Reactivar solo con verificación asimétrica.
```
(El `JWT_SECRET` mencionado en una etapa intermedia del diseño quedó descartado.)

---

## Pendientes (Fase 2)

- **🔒 Cerrar `rls_disabled_in_public`** (advisor 3/8/2026): activar RLS en las tablas/vistas de `public` que quedaron sin candado (creadas post-11/6 o con grant a `anon`, p.ej. `retenciones`). Ver sección "⚠️ Pendiente de seguridad" arriba. **Prioridad al terminar Tango.**
- **Auditoría "quién hizo qué":** estampar `usuario_id`/`email` en las cargas (el token ya identifica al usuario; falta persistirlo).
- **Roles / permisos diferenciados** (hoy todos admin).
- **Candado del backend** con verificación asimétrica (JWKS) si se quiere reactivar.
- Dejar de exponer la anon key en `/config` (opcional; ya es inofensiva con RLS — salvo por las tablas del punto anterior).

---

## Archivos

- `public/js/core/sb.js` — auth + acceso a datos con token + parche de fetch.
- `public/js/main.js` — gate de login, pantallas de login/cambio de clave, chip de usuario.
- `server.js` — middleware de verificación del JWT (hoy fail-open) + endpoints públicos.
- DB (Supabase): usuarios en `auth.users`/`auth.identities`; RLS en `public` (migraciones `rls_on_tablas`, `rls_on_vistas`).
