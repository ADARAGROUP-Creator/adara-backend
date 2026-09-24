# AGENTS.md — Reglas del repo ADARA

> Este archivo lo lee Codex automáticamente al abrir el repo.
> `CLAUDE.md` es su gemelo para Claude Code y dice **lo mismo**. Si cambiás una regla acá, cambiala allá.

---

## 1. Antes de tocar nada: leer la documentación

La documentación del proyecto está en **`docs/`** y es la fuente de verdad del negocio.

**Flujo obligatorio antes de modificar lógica:**

1. Leer `docs/ADARA-DOCS-INDEX.md` — índice de los 30 documentos.
2. Identificar el documento del dominio que se va a tocar.
3. Leer ese documento **completo** antes de proponer cambios.
4. `docs/ADARA-DECISIONES.md` es la constitución: reglas duras ya validadas contra datos reales. **No se contradicen sin discutirlo.**

No reinventar soluciones que ya están documentadas. Si algo parece un bug, primero verificar si es una decisión deliberada.

---

## 2. Reglas duras del negocio

Estas están validadas contra datos reales (despachos de aduana, DJ de ARCA, resúmenes de proveedores). Romperlas rompe números que se le informan a AFIP.

### Costeo y stock
- El costo nace **exclusivamente** de la factura de compra, se guarda por lote en `lotes.costo_unitario` y es **inmutable** (CF6).
- El stock se descuenta y el CMV se reconoce **a la fecha de la venta**, no de la entrega (S9).
- **Un solo sistema mueve stock.** Dos procesos descontando rompen el FIFO.
- Prioridad de costeo: FIFO real → CMV congelado → residual dinámico.
- El margen se calcula sobre neto sin IVA (P11). El IVA es flujo financiero, no P&L.

### Fiscal
- IIBB determinado y Ganancias reducen rentabilidad; percepciones, retenciones y anticipos son pagos a cuenta, **no costos**. Restar ambos es doble conteo (CF9–CF11, P12).
- La fuente canónica del monto de IIBB es `ventas_ml.impuestos`. La tabla `retenciones` sirve **solo** para la apertura por jurisdicción, nunca se suma aparte.
- Derechos, estadística, flete, seguro y despachante **capitalizan** al costo del lote. IVA, IIBB y Ganancias **no**: son crédito fiscal.

### Importaciones
- Aduana prorratea **flete y seguro por FOB**, no por peso. Validado contra despachos oficializados.
- La base fiscal usa el prorrateo por FOB (`fleteFiscal`); el costo del producto usa el prorrateo por peso (`fleteCostoShare`). **Son dos cosas distintas y conviven.**
- El ahorro para la coima se mide sobre **CIF**, no sobre FOB.
- La simulación se valida **contra el despacho oficializado**, no contra planillas de Excel.

### Datos
- **Toda query paginada necesita una columna de orden única como desempate.** Regla dura nacida de un bug con 4.802 filas compartiendo la misma fecha.
- La API REST de Supabase **corta silenciosamente en 1.000 filas**. Paginar siempre.
- `ventas_ml.periodo_cobro` no es confiable; la autoridad es `fecha_cobro`.
- `vinculos.monto` es magnitud positiva, nunca con signo.
- ML `ml_status='cancelled'` **no** significa que el producto no se envió: clasificar con `estado_envio` + campos `recepcion_*`.

### Mercado Libre
- El refresh token de ML es **de un solo uso** y lo rota el cron horario del backend. **Ningún otro proceso debe refrescarlo.**
- Ante sync caído: revisar primero el DevCenter (flujo Refresh Token habilitado + estado del grant), después el código.
- `workspace_config.ml_token_expires` es **bigint con epoch en milisegundos**, no timestamptz.

---

## 3. Base de datos

### Tablas nuevas (patrón post-A17, obligatorio)
```sql
alter table public.<tabla> enable row level security;
create policy "<tabla>_authenticated" on public.<tabla>
  for all to authenticated using (true) with check (true);
grant all on public.<tabla> to authenticated;
revoke all on public.<tabla> from anon;
```

### Migraciones
- Se numeran **por fecha**: `YYYYMMDDHHMMSS_descripcion.sql`. No por secuencia — dos personas crean la `062` el mismo día y se pisan.
- El DDL va por `apply_migration` (invalida el cache de schema). Las lecturas y escrituras puntuales por `execute_sql`.
- SQL multi-statement en `execute_sql` **solo devuelve el primer resultado**. Correr las queries por separado.

### Gotchas
- `periodo` es columna generada inmutable con `extract` + `lpad`, **nunca** `to_char` (es STABLE, no IMMUTABLE en Postgres).
- AR/AP **nunca se persisten**: se derivan de operaciones y vínculos.

---

## 4. Código

### Antes de entregar un `.js`
1. `node --check` sobre el archivo.
2. **Grep de funciones duplicadas.** `node --check` parsea como script, no como módulo, así que **no detecta nombres repetidos**. Es un error real que ya pasó.

### Frontend
- `bindDelegation()` se llama una sola vez en `render()`, **nunca** dentro de `renderBody()`. Si no, los event listeners se acumulan exponencialmente.
- Cache del browser: `Cache-Control: no-cache` en `express.static` **más** query string `?v=` en los imports de módulos ES.

### Deploy
- Front y backend: GitHub → Railway.
- La app de pricing: GitHub → Vercel.
- Después de deployar un `.js`, **verificar abriendo la URL servida** y buscando un símbolo nuevo conocido. No alcanza con que Railway diga "deployed".

---

## 5. Trabajo en paralelo

Somos dos personas con dos agentes distintos (Codex y Claude Code) sobre el mismo repo.

- **Rama por tarea**, merge por PR. Nunca commitear directo a `main`.
- Repartirse por **dominio**, no por archivo: importaciones/fiscal/costeo de un lado, pricing/ML/Tienda Nube del otro.
- Si una tarea toca `docs/ADARA-DECISIONES.md`, avisar antes: es una regla de negocio, no un refactor.
- Migraciones: avisar siempre que se crea una.

---

## 6. Forma de trabajar

- **Discutir antes de tocar código o datos.** Diagnosticar con datos reales primero.
- **Una decisión o pregunta por vez.**
- **Confirmación explícita antes de cambios de schema o de datos.**
- No inventar requisitos ni asumir datos no confirmados.
- Si falta contexto crítico, pedir **solo lo mínimo indispensable**.
- Español rioplatense técnico. Números concretos siempre. Respuestas cortas.
- Analizar **de forma integral**: si un cambio impacta DB, backend, frontend, cálculos, reportes, conciliaciones o impuestos, señalarlo aunque no se haya mencionado.

### Documentación
- Toda decisión técnica, regla de negocio nueva, excepción, auditoría relevante o bug resuelto **se documenta** en el `.md` del dominio correspondiente.
- No se borra información: si no corresponde a un doc, se propone moverla al correcto.
- Si un tema tiene entidad propia, proponer un `.md` nuevo y darlo de alta en `docs/ADARA-DOCS-INDEX.md`.
- Los cambios de documentación van **en el mismo commit** que el código que los motiva.
