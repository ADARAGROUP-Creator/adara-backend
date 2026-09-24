# ADARA — Frontend

Última actualización: 7 Agosto 2026 (**componente nuevo `core/skuPicker.js` · banner de token ML en la home · exportadores del simulador · fila de SKU en el simulador**. Ver "Actualización 7 Agosto 2026" al final.) · 5 Agosto 2026 (fuente de verdad del código = repo `ADARAGROUP-Creator/adara-backend`; Compras con detalle expandible y badge PDF; alícuota de IVA por `select` en los 3 lugares de edición; capitalización de gastos con **preview de impacto**) · 4 Agosto 2026 (Ventas ML: columnas **Factura** + **Facturado** de Tango con semáforo) · 13 Julio 2026 (Ventas ML: **conciliación manual bidireccional**, tablero mensual con anillos de progreso y diferir/traer entre meses) · 16 Junio 2026 (filtros por columna estilo Excel + **regla dura de paginación**: toda `sbGet` sobre tablas >1.000 filas ordena por columna única) · 13 Junio 2026 (pantallas **Saldos** y **Cuadre**; **router serializado**) · 10 Junio 2026 (pantalla **Posición Fiscal**) · 8 Junio 2026 (pantalla **Resultado**; Costeo → **Inventario**) · 28 Mayo 2026 (pantalla Movimientos v1)

Arquitectura, convenciones y patrones del frontend v22 (rediseño).
**Lectura obligatoria antes de tocar HTML/CSS/JS o agregar una pantalla nueva.**

---

## Fuente de verdad del código (confirmado 5/8/2026)

| Qué | Dónde |
|---|---|
| **Repositorio** | **`ADARAGROUP-Creator/adara-backend`** — público, branch **`main`** |
| **Frontend** | `public/` |
| **Backend** | `server.js` (raíz del repo) |

> ⚠️ **`adara-app-v21.html` (el que vive en el proyecto Claude) es LEGACY y NO corresponde a producción.** Es el monolito viejo: **tema oscuro**, **no menciona `compra_componentes`**, y **no tiene compras, SKUs ni prorrateables**. Consultarlo lleva a conclusiones equivocadas.

> ⚠️ **El entorno de Claude NO puede pushear al repo** (7/8/2026). Hay un proxy de git que sólo permite escribir en repos autorizados de la sesión, y rechaza el push con 403 antes de autenticar — un token de GitHub no cambia nada. **Los archivos los sube Sebastián a mano** por la web. Para archivos **nuevos**: *Add file → Create new file*, escribiendo la **ruta completa** en el nombre (`public/js/core/skuPicker.js`), que GitHub crea las carpetas solas. Si la descarga de un `.js` falla en el cliente, mandarlo **de a uno por mensaje** o pegar el contenido en el chat.

---

## Objetivo

Aplicación web interna que consume el schema nuevo directo desde Supabase y, para operaciones complejas, vía endpoints Express (A16). El v21 monolítico fue reemplazado por una estructura modular (A15) en mayo 2026.

> **Nota (11/6/2026 en adelante):** ya **hay login** (Supabase Auth + RLS, A17). El front entra con el token del usuario, no con la anon key. Ver `ADARA-AUTH.md`.

---

## Stack

| Capa | Tecnología | Por qué |
|------|------------|---------|
| HTML | Vanilla, sin framework | A15 — sin build step, sin Node local |
| CSS | Vanilla con variables, fuente Manrope | Legibilidad y tamaño base grande (15px) |
| JS | Módulos ES6 nativos | Sin bundler. Cada archivo es un módulo importable |
| Lecturas | Supabase REST API vía helper `SB` | A16 — vistas `v_*` y tablas simples |
| Mutaciones complejas | Express endpoints en `server.js` | A16 — cross-tabla y APIs externas |
| Deploy | Railway, auto-deploy desde GitHub | commit → 30s a producción |
| Auth | Supabase Auth + RLS (A17) | Token por usuario; RLS `authenticated`=todo, `anon`=nada |

> **Dependencias externas:** la app no tiene ninguna en el bundle. Lo único que se carga desde CDN es **SheetJS**, y **sólo bajo demanda** (al apretar un botón de exportar), con fallback a CSV si el CDN no responde. Cargar una librería en el arranque para una función que se usa una vez por semana no vale la pena.

---

## Estructura de carpetas

```
public/
├── index.html              # shell con nav + #app-screens
├── css/
│   ├── tokens.css          # variables CSS (paleta, radios, sombras)
│   └── base.css            # layout, tipografía, componentes
└── js/
    ├── main.js             # bootstrap + router + toast global
    ├── core/
    │   ├── sb.js           # helper Supabase + Auth + parche de fetch
    │   ├── mlTabs.js       # sub-pestañas del grupo Mercado Libre
    │   └── skuPicker.js    # ← NUEVO 7/8/2026: selector de SKU con búsqueda
    └── screens/
        ├── home.js · skus.js · movimientos.js · gastos.js · compras.js
        ├── conciliacion.js · resultado.js · costeo.js · psi.js
        ├── posicion-fiscal.js · flex.js · cuadre.js · saldos.js
        ├── ventas-ml.js · importaciones-sim.js
```

> ⚠️ Al subir archivos a GitHub web, caen en la carpeta donde estás parado. Verificá la ruta final.

---

## Convenciones

1. **Una pantalla = un archivo en `screens/`.** Exporta `loadX(): Promise<void>` que pinta en `#app-screens`.
2. **Límite blando: 500 líneas por archivo.** Si pasa, partirla.
3. **Estado local por pantalla**, sin estado global compartido.
4. **`core/sb.js` es la única vía de acceso a Supabase.** Nunca `fetch` directo desde una pantalla.
5. **`sbGet` pagina obligatoriamente.** Supabase corta en 1000 filas sin avisar. **Toda query paginada debe ordenar por una columna única** (`,id.asc`) o se saltean filas en el borde de página.
6. **Toast para feedback** de mutaciones.
7. **Edición inline > modal** para cambios simples.
8. **Idempotencia de subida.**
9. **Validar antes de mandar a la DB.** Confirmar el schema real (NOT NULL, CHECK, **FK**) antes de escribir el insert.
10. **Valor de lista cerrada → `select`, nunca texto libre** (5/8/2026). El texto libre habilita errores silenciosos y caros.
11. **Preview de impacto antes de una operación irreversible** (5/8/2026). Si una operación **reescribe datos históricos**, mostrar **qué números se van a mover** y pedir confirmación explícita.
12. **No cachear catálogos en variables de módulo sin invalidación** (7/8/2026). La app es **SPA y no recarga la página**: un catálogo cacheado la primera vez que se abre un modal **nunca se refresca**, y un SKU dado de alta después no aparece jamás. Fue un bug real. Los catálogos (SKUs, cuentas, líneas) se recargan al abrir el modal o al entrar a la pantalla. El costo es despreciable (~190 filas).
13. **Un dropdown dentro de un modal cuelga de `<body>`, no del componente** (7/8/2026). `.modal` tiene `overflow-y:auto`: un menú `position:absolute` adentro **se recorta** apenas la fila está cerca del borde inferior. Va con `position:fixed` anclado al `getBoundingClientRect()` del input, con `z-index` por encima de `.modal-overlay` (200), volteo hacia arriba si no hay lugar abajo, y **reposicionamiento en `scroll` con captura**. Contrapartida: hay que **destruirlo explícitamente** al re-renderizar o quedan menús huérfanos en el DOM.

---

## Helper `SB` (core/sb.js)

```js
import { sbGet, sbCount, sbPost, sbPatch, sbDelete } from './core/sb.js';

const todas = await sbGet('skus', 'order=codigo.asc');             // pagina solo
const pend  = await sbCount('skus', 'familia=is.null');            // count puro
await sbPatch('skus', `id=eq.${id}`, { familia: 'electronica' });
const [nuevo] = await sbPost('skus', { codigo: 'X1', descripcion: '...' });
```

Nunca `sbDelete` para SKUs/ventas/gastos — usar `activo=false` (soft delete).

**Parche de fetch:** `sb.js` intercepta `window.fetch` y agrega `Authorization: Bearer <token>` a las rutas relativas del backend, salvo las públicas (`/config`, `/health`, `/ml/auth`, `/ml/callback`). Un 401 del backend **no** cierra la sesión.

---

## Componente `core/skuPicker.js` (nuevo 7/8/2026)

Selector de SKU con búsqueda. Reemplaza al `<select>`, que con ~190 productos es inusable: hay que scrollear a ciegas y el nombre completo no entra en el ancho del combo.

```js
import { crearSkuPicker } from '../core/skuPicker.js';
const p = crearSkuPicker(contenedor, { skus: SKUS, value: it.sku_id, hiddenClass: 'com-it-sku' });
p.value        // id del SKU seleccionado ('' si ninguno)
p.destroy()    // OBLIGATORIO al re-renderizar o cerrar el modal
```

**Búsqueda:** por **código y descripción a la vez**, sin acentos, con términos sueltos — escribir `jbl 520` encuentra `JBL520BT — Auricular JBL 520 BT`. Orden de relevancia: código exacto → empieza con → contiene → resto. Máximo 60 resultados. Teclado completo (↑↓, Enter, Esc).

### El patrón de integración que lo hace barato

El componente mantiene un **`<input type="hidden">` con la clase que se le pida** y dispara un **`change` que burbujea**. Consecuencia: `row.querySelector('.com-it-sku').value` y los listeners delegados de `change` **siguen funcionando idénticos** a cuando era un `<select>`. Por eso enchufarlo en `compras.js` costó 15 líneas y no tocó nada de la lógica de ítems.

**Conviene reusar este patrón** para cualquier componente que reemplace un control nativo dentro de código con delegación de eventos.

### Cuándo NO usarlo

En `importaciones-sim.js` se usó **`<input list>` + `<datalist>` nativo** en vez del picker, a propósito: esa pantalla **re-renderiza el body entero** y ya tuvo el bug de listeners acumulándose (ver "bind-once" abajo). El picker exige montar y destruir por fila; el datalist es nativo, sobrevive al re-render y no acumula nada. **En un archivo grande con ese antecedente, gana lo aburrido.**

**Verificado** con 12 casos en Chromium headless: búsqueda por descripción, Enter, click, `change` disparado una sola vez, texto basura que no pisa el valor guardado, menú dentro del viewport, y `destroy()` sin huérfanos.

---

## Router

`main.js` implementa un router por `#hash` con **cola serializada** (`_routing`): `route()` encola `renderScreen` en vez de dispararlo en paralelo. Sin la cola, al saltar entre pantallas pesadas la carga lenta anterior terminaba *después* y pintaba el DOM encima de la nueva.

`NAV_ALIAS = { flex: 'ventas_ml' }` para el resaltado del grupo Mercado Libre.

---

## Estilo visual

| Aspecto | Decisión |
|---------|----------|
| Fondo | `#FAFAF9` (off-white cálido) |
| Surface | `#FFFFFF` con borde `#E7E5E4` |
| Texto primario | `#1C1917` · secundario `#78716C` |
| Acento | `#D97706` (ámbar 600) — color marca ADARA |
| Tipografía | Manrope + JetBrains Mono para códigos |
| Tamaños | base 15px, títulos 22px, KPI 36px |
| Radius | `--r` = 10px (cards), `--r-sm` = 6px |

Variables en `tokens.css`. No hardcodear hex — siempre `var(--...)`. Si una pantalla necesita colores propios puede inyectar un `<style>` scopeado con prefijo (`.mov-`, `.imp-`, `.pf`, `.com-`, `.skp-`, `.vef-`).

---

## Componentes CSS disponibles (base.css)

| Clase | Uso |
|-------|-----|
| `.card` + `.card-title` | Contenedor genérico con título |
| `.kpi-grid` + `.kpi` | Tarjetas de métricas |
| `.toolbar` + `.toolbar .grow` | Barra de búsqueda/acciones |
| `.pills` + `.pill` | Filtros por categoría con contadores |
| `.table-wrap` + `table.t` | Tabla limpia |
| `.inline-select` + `.inline-input` | Edición inline en celdas |
| `.input` + `.select` + `.btn` + `.btn-primary` + `.btn-ghost` | Form elements |
| `.modal-overlay` + `.modal` + `.field` + `.modal-actions` | Modal centrado (⚠️ `.modal` tiene `overflow-y:auto` — ver convención 13) |
| `.toast` (`window.toast(msg, type)`) | Feedback efímero |
| `.loading` + `.empty` + `.error` | Estados de la app |

---

## Patrón: agregar una pantalla nueva

1. Crear `public/js/screens/x.js` con `export async function loadX()`.
2. Registrar en `main.js` (`import` + entry en `screens`).
3. Sumar ítem en el nav de `index.html`.
4. Commit. **Verificá que cada archivo quede en su carpeta correcta.**

---

## Patrón obligatorio: bindear la delegación de eventos UNA sola vez

Si el contenedor de eventos **persiste entre re-renders** (solo se le cambia el `innerHTML`) y se le vuelve a hacer `addEventListener` en cada render, los listeners **se acumulan**: el mismo click se dispara 1→2→4→8… veces. En el simulador, "+ Concepto" llegó a agregar **50+ renglones de un click**.

```js
function render() {
  root.innerHTML = `... <div id="x-body"></div> ...`;
  bindDelegation(document.getElementById('x-body')); // ← una sola vez
  renderBody();
}
function renderBody() {
  document.getElementById('x-body').innerHTML = ...;   // NO rebindear acá
  paint();
}
```

Síntoma para diagnosticar: una acción que debería pasar una vez se multiplica exponencialmente.

---

## Pantallas implementadas

| Pantalla | Archivo | Estado |
|----------|---------|--------|
| Home | `home.js` | ✅ 5 KPIs + próximos pasos + **banner de token de ML** (7/8) |
| SKUs | `skus.js` | ✅ Lista + buscador + pills + edición inline. Alícuota por `select`; la columna guarda la **fracción** (0.105) |
| Movimientos | `movimientos.js` | ✅ Lista + filtros + KPIs + carga manual + **"+ Venta en efectivo"** (7/8). Ver `ADARA-MOVIMIENTOS.md` |
| PSI Recompra | `psi.js` | ✅ Matriz de ventas/semana + velocidad + días de stock + recompra |
| Inventario | `costeo.js` | ✅ (ex "Costeo", hash `#costeo`). Valorización + SKUs sin costo. `TC_USD=1465` hardcode |
| Resultado | `resultado.js` | ✅ Estado de resultado mensual por línea/canal hasta margen de contribución |
| Posición Fiscal | `posicion-fiscal.js` | ✅ IVA con apertura + arrastre de dos saldos + chip de estado |
| Saldos | `saldos.js` | ✅ Saldo en vivo por cuenta = ancla + Σ movimientos posteriores |
| Cuadre | `cuadre.js` | ✅ Tablero de control de conciliación |
| Compras | `compras.js` | ✅ Facturas + Cuenta corriente. Detalle expandible, badge PDF, "+ Nuevo SKU", **buscador de SKU** y **check "Compra sin comprobante"** (7/8) |
| Gastos | `gastos.js` | ✅ Alta/lista + capitalización a compra con preview de impacto |
| Sim. Importaciones | `importaciones-sim.js` | ✅ Fase 1 + **fila de SKU por producto**, **cuenta en pagos** y **exportadores PDF/Excel** (7/8). Ver `ADARA-IMPORTACIONES-SIM.md` |
| ML — Ventas ML | `ventas-ml.js` | ✅ Control + Conciliar (tablero bidireccional). ~1.900 líneas → candidato a partir |
| ML — Flex | `flex.js` | ✅ Control semanal contra el resumen del proveedor |

---

## Cuándo migrar a SPA con framework

Migrar a Preact/Vue + Vite cuando: el mismo bug aparece arreglado en 2+ lugares · pantallas > 15 y cada `loadX()` pasa de 800 líneas · lista grande (>2k filas) con perf insuficiente · se incorpora un segundo desarrollador.

> ⚠️ **`ventas-ml.js`** (~1.900) e **`importaciones-sim.js`** (~1.230 tras los exportadores) están muy por encima del límite blando de 500. El doc del simulador ya pide partirlo en `calc.js` + `screen.js` al entrar a Fase 2.

---

## Pendientes y deuda técnica conocida

- **Caché en producción**: cambios en `screens/*.js` pueden quedar cacheados tras un deploy. Mitigación: `Ctrl+Shift+R` o versionado en el query string.
- **Sin tests ni linting.** A partir de 5+ pantallas conviene sumar smoke tests. *(Nota 7/8: el `skuPicker` se validó con Playwright headless — el patrón es replicable para componentes aislados.)*
- **Clases CSS inexistentes en `base.css`**: `.num` y `.psi-aviso` se usan y no están definidas.
- **`TC_USD = 1465` hardcodeado** en `costeo.js`. Futuro: cotización del BCRA.
- **`adara-app-v21.html` es legacy.** Marcarlo como histórico o reemplazarlo.
- **Hashchange no recarga `loadHome()`** al volver desde otra pantalla si los datos cambiaron.
- **`var(--red-strong)` no está definido en `tokens.css`** — el toast usa el fallback `#B42318`.
- **Preview de impacto pendiente de replicar** en las demás operaciones que reescriben datos históricos.
- **Auditar las `sbGet` del proyecto** por orden único (regla de paginación).

---

## Documentos relacionados

- `ADARA-DECISIONES.md` — A15, A16, A17
- `ADARA-SCHEMA.md` · `ADARA-MOVIMIENTOS.md` · `ADARA-AUTH.md` · `ADARA-TFACTURA.md` · `ADARA-COMPRAS-IMPORTACIONES.md` · `ADARA-GASTOS.md` · `ADARA-IMPORTACIONES-SIM.md` · `ADARA-VENTAS-EFECTIVO.md` · `ADARA-VENTAS-ML.md`

---

## Actualización — 11 Junio 2026 (login + costo en SKUs)

- `main.js`: antes de `route()` corre `requireAuth()` (login; si `must_change`, pantalla de cambio de contraseña). Chip de usuario + "Salir". Listener `adara-auth-expired`.
- `core/sb.js`: maneja **Supabase Auth** — `login()`, `changePassword()`, `logout()`, `getValidToken()` (refresco single-flight, robusto a múltiples pestañas) + **parche de `window.fetch`**.
- `skus.js`: columna **Costo actual** desde `v_costo_sku_actual`.

---

## Actualización — 13 Junio 2026 (Saldos, Cuadre, router serializado)

Pantallas **Saldos** (`#saldos`, lee `v_saldo_cuenta`) y **Cuadre** (`#cuadre`). **Router serializado** con cola para evitar el race entre pantallas pesadas.

---

## Actualización — 19 Junio 2026 (Sim. Importaciones + patrón bind-once)

Pantalla **Sim. Importaciones** (`#importaciones_sim`), estilos scopeados `.imp`, persistencia en `importacion_sim` (tabla única + JSONB, sin endpoint). **Patrón bind-once** documentado arriba tras el bug del "+ Concepto".

---

## Actualización — 21 Junio 2026 (grupo Mercado Libre + Flex + adjuntos)

- Nav: **un solo "Mercado Libre"** con sub-pestañas vía `core/mlTabs.js`.
- Pantalla **Flex** (`#flex`): selector de semana, grilla día×zona, cuadre contra el proveedor, asignador de zonas, precios editables.
- **Adjuntos**: la operación se crea primero, se toma el id y se sube a `/adjuntos`. En Compras el 📎 fue reemplazado por un **badge `PDF`** (5/8).

---

## Actualización — 13 Julio 2026 (Ventas ML — conciliación manual bidireccional)

Tablero mensual de dos tablas, manual y bidireccional. Estado nuevo: `VISTA`, `CONC_SEL`, `CONC_HIDE_DONE`, `MOV_MP`.

- **Filtro por mes** (`efPeriodo`) + **KPIs con anillos de progreso** (donut SVG, paleta ADARA).
- **Tabla izquierda** (Ventas del mes): `#Venta`, Entrega, SKU, Producto, Objetivo, Estado, Acción.
- **Tabla derecha** (Extracto MP completo): **TODOS** los movimientos del mes, no sólo cobros.
- **Selección bidireccional** con sugerencia determinística — **no se reescribió el motor de matching**, sólo la UI.
- **Cross-mes**: `Diferir→` / `traer ↰` / `↩` sobre `ventas_ml.conciliacion_periodo`.
- **Alcance**: es **el eje plata**. No toca stock, CMV, P&L ni el mes fiscal (S9).
- **Removido**: "Conciliar todas" + `conciliarTodas`/`armarLoteConciliacion`.

---

## Actualización — 3 Agosto 2026 (+ Nuevo SKU en Compras + Posición Fiscal con arrastre)

- **`compras.js`**: botón **"+ Nuevo SKU"** con mini-modal `openNuevoSku()` que hace `sbPost('skus', …)`, empuja a `SKUS`/`SKU_BY_ID` y auto-selecciona en la primera fila vacía. **El IVA no se asume: se pregunta.**
- **`posicion-fiscal.js`**: columnas/KPIs `saldo_tecnico_favor` y `libre_disponibilidad` + **chip de estado** apertura/fino/incompleto.

---

## Actualización — 4 Agosto 2026 (Ventas ML — columnas Factura + Facturado de Tango)

Dos columnas nuevas al lado del Bruto: **Factura** (`tipo_factura` + `nro_factura`, filtrable) y **Facturado** (`total_facturado_tango` con semáforo ✓/⚠ contra el neto del comprador).

Vars nuevas `PACK_CANT` / `PACK_NETO`. **Combo sólo si `PACK_CANT[pack_id] >= 2`** — la mayoría de los "packs" son un solo producto.

> Lección de diseño: el primer intento metió el dato como renglones chiquitos bajo `#Venta`/`Bruto` y quedó ilegible. **Un dato que se compara va en su propia columna, en el mismo renglón.**

---

## Actualización — 5 Agosto 2026 (Compras con detalle, select de IVA, capitalización en Gastos)

### Compras
**Detalle expandible** al click en la fila (una por vez, a demanda, cacheado): encabezado, productos, otros costos (ámbar), impuestos (azul, aclarando que son crédito y no costo), lotes con barra de consumo, gastos capitalizados, tarjeta de totales. Layout de dos columnas que se apila bajo 1100px. **Adjunto: de 📎 a badge `PDF`.** **KPIs respetan el filtro de período.** Los botones de acción cortan la propagación del click.

### SKUs — alícuota por `select`
En los tres lugares de edición (`#nf-iva`, `#ns-iva`, `.in-iva`). Motivo: el texto libre permitía **errores silenciosos y caros** — un SKU al 10% en vez de 10,5% calcula mal el crédito de cada compra futura, y escribir `0.105` en vez de `10.5` da 0,105%. **La columna guarda la FRACCIÓN.** Un valor fuera de lista se muestra como "(fuera de lista)" en vez de pisarse.

### Gastos — capitalización con preview de impacto
Selector "¿Va al costo de una compra?" + cuadro que consulta `GET /compras/:id/impacto-capitalizacion` y muestra lotes que cambian, delta de stock y delta de CMV por período, con checkbox obligatorio. La anulación de un gasto capitalizado se frena.

**Patrón reutilizable:** *antes de ejecutar una operación que reescribe datos históricos, mostrar qué números se van a mover y pedir confirmación explícita.*

---

## Actualización — 7 Agosto 2026 (skuPicker · banner de ML · exportadores · SKU en el simulador)

### 1. `core/skuPicker.js` — componente nuevo
Ver la sección propia arriba. Enchufado en **la venta en efectivo** (`movimientos.js`) y en **los ítems de compra** (`compras.js`). El diff de `compras.js` fueron **15 líneas** gracias al patrón hidden-input + `change` que burbujea.

### 2. Banner de token de ML en la home (`home.js`)
**Primer consumo de `/health` desde el front.** El endpoint existía y calculaba bien `ml_token`, pero **nadie lo miraba** — por eso una caída del token se detectaba días después, al notar que faltaban ventas.

Distingue tres casos: **sin refresh token** (rojo, con el instructivo de `offline_access`), **refresh revocado** (rojo) y **vencido pero renovable** (naranja). Con link a `/ml/auth`. **Falla en silencio si el backend todavía no tiene el bloque `ml`**, así que los archivos se pueden deployar en cualquier orden. Ver `ADARA-VENTAS-ML.md`.

### 3. Movimientos — botón "+ Venta en efectivo"
Modal con fecha, línea, ítems múltiples (SKU + cantidad + precio + subtotal en vivo), cliente opcional y total. Postea a `POST /ventas/efectivo` y maneja el **409 del guardarriel de fecha** pidiendo confirmación. Ver `ADARA-VENTAS-EFECTIVO.md`.

> **Bug que dejó la convención 12:** el modal cacheaba los SKUs en una variable de módulo que nunca se refrescaba. Como la app es SPA, los SKUs dados de alta después de entrar a Movimientos **no aparecían nunca**.

### 4. Compras — check "Compra sin comprobante"
Al tildarlo bloquea el N° de factura, apaga el bloque de impuestos, borra las percepciones cargadas y fuerza todos los ítems a **Exento** — también los que se agreguen después (la alícuota del SKU deja de aplicarse). En la lista, badge gris **"sin comprobante"** en vez de "factura pendiente", sin botón "asignar". Ver C20 en `ADARA-DECISIONES.md`.

### 5. Simulador — fila de SKU, cuenta en pagos y exportadores
- **Fila de SKU por producto** con `<input list>` + `<datalist>` (ver "Cuándo NO usarlo" arriba) y un estado al costado: ✓ vinculado · ⚠ código inexistente **o alícuota distinta a la de la sim** · gris sin vincular. El campo **no dispara `renderBody()` en cada tecla** — sólo actualiza el cartelito de esa fila, para no perder el foco.
- **Columna "Sale de"** en los pagos con las cuentas reales.
- **Exportadores**: **PDF** vía ventana de impresión del navegador (sin librerías: la app no tiene build step y una dependencia para esto sería desproporcionada) y **Excel** comparativo declarado vs real con SheetJS lazy desde CDN + fallback a CSV con `;` y BOM.
- Los dos exportadores salen del **mismo `calc()`** que pinta la pantalla: no hay un segundo cálculo que pueda divergir de lo que se está viendo.
