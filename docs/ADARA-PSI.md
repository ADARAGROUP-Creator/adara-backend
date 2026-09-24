# ADARA — PSI (Planificación de recompra por SKU)

Última actualización: 13 Junio 2026 (Fase 2 implementada: lead time + colchón, "Recomprar YA", export filtrado, limpiar búsqueda).

> Dominio **planificación de stock / recompra**. Pantalla `#psi` (`public/js/screens/psi.js`). Reglas duras en `ADARA-DECISIONES.md`.

---

## Qué resuelve

Decir, por cada SKU, **cuánto comprar** para no quebrar stock, en base al ritmo de venta. Muestra una matriz de ventas por semana, KPIs (SKUs con venta, uds vendidas, quebrados, críticos <7 días, a reponer) y, por fila, velocidad, stock, días de stock y **Recompra** sugerida.

## Cómo calcula (lógica)

```
velocidad_diaria = velocidad_semanal / 7
necesario        = ceil(velocidad_diaria × Cobertura objetivo)
Recompra         = max(0, necesario − stock actual)
días_de_stock    = stock / velocidad_diaria
```
- **Recompra es prospectiva:** es cuánto pedir **ahora para la próxima compra**, usando el historial reciente como predicción de la demanda futura.
- Lee **ventas** (rango Desde/Hasta), **stock** (lotes, `STOCK_BY_COD`) y **catálogo** (`skus`).

## Velocidad — mediana de semanas con venta (clave)

Por defecto, la velocidad se calcula con la **MEDIANA de las semanas que tuvieron venta**, ignorando las semanas en cero (toggle **"Ignorar semanas sin venta (quiebres)"**, prendido por default).

**Por qué:** una semana en cero casi siempre es **quiebre de stock**, no falta de demanda. La venta durante un quiebre es 0, pero la demanda no. Si se promedia sobre todas las semanas (incluidas las de quiebre), el número **se subestima** y además **depende de cuántas semanas tomes** (caso real SKU 178: 2 semanas → recompra 206; rango largo con quiebres → 106). La mediana sobre semanas con venta da un número **estable** y cercano a la demanda real, y resiste tanto los quiebres parciales (semanas con venta muy baja) como los picos.

**Cuidado:** para productos de **baja rotación** (una semana en cero puede ser venta real, *había* stock) conviene **destildar** el toggle, si no sugiere comprar de más. Para los que rotan (la mayoría del catálogo) dejarlo prendido.

**Pendiente (lo fino):** medir la velocidad sobre los **días que el SKU realmente tuvo stock** (no días de calendario). Requiere reconstruir el **histórico de stock** (capa 6 / reconstrucción desde compras). Con eso, una semana se marca como quiebre solo si el stock era 0, sin heurística.

## Cobertura objetivo — cómo setearla

No es solo el lead time. Es **cuántos días de stock querés tener cubiertos**, y tiene que abarcar todo el ciclo de reposición:

```
Cobertura objetivo = cada cuánto comprás (ciclo) + lo que tarda en llegar (lead time) + colchón
```
- Si ponés solo el lead time, llegás al día de la entrega con stock 0 y cualquier demora te quiebra.
- Ejemplo: comprás 1 vez/mes (30 días) y tarda 15 en llegar → la compra de hoy debe durar hasta que llegue la próxima (día 45) → cobertura ≈ 45-50. Si comprás cada 15 → ≈ 30-35.

## Cantidad vs. Momento (timing)

- La **cantidad** (Recompra) ya descuenta el stock actual y cubre el ciclo.
- El **momento** de disparar la compra lo mirás con **"días de stock"**: con un lead largo (ej. 25 días), hay que recomprar cuando todavía quedan ~`lead + colchón` días de stock (ej. ~30-32), **no** esperar el alerta rojo (<7 días) — para entonces ya te quebraste durante el tránsito.
- **Implementado (Fase 2):** parámetros globales **Lead time** (default 15 días) y **Colchón** (default 7 días) en la toolbar. El **punto de reorden = lead + colchón** días. Cuando un SKU tiene `recompra > 0` y `días de stock ≤ (lead + colchón)`, se marca **🛒 Recomprar YA**: badge en la fila, KPI propio "Recomprar YA", y esas filas suben al tope del orden. El lead time es **global** (por-SKU queda pendiente, requiere columna en DB).

## Dependencia importante

La **Recompra** solo es confiable si el **stock cargado es el real y completo**. Mientras haya SKUs sin lote/stock cargado (pendiente: reconteo físico + lotes iniciales de los 18 SKUs), para esos la recompra sale inflada (los ve en 0). La velocidad y la matriz de ventas ya son válidas.

## UI

- **Buscador** "Buscar SKU o producto…": filtra la tabla al instante por código o nombre (no recalcula, no recarga).
- **Columnas fijas:** SKU y Producto quedan congeladas; la tabla **scrollea a lo ancho** para ver las semanas. El nombre se muestra **completo** (sin recorte).
- **Exportar Excel:** respeta el **filtro de búsqueda** (si hay texto, exporta solo lo filtrado con sufijo `_filtrado` en el nombre del archivo) e incluye la columna **"Recomprar YA"**.
- Botón **✕** para limpiar la búsqueda al instante.
- Parámetros: Desde / Hasta / Cobertura objetivo (días) / **Lead time** / **Colchón** / toggle quiebres / Recalcular.

## Pendientes

- **Lead time por-SKU** (hoy es global): requiere columna en `skus` y UI por fila.
- Velocidad sobre **días con stock real** (requiere histórico de stock reconstruido) → detección automática de quiebres.

## Archivo

`public/js/screens/psi.js` (frontend; sin endpoints de backend propios — lee Supabase directo vía `sb.js`).

---

## Actualización — 7 Julio 2026 (auditoría margen junio)

Junio 2026 dio **6,5%** de margen operativo (vs 7,4% mayo, 8,1% abril). **No es error de costeo:** es **mix-driven**. Junio vendió **menos unidades al doble de ticket** ($192.824/u neto vs $95.025 en mayo; 2.057 vs 3.101 u) → corrimiento a productos de alto ticket/bajo margen. CMV genuino ~66,8% (tablet Redmi Pad 2 SKU 178 = 36% del neto a 63,6%; TVs Enova SKU 302/303/301 a ~70%). Buckets de costeo consistentes (FIFO real 67,6% / estimado 66,2%), cobertura 99,2% del neto. El cartel "crédito incompleto" del panel IVA es sobre el **IVA a pagar**, no sobre el CMV. Palanca de mejora: precio/mix, no ajuste de sistema. (Dos data-fixes que mejoran junio al corregirse: Kindle sin costo y disco DS001N mal costeado — ver `ADARA-COSTEO-FIFO.md`.)
