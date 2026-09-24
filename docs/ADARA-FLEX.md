# ADARA — Flex (logística de envíos)

Última actualización: 23 Septiembre 2026 (**cascada de resolución de zona definida + caché localidad→partido**: se cierra el "cómo" del plan F7. La zona sale del dato de la venta (`lat/long` → Georef → partido), y el resultado se **cachea** en una tabla nueva `flex_localidad_partido`; el nombre de la localidad nunca decide por sí solo. Lista de **localidades ambiguas** que no se cachean jamás (Villa Adelina, Gerli, Canning, Tortuguitas, El Palomar, San Francisco Solano). Resuelto el pendiente **La Tablada = MATANZA NORTE (GBA 1)** por etiqueta MEF. El diccionario de zonificación de MEF se mudó a `ADARA-FLEX-ZONAS-MEF.md`.) · 21 Junio 2026 (**resolución de partido por Georef + carga de la grilla MEF**: se cargaron 30 partidos→zona (cobertura 41% de los envíos sin zona, 3.478→2.055); decisión de fondo F7 — el partido NO se infiere por nombre de localidad, **sale del dato de ML vía Georef** (lat/long → partido oficial); ML no publica grilla de zonas (su tarifa es por distancia, la grilla CABA/GBA1/GBA2/GBA3 es de MEF). Doc reescrito a v22; la referencia v21 queda al final.)

> 📌 **Doc vigente (v22).** El dominio Flex vive en DB bajo el schema del rediseño. Reglas duras: `ADARA-DECISIONES.md` (F1–F7). La referencia técnica del v21 (congelado) está al final de este doc.

---

## Logística (operador MEF)

- Empresa: **Logística de Envíos MEF** (@mercadoenviosflex) — transportadora tercerizada, NO es Mercado Libre.
- Dirección retiro: Alvear 2571, Ramos Mejía.
- Horario corte: 12:00. MEF pasa 13:00–13:20. Colecta pasa 14:00–17:00.
- Días operativos: lunes a viernes (NO sábados, NO feriados).
- Semana de facturación: lunes a sábado (lo que se vende sábado se entrega/despacha lunes).

El rediseño contempla logística **multi-transportadora**; MEF es la principal.

---

## Modelo de datos (v22)

**Tablas (4):**
- `flex_logistica` — tipos de logística (`mef`, `caba_tardia`).
- `flex_zona` — zonas tarifarias (`CABA`=1, `GBA1`=2, `GBA2`=3, `GBA3`=4).
- `flex_precio` — precio por **logística × zona** (editable, no hardcode).
- `flex_partido_zona` — mapeo **partido → zona** (`UNIQUE(partido)`).
- `flex_localidad_partido` — **(a crear, 23/9)** caché **localidad → partido** derivado de Georef: `localidad_norm` UNIQUE, `partido`, `fuente` ('georef'|'manual'), `ambigua` bool. No es un diccionario a mano: se completa con el resultado del backfill.

**Vistas (3):**
- `v_flex_envios` — un envío por venta flex; asigna zona **en vivo** (`LEFT JOIN flex_partido_zona pz ON pz.partido = b.partido`) y precio (`flex_precio` por logística×zona).
- `v_flex_semanas` — agregado semanal (importe estimado, conteos).
- `v_flex_partidos_sin_zona` — partidos/localidades con envíos pero sin zona mapeada (lo que falta resolver).

**Pantalla:** `#flex`, sub-pestaña del grupo "Mercado Libre" (barra `core/mlTabs.js`). Control semanal contra el resumen del proveedor + asignador de partidos sin zona. Lee `v_flex_envios` de la semana seleccionada y arma la grilla día×zona; abre por defecto en la semana más reciente.

> Todas las tablas siguen el patrón post-A17: RLS on + policy `authenticated` + GRANT `authenticated` + REVOKE `anon`. Las vistas NO son `security_invoker` (corren como owner). El backend (service_role) bypassa RLS.

---

## Zonas y precios (MEF, validados)

| Zona | id | Precio | Estado |
|------|----|--------|--------|
| CABA | 1 | $3.450 | Activa |
| GBA 1 | 2 | $4.750 | Activa |
| GBA 2 | 3 | $5.350 | Activa |
| GBA 3 | 4 | $7.050 | **No activa** (cargada por si se usa más adelante) |

Esta grilla es **de MEF** (la transportadora), no de Mercado Libre. ML no publica una grilla de zonas: su tarifa de Flex es **por distancia** origen→destino, y en el panel solo se ven/eligen las áreas de cobertura. Por eso la lista partido→zona es comercial de MEF.

`caba_tardia` todavía **no tiene precio cargado** en `flex_precio` (pendiente).

---

## Clasificación del partido (decisión F7 — el partido sale del dato, no se infiere)

**El problema.** El sync arma el partido con `receiver_address.city.name` (server.js, ~líneas 660–682). Pero ML manda en `city.name` a veces el **partido** (Morón) y a veces la **localidad** (Castelar). Resultado: ~la mitad de los envíos caen como localidad y no matchean ningún partido. Mapear localidad→partido **a mano es suponer** y se rompe (variantes: "Lanús"/"Lanús Oeste"/"lanus oeste"/"Lanuseste").

**Qué manda ML realmente** (verificado con `/debug-order/:id`, orden de Castelar):
- `municipality`: `{id:null, name:null}` → **viene vacío**, no sirve.
- `city.name`: "Castelar" → la localidad (por eso cae mal).
- **`zip_code`: "1712"** y **`latitude/longitude`** → **estos sí vienen siempre y son oficiales.**

**La decisión (F7).** El partido se resuelve con **Georef** (Servicio de Normalización de Datos Geográficos del gobierno, `apis.datos.gob.ar/georef`): `GET /ubicacion?lat=..&lon=..` devuelve el **departamento**, que en provincia de Buenos Aires **es el partido** (ej. id 06427 = Partido de La Matanza, fuente ARBA). Es geométrico (punto dentro del polígono), gratis y determinístico → no se equivoca como el match por nombre. Pipeline:

```
ML → lat/long (y zip_code)  →  Georef: lat/long → PARTIDO oficial  →  flex_partido_zona: PARTIDO → ZONA
```

**Estado actual (21/6):** 30 partidos cargados en `flex_partido_zona` (la grilla MEF: CABA, La Matanza Norte/Sur, Morón, Lanús, Lomas de Zamora, Avellaneda, Quilmes, Tigre, etc.). Cobertura: de **3.478 → 2.055** envíos sin zona (mapeados ~1.423, **41%**). Lo que queda sin zona son **localidades** (287 strings) que ML manda en vez del partido.

### Cascada de resolución de zona (23/9/2026)

El orden es fijo. Cada nivel solo corre si el anterior no resolvió:

```
1. lat/long del envío  → Georef → partido          (fuente de verdad, F7)
2. flex_localidad_partido (caché, ambigua=false)   → partido
3. flex_partido_zona: partido → zona
4. sin match → queda sin zona (REVISAR), no se fuerza
```

**El caché no es un criterio, es memoria.** La zona nunca se decide por el nombre de la localidad: se deriva del lat/long una vez y se guarda contra ese nombre para no volver a consultar Georef. Esto es lo que respeta F7 y a la vez evita 287 consultas repetidas.

**Localidades ambiguas — nunca se cachean por nombre.** Un mismo string pertenece a dos partidos distintos según la dirección:

| Localidad | Partidos posibles |
|-----------|-------------------|
| Villa Adelina | Vicente López / San Isidro |
| Gerli | Avellaneda / Lanús |
| Canning | Esteban Echeverría / Ezeiza |
| Tortuguitas | Malvinas Argentinas / Pilar / José C. Paz |
| El Palomar | Morón / 3 de Febrero |
| San Francisco Solano | Quilmes / Almirante Brown |
| Nordelta | urbanización, no partido — verificar dirección |

Se marcan `ambigua = true` y se resuelven **siempre** por lat/long. Los envíos históricos sin lat/long quedan en REVISAR: no se adivinan.

**Plan de resolución (pendiente, Flex no está activo):**
1. **Backfill Georef de las 287 localidades** → poblar `flex_localidad_partido`. No toca el sync y es lo que más cobertura gana. Tirar el backfill con pausa entre llamadas y persistir el resultado; Georef no se consulta en vivo por envío.
2. **Capturar `lat/long` + `cp` en `ventas_ml`** (columnas nullable, migración aditiva) y resolver el partido por lat/long en el sync. De ahí en más sale solo, sin suponer, y las localidades nuevas se auto-resuelven.
3. **`v_flex_envios` con la cascada**: `flex_partido_zona` por partido → si no matchea, `flex_localidad_partido` → si no, sin zona.

**El ZPL de las etiquetas MEF no hace falta** para el flujo normal: el dato de la venta alcanza. Sirve como control y para dirimir excepciones comerciales (ver `ADARA-FLEX-ZONAS-MEF.md`).

---

## La Matanza Norte vs Sur

El split Norte/Sur es **sub-zona comercial de MEF**, NO un partido oficial: Georef devuelve "La Matanza" (un solo partido). Por eso el split queda por **localidad/CP**, con la lista `LA_MATANZA_SUR` hardcodeada en server.js.

**Norte → GBA 1:** Lomas del Mirador, San Justo, Villa Luzuriaga, Villa Celina, Ramos Mejía, Ciudad Madero, Villa Madero.

**Sur → GBA 2:** Isidro Casanova, Gregorio de Laferrere, Rafael Castillo, González Catán, Ciudad Evita, Virrey del Pino, Aldo Bonzi.

**La Tablada → NORTE (GBA 1)** — resuelto el 23/9/2026. Las etiquetas de MEF la clasifican como `LA MATANZA NORTE` y la etiqueta es el criterio de quien factura. La auditoría que sugería GBA 2 queda descartada.

> El split Norte/Sur **no sale de Georef ni del lat/long**: es zonificación comercial. Por eso acá sí manda la lista de localidades, que vive en `ADARA-FLEX-ZONAS-MEF.md` junto con el resto del diccionario de MEF.

---

## Frontend Flex

- Filtra por `fecha_despacho_flex` (NO `fecha_entrega`).
- Semana lunes a sábado.
- Deduplicación por `pack_id` (primer item costo normal, adicionales $0).
- Cancelada sin entrega: no cuenta como envío (badge CANC, fila tachada).
- Devolución con entrega: sí cuenta como envío (badge DEVOL).
- El KPI "Partidos sin zona" seguirá alto hasta resolver la cola de localidades (es esperado, no es error).

---

## Pendientes Flex

- **Crear `flex_localidad_partido`** (post-A17: RLS + policy + GRANT + REVOKE) y correr el **backfill Georef** de las 287 localidades sin zona. Resuelve la cola sin tocar el sync.
- **Capturar `lat/long` + `cp`** en `ventas_ml` y resolver partido por Georef en el sync (solución definitiva).
- **Cargar las excepciones comerciales de MEF** que ningún dato geográfico resuelve (ver auditoría 16/02: 2 envíos que ADARA clasificó CABA y MEF cobró GBA 1, barrio limítrofe).
- **Precio de `caba_tardia`** (falta cargarlo en `flex_precio`).
- **Afinar regla "CABA tardía"** (F1): franja 12–16h sobre-captura; semana 01–06/06 ADARA cuenta 197 envíos vs 156 del resumen MEF — investigar corte de fechas, sábado→lunes, devoluciones, no_preparado.
- **Definir cuál resumen del 08/06 vale:** $495.500 [36/41/33] vs $421.450 [29/35/29].
- **Flex Fase 3 (F6):** botón "cargar total semanal como gasto" (factura A neto+IVA, imputación Electrónica/ml 100%, con adjunto) → baja al P&L.

---
---

# Referencia v21 (CONGELADO)

> Las tablas del v21 (`flex_partidos`, etc.) se borraron en el reset del 27/05/2026. Esto se conserva solo como referencia técnica de las integraciones y de las auditorías históricas.

## Cómo determinaba el partido el v21 (sync)
- **CABA:** `state.id = "AR-C"` → partido=CABA, ciudad_destino=barrio.
- **La Matanza:** `city.name = "La Matanza"` → split norte/sur por neighborhood.
- **Resto GBA:** partido=`city.name`, ciudad_destino=`neighborhood.name`. (Este es el origen del problema que F7 resuelve.)

## Auditorías contra facturas MEF (históricas)
- **Semana 23/02–28/02 — $314.000 ✅ PERFECTO:** 70 envíos (23 CABA + 28 GBA1 + 19 GBA2), exacto con MEF.
- **Semana 16/02–21/02 — Dif $2.600:** 174 envíos coinciden; 2 clasificados CABA que MEF pone GBA 1 (barrio limítrofe).

## Localidades a confirmar con MEF (excepciones donde MEF zonifica distinto a la geografía)
- Don Torcuato — actualmente GBA 2
- Banfield — movido a GBA 1
- Bernal Oeste / Bernal Este — actualmente GBA 2
- La Tablada — actualmente GBA 2, etiquetas dicen LA MATANZA NORTE
- Barrios CABA limítrofes (Liniers, Villa Lugano, Villa Riachuelo, Mataderos)
