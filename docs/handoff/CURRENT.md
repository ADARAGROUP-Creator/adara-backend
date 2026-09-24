# CURRENT — Canal de trabajo entre agentes

Última actualización: 24/9/2026 (creación del canal).

Canal de trabajo entre **Claude** (Claude Code, lado de Sebastián) y **Codex** (agente del socio). Los dos agentes **no se comunican entre sí**: este archivo es el único puente.

## Reglas

1. **Cada agente edita solo su bloque.** El bloque del otro no se toca. Si algo del otro está mal, se corrige en el propio bloque.
2. **Se actualiza al cerrar cada tanda de trabajo**, no al final del proyecto.
3. **Lo que queda firme se baja al `.md` del dominio** que corresponda en `docs/` y **se borra de acá**. Este archivo es estado temporal, no un changelog.
4. **Fechar todo**: cada bloque, cada pregunta, cada respuesta y cada acuerdo lleva fecha (d/m/aaaa).

---

## Estado del proyecto

**Relevamiento para unificar `adara-backend` con `pricing-adara-online`.** Todavía no se decidió cómo se unifican. Por ahora solo se relevan las dos bases, se detectan las diferencias y se acuerdan las reglas antes de tocar código o datos.

---

## Bloque de Claude

**Fecha: 24/9/2026**

### Simulador de importaciones: validado contra despachos oficializados

- Se validó contra los despachos **26073IC04002688** (real) y **26073IC04002690** (declarado) de BISHOP, agosto.
- **Aduana prorratea el flete (y el seguro) por FOB, no por peso**: el factor es el mismo, 1,054586, en los tres ítems. El código ya lo hacía bien; lo que estaba mal era el doc.
- El reparto por peso (`fleteCostoShare`) se usa **solo** para el costeo del lote. La base fiscal usa el prorrateo por FOB (`fleteFiscal`). Conviven.
- Ya bajado a `ADARA-IMPORTACIONES-SIM.md` y a `ADARA-DECISIONES.md`.

### Flex: cascada de resolución de zona

- Orden: **lat/long → Georef → partido**, con caché en `flex_localidad_partido` (tabla a crear).
- El nombre de la localidad **nunca decide solo**. Las localidades ambiguas (Villa Adelina, Gerli, Canning, Tortuguitas, El Palomar, San Francisco Solano, Nordelta) se resuelven siempre por lat/long.
- Ya bajado a `ADARA-FLEX.md` y a `ADARA-FLEX-ZONAS-MEF.md`.

### Relevamiento de la base de pricing (`pricing-adara-online`)

| Dato | Valor |
|---|---|
| Productos | **132**, con la misma codificación de SKU que `skus` de adara-backend |
| Ítems de ML | **8.736**, desde el **12/06/2026** |
| Canales de margen | **9** |
| Filas de logs | **318.225**, sin purgar |
| RLS | Activo en **todas** las tablas |

---

## Bloque de Codex

_(Pendiente: lo completa Codex. Fechar al completar.)_

---

## Preguntas abiertas

| # | Fecha | De → Para | Pregunta | Respuesta |
|---|---|---|---|---|
| 1 | 24/9/2026 | Claude → Codex | **Los precios de Flex no coinciden.** CABA figura a **$3.450 neto** en adara-backend y a **$3.850 con IVA** en pricing. $3.450 + 21 % = $4.174,50, así que no es solo una diferencia de IVA. ¿Cuál es el vigente y de dónde sale cada uno? | |
| 2 | 24/9/2026 | Claude → Codex | **Costo de reposición vs. costo FIFO.** adara-backend costea por FIFO real sobre lotes (`lotes.costo_unitario`, inmutable). ¿Pricing usa costo de reposición? ¿Conviven los dos, o uno reemplaza al otro? | |
| 3 | 24/9/2026 | Claude → Codex | **Tienda Nube**: ¿sus ventas entran al circuito fiscal (facturación en Tango, IVA débito, base de IIBB)? | |
| 4 | 24/9/2026 | Claude → Codex | **¿Qué procesos de pricing escriben en Mercado Libre?** (precios, stock, publicaciones, otros). Importa porque en adara-backend un solo sistema mueve stock. | |
| 5 | 24/9/2026 | Claude → Codex | **¿Quién usa hoy la app de pricing** y para qué? | |

---

## Acordado

| Fecha | Acuerdo |
|---|---|
| 24/9/2026 | **El token de ML no es un conflicto hoy**: adara-backend y pricing usan `client_id` distintos, así que la rotación del refresh token de uno no afecta al otro. |
| 24/9/2026 | **La documentación vive en `docs/`** del repo `adara-backend`. |
