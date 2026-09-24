# ADARA — Sistema v21 (versión actual en producción)

Última actualización: 5 Mayo 2026

> ⚠️ **REFERENCIA v21 — CONGELADO. No es el estado actual.** Las tablas del v21 se borraron en el reset del 27/05/2026; la DB vive bajo el schema del rediseño (`ADARA-SCHEMA.md`). Este doc se conserva solo como referencia técnica de las integraciones. Estado vigente: `ADARA-DOCS-INDEX.md`.

> 📌 **Este documento describe el sistema actual** (versión v21) que está corriendo en Railway. Para el **rediseño en curso** ver `ADARA-DOCS-INDEX.md` y los docs `ADARA-*.md` del rediseño (PNL, GASTOS, IMPUESTOS, COMPRAS-IMPORTACIONES, STOCK, CONCILIACION-BANCARIA, etc.).
>
> Las reglas duras consolidadas viven en `ADARA-DECISIONES.md` (rediseño).

---

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Frontend | HTML/CSS/JS monolítico — `adara-app-v21.html` |
| Backend | Node.js — `server.js` |
| Deploy | Railway — https://adara-backend-production.up.railway.app |
| Base de datos | Supabase (PostgreSQL + REST API) — proyecto `dlazhftkwrcordsdbeah` |
| APIs externas | Mercado Libre, MercadoPago, Tango Factura, nolaborables.com.ar |

---

## Principios de desarrollo (vigentes durante v21 y rediseño)

1. Arquitectura simple intencional. Frontend monolítico + backend único.
2. Priorizar claridad sobre complejidad.
3. Toda lógica importante documentada.
4. No romper decisiones técnicas sin revisar impacto (→ `ADARA-DECISIONES.md` del rediseño).
5. Cada dominio tiene su sección "⚠ Guardrails técnicos". Leer ANTES de codificar.
6. Paginación obligatoria: Supabase REST API máximo 1000 filas por GET.
7. Charlemos antes de hacer nada — discutir y acordar antes de implementar.

---

## Módulos actuales (v21)

Ventas ML, Flex, Conciliación MP, Movimientos sin conciliar, Gastos, P&L, Líneas de negocio, Impuestos, Cancelaciones/Devoluciones, Stock, PSI Recompra, Órdenes de Compra, Importaciones.

Cada módulo tiene su documento. Ver `ADARA-DOCS-INDEX.md`.

> El rediseño expande significativamente este alcance con: conciliación bancaria universal (Supervielle), tesorería e inversiones centralizadas, patrimonial, reclamos a proveedores, integración Tango con vínculo ml_order_id↔ExternalID, lotes con costos reales FIFO, gastos con flujo proactivo/reactivo. Ver docs del rediseño para detalles.

---

## Tablas principales (DB actual)

```
ventas_ml, movimientos_mp, gastos, lineas_negocio, catalogo_skus, 
comprobantes, devoluciones, flex_zonas, flex_partidos, flex_envios, 
meses_cerrados, workspace_config, facturas_tango, cuentas, 
movimientos_bancarios
```

---

## Tablas operativas adicionales

```
gastos_lineas, sueldos, sueldos_lineas, proveedores, inversores, 
importaciones, importacion_sku, ventas_b2b, proveedores_comex, 
proveedores_comex_mov, ventas_efectivo, stock_devoluciones, 
ordenes_compra, orden_compra_skus
```

---

## Sobre el rediseño

El rediseño documentado en mayo 2026 introduce nuevas tablas y refactoriza varias existentes. Los cambios principales:

- Tabla `lotes` con costos reales por unidad (reemplaza el cálculo simple actual)
- Tabla `consumo_lote` para reconstruir CMV histórico
- Tabla `cuentas` extendida para incluir caja física ARS y USD
- Tabla `inversiones` para Tesorería General
- Tabla `reclamos` extendida con destinatario (ML o Proveedor)
- Tabla `snapshot_patrimonial_mensual` para fotos de cierre
- Cambios en `movimientos_bancarios` para el parser real de Supervielle

El detalle vive en los docs del rediseño (`ADARA-PNL.md`, `ADARA-STOCK.md`, etc.).
