# ADARA — Movimientos sin conciliar (sistema v21)

Última actualización: 19 Mayo 2026

> ⚠️ **REFERENCIA v21 — MOTOR MUERTO. No usar.** Los endpoints `POST /mp/vincular` y `POST /mp/descartar` de acá operan sobre `movimientos_mp`, tabla que **se borró en el reset del 27/05/2026 y no existe**. La conciliación VIVA es el front (`ventas-ml.js` + `vinculos`); ver `ADARA-VENTAS-ML-V22.md` y `ADARA-DECISIONES.md` O7.

> 📌 **Doc del sistema v21 en producción.** Documenta el manejo actual del buzón de movimientos MP sin conciliar.
>
> El rediseño expande este concepto a **conciliación universal** que cubre Supervielle + MP + caja + tarjeta, no solo MP. Ver `ADARA-CONCILIACION-BANCARIA.md` para el modelo completo.

---

## Fuente

Tabla movimientos_mp, conciliado=false, filtrado por período.
Rango: mes del período + mes siguiente (AS incluye fechas del mes siguiente por arrastre MP).

---

## Categorías y acciones

| Categoría | Acción |
|---|---|
| venta_ml (liquidación) | 📎 Vincular con venta |
| bonificacion_envio | 📎 Vincular (manual o auto por monto) |
| Todas las demás | 💸 Pasar a gasto |
| Cualquiera | 🗑 Descartar (irrelevante/duplicado) |

---

## Causas principales

1. **Arrastre entre meses:** liquidación mes N+1 de venta mes N
2. **Bonificaciones Flex:** otra referencia, auto-matching puede fallar
3. **Movimientos sin referencia a ventas:** transferencias, rendimientos, retenciones → gasto o descarte
4. **Débitos sin payment_id:** van a gasto

---

## Acciones implementadas

### Vincular a venta (POST /mp/vincular)
Búsqueda dinámica de venta (payment_id, order_id, SKU, título). Vincular → asigna venta_ml_id + recalcula balance.

### Pasar a gasto
Crea registro en gastos con movimiento_mp_id, marca conciliado=true. Ver ADARA-GASTOS.md.

### Descartar (POST /mp/descartar)
Marca conciliado=true sin vincular a venta.

---

## Filtros

Categoría (dropdown), búsqueda (referencia/descripción), paginación 50/pág.

---

## Pendientes

1. Indicador visual vinculados vs huérfanos
2. Filtro por rango de montos
