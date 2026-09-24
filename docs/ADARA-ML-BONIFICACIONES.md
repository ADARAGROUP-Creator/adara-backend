# ADARA — Bonificaciones / Promos ML en el P&L

Última actualización: **4 Agosto 2026** (§12 — con la integración Tango, `total_facturado_tango` y `importe_facturado` conviven en la base y el aporte / delta de IVA ya son **calculables en SQL**, sin ir a la API orden por orden. Destraba §10/§11 paso 2.) · 16 Julio 2026 (gap de ingreso DESCARTADO + dimensionamiento fiscal §10-§11).

> Estado: **gap de ingreso CERRADO** (el aporte ya está en `importe_bruto`). Punto abierto = fiscal (¿el aporte de ML es base imponible de IVA?) — lo define el contador. Medición ahora en SQL (§12).

---

## 1. Qué son las promos con aporte de ML

Mercado Libre ofrece promociones donde el **descuento al comprador se reparte** entre el vendedor y ML: el comprador paga con el descuento total, y **ML aporta su parte**. Ejemplo confirmado (SKU 178, orden 2000015889986966): precio deal 487.762,30 → factura al comprador 465.618 → aporte de ML = 22.144,30 (4,54%).

---

## 2-9. (Sin cambios) Mecánica confirmada

- `total_amount` de ML **incluye** el aporte; ADARA guarda `importe_bruto = total_amount`. El comprador paga `transaction_amount` (menor). **Aporte = total_amount − transaction_amount**, embebido.
- ADARA guarda `aporte_ml` (Σ de charges coupon/rebate del pago) e `importe_facturado = importe_bruto − aporte_ml` (= neto del comprador).
- **No hay ingreso perdido:** el aporte está dentro del bruto; comisión y `por_cobrar` reconcilian. ML-BON1 descartado.
- El aporte es **frecuente y variable (0%–~5%)**, no se infiere del precio.

---

## 10-11. (Sin cambios) Dimensionamiento y recomendación fiscal

- Aporte ponderado ~2,2% del bruto en la muestra del SKU 178; extrapolación ~$8,2M en el 178 → ~$0,78M de IVA en juego (banda a medir, no a declarar).
- **Gate al contador:** ¿el aporte de ML es base imponible de IVA del vendedor? Si sí → ADARA ya lo refleja. Si no → el IVA débito debe salir sobre el neto del comprador, no sobre `importe_bruto` → corrección + backfill.

---

## 12. Actualización 4/8/2026 — medición ahora calculable en SQL (integración Tango)

Con la integración de Tango Factura implementada (ver `ADARA-TFACTURA.md`), `ventas_ml` ahora guarda **ambos** montos de forma estable:

- `importe_facturado` = **neto del comprador** ML (`importe_bruto − aporte_ml`).
- `total_facturado_tango` = **total real facturado en Tango** (`ObtenerInfoMovimiento.Total`).

**Consecuencia importante:** lo que en §10 requería "ir a la API orden por orden" **ahora es SQL directo**, para todas las ventas ML que ya tienen factura vinculada:

- **Aporte por venta** = `importe_bruto − importe_facturado` (= `aporte_ml`).
- **Base imponible real de la factura** = `total_facturado_tango`.
- **Delta de IVA en juego** (si el aporte NO fuera gravado) = aporte × `alicuota_iva/(1+alicuota_iva)`, agregable por período/SKU sin llamar a ML.
- **Control de descuadres** = casos donde `total_facturado_tango ≠ importe_facturado` (neto comprador). Esos NO son el aporte normal: son **facturas que no cierran contra lo que pagó el comprador**, a revisar.

### Descuadres detectados (4/8/2026) — 3 patrones
1. **Tango facturó menos que el bruto pero ML no registró aporte** (`aporte_ml=0` y `total_facturado_tango < importe_bruto`): hay un descuento en la factura sin aporte que lo explique.
2. **ML tiene aporte pero Tango facturó el bruto completo** (`total_facturado_tango = importe_bruto`, `aporte_ml > 0`): la factura no aplicó la bonificación.
3. **Aporte y bonificación no coinciden** (queda un resto, casi siempre redondo: $8.500, $17.000, etc.).

En la muestra, la gran mayoría cierra exacto (`total_facturado_tango = importe_facturado`), con ~pocos descuadres por período. Pendiente: revisarlos caso por caso (hay/habrá un export de descuadres).

### Próximos pasos (actualizados)
- [x] Capturar el total real de Tango por venta → **hecho** (`total_facturado_tango`).
- [ ] **Fiscal:** definir con el contador si el aporte es base de IVA (sin cambios; ahora medible en SQL para dimensionar exacto).
- [ ] Revisar los descuadres reales (patrones 1-3).
- [ ] Si el aporte NO es gravado → ajustar el cálculo de IVA débito (sobre `importe_facturado`, no `importe_bruto`) + backfill controlado (guardarraíl `ADARA-COSTEO-FIFO.md`).

---

## Documentos relacionados
`ADARA-TFACTURA.md` (integración Tango, columnas), `ADARA-VENTAS-ML.md`, `ADARA-IMPUESTOS.md`, `ADARA-PNL.md`, `ADARA-DECISIONES.md`.
