# ADARA — Estado Patrimonial

Última actualización: 19 Mayo 2026

La **foto financiera** de la empresa en cualquier momento: cuánto tiene (activos), cuánto debe (pasivos), y cuánto vale neto (patrimonio).

Complemento del P&L: mientras el P&L mide el **flujo** (cuánto generó este mes), el patrimonial mide el **stock** (cuánto vale ahora).

---

## Objetivo

Responder en tiempo real:

1. **¿Cuánto vale mi empresa hoy?** (activos − pasivos)
2. **¿Dónde está la plata?** (cuentas, stock, inversiones, etc.)
3. **¿A quién le debo y cuánto?** (proveedores, fisco, sueldos)
4. **¿Cuánto me deben?** (clientes, ML por reclamos)
5. **¿Cómo evolucionó mi patrimonio mes a mes?**

Y por línea de negocio donde aplique (algunas cosas son compartidas, como la tesorería y los créditos fiscales).

---

## Concepto: foto financiera

```
ACTIVO  (lo que tengo)
   −
PASIVO  (lo que debo)
   =
PATRIMONIO NETO  (cuánto vale mi empresa)
```

A diferencia del P&L (que es por período), el patrimonial es una **foto a un momento dado**. Se puede consultar al instante o como snapshot histórico.

---

## Componentes del Activo

### 1. Stock valorizado

Suma de todos los lotes activos por su costo unitario real (FIFO):

```
stock_valorizado = Σ (cantidad_actual_lote × costo_unitario_lote)
```

Fuente: tabla `lotes` (ver `ADARA-COMPRAS-IMPORTACIONES.md` y `ADARA-STOCK.md`).

### 2. Saldos en cuentas operativas

| Cuenta | Cálculo |
|--------|---------|
| Banco Supervielle ARS | Saldo del último extracto + movimientos posteriores conocidos |
| MercadoPago ARS | Saldo del último AS + movimientos posteriores conocidos |
| Caja física ARS | Saldo manual (cargas y descargas) |
| Caja física USD × TC | Cantidad de USD × TC blue del día |

### 3. Tesorería General (inversiones)

Suma de todas las inversiones activas (ver `ADARA-INVERSIONES.md`):

| Instrumento | Valor actual |
|------------|--------------|
| FCI Corto Plazo Supervielle | Capital actual (suscriptos − rescatados + rendimientos devengados) |
| Plazos fijos | Capital inicial + intereses devengados a la fecha |
| LECAPS / bonos | Valor de cotización al día |
| Saldos remunerados | (ya incluidos en los saldos bancarios) |
| Caja USD | (ya incluida en cuentas operativas) |

### 4. Cuentas por Cobrar (AR)

Suma de saldos pendientes de cobro (ver `ADARA-CONCILIACION-BANCARIA.md`):

- Ventas ML con liquidación MP pendiente
- Ventas Tienda Nube con transferencia pendiente
- Facturas B2B esperando pago
- Cheques diferidos recibidos pero no acreditados todavía
- Remitos a cobrar (WhatsApp / efectivo entregado, plata por entrar)
- Reclamos pendientes con ML (ver `ADARA-RECLAMOS.md`)

### 5. Crédito fiscal

Saldos a favor pendientes de aplicar (ver `ADARA-IMPUESTOS.md`):

- IVA crédito acumulado (si supera al débito del período)
- IIBB anticipos / retenciones / percepciones a favor
- Ganancias anticipos + retenciones / percepciones acumuladas a cuenta del año

---

## Componentes del Pasivo

### 1. Cuentas por Pagar (AP)

Saldos a proveedores y otros acreedores:

- Compras / importaciones con saldo pendiente (ver `ADARA-COMPRAS-IMPORTACIONES.md`)
- Facturas de servicios sin pagar
- Honorarios profesionales pendientes
- Saldo de resumen de tarjeta de crédito sin pagar

### 2. Impuestos a pagar

- IVA del último período cerrado, pendiente de pago a AFIP
- IIBB del último período cerrado, pendiente a ARBA/CABA
- Anticipos de Ganancias del mes pendientes
- Estimación de Ganancias proyectada al cierre del año

### 3. Sueldos a pagar

Sueldos del mes en curso devengados pero no pagados todavía (parcial o total).

### 4. Plan de pagos AFIP

Si hay un plan de pagos con AFIP activo (ej: `PLANRG5321` visto en el extracto), las cuotas futuras pendientes. Solo se cuentan **cuotas vencidas + próximas 3 meses** (no todo el saldo restante, porque eso ya no es deuda corriente).

### 5. Cheques emitidos no debitados

Cheques que vos emitiste a proveedores pero que todavía no fueron presentados al cobro. Son pasivo: sale plata en cualquier momento.

---

## Patrimonio Neto = Activo − Pasivo

Vista resumida del estado patrimonial:

```
ESTADO PATRIMONIAL — al 15/05/2026

ACTIVO
  Stock valorizado                    $  14.560.000
  Banco Supervielle ARS               $  22.048.035
  MercadoPago ARS                     $     890.000
  Caja física ARS                     $     120.000
  Caja física USD (× TC blue)         $   2.150.000
  FCI Corto Plazo (Tesorería)         $  90.000.000
  Cuentas por cobrar (AR)             $   2.340.000
  Crédito fiscal                      $     640.000
  ─────────────────────────────────────────────────
  TOTAL ACTIVO                        $ 132.748.035

PASIVO
  Cuentas por pagar (AP)              $   8.200.000
  Saldo tarjeta de crédito            $     780.000
  IVA a pagar (período cerrado)       $   1.606.000
  IIBB a pagar                        $     315.000
  Sueldos del mes pendientes          $   1.450.000
  Plan AFIP — próximas cuotas         $   6.200.000
  Cheques emitidos no debitados       $   3.500.000
  ─────────────────────────────────────────────────
  TOTAL PASIVO                        $  22.051.000

═════════════════════════════════════════════════════
PATRIMONIO NETO                       $ 110.697.035
═════════════════════════════════════════════════════
```

---

## Vista por línea de negocio (parcial)

Algunos conceptos se pueden imputar por línea, otros son compartidos:

| Concepto | Por línea | Compartido |
|----------|-----------|------------|
| Stock valorizado | Por familia de SKU (no estrictamente por línea, pero aproximable) | — |
| Saldos bancarios | Calculados desde movimientos imputados | — |
| AR | Por línea (heredado de la venta) | — |
| AP | Por línea (heredado de la compra) | — |
| Sueldos a pagar | Por línea (con el % de cada empleado) | — |
| Crédito fiscal | Compartido (IVA es uno solo a nivel CUIT) | ✓ |
| Tesorería / FCI | Compartido | ✓ |
| Impuestos a pagar | Compartido (DDJJ es una sola) | ✓ |
| Plan AFIP | Compartido | ✓ |

### Vista por línea (lo imputable)

```
PATRIMONIO POR LÍNEA — al 15/05/2026 (componente atribuible)

                         ML Elec   off-ML   Lumi    Moch S  Moch I    Suma
  Stock                  8.450     -        2.100   1.460    1.000   13.010 (*)
  Saldos cobros          850       45       180     200      55      1.330
  AR                     890       95       680     340      120     2.125
  − AP                   −6.200    −180     −1.200  −350     −270    −8.200
  − Sueldos a pagar      −580      −145     −145    −145     −145    −1.160
  ─────────────────────────────────────────────────────────────────────────
  Subtotal por línea     3.410    −185     1.615   1.505     760     7.105

(*) El stock real son $14.560.000. La diferencia ($1.550.000) corresponde a
    luminarias y mochilas que comparten lotes, distribuido por familia.

Compartidos (no por línea):
  + Caja física ARS + USD          + $  2.270.000
  + FCI Tesorería                  + $ 90.000.000
  + Crédito fiscal                 + $    640.000
  − IVA + IIBB + Ganancias         − $  1.921.000
  − Tarjeta + Plan AFIP + Cheques  − $ 10.480.000
  
  Saldo compartido neto             $ 80.509.000

PATRIMONIO NETO TOTAL                $ 87.614.000  (≠ del primero porque excluye reclasificaciones)
```

> Nota: el patrimonio por línea es **informativo**, no se cuadra exactamente con el total porque hay reclasificaciones. Sirve para entender qué línea aporta más al activo neto, no como contabilidad estricta.

---

## Vistas en la app

### Vista 1: Estado patrimonial actual

El cuadro completo (Activo / Pasivo / Patrimonio Neto) con la fecha de corte = hoy. Cada renglón es drill-down al detalle.

### Vista 2: Por línea de negocio

El subtotal atribuible a cada línea, con los compartidos al pie.

### Vista 3: Snapshot histórico

Foto del patrimonio al cierre de cada mes. Permite ver evolución:

```
PATRIMONIO NETO — Evolución mensual

  Ene 2026    $ 78.450.000
  Feb 2026    $ 82.300.000
  Mar 2026    $ 85.760.000
  Abr 2026    $ 89.130.000
  May 2026    $ 91.205.000  (parcial al 15/05)
```

### Vista 4: Composición visual

Gráfico de torta del activo: qué % es stock, qué % es banco, qué % es tesorería, etc. Para ver la "diversificación" del patrimonio.

---

## Cálculo dinámico vs persistido

**Las cifras del patrimonial se calculan al vuelo**, no se persisten:

- Stock valorizado → suma de lotes activos
- Saldos en cuentas → saldo inicial + movimientos hasta la fecha de corte
- AR / AP → ventas/compras con saldo pendiente
- Tesorería → suma de inversiones activas
- Crédito fiscal → ledger por impuesto, neto del período

**Excepción**: los **snapshots de cierre mensual** sí se persisten en una tabla `snapshot_patrimonial_mensual` para evitar recalcular meses cerrados. Se calculan una vez al cerrar el mes y quedan inmutables.

---

## Reglas duras

1. **Mes cerrado es inmutable**: el snapshot patrimonial del mes cerrado no se vuelve a calcular. Si hay ajustes, van al mes en curso.
2. **Stock valorizado al costo histórico de los lotes** (FIFO). No se revalúa por inflación.
3. **Caja USD valorizada al TC blue del día de cálculo**. Las diferencias de cambio realizadas (al vender USD) van al P&L, las no realizadas se reflejan en el patrimonial.
4. **AR y AP se calculan dinámicamente** desde ventas/compras con saldo pendiente. No hay tabla de saldos AR/AP persistida.
5. **El patrimonio por línea es informativo**, no se cuadra exactamente con el total (por los conceptos compartidos).
6. **La tesorería es central** (ver `ADARA-INVERSIONES.md`): no se asigna a línea en patrimonial.

---

## Pendientes y TBD

- **Snapshot inicial al 31/12/2025**: Sebastián carga al arrancar la operación. Sin este punto de partida, no hay foto patrimonial confiable. Datos requeridos:
  - Stock valorizado por SKU al 31/12/2025
  - Saldos de cada cuenta al 31/12/2025
  - AR al 31/12/2025 (clientes que debían)
  - AP al 31/12/2025 (a quién se le debía)
  - Saldos fiscales al 31/12/2025 (créditos a favor o saldos a pagar)
  - Inversiones activas al 31/12/2025

- **Política de revaluación de inventario**: si en el futuro se quiere reflejar el valor de reposición de los lotes (no el costo histórico), definir cuándo y cómo. Por ahora se mantiene costo histórico.

- **Manejo de USD físico**: ¿se valoriza siempre al TC blue del día actual, o se mantiene al TC de compra original? Decisión actual: TC blue actual (refleja realidad económica), pero diferencias no realizadas se identifican como tales.

- **Comparativos vs años anteriores**: cuando haya histórico de varios años, agregar vista anual.

- **Exportación para contador**: formato/cadencia (mensual, trimestral, anual) a definir con el contador.

---

## Documentos relacionados

- `ADARA-PNL.md` — la otra mitad: el flujo del período (vs el stock del patrimonial)
- `ADARA-STOCK.md` — valorización del stock con lotes y costos reales
- `ADARA-COMPRAS-IMPORTACIONES.md` — origen del valor de los lotes
- `ADARA-CONCILIACION-BANCARIA.md` — cálculo de saldos en cuentas + AR + AP
- `ADARA-INVERSIONES.md` — Tesorería General en el activo
- `ADARA-IMPUESTOS.md` — crédito fiscal y obligaciones fiscales
- `ADARA-LINEAS-NEGOCIO.md` — qué se imputa por línea y qué no
- `ADARA-RECLAMOS.md` — AR especial con Mercado Libre
- `ADARA-DECISIONES.md` — reglas consolidadas
