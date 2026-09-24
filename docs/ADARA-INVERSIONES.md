# ADARA — Inversiones y Tesorería

Última actualización: 15 Mayo 2026

Cómo se trackean las inversiones financieras del negocio: FCIs, plazos fijos, saldos remunerados y otros instrumentos. Todo agrupado en una **Tesorería General** única (sin asignación por línea), con rendimientos que se imputan al P&L de ML Electrónica.

---

## Objetivo

Responder:

1. **¿Cuánto tengo invertido en total?** (suma de instrumentos activos)
2. **¿Cuánto rinde cada inversión?** (capital, intereses devengados, rendimiento %)
3. **¿Cuánto generó la tesorería este mes?** (resultado financiero del período)
4. **¿Cuándo vencen mis instrumentos?** (próximos rescates, renovaciones)

Sumar el rendimiento financiero al resultado total de la empresa sin contaminar la lectura operativa de cada línea.

---

## Concepto: Tesorería General

**Las inversiones NO se asignan a línea de negocio.** Conviven en un único "centro" llamado **Tesorería General**.

Razón: la plata invertida es plata excedente del negocio en su conjunto, no es plata "de una línea". Asignar arbitrariamente confundiría el análisis de rentabilidad operativa.

```
TESORERÍA GENERAL al 15/05/2026
─────────────────────────────────────────────────────────
FCI Corto Plazo Supervielle              $ 90.000.000
Plazo Fijo Banco X (vto 30/06)           $      -
Saldo remunerado Supervielle /3          (incluido en cuenta operativa)
Caja USD (× TC)                          $  2.150.000
─────────────────────────────────────────────────────────
Total tesorería invertida                $ 92.150.000
```

---

## Asignación a P&L

Aunque la tesorería es central, **los rendimientos generados se imputan al P&L de ML Electrónica** (la línea que tradicionalmente genera el cashflow excedente que alimenta la tesorería).

Esto agrega a ML Electrónica una sección nueva:

```
ML ELECTRÓNICA — ABRIL 2026

RESULTADO OPERATIVO (de ventas, gastos, sueldos)     $ 1.211.000
+ RESULTADO FINANCIERO (de tesorería)                $    68.502
  Rendimiento saldo remunerado            $ 69.602
  − Retención Ganancias sobre rendimientos  − $ 1.100
─────────────────────────────────────────────────────
RESULTADO NETO DEL MES                               $ 1.279.502
```

Las otras 4 líneas **no tienen** sección "Resultado financiero" — solo ML Electrónica recibe el aporte de la tesorería.

---

## Tipos de inversiones soportadas

| Tipo | Descripción | Operaciones |
|------|-------------|-------------|
| **FCI** | Fondo Común de Inversión (Corto Plazo, Tasa Fija, etc.) | Suscripción, rescate, valuación periódica |
| **Plazo Fijo** | PF tradicional, vencimiento fijo, tasa nominal | Constitución, vencimiento o renovación |
| **Plazo Fijo UVA** | PF ajustable por inflación | Idem PF + actualización por UVA |
| **LECAPS / LEDES / BOPREAL** | Letras del Tesoro y bonos cortos | Compra, vencimiento |
| **Saldo remunerado** | Interés por saldo en cuenta corriente | Acreditación periódica de intereses |
| **Caja USD** | Dólares físicos guardados (no es inversión técnica pero es tesorería) | Compra (entrada), venta (salida) — tracking por TC |
| **Otros** (a futuro) | Acciones, criptos, etc. | A definir cuando hagan falta |

A futuro se pueden agregar más tipos sin desarrollo (similar al modelo de logísticas extensibles).

---

## Modelo de datos

Estructura conceptual:

```
TABLA inversiones
  id              uuid PK
  tipo            text  (fci / plazo_fijo / leca / saldo_remunerado / caja_usd / etc.)
  nombre          text  ("FCI Corto Plazo Supervielle")
  cuenta_origen   uuid FK -> cuentas
  moneda          text
  fecha_inicio    date
  fecha_vencimiento date  (nullable)
  capital_inicial numeric
  capital_actual  numeric  (puede actualizarse por valuación periódica)
  tasa_o_rendimiento text  (descriptivo, opcional)
  estado          text  (activa / vencida / rescatada / renovada)
  config_jsonb    jsonb  (configuración específica del instrumento)

TABLA inversion_movimientos
  id              uuid PK
  inversion_id    uuid FK -> inversiones
  fecha           date
  tipo            text  (suscripcion / rescate / rendimiento / retencion_ganancias / ajuste_valuacion)
  monto           numeric
  movimiento_bancario_id uuid FK -> movimientos_bancarios  (nullable, vincula al banco si corresponde)
  notas           text
```

---

## Operaciones que afectan las inversiones

### Suscripción (entrada a la inversión)

Cuando se suscribe a un FCI, se constituye un PF, se compra una LECAP:

1. Sale plata de la cuenta corriente → movimiento bancario (egreso)
2. Entra al instrumento → registro en `inversion_movimientos` tipo `suscripcion`
3. El capital de la inversión sube
4. Es un **movimiento interno** (no es egreso real, es traslado a otra forma de tesorería)

Ejemplo del extracto:
```
30/04/26  Suscripcion / Rescate FCI 'Corto Pla    -    $ 90.000.000
```

### Rescate (salida de la inversión)

Cuando se rescata el FCI, vence el PF, se cobra la LECAP:

1. Sale del instrumento → registro tipo `rescate`
2. Entra a la cuenta corriente → movimiento bancario (ingreso)
3. El capital de la inversión baja
4. La diferencia entre rescate y suscripción acumulada → **rendimiento realizado** del período

### Rendimiento devengado (sin movimiento de plata)

Para instrumentos que devengan rendimiento sin movimiento concreto (FCI con valuación diaria, saldo remunerado):

1. Periódicamente (diario o mensual) se actualiza el capital actual de la inversión
2. La diferencia con el período anterior = rendimiento devengado
3. Va al ledger de "resultado financiero"

### Acreditación de intereses (con movimiento de plata)

Para saldos remunerados que pagan interés en la cuenta:

```
10/04/26  Remuneración de Saldo   $ 14.425  → crédito en cuenta
10/04/26  Impuesto a las Ganancias  $ 432    → retención sobre el interés
```

Esto se reconoce como:
- Ingreso financiero: $14.425
- Retención Ganancias: $432 (va al crédito fiscal de Ganancias)
- Rendimiento neto: $13.993

### Diferencia de cambio en caja USD

Si la caja USD tiene 1000 USD y el TC blue subió:
- Capital inicial: 1000 USD × $1000 = $1.000.000
- Capital actualizado: 1000 USD × $1100 = $1.100.000
- Diferencia: $100.000 → **rendimiento de tesorería**

(Solo se registra cuando se vende USD efectivamente. Mientras se mantiene, queda valorizado al TC del momento.)

---

## Retenciones de Ganancias sobre rendimientos

Los bancos retienen automáticamente Ganancias sobre los intereses pagados (saldos remunerados, plazos fijos). Se ve en el extracto como:

```
Remuneración de Saldo            $ 14.425   ← ingreso bruto
Impuesto a las Ganancias         $ 432      ← retención automática
```

El parser identifica esto y:
1. Suma el ingreso bruto a Tesorería General
2. La retención va al **ledger de Ganancias** como crédito fiscal (pago a cuenta)
3. El P&L registra el ingreso bruto y resta la retención por separado

---

## Vista en patrimonial

Las inversiones aparecen en el activo del estado patrimonial:

```
ACTIVO
  Stock valorizado                 $ 14.560.000
  Banco Supervielle ARS            $ 22.048.035
  MercadoPago ARS                  $    890.000
  Caja física ARS                  $    120.000
  Caja física USD (× TC)           $  2.150.000   (parte de tesorería)
  Cuentas por cobrar               $  1.580.000
  Crédito fiscal                   $    640.000
  ─── INVERSIONES (TESORERÍA) ──────────────────
  FCI Corto Plazo                  $ 90.000.000
  Plazos fijos                     $       -
  LECAPS / bonos                   $       -
  ──────────────────────────────────────────────
  TOTAL ACTIVO                     $ 131.988.035
```

La caja USD se cuenta tanto en "caja física USD" como en "tesorería" para evitar dobles contados — figura una vez en el patrimonial.

---

## Vista en P&L (impacto sobre ML Electrónica)

```
ML ELECTRÓNICA — ABRIL 2026

INGRESOS                                          $ 12.000.000
COSTOS DIRECTOS                                   − 6.800.000
MARGEN BRUTO                                      $  5.200.000

GASTOS DE LA OPERACIÓN                            − 3.989.000
RESULTADO OPERATIVO                               $  1.211.000

+ RESULTADO FINANCIERO (Tesorería)                $     68.502
  Rendimiento saldo remunerado    $ 69.602
  − Retención Ganancias           − $ 1.100
─────────────────────────────────────────────────────────
RESULTADO NETO DEL MES                            $  1.279.502
```

El "Resultado Financiero" no aparece en el P&L de las otras 4 líneas (Electrónica off-ML, Luminarias, Mochilas Sindicatos, Mochilas Individuos).

En la vista de total empresa, el resultado financiero aparece consolidado en la fila de ML Electrónica.

---

## Detección automática desde extractos

El parser del extracto Supervielle identifica los siguientes patrones y los rutea a Tesorería General:

| Concepto en extracto | Acción |
|----------------------|--------|
| `Suscripcion / Rescate FCI` | Crear/actualizar inversión FCI, movimiento bancario interno |
| `Remuneración de Saldo` | Sumar como rendimiento al saldo remunerado de la cuenta correspondiente |
| `Impuesto a las Ganancias` (sobre rendimientos) | Registrar como retención en ledger Ganancias, asociar al rendimiento del mismo día |
| `Plazo Fijo Constitución/Vencimiento` | Crear/cerrar inversión PF |
| Operaciones de letras del tesoro | Crear/cerrar inversión LECAP/LEDE |

Los movimientos de inversión que son traslados internos (suscripción/rescate) se marcan como **movimientos internos** y no impactan ingreso/egreso operativo.

---

## Reglas duras

1. **Tesorería General es centralizada.** No se asigna a línea de negocio.
2. **Rendimientos van al P&L de ML Electrónica.** Las otras líneas no tienen resultado financiero.
3. **Suscripciones y rescates son movimientos internos.** Trasladan saldo entre formas de tesorería (cuenta → instrumento), no son ingreso/egreso real.
4. **Rendimientos brutos se registran enteros.** La retención de Ganancias se imputa por separado al ledger fiscal.
5. **Valuación al TC del momento** para caja USD y otras posiciones en moneda extranjera. Diferencias de cambio realizadas → P&L.
6. **Una inversión activa no se modifica retroactivo.** Cambios solo via movimientos nuevos.

---

## Pendientes y TBD

- **Tasa real de Ganancias sobre rendimientos**: chequear con contador qué % aplica (varía por tipo de instrumento)
- **Valuación periódica de FCI**: ¿se hace al rescate o se actualiza diariamente con la cuotaparte? Para simplicidad arrancamos solo al rescate.
- **Política para vencimientos de PF**: alerta automática X días antes
- **Carga inicial de inversiones al 31/12/2025**: Sebastián carga al arrancar la operación
- **Posición en USD**: si en el futuro se compra USD MEP/CCL como inversión, modelar como instrumento aparte de la caja USD

---

## Documentos relacionados

- `ADARA-FLUJO-OPERATIVO.md` — cuándo se trabaja con tesorería en el día a día
- `ADARA-CONCILIACION-BANCARIA.md` — clasificación automática de movimientos de inversión desde el extracto
- `ADARA-PNL.md` — sección "Resultado financiero" en ML Electrónica
- `ADARA-PATRIMONIAL.md` — inversiones como parte del activo
- `ADARA-IMPUESTOS.md` — retenciones de Ganancias sobre rendimientos como crédito fiscal
- `ADARA-LINEAS-NEGOCIO.md` — por qué la tesorería no se imputa por línea
- `ADARA-DECISIONES.md` — reglas consolidadas
