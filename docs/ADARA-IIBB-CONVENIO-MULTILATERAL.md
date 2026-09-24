# ADARA — IIBB Convenio Multilateral

Última actualización: 10 Agosto 2026 (**doc NUEVO — módulo de IIBB implementado**). Nace de un hallazgo de Sebastián: *"¿en el resultado final están calculados los IIBB que tengo que pagar a fin de mes? Si no, ese porcentaje no sería real."* Tenía razón: la pantalla Resultado restaba la **retención** de ML (~1,05 % de las ventas) en lugar del **impuesto determinado** (~5,29 %), y por eso mostraba entre **3 y 5 puntos de margen de más en todos los meses del año**.

> ⚠️ Este documento cubre **IIBB Convenio Multilateral**. El IVA, Ganancias y la posición fiscal general siguen en `ADARA-IMPUESTOS.md`.

---

## 1. Por qué existe este documento

Hasta el 10/8/2026 `v_resultado_mensual` tenía una columna `impuestos` que salía de `ventas_ml.impuestos` — lo que Mercado Libre te retiene por orden. Ese número es notablemente estable (1,01 %–1,11 % del ingreso neto) y se estaba usando como si fuera el costo de IIBB del mes.

**No lo es.** La retención es un **pago a cuenta**. El costo es el **impuesto determinado**: base imponible × coeficiente × alícuota, por cada una de las 24 jurisdicciones.

La confusión costaba, con los números reales de junio 2026:

| | Junio 2026 |
|---|--:|
| IIBB determinado (CM03, dato duro) | **$20.765.310,42** |
| Lo que ADARA restaba (retención ML) | $4.346.103,85 |
| **Diferencia no contabilizada** | **$16.419.206,57** |

Ese faltante era **más de la mitad del resultado operativo del mes**.

---

## 2. Cómo funciona Convenio Multilateral

Es la parte que hay que tener clara para entender el modelo de datos.

**No se atribuye cada venta a su provincia.** El mecanismo es:

1. Se toma la **base imponible total del mes** (todas las ventas, de todo el país).
2. Se reparte entre jurisdicciones según el **coeficiente unificado**, que es **fijo durante todo el año**.
3. Cada jurisdicción aplica **su propia alícuota** sobre la porción que le tocó.
4. Se paga a **cada una por separado** (ARBA, AGIP, API Santa Fe, Rentas Córdoba…) vía **SIFERE Web**.

**Dónde sí importa la provincia de cada venta:** en el coeficiente del *año siguiente*. El coeficiente unificado es el promedio de dos:

- **Coeficiente de ingresos (50 %)** — cuánto se vendió en cada jurisdicción el ejercicio anterior
- **Coeficiente de gastos (50 %)** — dónde están depósito, sueldos, alquileres, fletes

Para **venta a distancia / e-commerce el ingreso se atribuye al domicilio del comprador**. ADARA ya tiene ese dato (partido/provincia, vía el módulo Flex), así que el coeficiente de ingresos es calculable internamente y contrastable contra el del contador. **Pendiente, no implementado.**

### Los formularios

| Form. | Qué es | Para qué sirve acá |
|---|---|---|
| **CM03** | DDJJ **mensual** | **La fuente del módulo.** Trae determinado por jurisdicción, valores que restan, saldos a favor y a pagar |
| **CM05** | DDJJ **anual** | Determina los **coeficientes unificados** del año. No trae alícuotas |
| — | Ley impositiva de cada provincia | Fija las **alícuotas** según código de actividad |

Los coeficientes también se ven en **SIFERE Web** con clave fiscal.

---

## 3. El truco que destrabó la implementación

El CM03 **no expone base imponible ni coeficientes por separado** — solo el impuesto determinado por jurisdicción. Parecía bloqueante y no lo era:

```
determinado_j = base_total × coeficiente_j × alicuota_j

⇒  determinado_j / base_total  =  coeficiente_j × alicuota_j
```

El cociente **ya es el factor completo**. Para liquidar **nunca hizo falta separar coeficiente de alícuota**. Por eso el módulo se pudo construir con un CM03 en la mano, sin esperar el CM05 ni la tabla de alícuotas del contador.

---

## 4. Qué se implementó

### Tablas

**`iibb_parametros`** — un renglón **por período**, cargado desde cada CM03 presentado.

| Columna | Notas |
|---|---|
| `periodo_referencia` | `'2026-06'`, unique |
| `determinado_total` | del CM03 |
| `base_referencia` | **base imponible TOTAL declarada** (todas las líneas, no solo ML) |
| `base_confirmada` | `false` mientras haya placeholder |
| `alicuota_efectiva` | **GENERATED** = `determinado_total / base_referencia` |
| `vigente`, `origen`, `notas` | trazabilidad |

**`iibb_jurisdiccion`** — los 24 renglones del CM03: `determinado_ref`, `valores_restan`, `valores_suman`, `a_favor_contribuyente`, `a_favor_fisco`. Códigos en **`snake_case`**, el mismo vocabulario que `retenciones.jurisdiccion` y las percepciones de compra, para que crucen por provincia.

Ambas con RLS `auth_all` (mismo patrón que el resto).

### Vistas

| Vista | Qué hace |
|---|---|
| `v_iibb_base` | Base imponible por período × línea × canal. **Solo ventas gravadas** |
| `v_iibb_determinado` | `base × alicuota_efectiva` + cobertura y facturación fuera de ADARA |
| `v_iibb_jurisdiccion_mensual` | Apertura por provincia: determinado, retenido, percibido, saldo |
| `v_impuesto_cheque_mensual` | Impuesto a los débitos y créditos, desde `retenciones` |

### Vistas extendidas

`v_resultado_mensual` y `v_resultado_linea_mensual` **conservan todas sus columnas anteriores** (nombre, orden y semántica) y suman al final: `base_gravada`, `iibb_determinado`, `impuesto_cheque`, `iibb_base_confirmada`, y en la de línea también `margen_contribucion_real` y `resultado_operativo_real`. El frontend viejo sigue funcionando sin cambios.

> Los dos costos nuevos vienen con **signo negativo**, igual que `comision` / `envio` / `costo_financiero`.

### Prorrateo entre línea y canal

Por **base gravada**, no por ingreso. El canal `efectivo` (venta sin comprobante) tiene base gravada 0 → **no absorbe IIBB**, que es correcto: no se declara. Verificado: julio canal `efectivo` da `iibb_determinado = 0,00`.

### Migraciones aplicadas (10/8/2026)

`iibb_parametros_y_jurisdicciones` · `fix_jurisdiccion_iibb_tucuman` · `vistas_iibb_determinado` · `resultado_con_iibb_determinado` · `resultado_linea_con_iibb` · `iibb_peso_base_null_safe` · `iibb_alicuota_por_periodo_y_cobertura`

---

## 5. El CM03 de referencia — anticipo 202606

**ADARA RS SRL · CUIT 30-71747647-2 · Sede 901 · Form. 5866 · N° Verificador 786149 · presentada ante ARCA el 15/07/2026 · tx 1182208938 · estado PRESENTADA.**

| Concepto | Monto |
|---|--:|
| Anticipo impuesto **determinado** | **$20.765.310,42** |
| Valores restan | $15.471.267,94 |
| Valores suman | $10.094,76 |
| A favor contribuyente | $2.867.481,49 |
| **Total a pagar** | **$8.171.618,73** |

Las seis sumas de control de `iibb_jurisdiccion` coinciden con el papel.

### Distribución por jurisdicción

**Buenos Aires y CABA son el 78,9 %** del impuesto.

| Jurisdicción | Determinado | % del total |
|---|--:|--:|
| Buenos Aires | 8.328.416,69 | 40,11 % |
| CABA | 8.054.314,45 | 38,79 % |
| Córdoba | 968.919,99 | 4,67 % |
| Entre Ríos | 657.496,18 | 3,17 % |
| Santa Fe | 593.896,79 | 2,86 % |
| Tucumán | 411.501,47 | 1,98 % |
| Chubut | 357.489,04 | 1,72 % |
| Mendoza | 324.836,96 | 1,56 % |
| Catamarca | 293.570,06 | 1,41 % |
| Neuquén | 282.540,80 | 1,36 % |
| Salta | 259.087,34 | 1,25 % |
| Jujuy | 123.877,34 | 0,60 % |
| Corrientes | 122.589,87 | 0,59 % |
| Misiones | 120.814,79 | 0,58 % |
| Chaco | 100.947,60 | 0,49 % |
| San Luis | 90.322,02 | 0,43 % |
| San Juan | 75.426,07 | 0,36 % |
| Sgo. del Estero | 58.549,87 | 0,28 % |
| La Rioja | 34.710,07 | 0,17 % |
| Formosa | 34.514,84 | 0,17 % |
| Río Negro | 23.948,41 | 0,12 % |
| La Pampa | 14.588,93 | 0,07 % |
| Tierra del Fuego | **−15.831,19** | −0,08 % |
| Santa Cruz | **−551.217,97** | −2,65 % |

**Está inscripto en las 24 jurisdicciones.**

### Saldos a favor — $2.867.481,49 inmovilizados

| Jurisdicción | A favor | Determinado del mes |
|---|--:|--:|
| Santa Fe | 886.893,95 | 593.896,79 |
| Santa Cruz | 725.600,97 | −551.217,97 |
| Corrientes | 707.678,62 | 122.589,87 |
| Catamarca | 327.197,08 | 293.570,06 |
| La Pampa | 143.246,24 | 14.588,93 |
| Río Negro | 54.113,39 | 23.948,41 |
| Tierra del Fuego | 22.751,24 | −15.831,19 |

En **Corrientes** retienen ~6× y en **La Pampa** ~10× lo determinado. Eso no se corrige solo: o se pide reducción de alícuota de retención, o el saldo crece todos los meses. **Predicho antes de ver el CM03** a partir de las retenciones desproporcionadas de Santa Fe y Corrientes en `v_retenciones_iibb`, y confirmado por el papel.

---

## 6. El impacto real en el margen

Con la alícuota techo (5,2852 %) y el impuesto al cheque incorporado:

| Período | Ventas netas | Resultado ANTES | Margen antes | IIBB | Imp. cheque | **Resultado REAL** | **Margen real** |
|---|--:|--:|--:|--:|--:|--:|--:|
| 2026-01 | 106.318.808 | 11.880.738 | 11,17 % | −5.619.123 | — | **7.445.419** | **7,00 %** |
| 2026-02 | 165.076.234 | 23.195.641 | 14,05 % | −8.724.549 | — | **16.229.078** | **9,83 %** |
| 2026-03 | 236.694.998 | 26.606.241 | 11,24 % | −12.509.717 | +9.192 | **16.702.848** | **7,06 %** |
| 2026-04 | 261.425.076 | 21.162.148 | 8,09 % | −13.816.742 | −1.100.185 | **9.021.421** | **3,45 %** |
| 2026-05 | 294.518.047 | 21.697.004 | 7,37 % | −15.565.760 | −2.374.435 | **6.985.072** | **2,37 %** |
| 2026-06 | 392.898.174 | 25.073.177 | 6,38 % | −20.765.310 | −2.414.469 | **6.239.501** | **1,59 %** |
| 2026-07 | 525.000.062 | 45.661.154 | 8,70 % | −27.318.151 | −1.885.617 | **21.830.311** | **4,16 %** |
| 2026-08 (parcial) | 262.447.574 | 31.008.232 | 11,82 % | −13.870.783 | −440.075 | **19.381.120** | **7,38 %** |

**El patrón que aparece cuando el número está bien: cuanto más se vende, peor margen.** De enero a junio la facturación se multiplicó por 3,7 y el margen cayó de 7,00 % a 1,59 %. Eso estaba tapado.

---

## 7. La limitación vigente — la alícuota es un TECHO

**Confirmado por Sebastián el 10/8/2026:** el determinado del CM03 cubre **toda** la facturación, incluida la **B2B de luminarias**, que ADARA no tiene cargada.

Como `base_referencia` está puesta en la base que ADARA conoce ($392.898.173,69, solo ML), la alícuota de 5,2852 % está **inflada** y **la línea ML está absorbiendo también el IIBB del B2B**.

Sensibilidad sobre junio:

| B2B luces | Base total | Alícuota | IIBB s/ ML | Resultado junio | Margen |
|---|--:|--:|--:|--:|--:|
| $0 *(valor actual)* | 392.898.174 | 5,285 % | 20.765.310 | 6.239.501 | 1,59 % |
| $100M | 492.898.174 | 4,213 % | 16.552.410 | 10.452.402 | 2,66 % |
| $200M | 592.898.174 | 3,502 % | 13.760.630 | 13.244.182 | 3,37 % |
| $300M | 692.898.174 | 2,997 % | 11.774.678 | 15.230.134 | 3,88 % |

**Regla al ojo: cada $100M de B2B que aparezcan, el margen de ML sube ~1,07 puntos.** Aun con $300M de B2B el margen de junio sería 3,88 % contra el 6,38 % que se mostraba: **la conclusión de fondo no se mueve**.

El frontend muestra un banner de "IIBB provisorio" y un asterisco en el KPI mientras `base_confirmada = false`.

### Por qué NO se cargó el remanente como línea de negocio

Sebastián propuso tomar `base_declarada − base_ML` como facturación de Luminarias. **Se descartó**: esa línea tendría ingresos sin CMV ni costos, o sea margen ~100 %, y mostraría a las luces como el mejor negocio de la empresa cuando en realidad es una resta. Se modeló como **cobertura** (`cobertura_pct`, `facturacion_fuera_de_adara`), que es honesto y además convierte el agujero en una métrica que se actualiza sola.

---

## 8. Operación mensual

Cuando llega el CM03:

```sql
insert into iibb_parametros
  (periodo_referencia, determinado_total, base_referencia, base_confirmada, origen)
values
  ('2026-07', <determinado total>, <base imponible declarada>, true, 'CM03 202607')
on conflict (periodo_referencia) do update
  set determinado_total = excluded.determinado_total,
      base_referencia   = excluded.base_referencia,
      base_confirmada   = true;
```

Y para corregir junio, con la base real:

```sql
update iibb_parametros
   set base_referencia = <base imponible declarada>, base_confirmada = true
 where periodo_referencia = '2026-06';
```

Los meses **sin** CM03 presentado usan la alícuota del último disponible y quedan marcados con `alicuota_del_periodo = false`. Todo se recalcula solo: no hay nada que tocar en código.

---

## 9. Roadmap — de leer el papel a producirlo

El flujo hoy va al revés de como debería: *operación → contador arma la DDJJ → ADARA copia el número*. Debería ser *operación → ADARA arma los números → contador presenta y controla*.

| Paso | Qué se hace | Qué da | Tamaño |
|---|---|---|---|
| **0 ✅ (10/8)** | Parámetro derivado del CM03 | El margen deja de mentir | hecho |
| **1** | Guardar los movimientos **no-ML** de Tango en vez de descartarlos | ADARA calcula la base imponible sola → se deja de copiar del CM03 | **chico** |
| **2** | Cargar coeficientes (CM05) + alícuotas por jurisdicción | ADARA determina el IIBB sin depender del determinado del CM03 | mediano |
| **3** | Las ventas B2B entran completas (SKU, CMV, costos) | P&L real de luminarias | grande |

> 🔑 **Hallazgo del 10/8 que abarata el paso 1:** `syncTangoFacturas()` en `server.js` (~línea 3280) ya trae **todas** las facturas de Tango con `ListarMovimientos` y pide el detalle de cada una, pero **descarta** las que no son de Mercado Libre:
> ```js
> if (!(dae && dae.AplicacionNombre === 'Mercado Libre' && dae.ExternalID)) { stats.no_ml++; continue; }
> ```
> La cañería ya existe y ya transporta la facturación B2B. Le falta un `else`. El contador `stats.no_ml` de `/tango/sync` **mide hoy mismo** cuántas facturas B2B hay en el período, sin escribir código.

**Base imponible ≠ P&L.** Para el IIBB alcanza con el monto facturado (paso 1); para el margen de luminarias hacen falta SKU y costo (paso 3). Son esfuerzos muy distintos y el primero es mucho más barato.

### Lo que nunca va a salir de ADARA

Por más completo que quede el sistema, **el coeficiente unificado** (determinación anual, CM05) y **las alícuotas por jurisdicción** (ley impositiva provincial según código de actividad) son **parámetros que se cargan**, no resultados que se calculan. Igual que la tasa de Ganancias.

### División del trabajo objetivo

- **ADARA**: la base imponible desde la facturación real, y el determinado aplicando los parámetros.
- **El contador**: los parámetros, la presentación formal y el control.
- **El CM03**: deja de ser **fuente** y pasa a ser **control** — se compara contra lo que ADARA calculó, igual que el Cuadre contra Mercado Libre.

---

## 10. Reglas duras

1. **El gasto de IIBB es el impuesto DETERMINADO.** La retención de ML y las percepciones de compra son **anticipos**: reducen lo que se paga en caja, no el costo del período. Usar la retención como gasto subestimaba el costo fiscal en ~4 puntos de margen.
2. **Solo la base GRAVADA genera IIBB.** Una venta sin comprobante no se declara y no tributa. Mismo criterio que el IVA débito (regla V2).
3. **La alícuota efectiva se deriva de un CM03 presentado**, nunca se estima ni se tipea a mano. Si no hay CM03 del período, se hereda el último y queda marcado con `alicuota_del_periodo = false`.
4. **`base_referencia` es la base TOTAL declarada**, no la de ADARA. Mientras `base_confirmada = false` la alícuota es un **techo** y la pantalla lo advierte.
5. **No se inventan líneas de negocio con el remanente.** La facturación que ADARA no ve se expone como **cobertura**, no como ingresos sin costos.
6. **Los códigos de jurisdicción son `snake_case` compartidos** con `retenciones.jurisdiccion` y las percepciones de compra. Nunca texto libre.
7. **El coeficiente unificado es de la empresa, no de la línea.** El reparto de IIBB entre líneas es contabilidad de gestión interna: se hace por base gravada.

---

## 11. Pendientes

### Preguntas al contador (bloqueantes)

1. **Base imponible total declarada del CM03 202606.** Un solo número; convierte la alícuota de techo en exacta y además mide cuánta facturación no ve ADARA.
2. **¿La DDJJ declara una actividad o más de una?** Bajo CM el coeficiente es único para la empresa, pero **la alícuota puede diferir por actividad**. Si "comercio electrónico" y "venta mayorista de iluminación" tributan distinto, `iibb_parametros` necesita **un renglón por actividad**, no uno. Es un cambio chico si se sabe ahora.
3. **Determinado negativo en Santa Cruz (−$551.217,97) y Tierra del Fuego (−$15.831,19).** No es normal; puede ser ajuste, notas de crédito o error de carga.
4. **Reducción de alícuota de retención en Corrientes, Santa Fe y La Pampa** — $1,7M de saldos a favor solo en esas tres.
5. **¿Hay SIRCREB en el Supervielle?** Ver abajo.

### Técnicos

- **Falta capturar retenciones.** El CM03 computó **$15.471.267,94** de "Valores Restan" en junio; ADARA ve **$1,53M** por el settlement de MP y $4,35M por la API de ML. La diferencia es casi seguro **SIRCREB (recaudaciones bancarias)**, que no se ingesta. No afecta el margen (la retención no es gasto) pero **sí el saldo a pagar** de `v_iibb_jurisdiccion_mensual`.
- **Percepciones aduaneras de IIBB.** La hoja de Tucumán del CM03 muestra $59.316,88 de percepciones aduaneras, de importaciones. ADARA no las tiene porque los despachos siguen sin cargar (los 9 borradores del simulador).
- **Coeficiente de ingresos calculable internamente** a partir del partido/provincia del comprador (módulo Flex), para contrastar contra el del contador.
- **Actualización anual de coeficientes**: cuando salga el CM05 nuevo se carga una fila nueva en `iibb_parametros`/`iibb_jurisdiccion` con `periodo_referencia` nuevo; la vieja pasa a `vigente = false` y el histórico queda auditable.

---

## 12. Documentos relacionados

- `ADARA-IMPUESTOS.md` — IVA, Ganancias, posición fiscal general
- `ADARA-RETENCIONES-IIBB.md` — ingesta de retenciones desde el settlement de MP
- `ADARA-PNL.md` — estado de resultado (los márgenes publicados antes del 10/8 estaban sobrestimados)
- `ADARA-SCHEMA.md` — tablas y vistas
- `ADARA-TFACTURA.md` — el sync de Tango y el descarte de los no-ML
- `ADARA-COMPRAS-IMPORTACIONES.md` — percepciones de IIBB de compra
- `ADARA-DECISIONES.md` — reglas consolidadas
