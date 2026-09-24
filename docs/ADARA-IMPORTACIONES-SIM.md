# ADARA — Simulador de Importaciones

Última actualización: 23 Septiembre 2026 (**validación contra despachos oficializados + corrección del prorrateo del flete**: los despachos **26073IC04002688** (real) y **26073IC04002690** (declarado) de BISHOP agosto confirman que Aduana prorratea **flete y seguro por FOB, no por peso** — factor idéntico 1,054586 en los tres ítems. El doc decía "por peso" y estaba mal; el código (v3) ya lo hacía por FOB. Ahorro oficial derechos+estadística **USD 14.639,57** → coima 30% **USD 4.391,87**. Regla nueva: **la simulación se valida contra el despacho oficializado, no contra planillas**. Documentado el schema `datos v:3`.) · 7 Agosto 2026 (**spec de Fase 2 acordado con Sebastián** — confirmar despacho → compra de importación + lotes + crédito fiscal + pagos. Decisiones: **IMP-F2-1** la sim se archiva (`confirmada`), no se borra; **IMP-F2-2** `sku_id` opcional en la carga y obligatorio al confirmar, con pantalla de mapeo; **IMP-F2-3** check "viene con factura aparte" por concepto del bolsón, que resuelve la decisión pendiente #2 (IVA de servicios locales); **IMP-F2-4** el costo se congela con el TC de la simulación; **IMP-F2-5** `pagos[]` suma cuenta de origen y se materializa como movimientos vinculados. Hallazgo bloqueante: los productos del JSONB **no tienen `sku_id`** — sólo `nombre` libre.) · 22 Julio 2026 (**fix flete fuera del ahorro/coima** — el CIF real sombra usa flete y seguro DECLARADOS; el ahorro nace solo de subdeclaración de FOB/cantidad/posición del producto; la diferencia flete real−declarado es costeo puro. Caso HOKU id 8. Ver P15.) · 7 Julio 2026 (**arancel SIM**, **cantidad declarada** por producto, **coima sobre ahorro total** con CIF real sombra + tablota por producto; coima movida al bolsón).

> **Estado:** **Fase 1 desplegada y en uso.** Pantalla `public/js/screens/importaciones-sim.js` (~1064 líneas), ruta `#importaciones_sim` (registrada en `main.js`), ítem de sidebar en `index.html`, tabla `importacion_sim` en Supabase (patrón post-A17). Calcula el costeo y guarda/recupera simulaciones como `borrador`. **No crea nada** (ni stock, lotes, compras ni caja). Validado al centavo contra las planillas reales de Sebastián. **Fase 2 especificada (7/8/2026), pendiente de construir.** Fase 3 (rentabilidad) pendiente.

Herramienta **what-if** para decidir, **antes de comprar/importar**:
1. El **costo final por producto puesto en Argentina** (costo de lote / CMV, neto de IVA y de percepciones, congelado en ARS).
2. Si ese producto es rentable contra su precio de venta (Fase 3, no construida).

Complementa a `ADARA-COMPRAS-IMPORTACIONES.md`: ese doc describe cómo se **registra** una importación pasada; este la **proyecta** antes de hacerla.

---

## Por qué Fase 2 es la prioridad (foto al 7/8/2026)

Los 9 borradores acumulados representan:

| | |
|---|---|
| Costo de mercadería no ingresado al stock | **$1.041.161.264,59** |
| Crédito fiscal no capturado | **$199.339.793,17** |
| IVA a pagar julio 2026 | $27.036.258,42 |
| IVA a pagar agosto 2026 | $16.014.132,69 |

Un solo despacho (BISHOP, $32,5M de crédito) cubre todo el a pagar de julio. En paralelo, el **86,7% del CMV de julio es estimado** ($298.099.423,47 sobre $343.862.983,22): 18 SKUs vendidos no tienen **ninguna** compra cargada y 16 más agotaron sus lotes. Fase 2 es lo que cierra las dos cosas a la vez.

> ⚠️ Los borradores 1, 5 y 6 fueron creados en junio. **Junio ya está declarado** (apertura, F.2051 tx 1182373081): si esos despachos son de junio, su crédito pertenece a un período cerrado y requiere rectificativa. Decisión de Sebastián + contador, no del sistema.

---

## Reglas de la constitución que respeta (no reinventar)

- **P12** — Capitalizan al costo del lote: derechos, tasa estadística, impuestos internos, flete, seguro, nacionalización/despachante, **arancel/tasa SIM**, gastos sin factura, **coima**. **No** capitalizan: percepciones de IVA/IIBB/Ganancias (son pagos a cuenta → Posición Fiscal). El "ahorro" fiscal no es rentabilidad.
- **CF6** — El costo nace **neto de IVA** e inmutable, originado en la compra.
- **CF9–CF11** — IIBB y Ganancias **determinados** reducen rentabilidad; sus **percepciones** son pagos a cuenta. Meterlas en el costo *y* computarlas en Posición Fiscal sería doble conteo. → En el simulador, las percepciones de IVA/IIBB/Ganancias son **crédito fiscal, no costo.**
- **C11–C13** — Las compras vienen en USD; el TC es un input y el costo de lote se **congela en ARS**.
- **P11** — Margen sobre precio de venta neto s/IVA (Fase 3).
- **FO5** — Un despacho se confirma **cuando la mercadería llega al depósito**, no al pagar ni al embarcar.

---

## El costo del simulador NO es el "precio con IVA" de las planillas

Punto clave validado contra la planilla (hoja "40 pies", contenedor de carteles LED). El total "con IVA incluido" de la planilla (ej. US$ 366.285,84) **no es un costo**; tiene dos cosas que el simulador (que es de costeo) no mete:

1. **Markup "Costos Importación ADARA"** (ej. 52% del CIF = US$ 88.214,96). Es el **margen / precio de venta**, no costo (Fase 3).
2. **IVA + IIBB percepción + Ganancias percepción** (ej. US$ 61.580,82). Son **crédito fiscal / pagos a cuenta**, no costo (CF9–CF11). La planilla los deja dentro del "costo"; el simulador los separa.

Equivalencias para reconciliar contra la planilla:
- Simulador **Costo s/IVA** ↔ planilla "costo total sin iva" (CMV real). *(La planilla, además, prorratea todo por KG%; el simulador calcula derechos/estadística por producto sobre su propio CIF, que es más preciso, así que los unitarios difieren aunque el total cierre.)*
- Simulador **Costo + Crédito fiscal** ↔ total desembolso del despacho **antes del markup**.
- Total "con IVA" de la planilla = lo anterior **+ markup** = precio de venta (Fase 3).

---

## Hallazgos que corrigen el doc de compras

Validado contra las planillas reales:

1. **El flete se prorratea por PESO (kg), no por FOB.**
2. **Existen DOS fletes:** el **declarado** (entra al CIF y a la base de impuestos) y el **real**. La diferencia (real − declarado) capitaliza al costo pero **no** a la base aduanera. En el simulador esa diferencia se inyecta al **bolsón de fijos** (ver abajo).
3. **Subfacturación de FOB:** se declara un FOB menor al real. Los tributos caen sobre el **declarado**; la diferencia no declarada capitaliza al costo **sin crédito fiscal** (P12). Es salida de caja por fuera del banco.
4. **Subfacturación de CANTIDAD:** se declara **menos (o más) unidades** que las reales. Ver "Tres ejes de subdeclaración".
5. **El despachante NO entra al CIF.** Es un servicio local; capitaliza al costo pero va al **bolsón de fijos**, no a la base aduanera.
6. **Reclasificación de posición arancelaria + coima** (ver sección propia).
7. **Base de IVA = CIF + derechos + estadística + imp. internos** (confirmado: en la planilla, Ganancias da 6% exacto sobre esa base, no sobre el CIF solo).
8. **Arancel/tasa SIM (Sistema Informático MALVINA):** monto fijo que se paga en el VEP y capitaliza al costo, pero **NO** integra la base de IVA ni genera crédito (ver sección propia).

---

## Tres ejes de subdeclaración (FOB · alícuota · cantidad)

El modelo soporta declarar distinto de lo real en tres dimensiones independientes, todas bajo el check **"Declaro distinto"**:

1. **Precio (FOB):** `fob_decl_u` < `fob_real_u`.
2. **Alícuota:** `derechos_pct_decl` / `estadistica_pct_decl` < las reales (reclasificación de posición).
3. **Cantidad:** `cantidad_decl` ≠ `cantidad` real.

**Regla clave de la cantidad:** las unidades que **entran de verdad** (`cantidad`) son **stock real** y absorben el costo total; las **declaradas** (`cantidad_decl`) definen la base aduanera. Por eso:
- El **costo total** y el **divisor del costo unitario** usan la **cantidad real** (incluidas las no declaradas).
- El **CIF / derechos / IVA / tributos / crédito** usan la **cantidad declarada**.

Soporta también `cantidad_decl > cantidad` (sobre-declarar ítems de arancel 0% para "tapar" la subdeclaración de los que sí pagan; el peso/FOB declarado se corre hacia ellos y baja el CIF de los dutiables). En ese caso "No declarado" puede dar negativo por producto; es esperado.

---

## Inputs del modelo (Fase 1, modelo v2)

### Detalles generales
- **TC ($/USD)** — congela el costo en ARS.
- **Flete declarado (USD, total)** — va al CIF; se prorratea por peso declarado.
- **Flete real (USD, total)** — costo verdadero; la diferencia (real − declarado) va al bolsón.
- **Seguro %** y **Seguro $ (USD)** — si se carga el monto, **pisa** al %. Total = monto, o `% · (Σfob_decl + flete_declarado)`. Se prorratea por FOB declarado.
- **Despachante %** y **Despachante $ (USD)** — si se carga el monto, **pisa** al %. Total = monto, o `% · ΣCIF`. **No entra al CIF**; va al bolsón.
- **IIBB percepción %** · **Ganancias percepción %** — crédito fiscal.
- **Coima %** — sobre el ahorro total de derechos+estadística (ver sección coima).
- **Arancel SIM (USD)** — **default 10, editable.** Tasa fija; capitaliza vía bolsón y suma a tributos, **sin** crédito ni base IVA.
- Tira de **bases calculadas** (solo lectura): Σ FOB declarado, Σ FOB real, Σ peso, CIF total, seguro total, despachante total.

### Costos fijos (bolsón, USD)
Lista de conceptos manuales (TCA/almacenaje, SEDI, gastos origen, gastos operativos, transporte, seguridad, **Multa**, etc.). Más **cuatro renglones automáticos y bloqueados** que se inyectan al bolsón:
- **Diferencia flete (real − declarado).**
- **Despachante** (% del CIF o monto).
- **Arancel SIM.**
- **Coima.**

Se recalculan solos. El bolsón efectivo = fijos manuales + diferencia de flete + despachante + arancel SIM + coima.

> **Nota:** "Sim" ya NO va como concepto fijo manual (tiene campo propio `arancel_sim_usd`). Cargarlo a mano además sería doble conteo.

### Por producto (modelo v2: el campo primario es el REAL)
- **nombre**, **cantidad** (real), **peso unitario (kg)**.
- **FOB u (USD)** = FOB real (siempre).
- **Derechos %**, **Estadística %** = tasas **reales** (siempre).
- **Imp. int. %** = impuestos internos (opcional; se trata **como derechos**: capitaliza y entra a la base de IVA).
- **IVA %** = desplegable **21 % / 10,5 %**.
- **Fijo %/$** = reparto del bolsón. **Editable a mano** (en modo $ podés cargarle más a un producto y menos a otro; el indicador "Resto" avisa cuánto falta/sobra del bolsón).
- Check **"Declaro distinto (subfacturación / reclasificación)"** → despliega overrides opcionales:
  - **FOB declarado u** — lo que declarás (≤ real). Vacío = real → sin subfacturación.
  - **Cant. declarada** — unidades declaradas. Vacío = cantidad real → sin subdeclaración de cantidad.
  - **Derechos declarado %**, **Estad. declarado %** — lo que pagás. Vacío = real → sin reclasificación/coima.

> **Cambio de criterio vs v1:** antes el campo primario era el declarado y el real era el opcional. En v2 es al revés (se carga el real una sola vez; el declarado es la excepción). La migración v1→v2 es automática y preserva resultados (ver Persistencia).

---

## Fórmulas (por producto *i*)

`eff(override, real)` = `override` si está cargado, si no `real`.
Dos cantidades: `cant_real_i` = `cantidad` (entra → stock → costo); `cant_decl_i` = `eff(cantidad_decl, cantidad)` (aduana).
Bases declaradas: `Σpeso = Σ(peso_u·cant_decl)`, `Σfob = Σ(fob_decl_eff·cant_decl)`, `seguro_total = monto || %·(Σfob + flete_declarado)`.

**CIF DECLARADO (base aduanera, usa cantidad DECLARADA):**
```
fob_decl_total_i = eff(fob_decl_u_i, fob_real_u_i) · cant_decl_i
CIF_i = fob_decl_total_i
      + flete_declarado · (fob_decl_total_i / Σfob)      ← flete por FOB declarado (fleteFiscal)
      + seguro_total    · (fob_decl_total_i / Σfob)      ← seguro por FOB declarado
```

> **El flete fiscal se prorratea por FOB, no por peso (corrección 23/9/2026, validado contra despacho).** En el despacho
> **26073IC04002688** el Valor en Aduana de los tres ítems sale de multiplicar el FOB por el **mismo** factor
> `1,054586` = `1 + (flete 5.747,70 + seguro 611,20) / FOB 116.493,00`, sin importar el peso de cada ítem
> (tablets 288,92 kg · smartwatch 116,14 kg · auriculares 94,94 kg). El reparto **por peso** (`fleteCostoShare`) se usa
> **solo para el costeo del lote**, no para la base aduanera. Son dos prorrateos distintos y conviven: `fleteFiscal` (FOB)
> para CIF y tributos, `fleteCostoShare` (peso) para el costo unitario.

**Tributos que se PAGAN (tasa declarada; capitalizan; entran a base IVA):**
```
derechos_i    = CIF_i · eff(derechos_decl%_i, derechos%_i)
estadística_i = CIF_i · eff(estad_decl%_i, estad%_i)
imp_internos_i= CIF_i · imp_int%_i
```

**Crédito fiscal (NO capitaliza → Posición Fiscal):**
```
base_IVA_i  = CIF_i + derechos_i + estadística_i + imp_internos_i
IVA_i       = base_IVA_i · IVA%_i
IIBB_i      = base_IVA_i · IIBB%
Ganancias_i = base_IVA_i · Ganancias%
```

**CIF REAL SOMBRA (solo mide el ahorro para la coima; NO afecta el costo):**
```
fob_real_total_i = fob_real_u_i · cant_real_i
Σpeso_real = Σ(peso_u · cant_real)
CIF_real_i = fob_real_total_i
           + flete_declarado · (fob_real_total_i / Σfob_real)      ← flete DECLARADO, prorrateado por FOB real
           + seguro_total    · (fob_real_total_i / Σfob_real)      ← seguro DECLARADO (mismo monto que CIF declarado)
```
> **El sombra usa flete y seguro DECLARADOS, no reales (fix 22/7/2026, caso HOKU).** El CIF sombra difiere del CIF declarado **únicamente** por FOB y cantidad reales — que son ejes de subdeclaración del producto. El **flete NO genera ahorro**: la diferencia `flete_real − flete_declarado` es costeo puro (va al bolsón, capitaliza al lote) y no es subdeclaración aduanera. En consolidados el "flete real" suele traer gastos en origen adentro; prorratearlo en el sombra generaba **ahorro fantasma** (HOKU sin subdeclaración de producto daba USD 843 puros de flete). Corregido.

**Coima (ver sección propia):**
```
der_real_full_i = CIF_real_i · derechos%_i        (tasa real sobre CIF real)
est_real_full_i = CIF_real_i · estadística%_i
ahorro_i        = (der_real_full_i − derechos_i) + (est_real_full_i − estadística_i)
coima_total     = Coima% · Σ ahorro_i             ← GLOBAL, capitaliza vía bolsón, SIN crédito, NO tributo
```

**Bolsón efectivo y reparto:**
```
bolsón = Σ fijos_manuales + (flete_real − flete_declarado) + despachante_total + arancel_SIM + coima_total
fijo_i = (modo %)  fijo_asignado%_i · bolsón
       | (modo $)  fijo_asignado$_i
```

**Costo s/IVA (capitaliza al lote):**
```
no_declarado_i = fob_real_total_i − fob_decl_total_i   (incluye precio Y cantidad no declarados)
flete_costo_i  = flete_declarado · (peso_decl_total_i / Σpeso)   ← por PESO (fleteCostoShare); solo el declarado, la dif. va en fijo_i
seguro_i       = seguro_total · (fob_decl_total_i / Σfob)

Costo_sIVA_i (USD) = fob_real_total_i + flete_costo_i + seguro_i
                   + derechos_i + estadística_i + imp_internos_i + fijo_i

Costo_unit (USD) = Costo_sIVA_i / cant_real_i ;  (ARS) = · TC
```

> **Despachante, diferencia de flete, arancel SIM y coima** NO son líneas propias del costo: viven dentro de `fijo_i` (bolsón). El total del costo es invariante. No hay doble conteo (validado). El **divisor del unitario es la cantidad REAL**.

---

## Reparto de costos fijos

Bolsón (manual + dif. flete + despachante + SIM + coima) repartido entre productos, modo global:
- **Modo %**: cada producto un % del bolsón; el $ se recalcula al cambiar el bolsón. Indicador: **falta para 100%**.
- **Modo $**: cada producto un monto (**editable a mano** para cargar más a uno y menos a otro); al cambiar el bolsón los montos **se reescalan proporcional**. Indicador: **resto** del bolsón.
- **Precarga** por **kg** o por **FOB** (default kg, %).
- Botón **"Repartir resto"**: reparte lo que falta entre los productos en cero, por el criterio activo.
- Para que dif. flete / despachante / SIM / coima queden 100% costeados, hay que **Precargar** (repartir el bolsón completo). Al agregar SIM/coima en modo $, aparece "resto" sin asignar hasta precargar.

---

## Tabla total del despacho

Resumen consolidado (USD y ARS): Total CIF · Derechos · Estadística · Imp. internos · Base de IVA · IVA · IIBB · Ganancias · **Arancel SIM** · **Total tributos del despacho**. Color: borde ámbar = capitaliza, azul = crédito fiscal, gris = base. El **Arancel SIM** es ámbar (capitaliza) y va **fuera de la base de IVA**, sumando al total de tributos (es plata que se paga en el VEP). La fila "Total tributos" es lo que se paga al fisco/aduana, **no** el costo del lote. (Despachante y coima no figuran acá: no son tributos.)

---

## Coima — tablota de ahorro por producto

Reemplaza a la tarjeta resumen anterior. Tabla por producto: **Derechos pagás · Derechos real · Estad. pagás · Estad. real · Ahorro (USD/ARS)**, con total al pie + **% coima → Coima a pagar (→ bolsón) → Neto a favor (ahorro − coima)**.

- **Ahorro** = lo que pagarías de derechos+estadística declarando **TODO real de PRODUCTO** (FOB + cantidad + alícuota, vía CIF real sombra) **menos** lo que pagás declarando. Captura los tres ejes de subdeclaración del producto (no solo la reclasificación de alícuota, como en la versión anterior). **El flete NO integra el ahorro** (ver fix 22/7 en la fórmula del CIF sombra).
- **Coima global** = `Coima%` × ahorro total. **Capitaliza vía bolsón** (se reparte por `fijo_asignado`), **no** genera crédito, **no** es tributo. Es **salida de caja** → debe figurar como pago en el cierre.
- Ítems de arancel 0% aportan $0 al ahorro (aunque se declaren de más/menos).
- **La base del ahorro es CIF, no FOB.** Aduana liquida derechos y estadística sobre el Valor en Aduana (FOB + flete + seguro). Calcular sobre FOB subestima el ahorro ~4,5% (el factor del prorrateo). Si el despachante pactara la coima sobre FOB, haría falta un parámetro `coima_base` ('cif'|'fob'); hoy **no existe** y la app calcula siempre sobre CIF.

> **BISHOP agosto (id 11, 23/9/2026)** — caso testigo contra despachos oficializados. Se emitieron dos despachos por la misma factura `ADAR26082601MA`: **26073IC04002688** (todo real) y **26073IC04002690** (declaración modificada). Derechos + estadística: real **USD 20.222,82** vs declarado **USD 5.583,25** → **ahorro USD 14.639,57**, coima 30% = **USD 4.391,87**. La app, cargada con los datos del despacho (declaración del 2690 y **flete declarado 5.747,70**), da **USD 14.631,35** → coima **USD 4.389,41**. El gap de USD 8 es porque el despachante escaló los FOB del despacho "real" para que sumen 116.493,00 igual que el modificado (el FOB facturado real es 116.443,00; el Pad 7 figura a 263,1129 en vez de 263). Despreciable.
>
> **Regla operativa (nueva):** la simulación se valida **contra el despacho oficializado**, no contra planillas de Excel. El `flete_declarado` se toma del campo *Flete Total* del despacho. Dos desvíos frecuentes detectados en esta sesión: la sim tenía flete 4.731,85 contra 5.747,70 del despacho, y una declaración distinta de la que efectivamente se oficializó (tablets 32.535,50 en la sim vs 22.024,00 en el 2690).
>
> **Estadística sin tope en este despacho:** 3% sobre 23.328,16 = 699,84. El `tope_estadistica_usd` (180) no intervino.

> **HOKU (id 8, 22/7/2026)** — caso testigo del fix: contenedor consolidado, **sin subdeclaración de producto** (todos los overrides en null), `flete_declarado 3.800 / flete_real 8.388` (el "real" incluye gastos en origen). El modelo previo daba **USD 843 de ahorro fantasma** puro por flete → coima 50% = USD 421. Con el fix (sombra usa flete declarado), ahorro = **0** y coima = **0**, que es lo correcto. La diferencia de flete sigue capitalizando al costo vía bolsón sin cambios.

> Xiaomi (id 6): despacho con subdeclaración **real** en los tres ejes (cantidad, FOB, y algunos overrides de precio). Su ahorro es **genuino** y > 0. El número validado el 7/7 (ahorro USD 6.869,05 / coima 30% USD 2.060,71) incluía el efecto flete y quedó **obsoleto**: con el fix el ahorro baja y la coima se recalcula sobre los ejes de producto. Pendiente re-validar contra la planilla real con el criterio nuevo.

---

## Costo final por producto + detalle

- **Tarjeta "Costo final por producto"** (siempre visible): # · Producto · Cantidad (real) · Costo unit. USD/ARS · Costo total USD/ARS + fila Total.
- **Detalle por producto** (expandible, una tabla alineada, USD y ARS, unitario y total):
  - **Costo s/IVA**: FOB declarado, no declarado, flete declarado, seguro, derechos, estadística, imp. internos, **fijos (incl. flete-dif. + despachante + SIM + coima)** → subtotal.
  - **Crédito fiscal** (no es costo): IVA, IIBB, Ganancias → subtotal.
  - **Bases**: **Cantidad real / declarada** (con las sin declarar) · CIF del producto.

---

## Control de cierre de pagos (TC por pago)

Tabla de pagos: concepto + monto + **moneda (USD/ARS)** + **TC del pago** + equivalente en USD. Cada pago lleva **su propio TC**. Invariante:

```
Σ pagos (USD)  =  Costo capitalizado total  +  Crédito fiscal total
```

Los pagos reales **incluyen** las percepciones (VEP), que no son costo; por eso se compara contra costo **+** crédito. Comparación en **USD**. La **coima**, el **arancel SIM** y el **no declarado** son salida de caja → deben figurar como pagos. Diferencia ≠ 0: falta cargar un gasto como costo (+) o anotar un pago (−). *Es normal que el cierre dé aproximado mientras falten facturas de transferencia de costos.*

> ⚠️ Al 7/8/2026 `pagos[]` está **vacío en todas las simulaciones**. La tabla existe pero no se usó todavía.

---

## Persistencia y estados

`borrador` (Fase 1) | `confirmada` (Fase 2). Tabla `importacion_sim` (post-A17: RLS + policy `authenticated` + GRANT a `authenticated` + REVOKE de `anon`):

```
importacion_sim
  id, nombre, estado('borrador'|'confirmada'),
  datos jsonb,                 -- escenario completo, versionado con "v"
  costo_total_ars, credito_total_ars,   -- cacheados para el listado
  compra_id bigint null,       -- se completa al confirmar (Fase 2); SIN FK por ahora
  creado_en, actualizado_en, creado_por
```

**`datos` (JSONB), `v:3` (vigente desde el 7/8/2026):**
- Todo lo de `v:2` **más**: `sku_id` por producto (opcional en la carga, obligatorio al confirmar — IMP-F2-2), `cuenta` en `pagos[]`, y en `params`: **`tope_estadistica_usd`** (tope de la tasa de estadística, default 180), **`iibb_monto`** y **`ganancias_monto`** (montos fijos que pisan el % cuando están cargados).
- Migración v2→v3: aditiva, los campos nuevos quedan en null/default y las sims viejas siguen calculando igual.

**`datos` (JSONB), `v:2` (base, se mantiene en v3):**
- `params`: tc, flete_declarado_usd, flete_real_usd, seguro_pct, seguro_monto, despachante_pct, despachante_monto, iibb_pct, ganancias_pct, coima_pct, **arancel_sim_usd** (default 10; fallback 10 al cargar si falta).
- `fijos[]`: {concepto, monto_usd} (solo manuales; los auto se derivan).
- `fijos_modo` ('pct'|'monto'), `fijos_criterio` ('kg'|'fob').
- `productos[]`: nombre, fob_real_u, fob_decl_u (override null=real), cantidad, **cantidad_decl (override null=real)**, peso_u, derechos_pct, derechos_pct_decl, estadistica_pct, estadistica_pct_decl, imp_internos_pct, iva_pct, fijo_asignado, declaro_distinto.
- `pagos[]`: {concepto, monto, moneda, tc}.

**Migración v1→v2 (automática, preserva resultados):** real = (real v1 si existía, si no el declarado v1); override declarado = el declarado v1 solo si difería; `declaro_distinto` = true si quedó algún override. `cantidad_decl` y `arancel_sim_usd` heredan default (vacío/10) en sims viejas. Pagos v1 sin `tc` → TC general.

> Guardar la simulación **no crea nada**.

---

# FASE 2 — Confirmar despacho (spec acordado 7/8/2026)

Convierte un borrador en mercadería real: **compra de importación + lotes con costo congelado + crédito fiscal + movimientos de pago**. Es la operación **más irreversible del sistema** — crea lotes cuyo costo es inmutable (CF6) y alimenta el CMV hacia atrás.

## Hallazgo bloqueante: los productos no tienen SKU

`datos.productos[]` guarda **`nombre` como texto libre** y ningún identificador de SKU. Ejemplos reales del despacho BISHOP (id 9): *"Xiaomi Smart Band 10"*, *"Redmi Buds 8 Pro"*, *"Redmi Pad 2 9.7 4+128GB"*. Los SKUs de ADARA son `SW001N`, `TA005G`, `TA008G`. **No hay puente, ni siquiera aproximado.** Sin resolver esto no se puede crear un solo lote.

## Decisiones (IMP-F2-1 … IMP-F2-5)

| # | Decisión | Fundamento |
|---|----------|------------|
| **IMP-F2-1** | Al confirmar la sim **se archiva**, no se borra: `estado='confirmada'` + `compra_id`, sale de la lista por defecto (solapa "Confirmadas") y queda **bloqueada para edición** | El costo del lote es inmutable (CF6) y la simulación **es su memo de cálculo**. Borrarla deja un costo sin respaldo audible. Editarla después sería cambiar el memo de un costo ya congelado |
| **IMP-F2-2** | `sku_id` **opcional** en la carga, **obligatorio** al confirmar, vía **pantalla de mapeo** | Una sim what-if puede ser de un producto que todavía no traés; forzar el SKU en Fase 1 rompe el uso original. Pero un lote sin `sku_id` es imposible |
| **IMP-F2-3** | Cada concepto del bolsón lleva check **"viene con factura aparte"**. Los tildados **no capitalizan al confirmar**: se descuentan del bolsón y entran después por Gastos con `capitaliza_compra_id` | Evita el doble conteo (capitalizar en la sim *y* después con la factura) y **resuelve la decisión pendiente #2**: el IVA de los servicios locales se toma como crédito en la fecha de SU factura, en vez de quedar enterrado en el costo |
| **IMP-F2-4** | El costo se congela con el **TC de la simulación** (`params.tc`) | Es el TC con el que se calculó y se validó contra la planilla. Coherencia entre lo que se miró y lo que se guardó |
| **IMP-F2-5** | `pagos[]` suma **cuenta de origen**; al confirmar cada pago se materializa como **movimiento + vínculo** `op_tipo='compra'` | Engancha con la conciliación existente sin circuito nuevo: el pago aparece en el extracto y se cruza como cualquier otro |

## Las tres capas que NO hay que mezclar

El error más caro de esta fase es el doble conteo. Son tres cosas distintas:

| Capa | Qué es | Dónde vive |
|---|---|---|
| **(a) Costo** | Flete, seguro, derechos, estadística, imp. internos, despachante, SIM, coima, fijos, no declarado | Dentro de `lotes.costo_unitario`. **Ya está adentro al confirmar** |
| **(b) Crédito fiscal** | IVA + percepciones IIBB/Ganancias del despacho | `compra_componentes` tipos `iva` / `iibb_percepcion` / `ganancias_percepcion` → `v_control_mensual` → Posición Fiscal |
| **(c) Salida de caja** | Pago al proveedor del exterior, VEP, despachante, no declarado, coima | `movimientos` + `vinculos` (`op_tipo='compra'`) |

> 🚨 **Si un concepto ya capitalizó en (a), su factura NO se carga después como gasto capitalizable.** Se capitaliza dos veces y el CMV queda inflado. Ese es exactamente el riesgo que ataca IMP-F2-3.

## Flujo de confirmación

1. **Fecha de ingreso al depósito** (obligatoria, regla FO5). Define `compras.fecha` y `lotes.fecha_alta` — y con eso **qué ventas hacia atrás puede costear el FIFO**.
2. **Mapeo de SKUs.** Una fila por producto: nombre de la sim → buscador de SKU (reusa `core/skuPicker.js`) → botón "crear SKU" precargado con el nombre y la alícuota (`iva_pct` 21/10,5 → `skus.alicuota_iva`). Sugerencia automática por similitud de nombre. **Aviso si la alícuota del SKU elegido no coincide con la de la sim** (cambia el crédito fiscal).
3. **Conceptos del bolsón**: check "viene con factura aparte" por renglón.
4. **Pagos**: cuenta de origen por pago.
5. **Resumen previo a confirmar**: SKUs, cantidades, costo unitario por producto, crédito fiscal total, movimientos a crear. Confirmación explícita.
6. **Ejecución**: compra tipo `importacion` + `compra_componentes` (producto/iva/percepciones) + lotes + movimientos + vínculos + `estado='confirmada'` + `compra_id`. Después, correr `fn_consumir_fifo` desde la fecha de alta para costear las ventas que estaban en estimado.

## Deshacer

Anular la compra (`estado='anulada'`) **sólo si ningún lote fue consumido por FIFO**. Una vez vendida una unidad no hay vuelta atrás por esta vía: corresponde ajuste de inventario con delta. El botón debe decirlo **antes** de apretarlo.

## Pendientes de definir dentro de Fase 2

- **Multi-proveedor / multi-factura por despacho** (hoy la compra asume un proveedor).
- Qué proveedor se asigna a la compra de importación (¿el del exterior? ¿uno genérico "Importación"?).
- Si el `no declarado` y la `coima` se materializan como movimientos de `caja_usd` automáticamente o se cargan a mano.

---

## Decisiones pendientes (fuera de Fase 2)

1. **TC por componente del costo.** *Parcialmente resuelto* (ya hay TC por pago). Falta, si se quiere, TC por componente (FOB/flete/VEP).
2. ~~**IVA de los servicios locales (despachante + fijos).**~~ → **Resuelto por IMP-F2-3** (7/8/2026): los conceptos con factura propia salen del bolsón y entran por Gastos capitalizable, tomando su crédito en la fecha de su factura.
3. **Candado de fijo por fila** (clavar un monto a un producto para que no se reescale al mover el bolsón). *Nice-to-have.*
4. **Criterio de reparto por concepto** (hoy todo el bolsón usa un único criterio kg/FOB).
5. **Multi-proveedor / multi-factura por despacho.**
6. **Rentabilidad (Fase 3).**

---

## Guardrails técnicos

- Archivo `importaciones-sim.js` ~1064 líneas; **partir en `calc.js` + `screen.js` al entrar a Fase 2.**
- **Delegación de eventos: bindear UNA sola vez.** `#imp-body` persiste entre re-renders; `bindDelegation` se llama una vez por `render()`, NO en `renderBody()`. Rebindear en cada render acumula listeners (1→2→4→8…) — fue el bug del "+ Concepto". (Ver `ADARA-FRONTEND.md`.)
- Repintado parcial: los inputs se renderizan una vez; `paint()` solo escribe celdas calculadas (spans con id) para no perder foco.
- El motor `calc()` corre en **dos pasadas**: (1) CIF declarado + CIF real sombra + tributos + ahorro; (2) coima total → bolsón → reparto `fijo_i` → costo. La coima no puede computarse antes del bolsón porque depende de los tributos, y el bolsón depende de la coima → por eso el orden.
- Persistencia con `sbPost`/`sbPatch`/`sbGet` directos (tabla única + JSONB); sin endpoint en `server.js`. **Fase 2 sí necesita endpoint** (`POST /importaciones-sim/:id/confirmar`): crea compra + lotes + movimientos y necesita ser atómico con rollback, como `/compras` y `/ventas/efectivo`.
- Overrides (declarado/real, cantidad_decl) se persisten como `null` cuando están vacíos, para distinguir "sin override" de "0".

---

## Plan de implementación

- **Fase 1 — ✅ desplegada.** Costeo (con arancel SIM, cantidad declarada, coima sobre ahorro total) + guardar/recuperar borradores. No crea compra/lotes/stock/caja.
- **Fase 2 — 📋 especificada 7/8/2026, en construcción.** Ver sección propia arriba. Orden: (1) `sku_id` en `productos[]` + `cuenta` en `pagos[]`, (2) pantalla de mapeo, (3) endpoint atómico de confirmación, (4) archivo/bloqueo de la sim confirmada.
- **Fase 3 —** rentabilidad por canal (precio venta → comisión ML → envío → IIBB → resultado %).
