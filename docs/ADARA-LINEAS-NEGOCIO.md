# ADARA — Líneas de Negocio

Última actualización: 8 Junio 2026 (CAMBIO DE MODELO: línea = producto/familia, canal = dimensión separada; se unificó electrónica en una sola línea "Electrónica" y se eliminó "Electrónica off-ML"; regla pasa a `familia → línea`) · 8 Junio 2026 (alta de líneas Repelentes y Vasos térmicos) · 27 Mayo 2026

Cómo se desagrega todo el negocio en líneas independientes para análisis de rentabilidad.
**Lectura obligatoria antes de tocar lógica de imputación, P&L o conciliación por línea.**

---

## Objetivo

Cada **venta, gasto, sueldo, movimiento bancario, impuesto y reclamo** se imputa a una línea de negocio. Eso permite:

- Estado de resultado mensual independiente por línea
- Saber cuánto vale cada línea (stock + plata propia)
- Detectar qué línea gana o pierde plata
- Decidir dónde invertir o desinvertir con datos

Sin imputación por línea, todo se mezcla y no se puede analizar nada.

---

## Modelo de imputación: línea = producto, canal = dimensión

**Decisión (8/6/2026):** la **línea de negocio = el producto (familia)**. El **canal** de venta
(ML, Tienda Nube, WhatsApp/efectivo, B2B) es una **dimensión separada**, NO parte la línea.
Así, "Electrónica" es una sola línea que se vende por varios canales; la rentabilidad por canal
se obtiene cruzando esa dimensión (ej. en la pantalla Resultado, selector de canal), no creando
líneas distintas. Esto reemplaza el modelo viejo que partía electrónica en "ML Electrónica" y
"Electrónica off-ML".

## Líneas activas hoy

| Línea | Productos | Canales | Tipo cliente |
|-------|-----------|---------|--------------|
| **Electrónica** | Auriculares, smartwatches, parlantes Bluetooth, accesorios | ML + Tienda Nube + WhatsApp/efectivo + B2B | Consumidor final / empresas |
| **Luminarias** | Alumbrado público | B2B (Tango) | Empresas / municipios |
| **Mochilas Sindicatos** | Mochilas a medida para sindicatos | B2B (Tango) | Sindicatos |
| **Mochilas Individuos** | Mochilas para personas | ML + Tienda Nube + WhatsApp | Consumidor final |
| **Repelentes** | Repelentes (línea propia) | B2B | Empresas |
| **Vasos Térmicos** | Vasos térmicos Voox, mate listo Voox | B2B | Empresas |

> Son **6 líneas** (se eliminó "Electrónica off-ML": sus ventas ahora son la línea **Electrónica** por canal Tienda Nube / WhatsApp / B2B).
> Repelentes y Vasos Térmicos se vendían/venden **B2B** (no por ML ni Tienda Nube); sus ventas entran con el sync de Tango.
> Familias en el catálogo (`skus.familia`): `electronica`, `luminaria`, `mochila_sindical`, `mochila_individual`, **`repelente`** (singular, 1 SKU) y **`vaso_termico`** (singular, 2 SKUs).
> **Pendiente:** reglas `familia → línea` para `repelente` y `vaso_termico` en `lineas_negocio_reglas` (hacer junto con Tango) y el costo de apertura de esos SKUs (siguen en $0).

Todas las líneas comparten:
- Mismo CUIT
- Misma condición fiscal: IVA Responsable Inscripto, IIBB Convenio Multilateral, Ganancias inscripto

A futuro podrán agregarse nuevas líneas desde la propia app (ver sección "Agregar nuevas líneas").

---

## Modelo: cada movimiento se imputa a una línea

| Entidad | ¿Cómo se imputa? | Imputación obligatoria |
|---------|-------------------|------------------------|
| Venta | Automática por SKU + canal, override manual disponible | Sí |
| Gasto | Manual al cargar | Sí |
| Sueldo | Distribución por porcentaje configurado por empleado | Sí (suma 100%) |
| Movimiento bancario | Se hereda de la venta o gasto vinculado | Sí (incluso si va a "Sin asignar") |
| Impuesto | Se calcula por línea agrupando ventas/compras imputadas | Automático |
| Reclamo ML | Hereda la línea de la venta original que se reclama | Sí |
| Importación / compra | Manual al cargar (los lotes hijos heredan) | Sí |

Regla dura: **todo lo que mueve plata o stock se imputa a una línea**. No hay movimientos sin línea (salvo el bucket especial "Sin asignar" descripto abajo).

---

## Asignación de ventas a línea — regla automática

Cada SKU se clasifica internamente en una **familia de producto**:

| Familia | SKUs incluidos | Ejemplo |
|---------|----------------|---------|
| `electronica` | Auriculares, parlantes, smartwatches, accesorios | Redmi Buds, JBL Tune |
| `luminaria` | Alumbrado público | Luminaria LED 100W |
| `mochila_sindical` | Mochilas con logo o personalización para sindicatos | Mochila UOM 25L |
| `mochila_individual` | Mochilas para consumidor final | Mochila urbana 20L |

Cada venta tiene un **canal de origen**:

| Canal | Origen del registro |
|-------|---------------------|
| `ml` | Sync ML |
| `tienda_nube` | Sync Tango (cliente "Tienda Nube") |
| `b2b_tango` | Sync Tango (cliente con CUIT empresarial) |
| `whatsapp_efectivo` | Sync Tango (remito sin factura) |

La línea se determina automáticamente por **familia** (el canal NO parte la línea):

| Familia | Línea asignada |
|---------|----------------|
| `electronica` | Electrónica |
| `luminaria` | Luminarias |
| `mochila_sindical` | Mochilas Sindicatos |
| `mochila_individual` | Mochilas Individuos |
| `repelente` | Repelentes *(regla pendiente de cargar)* |
| `vaso_termico` | Vasos Térmicos *(regla pendiente de cargar)* |

> El canal (`ml`, `tienda_nube`, `whatsapp_efectivo`, `b2b_tango`) se guarda en la venta como
> **dimensión**, para cruzar rentabilidad por canal dentro de cada línea — pero no cambia a qué línea pertenece.

### Override manual

Si la regla automática se equivoca o hay un caso especial (ej: una mochila individual vendida en bulk a una empresa), se puede cambiar la línea manualmente desde la pantalla de la venta. Queda registrado como override (para auditoría).

### Configuración

El mapeo `familia → línea` vive en la tabla `lineas_negocio_reglas` (configurable). No está hardcodeado.

---

## Asignación de gastos a línea

Manual al cargar el gasto. Los dos flujos posibles (recordatorio de `ADARA-FLUJO-OPERATIVO.md`):

| Flujo | Cuándo se asigna la línea |
|-------|---------------------------|
| Proactivo: cargás el gasto en ADARA | Al momento de la carga, en el formulario |
| Reactivo: el gasto aparece desde el extracto bancario en "Mov sin conciliar" | Al "Pasar a gasto", en el modal de creación |

Si un gasto es compartido entre líneas (ej: alquiler del depósito), se cargan **N gastos separados** (uno por línea con su porcentaje correspondiente), no un gasto único con reparto. Más simple, más auditable.

---

## Asignación de sueldos a línea

Cada empleado tiene un **porcentaje configurado por línea** que suma 100%. El sueldo bruto del mes se reparte automáticamente según esos porcentajes.

Ejemplo:

```
Empleado: Juan Pérez
Distribución:
  Electrónica            55%
  Luminarias              5%
  Mochilas Sindicatos    20%
  Mochilas Individuos    20%
Total                   100%
```

Tablas involucradas: `empleados` (catálogo) y `empleado_linea_pct` (distribución `(empleado, línea) → porcentaje`, con UNIQUE en ese par). La validación de **suma = 100% por empleado** la hace la app (no hay trigger). La carga mensual del importe del sueldo vive en una tabla transaccional aparte que se construirá en la capa correspondiente. La configuración se puede editar; los meses cerrados quedan congelados.

---

## Asignación de movimientos bancarios

| Caso | Línea asignada |
|------|----------------|
| Mov vinculado a una venta | Hereda la línea de la venta |
| Mov vinculado a un gasto | Hereda la línea del gasto |
| Mov vinculado a un sueldo | Se reparte según los % del sueldo |
| Mov de pago a proveedor | Hereda la línea de la compra/OC |
| Mov de pago de impuesto | Hereda la línea proporcional según corresponda al período |
| Mov huérfano (transferencia entre cuentas propias, ajuste, sin contexto) | "Sin asignar" hasta que se resuelva |

### Bucket "Sin asignar"

Cuando un movimiento no se puede imputar a ninguna línea aún (ej: una transferencia interna entre cuentas, un ajuste contable, una devolución de impuestos), se imputa al bucket "Sin asignar".

**Regla**: el bucket "Sin asignar" tiene que tender a cero. Antes de cerrar el mes, todos los movimientos en "Sin asignar" deben resolverse (asignar línea o reclasificar como interno-cuenta).

---

## Saldo dinámico por línea

No hay tabla de saldos persistida. El saldo se calcula al vuelo a partir de los movimientos imputados.

### Fórmula

```
saldo(linea, cuenta, fecha) =
    saldo_inicial(linea, cuenta)                    [al 31/12/2025]
  + Σ ingresos imputados a la línea en esa cuenta hasta la fecha
  − Σ egresos  imputados a la línea en esa cuenta hasta la fecha
```

Dimensiones disponibles:
- Por línea
- Por cuenta (Supervielle ARS, MP ARS, Caja ARS, Caja USD)
- A una fecha de corte

### Validación de coherencia

En cada cuenta:

```
Σ saldo(linea_i, cuenta, hoy) por i ∈ líneas activas + "Sin asignar"  ==  saldo_real_cuenta_hoy
```

Si no coincide, hay un problema:
- Algún movimiento sin imputar a ninguna línea (bug del sistema, no debería pasar)
- Saldo inicial mal cargado
- Movimiento duplicado o faltante

La app muestra esta validación de forma destacada en el módulo "Patrimonial" para detectarlo a tiempo.

---

## Posición fiscal por línea

Aunque todas las líneas comparten CUIT (la DDJJ que va a AFIP es una sola, sumando todo), **internamente se desagrega para análisis de rentabilidad real**.

| Impuesto | Cálculo por línea |
|----------|-------------------|
| IVA débito | Σ IVA de ventas imputadas a la línea en el período |
| IVA crédito | Σ IVA de compras imputadas a la línea en el período |
| IVA neto | Débito − Crédito |
| IIBB | Σ Ventas netas de la línea × 3% (o alícuota correspondiente) |
| Retenciones sufridas | Σ Retenciones de movs imputados a la línea |
| Ganancias | Resultado operativo de la línea × tasa (a definir con contador) |

El total a pagar a AFIP/ARBA es la suma de los netos por línea. El interés interno es saber cuánto IVA "genera" o "recupera" cada línea, para entender la rentabilidad real.

---

## Reglas para agregar nuevas líneas

Cuando se suma una nueva línea (ej: a futuro "Importación de cámaras"):

1. Desde la pantalla de configuración → botón **"+ Nueva línea de negocio"**
2. Definir:
   - Nombre
   - Descripción corta
   - Familias de SKUs que la componen
   - Mapeo de canales (cuáles van a la nueva línea)
   - Saldo inicial por cuenta (puede ser 0 si arranca limpia)
3. Una vez creada:
   - Aparece en todos los formularios de imputación
   - Se incluye automáticamente en P&L, Patrimonial, Posición Fiscal
   - Las ventas nuevas con SKUs de las familias asociadas se autoimputan
   - Las ventas viejas no se reimputan (las líneas anteriores quedan como estaban)

---

## Pendientes y TBD

- **Saldo inicial al 31/12/2025 por línea por cuenta**: lo carga Sebastián al momento de arrancar la operación corriente. Pendiente la planilla con los valores.
- **Tasa efectiva de Ganancias por línea**: a definir con el contador.
- **Porcentajes de distribución de sueldos por empleado**: cargar al inicio en `empleado_linea_pct`.
- **Familias de SKUs en el catálogo actual**: hay que clasificar los 132 SKUs cargados desde Tango. Una vuelta inicial de etiquetado manual.

---

## Documentos relacionados

- `ADARA-FLUJO-OPERATIVO.md` — cadencia operativa, dónde aparecen las imputaciones en el día a día
- `ADARA-PNL.md` — estado de resultado mensual por línea
- `ADARA-PATRIMONIAL.md` — valor neto de la empresa, total y por línea
- `ADARA-IMPUESTOS.md` — posición fiscal en vivo
- `ADARA-CONCILIACION-BANCARIA.md` — cómo los movimientos heredan la línea
- `ADARA-GASTOS.md` — flujo proactivo y reactivo de gastos
- `ADARA-COMPRAS-IMPORTACIONES.md` — lotes y su imputación a línea
- `ADARA-DECISIONES.md` — reglas consolidadas
