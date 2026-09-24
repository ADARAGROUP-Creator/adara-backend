# ADARA — Reclamos (a ML y a Proveedores)

Última actualización: 19 Mayo 2026

Módulo para trackear reclamos donde ADARA es **acreedora**: pide que alguien le cubra una mercadería que vino mal. Dos destinatarios posibles:

- **Mercado Libre**: cuando la devolución llega rota / faltante / cambiada por culpa del transporte o del cliente
- **Proveedor**: cuando la mercadería tiene defecto de fábrica y cae en garantía

Amplía el alcance del módulo de reclamos: antes solo cubría reclamos a Mercado Libre, ahora también a proveedores.

---

## Objetivo

Responder en tiempo real:

1. **¿Cuánto le estoy reclamando a ML y a proveedores?** (total abierto, por destinatario)
2. **¿Cuánto me cubrieron históricamente?** (recuperado)
3. **¿Cuál es mi % de recupero por destinatario?**
4. **¿Cuánto perdí en reclamos cerrados sin cubrir?**
5. **¿Qué productos / SKUs tienen más reclamos?** (detección de problemas de calidad)

---

## Concepto: cuentas corrientes con destinatarios diversos

ML y los proveedores no son "clientes" en sentido estricto, pero te deben cuando la mercadería vino mal. Cada uno se modela como una cuenta corriente paralela:

```
CUENTAS CORRIENTES — al 19/05/2026

  ┌─ MERCADO LIBRE ──────────────────────────┐
  │  Reclamado histórico       $ 4.560.000   │
  │  Recuperado                $ 3.180.000   │
  │  Pendiente                 $   590.000   │
  │  Perdido                   $   790.000   │
  │  % de recupero              69,7%         │
  └────────────────────────────────────────────┘
  
  ┌─ XIAOMI (importador) ────────────────────┐
  │  Reclamado histórico       $ 1.250.000   │
  │  Recuperado                $   980.000   │
  │  Pendiente                 $   180.000   │
  │  Perdido                   $    90.000   │
  │  % de recupero              78,4%         │
  └────────────────────────────────────────────┘
  
  ┌─ AODELI-KAVEH ───────────────────────────┐
  │  Reclamado histórico       $   340.000   │
  │  Recuperado                $   220.000   │
  │  Pendiente                 $    60.000   │
  │  Perdido                   $    60.000   │
  │  % de recupero              64,7%         │
  └────────────────────────────────────────────┘
```

---

## Cuándo nace un reclamo

Un reclamo se genera cuando la mercadería **no es vendible**. Al procesar la devolución física en la pantalla de Devol/Canc, se marca "Recibido — no disponible" y se elige:

| Disparador | Destinatario sugerido |
|------------|------------------------|
| Producto roto en el transporte de vuelta | **Mercado Libre** |
| Faltan accesorios / caja incompleta | **Mercado Libre** (el cliente lo entregó así) |
| Cliente devolvió un producto distinto al que compró | **Mercado Libre** |
| Caja vacía al recibirla | **Mercado Libre** |
| Defecto de fábrica (no enciende, falla técnica, pieza floja) | **Proveedor** (garantía) |
| Lote defectuoso (varios casos del mismo SKU del mismo lote) | **Proveedor** (problema sistémico) |
| No se sabe la causa | A criterio del usuario |

ADARA permite al usuario **elegir el destinatario** al crear el reclamo (no es automático). Si después se descubre que era el otro, se puede cambiar el destinatario antes de cobrar/perder.

---

## Estados del reclamo

Aplican a ambos destinatarios (con matices propios de cada uno):

| Estado | Significado | Acción esperada |
|--------|-------------|-----------------|
| **Borrador** | Reclamo creado en ADARA pero no iniciado todavía | Iniciar el reclamo (en interfaz ML o contactando al proveedor) |
| **Abierto** | Reclamo activo, esperando respuesta | Esperar / insistir |
| **En revisión** | Pidieron documentación / fotos / envío del producto | Responder con lo que pidieron |
| **Cubierto** | Aprobaron — van a compensar | Esperar recupero |
| **Cobrado** | El recupero ya entró (ver formas abajo) | Cerrado — entra como recuperado |
| **Rechazado** | No aceptan el reclamo | Decisión: insistir o dar por perdido |
| **Perdido** | Cerrado, no se cobra | Cerrado — entra como pérdida en P&L |

---

## Información de cada reclamo

| Campo | Detalle |
|-------|---------|
| `id` | UUID interno del reclamo |
| `destinatario_tipo` | `mercado_libre` / `proveedor` |
| `destinatario_id` | FK a ML (uno solo) o al proveedor específico |
| `venta_id` | FK a la venta original (siempre asociado a una venta) |
| `claim_id_externo` | ID del reclamo en ML, o número de caso/ticket del proveedor |
| `fecha_apertura` | Cuándo se generó |
| `motivo` | Roto / faltante / defecto fábrica / etc. |
| `descripcion` | Detalle libre + observaciones |
| `monto_reclamado` | Lo que se le reclama al destinatario |
| `estado` | Uno de los estados de arriba |
| `fecha_resolucion` | Cuándo se cerró |
| `tipo_recupero` | `plata` / `stock_reemplazo` / `nota_credito` / `descuento_proxima_compra` |
| `monto_recuperado` | Lo cobrado/acreditado/recibido en valor |
| `movimiento_mp_id` | Vinculado si fue pago en MP |
| `nota_credito_id` | Vinculado si fue nota de crédito del proveedor |
| `lote_reemplazo_id` | Vinculado si fue producto reemplazo (vuelve al stock) |
| `linea_negocio` | Heredada de la venta original |
| `sku_afectado` | El producto del reclamo |
| `evidencia_urls` | Links a fotos / docs (opcional) |

---

## Vínculo con la venta original

Cada reclamo está **siempre asociado a una venta** (`venta_id`). Esto permite:

- Trazabilidad completa: qué venta, qué cliente, qué SKU, qué lote
- Evita reclamos duplicados por la misma venta
- Análisis por SKU: tasa de reclamos por producto
- Análisis por proveedor: qué proveedores tienen más defectuosos

---

## Mecánica de resolución

### Reclamo a Mercado Libre

El recupero es **siempre en plata** (MP):

1. ML aprueba el reclamo
2. Aparece movimiento en el AS de MP con concepto tipo "Compensación por reclamo"
3. ADARA matchea automáticamente por monto + `claim_id`
4. Reclamo pasa a "Cobrado"
5. Entra al P&L como **recupero del mes en que se cobró**

### Reclamo a Proveedor — 4 formas de recupero

Más variadas porque los proveedores manejan distinto:

| Tipo de recupero | Cómo se registra en ADARA |
|------------------|----------------------------|
| **Plata (transferencia / depósito)** | Aparece como movimiento bancario → se vincula al reclamo (igual mecánica que ML) |
| **Nota de crédito** | Proveedor emite NC contra una compra anterior. ADARA registra la NC, descuenta el saldo de la compra (AP). El reclamo pasa a "Cobrado" |
| **Producto reemplazo** (nuevo) | Proveedor te envía la unidad de reemplazo. ADARA **suma al stock como lote nuevo** con cantidad = unidades reemplazadas y costo = 0 (porque ya pagaste el original). El reclamo pasa a "Cobrado" — el recupero está en stock, no en plata |
| **Descuento próxima compra** | Proveedor acredita el monto contra una compra futura. ADARA mantiene el reclamo en estado "Cubierto pendiente de aplicar" hasta que aparezca la próxima compra que lo use |

### Caso especial: producto reemplazo y stock

Cuando el proveedor te manda un producto nuevo en reemplazo del defectuoso, se crea un **micro-lote** con esa unidad:

```
Reclamo: SKU 23B Redmi Buds 8 Active Black
  Defectuoso recibido: 1 unidad (no se devuelve a stock)
  Reclamado a Xiaomi: $ 6.124 (costo unitario original del lote)
  Resolución: producto reemplazo

Stock generado:
  Lote nuevo "Reemplazo-#R001" 
    Cantidad: 1
    Costo unitario: $ 6.124  (mismo del original, no genera ganancia ni pérdida)
    SKU: 23B
    Procedencia: "Reemplazo por garantía"

Resultado en P&L:
  Reclamo cobrado: + $ 6.124
  Lote nuevo creado: + 1u valorizada en $ 6.124
  Diferencia: 0 (se compensa)
```

Si el costo del lote original era distinto al actual (porque hubo inflación o cambio en el costo de reposición), igual se usa el **costo del lote original** para que no haya ganancia/pérdida artificial.

---

## Métricas y % de recupero

### Indicadores clave (por destinatario)

| Métrica | Cálculo |
|---------|---------|
| Saldo pendiente | Σ reclamos abiertos por destinatario |
| % de recupero histórico | Σ recuperado / Σ reclamado (cerrados) |
| Tiempo promedio de resolución | Días promedio desde apertura hasta cierre |
| Reclamos por SKU | Cantidad y monto agrupado por producto |
| Reclamos por proveedor | Para identificar proveedores problemáticos |

### Análisis por SKU (para detectar problemas de calidad)

```
RECLAMOS POR SKU — últimos 6 meses

  SKU 218 (Compresor Xiaomi)        12 reclamos   $ 95.000  ⚠ tasa 3,2%
    └─ 8 a Mercado Libre   |   4 a Xiaomi (garantía)
  SKU AU004B (JBL Tune 520BT)        8 reclamos   $ 62.000     tasa 1,1%
    └─ 7 a Mercado Libre   |   1 a Aodeli-Kaveh (defecto)
  ...
```

---

## Vistas en la app

### Vista 1: Lista de reclamos abiertos

Lista filtrable por destinatario, estado, antigüedad. Priorización por antigüedad. Cada renglón muestra fecha, destinatario, venta de origen, SKU, monto, estado, acciones.

### Vista 2: Cuentas corrientes

El cuadro de arriba con totales por destinatario (ML + cada proveedor con reclamos).

### Vista 3: Análisis por SKU / proveedor

Detección de productos con tasa anormal y proveedores con problemas recurrentes.

### Vista 4: Detalle del reclamo

Toda la info + historial de cambios + acciones + link a venta original + link al recupero (cuando se cobra).

---

## Decisiones operativas que habilita

| Decisión | Cuándo |
|----------|--------|
| Insistir con un reclamo | Después de X días sin respuesta |
| Cambiar destinatario | Si descubrís que la causa real era otra (de ML a Proveedor o viceversa) |
| Dar por perdido | Cuando ya no vale la pena insistir |
| Reportar lote defectuoso al proveedor | Cuando un SKU tiene racha de defectos del mismo lote |
| Cambiar de proveedor | Cuando un proveedor tiene tasa de defectos sistemáticamente alta |
| Subir precio del SKU problemático | Para cubrir el % esperado de pérdida |

---

## Reglas duras

1. **Un reclamo siempre está asociado a una venta y un destinatario.** No hay reclamos sueltos.
2. **Un reclamo tiene un solo destinatario en su ciclo de vida.** Si se cambia, queda registro del cambio.
3. **No se pueden hacer dos reclamos abiertos por la misma venta** simultáneamente.
4. **Los reclamos perdidos se imputan al P&L como "Pérdida por mercadería no vendible"** (rubro propio, NO es gasto operativo).
5. **El cobro / recupero se imputa al P&L del mes en que se cobra**, no del mes del reclamo original.
6. **Producto reemplazo entra al stock como lote nuevo** con costo igual al lote original (sin ganancia/pérdida artificial).
7. **Nota de crédito reduce el saldo pendiente con el proveedor** (AP), no entra como ingreso.
8. **Mes cerrado es inmutable**: movimientos posteriores se imputan al mes en curso.

---

## Pendientes y TBD

- **Catálogo de proveedores** con datos de contacto y políticas de garantía documentadas (cuánto tiempo, qué cubren, cómo reclamar).
- **Plazos de seguimiento** automatizados: alertar cuando un reclamo lleva > X días sin avanzar (sugerencia inicial: 7 días para "Abierto", 15 para "En revisión").
- **Política de "dar por perdido"**: ¿automático después de Y días, o siempre manual? Sugerencia: siempre manual.
- **Integración con API de claims de ML**: hoy se gestiona en la interfaz de ML. Para evolución futura, automatizar.
- **Subir evidencia desde ADARA**: centralizar fotos/docs en ADARA en vez de solo en ML/proveedor.
- **Tasa esperada por SKU**: con histórico se podrá alertar cuando un SKU se desvía de su patrón normal.
- **Reclamos en bulk al proveedor**: cuando hay un lote defectuoso, agrupar varios reclamos individuales en uno solo de mayor monto.

---

## Documentos relacionados

- `ADARA-FLUJO-OPERATIVO.md` — flujo diario donde se generan los reclamos
- `ADARA-CONCILIACION-BANCARIA.md` — matching del cobro del reclamo en MP/banco
- `ADARA-PNL.md` — pérdida por reclamos como rubro propio (NO gasto operativo)
- `ADARA-PATRIMONIAL.md` — saldo pendiente con ML y proveedores como parte de AR
- `ADARA-LINEAS-NEGOCIO.md` — imputación de pérdidas/recuperos a la línea
- `ADARA-STOCK.md` — devolución no vendible no vuelve a stock + lote nuevo por reemplazo
- `ADARA-COMPRAS-IMPORTACIONES.md` — notas de crédito de proveedores como reducción de AP
- `ADARA-GASTOS.md` — diferenciación: pérdida por mercadería ≠ gasto operativo
- `ADARA-DECISIONES.md` — reglas consolidadas
