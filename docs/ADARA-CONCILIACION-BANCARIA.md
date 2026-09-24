# ADARA — Conciliación Bancaria (universal)

Última actualización: 16 Junio 2026 (bonis abril cerradas **158/167** vía backfill `shipment_id` mar+ene/feb; **9 residuales** = envíos 2025; `cobro_venta` sin conciliar 500→342. Pantalla con **filtros por columna estilo Excel** + **selector de mes** + toggle Movimientos/Ventas/Ambas; repintado parcial del tbody. Apéndice operativo: `/mp/conciliar-bonificaciones` se dispara desde la **consola del navegador** (dry→write). Estado real de `shipment_id` por mes: ene 100%, feb 100%, mar 3.707/3.998, abr 3.101/3.134, **may 1.536/3.166 (parcial)**, jun 100%. **Solapamiento** Conciliación/Ventas ML/Cuadre anotado para revisar. Diagnóstico abril: 1.416 ventas/$102M sin vínculo venta↔cobro — no falta plata, falta completar conciliación; `periodo_cobro` no confiable, usar `fecha_cobro`. Ver CHANGELOG 16/6) · 13 Junio 2026 (Cuadre + saldos por cuenta + puente bonificaciones Flex + fix token ML)

> 📌 Doc de dominio del rediseño. Las **reglas duras** viven en `ADARA-DECISIONES.md` (CB1–CB12); el **schema** (tablas y vistas) en `ADARA-SCHEMA.md` (capa 4). Este doc no las repite: explica el modelo, la pantalla y la operatoria.
>
> El manejo del v21 (buzón MP `conciliado=false`) está en `ADARA-MOV-SIN-CONCILIAR.md` — referencia técnica únicamente.

---

## Qué es

Conciliación **universal** (Principio 1, CB1): toda venta, gasto y compra debe cerrar contra un movimiento real (banco / MP / caja / tarjeta), o vivir en **AR** (por cobrar) / **AP** (por pagar) hasta que cierre. El extracto es la fuente de verdad del cobro/pago; el devengado (P&L) no depende de la conciliación (Principio 2).

---

## Modelo de datos

- **`movimientos`** — extracto unificado de todas las cuentas. Inmutable (CB2): refleja eventos reales del banco/MP. La carga manual se limita a caja (CB11).
- **`vinculos`** — relación N:N polimórfica `movimiento ↔ operación`: `movimiento_id`, `op_tipo` ∈ {`venta`,`compra`,`gasto`,`reclamo`,`transferencia`,`ajuste`}, `op_id` (sin FK formal), `monto`.

### Convención de signo (CRÍTICA)

`vinculos.monto` = **magnitud positiva imputada**, nunca el signo del movimiento. Un pago de $242.000 (movimiento −242.000) genera un vínculo de **+242.000**. Todas las vistas (`v_movimientos_estado`, `v_gastos_ap`, `v_compras_ap`, `v_ventas_ar`) suman magnitudes positivas y restan de un valor también positivo. Romper esta convención desincroniza la conciliación entera (ver Historial de errores).

### Estados (`v_movimientos_estado`)

Por movimiento: `saldo_pendiente = abs(monto) − Σ vinculos.monto`, y `estado` derivado:

| estado | condición |
|---|---|
| `auto` | `conciliado_auto = true` (ruido clasificado por el importador) |
| `conciliado` | tiene vínculos y `abs(saldo_pendiente) < 0.02` (CB3) |
| `parcial` | tiene vínculos pero el saldo no cierra todavía |
| `pendiente` | sin vínculos |

---

## La pantalla (`public/js/screens/conciliacion.js`)

Muestra **todos los movimientos** de la cuenta (principio #1: nada escondido).

- **Filtro de cuenta** + **pills de estado**: *Por conciliar* (pendiente + parcial, default) · *Conciliados* · *Auto* · *Todos*.
- **KPIs**: Por conciliar (cantidad) · Monto a conciliar (solo pagos accionables) · Conciliados.
- **Acción por fila**, según el caso:
  - **Pago accionable** (salida que no espera venta): **✨** acepta en 1 clic la sugerencia cuando hay un gasto/compra cuyo saldo coincide exacto; **Vincular** abre el picker con gastos y compras juntos, ordenados por cercanía de monto (imputa `min(saldoMov, saldoOp)`).
  - **Atado a venta** (entrada, o categoría `cobro_venta`/`devolucion`): etiqueta **"espera venta"** — se concilia con el sync de ML (pendiente), no a mano. Igual ofrece **Transf. interna**, porque una entrada puede ser una transferencia y no un cobro.
  - **Conciliado / parcial**: chips de los vínculos con **✕** para desvincular.
- **Volumen**: renderiza miles de filas (igual que Movimientos). Si molesta el rendimiento, se agrega paginación.

---

## Endpoints (`server.js`, Express — A16)

| Método | Ruta | Cuerpo | Efecto |
|---|---|---|---|
| POST | `/vincular` | `{movimiento_id, op_tipo, op_id, monto>0}` | upsert en `vinculos` (`on_conflict movimiento_id,op_tipo,op_id`) |
| DELETE | `/vincular/:id` | — | borra el vínculo |
| POST | `/transferencia-interna` | `{movimiento_a, movimiento_b?}` | empareja dos movimientos (o marca uno sin par) |
| POST | `/mp/conciliar-bonificaciones` | `{desde, hasta, limit?, write?, inspect?}` | vincula bonificaciones de envío Flex a su venta (vía payment→shipment). Ver sección propia. |

---

## Multi-vínculo — un pago que cubre varios gastos (CB4)

Se resuelve **incremental**, sin UI especial: vinculás un gasto → el movimiento queda *parcial* con el saldo restante → vinculás otro → cierra. Y a la inversa: un gasto pagado en N movimientos acumula vínculos parciales hasta saldar. La relación N:N sale natural del modelo.

---

## Transferencias internas (CB8)

Movimientos entre cuentas propias (HomeBanking misma CUIT, MP ↔ banco) que **no suman al P&L**.

- **Con par** (las dos patas están en el sistema): se emparejan con **dos vínculos cruzados** `op_tipo='transferencia'`, `op_id = el OTRO movimiento`, `monto = magnitud de cada lado`. Cada pata concilia contra la otra. Chip: `↔ Transf. mov #X`.
- **Sin par** (la contrapartida no se trackea en ADARA): un vínculo **a sí mismo** (`op_id = el propio movimiento`). Queda conciliado solo. Chip: `Transf. interna (sin par)`.
- Los vínculos `op_tipo='transferencia'` **no entran** en `v_ventas_ar` / `v_compras_ap` / `v_gastos_ap` (que solo suman `venta`/`compra`/`gasto`) → no tocan P&L ni AR/AP.
- **FCI suscripción/rescate** ya entran como `conciliado_auto` desde el importador (no requieren emparejado manual).

> Nota de implementación: se descartó la idea original de una tabla agrupadora `transferencias` (ver SCHEMA, TBD viejo). El par de vínculos cruzados alcanza y evita una tabla extra.

---

## Auto-conciliación del ruido

El importador (Supervielle / MP) marca `conciliado_auto = true` lo que no tiene contrapartida operativa: impuestos, comisiones bancarias, intereses / rendimientos, FCI. Esos movimientos aparecen como **Auto** y no requieren acción. Detalle del clasificador en `ADARA-MOVIMIENTOS.md`.

---

## Saldos por cuenta — `v_saldo_cuenta` (13/06/2026)

Arranque de fondos **desde fecha de corte** (no se reconstruye desde el 31/12). El saldo de cada cuenta = **ancla** (saldo conocido al corte) **+ Σ movimientos posteriores al corte**.

- **Ancla**: fila en `saldos_iniciales` con `linea_id IS NULL` (las cuentas/caja **no** se dividen por línea → el ancla es a nivel cuenta; `saldos_iniciales.linea_id` pasó a NULLABLE). El corte por defecto es el cierre del 12/06/2026.
- **Vista `v_saldo_cuenta`** (`cuenta_id, codigo, nombre, tipo, moneda, ancla, fecha_corte, movimientos_post, saldo`): expone el saldo en vivo de cada cuenta. Detalle en `ADARA-SCHEMA.md`.
- **MP**: se carga como ancla solo el **dinero disponible**, NO el "por cobrar" (eso es AR que entra recién al liberarse — evita inflar el saldo).
- Pantalla **`#saldos`** (`public/js/screens/saldos.js`): tarjeta por cuenta con saldo, desglose ancla/movimientos y form para cargar/editar el ancla; KPIs Total ARS/USD.
- Cuenta nueva `santi_financiera` (CC con financiera en EEUU, tipo `banco`, moneda `USD`).

## Pantalla Cuadre (`public/js/screens/cuadre.js`, `#cuadre`)

Tablero de control de la conciliación (no concilia: orienta). Por cuenta: **semáforo de % conciliado por monto**, **cola accionable** ordenada por monto desc (botón ✨ aplica la sugerencia si hay match exacto compra/gasto, o link "Conciliar →" a `#conciliacion`), y bloque "espera motor" para cobros/devoluciones de venta. Reusa los criterios de `conciliacion.js`.

## Bonificaciones de envío Flex — puente payment→shipment (13/06/2026)

Las **bonificaciones de envío Flex** (ML reintegra parte del costo de envío Flex) llegan a `movimientos` como `categoria='cobro_venta'`, descripción "Bonificación por envío…". **Son ventas: van vinculadas a su venta**, no auto-conciliadas.

El puente NO está en el settlement report (ahí vienen como `CASHBACK`/`CASHBACK_CANCEL` con `ORDER_ID`/`SHIPPING_ID`/`PACK_ID` **vacíos** — descartado tras verificarlo sobre 572 filas). El puente está en el **payment crudo de MP**:

```
movimiento bonificación
  → SOURCE_ID (campo 2 de referencia_externa, formato fecha|ID|monto|saldo) = payment_id
  → GET /v1/payments/{id}
  → point_of_interaction.transaction_data.reference_id  (con reference_type='shipment')
  → ventas_ml.shipment_id
  → venta → vínculo op_tipo='venta_ml'
```

`ventas_ml.shipment_id` (poblado en el sync desde `o.shipping.id`) es la pieza clave.

**Endpoint `POST /mp/conciliar-bonificaciones`** — params `{desde, hasta, limit?, write?, inspect?}`:
- Trae las bonificaciones pendientes del rango, consulta el payment de cada una (concurrencia 8), extrae el shipment, lo cruza con `ventas_ml.shipment_id` y arma los vínculos.
- `write:false` (default) = dry-run (reporta `cruzadas`, `sin_payment`, `sin_shipment_en_payment`, `con_shipment_pero_sin_venta`). `write:true` escribe (idempotente, `on_conflict movimiento_id,op_tipo,op_id`).
- `limit:N` acota la muestra para probar rápido. `inspect:true` devuelve el payment crudo de 3 bonificaciones (debug).

**Estado abril 2026:** 1.375 bonificaciones; 1.208 vinculadas (519 escritas en la sesión del 13/06, $3,67M), **167 pendientes**. Las 167 son bonificaciones de abril por **envíos de marzo**: `shipment_id` solo está backfilleado en abr/may/jun (ene/feb/mar = 0). Se recuperan re-sincronizando ML de los meses previos (`POST /ml/sync {desde,hasta}`) y re-corriendo el `write` (idempotente).

> **A verificar:** 689 bonificaciones de abril ya tenían vínculo antes de esta corrida (origen no confirmado; chequear que no haya doble conteo).

## Integración ML/MP — persistencia del token (fix 13/06/2026)

El backend guarda el token de ML en la tabla **`workspace_config`** (`ml_access_token`, `ml_refresh_token`, `ml_token_expires`). Era la causa de "ML se desconecta solo": la tabla **no existía**, `saveMLToken` fallaba en silencio, el token vivía solo en memoria y **cada redeploy de Railway lo borraba**. Fixes aplicados:

1. **`workspace_config` creada** (RLS on, sin policies → solo backend service_role). El token ahora persiste y sobrevive redeploys.
2. **`refreshML` single-flight** (`_mlRefreshing`): evita que las llamadas paralelas del sync refresquen en simultáneo con el mismo refresh_token de un solo uso y se invaliden. `mlGet` reintenta una vez ante 401.
3. **`scope=offline_access read write`** en la URL de `/ml/auth` (para obtener `refresh_token`).

> **Pendiente token:** pese al scope, ML sigue sin devolver `refresh_token` (`tiene_refresh=false`) → el access vence cada 6h y no renueva solo. Falta **habilitar `offline_access` en la app del panel developers.mercadolibre.com.ar**. Hasta entonces, reconectar a mano vía `/ml/auth` antes de operar con ML/MP.

---

## Pendientes

- **Sync ML auto-concilia liquidaciones**: las ~4.800 liquidaciones de MP se concilian contra ventas cuando se construya el sync de ML (`op_tipo='venta'`, con comisión + impuestos + financing fees modelados como `monto < bruto`). Hasta entonces quedan **"espera venta"**. (TBD "mini-capa ML" en `ADARA-SCHEMA.md`.)
- **CB9 — desglose de tarjeta**: el pago `Cobranzas ResumenVisa` del extracto debe ofrecer desglose en N gastos individuales con su línea. No implementado.
- **Multi-currency en pagos** (`v_compras_ap` asume ARS): pendiente si se paga una compra USD desde `caja_usd`.

---

## ⚠ Guardrails técnicos

- **`vinculos.monto` SIEMPRE positivo** (magnitud). Es la convención sobre la que se apoyan las 4 vistas; romperla desincroniza todo.
- **`v_movimientos_estado` usa `abs(monto) − Σ`.** No volver a `monto` signed.
- **Vínculos `op_tipo='transferencia'` no se suman en AR / AP / P&L.**
- **Paginación obligatoria** en cualquier GET que pueda superar 1.000 filas (Supabase corta la respuesta REST en 1.000).
- **`op_id` polimórfico sin FK**: la app valida que exista en la tabla del `op_tipo` correspondiente.
- Si en el futuro hay un `recalcular-balance`, no debe pisar `conciliado_auto` ni registros descartados.

---

## Historial de errores

| Fecha | Síntoma | Causa | Fix |
|---|---|---|---|
| 29/05/2026 | `v_movimientos_estado` iba a conciliar con el saldo invertido | La vista original calculaba `saldo = monto − Σ` (signed), pero `v_gastos_ap` / `v_compras_ap` / `v_ventas_ar` y `POST /gastos` imputan `vinculos.monto` en **positivo**. Una salida de −$242.000 con vínculo +$242.000 daba saldo −$484.000 en vez de 0. | Se recreó la vista con `abs(monto) − Σ` y se fijó la convención **"`vinculos.monto` = magnitud positiva"**. La vista nunca había llegado a usarse (`vinculos` estaba vacía), así que no hubo datos que migrar. |
| 13/06/2026 | "ML se desconecta solo" en cada redeploy | La tabla `workspace_config` (donde se persiste el token ML) no existía → `saveMLToken` fallaba en silencio → token solo en memoria → cada redeploy lo borraba. Además, las 10 llamadas paralelas del sync refrescaban el token a la vez y se invalidaban. | Se creó `workspace_config` (RLS on, solo backend); `refreshML` single-flight + reintento 401 en `mlGet`; scope `offline_access` en `/ml/auth`. Queda pendiente habilitar `offline_access` en el panel de la app de ML para el `refresh_token`. |
