import { sbGet } from '../core/sb.js';

// ── Pantalla: Ventas ML (control diario/mensual + conciliación) ────────
// Lista las ventas de ML por día o por mes con su detalle real y las cruza
// contra los cobros del extracto de MP. Concilia (vincula venta ↔ cobro) las
// ventas cuyo cobro cierra con el por_cobrar. Soporta el caso de bonificación
// de envío: ML liquida la venta en DOS movimientos (pago principal + bonificación
// de envío, con número de operación distinto y monto neto). Cuando el pago solo
// no llega al por_cobrar, se busca una "Bonificación por envío" disponible cuyo
// monto complete la diferencia y se vinculan ambos movimientos a la venta.
// Las bonificaciones del mismo monto son intercambiables: cada una se usa una
// sola vez (se excluyen las ya vinculadas). Cruce del pago por mp_payment_id;
// vínculo op_tipo='venta_ml', op_id=ventas_ml.id, monto = monto de cada movimiento.

let VENTAS = [];
let COBROS_BY_REF = {};      // REFERENCE_ID del extracto -> movimiento (cobro_venta)
let COBROS_BY_ID = {};       // movimiento.id -> movimiento (cobro_venta) — para el detalle de operaciones
let BONIFS = [];             // movimientos cobro_venta de "bonificación por envío"
let SHIP_BY_VENTA = {};      // clave (v:venta_id | o:ml_order_id | p:pack_id) -> { liqIds:Set, retEnvio } — envío colecta O8 (retenciones SETTLEMENT_SHIPPING)
let VINC_BY_VENTA = {};      // ventas_ml.id -> [vínculos] (op_tipo='venta_ml')
let VINC_MOV_USADOS = new Set(); // movimiento_id ya vinculados a alguna venta
let DIAS = [];
let MESES = [];
let MODO = 'dia';            // dia | mes
let FECHA = '';              // YYYY-MM-DD (modo día)
let MES = '';                // YYYY-MM (modo mes)
let FILTRO = 'todas';        // todas | por_cobrar | cobradas | conciliadas | canceladas | devueltas

// ── Solapas de la pantalla ──────────────────────────────────────────────

// ── Devoluciones (solo lectura, v1) ─────────────────────────────────────
// Movimientos del extracto que son plata de devolución/cancelación y todavía
// NO están enganchados a ninguna venta. El extracto no trae el N° de orden,
// así que el enganche se sugiere por MONTO ESPEJO (el importe de la devolución
// refleja el de la venta) y se resalta cuando la venta tiene un reclamo (claim)
// detectado. Esta v1 solo muestra y sugiere — no escribe nada en la base.
const DEV_CATS = ['devolucion', 'venta_cancelada', 'cargo_envio_devolucion'];
const DEV_CAT_LBL = { devolucion: 'Devolución', venta_cancelada: 'Venta cancelada', cargo_envio_devolucion: 'Cargo envío devol.' };
let DEVOLS = [];             // movimientos de devolución (normalizados)
let DEV_FUENTE = '';         // 'movimientos' | 'movimientos_mp' | '' (de qué cajón se leyeron)
let MOV_VINCULADOS = new Set(); // movimiento_id de devolución ya enganchados a una venta
let DEV_LINK_BY_MOV = new Map(); // movimiento_id de devolución -> vínculo (para mostrar venta + deshacer)
let DEV_TOL = 1.00;          // tolerancia $ para sugerir match por monto espejo (legacy, sin uso en v2)

// ── Devoluciones v2: bundles resueltos por op_id (endpoint /devoluciones/resolver) ──
let DEV_BUNDLES = null;      // [{op_id, neto, capa, estado, venta, lineas[]}] | null = no cargado
let DEV_RESUMEN = null;      // {total, pendiente, parcial, vinculada, agregado, revision}

const TOL = 0.02;
const r2 = n => Math.round((Number(n) || 0) * 100) / 100;
const money = n => '$ ' + Math.abs(Number(n) || 0).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ddmm = f => `${(f || '').slice(8, 10)}/${(f || '').slice(5, 7)}`;
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const hoyISO = () => new Date().toISOString().slice(0, 10);
const mesDe = f => (f || '').slice(0, 7);
const fechaLarga = f => {
  if (!f) return '—';
  const [y, m, d] = f.split('-');
  return new Date(+y, +m - 1, +d).toLocaleDateString('es-AR', { weekday: 'long', day: 'numeric', month: 'long' });
};
const mesLargo = ym => {
  if (!ym) return '—';
  const [y, m] = ym.split('-');
  return new Date(+y, +m - 1, 1).toLocaleDateString('es-AR', { month: 'long', year: 'numeric' });
};

function paymentIds(v) {
  const ids = [];
  if (v.mp_payment_id) ids.push(String(v.mp_payment_id).trim());
  if (v.mp_payment_ids) String(v.mp_payment_ids).split(',').forEach(x => { const t = x.trim(); if (t) ids.push(t); });
  return [...new Set(ids)];
}
// Envío colecta a cargo del comprador (caso O8). La venta genera DOS liquidaciones en
// el extracto: el producto (con el mp_payment_id de la venta) y el pago del envío (con
// SOURCE_ID propio, que MP no asocia a la venta). La fila SETTLEMENT_SHIPPING de
// `retenciones` es el puente venta ↔ liquidación de envío. Se resuelve con PRIORIDAD
// venta_id → ml_order_id → pack_id: una sola clave por venta, NUNCA se suman las tres
// (así no se cuenta dos veces en un pack cuya retención cuelgue solo del pack_id).
function shipDe(v) {
  return SHIP_BY_VENTA[`v:${v.id}`]
    || (v.ml_order_id ? SHIP_BY_VENTA[`o:${String(v.ml_order_id).trim()}`] : null)
    || (v.pack_id ? SHIP_BY_VENTA[`p:${String(v.pack_id).trim()}`] : null)
    || null;
}
// Retención de IIBB sobre el envío (≤ 0). 0 si la venta no tiene envío colecta.
const retEnvioDe = v => { const s = shipDe(v); return s ? (Number(s.retEnvio) || 0) : 0; };
// Objetivo de cierre de la venta. Flex/sueltas: retEnvio = 0 → objetivo = por_cobrar (sin
// cambios). Colecta con envío del comprador: objetivo = por_cobrar + retEnvio (retEnvio ≤ 0),
// que es exactamente el cash que entra (producto + envío). Identidad O8.
const objetivoDe = v => r2((Number(v.por_cobrar) || 0) + retEnvioDe(v));

function matchCobros(v) {
  const vistos = new Set();
  const cobros = [];
  for (const id of paymentIds(v)) {
    const c = COBROS_BY_REF[id];
    if (c && !vistos.has(c.id)) { vistos.add(c.id); cobros.push(c); }
  }
  // Cobro del envío (colecta O8): su mp_source_id (= liqId) está embebido en la
  // referencia del movimiento del envío, así que ya vive en COBROS_BY_REF. Se suma
  // como un cobro más; el resto del flujo (conciliable, lote, detalle) lo trata igual.
  const ship = shipDe(v);
  if (ship) for (const liq of ship.liqIds) {
    const c = COBROS_BY_REF[liq];
    if (c && !vistos.has(c.id)) { vistos.add(c.id); cobros.push(c); }
  }
  return cobros;
}
const estaConciliada = v => !!(VINC_BY_VENTA[v.id] && VINC_BY_VENTA[v.id].length);

// Busca una bonificación de envío disponible (no usada) cuyo monto sea ≈ falta.
function buscarBonif(falta, usados) {
  if (falta <= TOL) return null;
  return BONIFS.find(b => !usados.has(b.id) && Math.abs((Number(b.monto) || 0) - falta) < TOL) || null;
}

// Asigna, de forma greedy, una bonificación a cada venta visible que la necesite
// (pago principal único + diferencia positiva que matchee una bonificación libre).
// Devuelve { ventaId: movimientoBonif }. Reserva cada bonificación una sola vez.
function asignarBonifs(ventas) {
  const usados = new Set(VINC_MOV_USADOS);
  const map = {};
  for (const v of ventas) {
    if (estaConciliada(v) || v.ml_status === 'cancelled' || v.devuelta) continue;
    const cb = matchCobros(v);
    if (!cb.length) continue;
    const sum = cb.reduce((s, c) => s + (Number(c.monto) || 0), 0);
    const falta = r2(objetivoDe(v) - sum);
    if (Math.abs(falta) < TOL || falta < 0) continue;
    const b = buscarBonif(falta, usados);
    if (b) { map[v.id] = b; usados.add(b.id); }
  }
  return map;
}

// Conciliable: pago principal único que cierra el por_cobrar, solo o con su bonificación.
function conciliable(v, bonifMap) {
  if (estaConciliada(v) || v.ml_status === 'cancelled' || v.devuelta) return false;
  const cb = matchCobros(v);
  if (!cb.length) return false;
  const sum = cb.reduce((s, c) => s + (Number(c.monto) || 0), 0);
  const pc = objetivoDe(v);
  if (Math.abs(sum - pc) < TOL) return true;             // cierra con los cobros
  const b = bonifMap[v.id];
  if (b && Math.abs(sum + (Number(b.monto) || 0) - pc) < TOL) return true; // cobros + bonificación
  return false;
}

function clase(v) {
  if (v.ml_status === 'cancelled') return 'canceladas';
  if (v.devuelta) return 'devueltas';
  if (estaConciliada(v)) return 'conciliadas';
  if (matchCobros(v).length > 0) return 'cobradas';
  return 'por_cobrar';
}
const ESTADO_LBL = {
  canceladas:  { txt: 'Cancelada',  cls: 'canc' },
  devueltas:   { txt: 'Devuelta',   cls: 'dev' },
  conciliadas: { txt: 'Conciliada', cls: 'conc' },
  cobradas:    { txt: 'Cobrada',    cls: 'cob' },
  por_cobrar:  { txt: 'Por cobrar', cls: '' },
};

export async function loadVentasML() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando ventas de ML…</div>`;
  DEV_BUNDLES = null;   // invalidar cache de bundles de devolución → se recarga al abrir la solapa
  try {
    VENTAS = await sbGet('ventas_ml', 'order=fecha.asc,hora_venta.asc');
    const cobros = await sbGet('movimientos', 'categoria=eq.cobro_venta&order=fecha.asc');
    COBROS_BY_REF = {};
    COBROS_BY_ID = {};
    BONIFS = [];
    for (const c of cobros) {
      const ref = String(c.referencia_externa || '').split('|')[1];
      if (ref) COBROS_BY_REF[ref.trim()] = c;
      COBROS_BY_ID[c.id] = c;
      if (/bonific/i.test(c.descripcion || '')) BONIFS.push(c);
    }
    // Envío colecta (O8): índice de la retención de IIBB sobre la liquidación del envío.
    // Cada fila se indexa por venta_id, ml_order_id y pack_id; la clave guarda los liqIds
    // (mp_source_id del envío, para encontrar su cobro en COBROS_BY_REF) y la suma de la
    // retención. La venta usa UNA sola clave al consultar (shipDe), nunca las tres.
    SHIP_BY_VENTA = {};
    try {
      const retEnvio = await sbGet('retenciones',
        'transaction_type=eq.SETTLEMENT_SHIPPING&select=venta_id,order_id,pack_id,mp_source_id,monto');
      const addShip = (pref, val, row) => {
        if (val == null) return;
        const t = String(val).trim();
        if (!t) return;
        const k = `${pref}:${t}`;
        const e = SHIP_BY_VENTA[k] || (SHIP_BY_VENTA[k] = { liqIds: new Set(), retEnvio: 0 });
        if (row.mp_source_id) e.liqIds.add(String(row.mp_source_id).trim());
        e.retEnvio = r2(e.retEnvio + (Number(row.monto) || 0));
      };
      for (const row of retEnvio) {
        addShip('v', row.venta_id, row);
        addShip('o', row.order_id, row);
        addShip('p', row.pack_id, row);
      }
    } catch (e) {
      // Si `retenciones` no estuviera disponible, la conciliación sigue igual que antes:
      // solo las colecta con envío del comprador quedarían en "revisar", como hasta ahora.
      console.warn('No se pudieron leer las retenciones de envío:', e.message);
      SHIP_BY_VENTA = {};
    }
    // Las devoluciones se cargan ANTES de los vínculos para poder separar, al
    // recorrer los vínculos, los que son de cobro (solapa Ventas) de los que son
    // de devolución (esta solapa). Así la solapa Ventas no ve los de devolución.
    await cargarDevoluciones();
    const DEV_IDS = new Set(DEVOLS.map(d => d.id));

    const vinc = await sbGet('vinculos', 'op_tipo=eq.venta_ml');
    VINC_BY_VENTA = {};
    VINC_MOV_USADOS = new Set();
    MOV_VINCULADOS = new Set();
    DEV_LINK_BY_MOV = new Map();
    for (const v of vinc) {
      if (DEV_IDS.has(v.movimiento_id)) {
        // Vínculo de una DEVOLUCIÓN: no entra en la lógica de cobro de Ventas.
        MOV_VINCULADOS.add(v.movimiento_id);
        DEV_LINK_BY_MOV.set(v.movimiento_id, v);
      } else {
        // Vínculo de COBRO (lo que usa la solapa Ventas, igual que siempre).
        (VINC_BY_VENTA[v.op_id] = VINC_BY_VENTA[v.op_id] || []).push(v);
        VINC_MOV_USADOS.add(v.movimiento_id);
      }
    }
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudieron cargar las ventas: ${esc(e.message)}</div>`;
    return;
  }
  DIAS = [...new Set(VENTAS.map(v => v.fecha).filter(Boolean))].sort();
  MESES = [...new Set(VENTAS.map(v => mesDe(v.fecha)).filter(Boolean))].sort();
  if (!FECHA || !DIAS.includes(FECHA)) FECHA = DIAS.length ? DIAS[DIAS.length - 1] : '';
  if (!MES || !MESES.includes(MES)) MES = MESES.length ? MESES[MESES.length - 1] : '';
  try { await cargarBundlesDevol(); }
  catch (e) { DEV_BUNDLES = null; DEV_RESUMEN = null; console.warn('bundles devoluciones:', e.message); }
  inyectarEstilo();
  render();
}

// Lee los movimientos de plata de devolución/cancelación. Primero del cajón
// nuevo (`movimientos`); si ahí no hay ninguno, prueba el cajón viejo del
// extracto (`movimientos_mp`) y lo deja marcado. Devuelve filas normalizadas:
// { id, fecha, monto, categoria, descripcion }.
async function cargarDevoluciones() {
  DEVOLS = [];
  DEV_FUENTE = '';
  const inFilter = `(${DEV_CATS.join(',')})`;
  // 1) Cajón nuevo
  try {
    const nuevos = await sbGet('movimientos', `categoria=in.${inFilter}&order=fecha.asc`);
    if (nuevos.length) {
      DEV_FUENTE = 'movimientos';
      DEVOLS = nuevos.map(m => ({
        id: m.id, fecha: m.fecha, monto: Number(m.monto) || 0,
        categoria: m.categoria, descripcion: m.descripcion || '',
        referencia: m.referencia_externa || ''
      }));
      return;
    }
  } catch (e) { console.warn('Devol (movimientos):', e.message); }
  // 2) Cajón viejo (extracto MP), por si todavía no se migraron
  try {
    const viejos = await sbGet('movimientos_mp', `categoria=in.${inFilter}&order=fecha.asc`);
    if (viejos.length) {
      DEV_FUENTE = 'movimientos_mp';
      DEVOLS = viejos.map(m => ({
        id: m.id, fecha: m.fecha, monto: Number(m.monto_neto ?? m.monto) || 0,
        categoria: m.categoria, descripcion: m.descripcion || m.tipo_operacion || '',
        referencia: m.referencia_mp || ''
      }));
    }
  } catch (e) { console.warn('Devol (movimientos_mp):', e.message); }
}

function paso(delta) {
  if (MODO === 'dia') {
    if (!DIAS.length) return;
    let i = DIAS.indexOf(FECHA); if (i === -1) i = DIAS.length - 1;
    FECHA = DIAS[Math.min(DIAS.length - 1, Math.max(0, i + delta))];
  } else {
    if (!MESES.length) return;
    let i = MESES.indexOf(MES); if (i === -1) i = MESES.length - 1;
    MES = MESES[Math.min(MESES.length - 1, Math.max(0, i + delta))];
  }
  render();
}

// Render principal de la pantalla Ventas ML (incluye, en el chip "Devueltas",
// la herramienta de conciliación de devoluciones por bundle).
function render() {
  return renderVentas();
}

function renderVentas() {
  const root = document.getElementById('app-screens');
  const hayVentas = VENTAS.length > 0;
  const esMes = MODO === 'mes';
  const base = !hayVentas ? [] : conjuntoActual();

  const BONIF_MAP = asignarBonifs(base);
  const conciliablesN = base.filter(v => conciliable(v, BONIF_MAP)).length;

  const cont = { todas: base.length, por_cobrar: 0, cobradas: 0, conciliadas: 0, canceladas: 0, devueltas: 0 };
  base.forEach(v => { cont[clase(v)]++; });

  const esDevueltas = FILTRO === 'devueltas';
  const perDev = bundlesPeriodo();   // bundles de devolución del período (por fecha de línea del AS)
  if (perDev) cont.devueltas = perDev.filter(b => b.estado === 'pendiente' || b.estado === 'parcial').length;

  const visibles = FILTRO === 'todas' ? base : base.filter(v => clase(v) === FILTRO);
  const totCobrar = base.reduce((s, v) => s + (Number(v.por_cobrar) || 0), 0);

  const modoHTML = `
    <div class="vml-modo">
      <button class="${!esMes ? 'active' : ''}" data-modo="dia">Día</button>
      <button class="${esMes ? 'active' : ''}" data-modo="mes">Mes</button>
    </div>`;

  let navHTML = '';
  if (hayVentas && esMes) {
    const i = MESES.indexOf(MES);
    navHTML = `
      <button class="btn btn-ghost" id="vml-prev" ${i <= 0 ? 'disabled' : ''}>‹ Mes anterior</button>
      <input type="month" class="input" id="vml-mes" value="${MES}" style="width:auto">
      <button class="btn btn-ghost" id="vml-next" ${i >= MESES.length - 1 ? 'disabled' : ''}>Mes siguiente ›</button>
      <span class="vml-fechalbl">${esc(mesLargo(MES))}</span>`;
  } else if (hayVentas) {
    const i = DIAS.indexOf(FECHA);
    navHTML = `
      <button class="btn btn-ghost" id="vml-prev" ${i <= 0 ? 'disabled' : ''}>‹ Día anterior</button>
      <input type="date" class="input" id="vml-fecha" value="${FECHA}" style="width:auto">
      <button class="btn btn-ghost" id="vml-next" ${i >= DIAS.length - 1 ? 'disabled' : ''}>Día siguiente ›</button>
      <span class="vml-fechalbl">${esc(fechaLarga(FECHA))}</span>`;
  }

  const pill = (val, label) =>
    `<button class="pill ${FILTRO === val ? 'active' : ''}" data-f="${val}">${label} <span class="num">${cont[val]}</span></button>`;
  const pills = hayVentas ? `<div class="pills" style="margin-top:12px">
      ${pill('todas', 'Todas')}
      ${pill('por_cobrar', 'Por cobrar')}
      ${pill('cobradas', 'Cobradas')}
      ${pill('conciliadas', 'Conciliadas')}
      ${pill('canceladas', 'Canceladas')}
      ${pill('devueltas', 'Devueltas')}
    </div>` : '';

  const lblPeriodo = esMes ? 'mes' : 'día';
  const vacioTxt = esMes ? `el mes de ${esc(mesLargo(MES))}` : `el ${esc(ddmm(FECHA))}`;

  root.innerHTML = `
    <div class="vml-bar">
      <button class="btn btn-primary" id="vml-sync">⟳ Sincronizar ventas</button>
      ${hayVentas ? modoHTML : ''}
      ${navHTML}
      ${(!esDevueltas && conciliablesN > 0) ? `<button class="btn btn-conc" id="vml-conc-todas">✓ Conciliar todas (${conciliablesN})</button>` : ''}
    </div>

    ${hayVentas ? `
      ${esDevueltas
        ? `${pills}${devolucionesHTML()}`
        : `
      <div class="kpi-grid" style="margin:14px 0">
        <div class="kpi"><div class="kpi-label">Ventas del ${lblPeriodo}</div><div class="kpi-value">${base.length}</div></div>
        <div class="kpi"><div class="kpi-label">Por cobrar (neto)</div><div class="kpi-value">${money(totCobrar)}</div></div>
        <div class="kpi"><div class="kpi-label">Cobradas sin conciliar</div><div class="kpi-value">${cont.cobradas}</div></div>
        <div class="kpi"><div class="kpi-label">Conciliadas</div><div class="kpi-value">${cont.conciliadas} <span class="vml-de">de ${base.length}</span></div></div>
      </div>
      ${pills}
      ${visibles.length === 0
        ? `<div class="empty" style="margin-top:14px">No hay ventas en este filtro para ${vacioTxt}.</div>`
        : `<div class="table-wrap" style="margin-top:14px"><table class="t" id="vml-tabla">
            <thead><tr>
              ${esMes ? '<th style="width:50px">Fecha</th>' : ''}
              <th style="width:120px"># Venta</th>
              <th>Producto</th>
              <th style="width:56px">SKU</th>
              <th style="width:34px;text-align:right">Cant</th>
              <th style="width:96px;text-align:right">Bruto</th>
              <th style="width:90px;text-align:right">Comisión</th>
              <th style="width:84px;text-align:right">Envío</th>
              <th style="width:84px;text-align:right">Impuestos</th>
              <th style="width:88px;text-align:right">Financiero</th>
              <th style="width:104px;text-align:right">Por cobrar</th>
              <th style="width:84px">Estado</th>
              <th style="width:230px">Cobro / Conciliación</th>
            </tr></thead>
            <tbody>${visibles.map(v => filaHTML(v, esMes, BONIF_MAP)).join('')}</tbody>
          </table></div>`}
        `}
    ` : `<div class="empty">Todavía no hay ventas cargadas. Tocá <b>Sincronizar ventas</b> para traerlas de Mercado Libre.</div>`}
  `;

  document.getElementById('vml-sync').addEventListener('click', openSyncModal);
  if (hayVentas) {
    root.querySelectorAll('.vml-modo button').forEach(b =>
      b.addEventListener('click', () => { MODO = b.dataset.modo; render(); }));
    const prev = document.getElementById('vml-prev');
    const next = document.getElementById('vml-next');
    if (prev) prev.addEventListener('click', () => paso(-1));
    if (next) next.addEventListener('click', () => paso(1));
    const inpF = document.getElementById('vml-fecha');
    const inpM = document.getElementById('vml-mes');
    if (inpF) inpF.addEventListener('change', e => { FECHA = e.target.value; render(); });
    if (inpM) inpM.addEventListener('change', e => { MES = e.target.value; render(); });
    root.querySelectorAll('.pill').forEach(p => p.addEventListener('click', () => { FILTRO = p.dataset.f; render(); }));
    if (esDevueltas) {
      root.querySelectorAll('.vml-tabla-dev').forEach(t => t.addEventListener('click', onDevClick));
    } else {
      const tabla = document.getElementById('vml-tabla');
      if (tabla) tabla.addEventListener('click', onTablaClick);
      const cTodas = document.getElementById('vml-conc-todas');
      if (cTodas) cTodas.addEventListener('click', conciliarTodas);
    }
  }
}

function celdaMonto(valor) {
  const n = Number(valor) || 0;
  if (n === 0) return `<td style="text-align:right" class="vml-mono vml-cero">—</td>`;
  return `<td style="text-align:right" class="vml-mono ${n < 0 ? 'vml-neg' : 'vml-pos'}">${money(n)}</td>`;
}

function cobroCellInner(v, bonifMap) {
  if (estaConciliada(v)) {
    const n = (VINC_BY_VENTA[v.id] || []).length;
    const detalle = n > 1 ? ' (varios mov.)' : '';
    return `<span class="vml-conc">✓ conciliada${detalle}</span>`
      + `<button class="vml-x" data-accion="desvincular" data-venta="${v.id}" title="Deshacer conciliación">✕</button>`;
  }
  const cobros = matchCobros(v);
  if (!cobros.length) return `<span class="vml-cobro-no">— sin cobro</span>`;

  const sum = cobros.reduce((s, c) => s + (Number(c.monto) || 0), 0);
  const ok = `<span class="vml-cobro-ok">✓ ${money(sum)}</span>`;

  if (v.ml_status === 'cancelled' || v.devuelta) {
    return `${ok}<span class="vml-rev" title="Cancelada/devuelta: revisar en Conciliación">revisar</span>`;
  }
  if (conciliable(v, bonifMap)) {
    const b = bonifMap[v.id];
    const extra = b ? `<span class="vml-bonif" title="Bonificación de envío que se suma al conciliar">+ bonif. ${money(b.monto)}</span>` : '';
    return `${ok}${extra}<button class="btn btn-primary vml-mini" data-accion="conciliar" data-venta="${v.id}">Conciliar</button>`;
  }
  const motivo = cobros.length > 1 ? 'varios cobros, no cierra' : 'monto ≠';
  return `${ok}<span class="vml-rev" title="No cierra con el por cobrar: conciliar a mano en Conciliación">revisar (${motivo})</span>`;
}

// Celda de cobro/conciliación: contenido base + un toggle "▾" para desplegar
// el detalle con los N° de operación del extracto (cuando hay cobros vinculados
// o la venta está conciliada).
function cobroCell(v, bonifMap) {
  const inner = cobroCellInner(v, bonifMap);
  const hayOps = operacionesDe(v).length > 0;
  const tog = hayOps
    ? `<button class="vml-toggle" data-accion="toggle" data-venta="${v.id}" title="Ver N° de operación del extracto (MP)">▾</button>`
    : '';
  return inner + tog;
}

// Operaciones del extracto (MP) asociadas a la venta:
//  - conciliada → los movimientos efectivamente vinculados (incluye bonificación si la hubo).
//  - cobrada (sin conciliar) → los cobros que matchean por mp_payment_id.
// Cada una expone su N° de operación (= ID de liquidación del extracto), fecha y monto.
function operacionesDe(v) {
  const out = [];
  const desdeMov = (mov) => {
    const op = String(mov.referencia_externa || '').split('|')[1];
    out.push({ operacion: (op || ('mov. #' + mov.id)).trim(), fecha: mov.fecha || '', monto: Number(mov.monto) || 0 });
  };
  if (estaConciliada(v)) {
    for (const vc of (VINC_BY_VENTA[v.id] || [])) {
      const mov = COBROS_BY_ID[vc.movimiento_id];
      if (mov) desdeMov(mov);
      else out.push({ operacion: 'mov. #' + vc.movimiento_id, fecha: '', monto: Number(vc.monto) || 0 });
    }
  } else {
    for (const c of matchCobros(v)) desdeMov(c);
  }
  return out;
}

function detalleHTML(v) {
  const ops = operacionesDe(v);
  const filas = ops.map(o => `
    <div class="vml-det-op">
      <span class="vml-det-num" title="N° de operación del extracto (MP)">${esc(o.operacion)}</span>
      <span class="vml-det-fecha">${o.fecha ? esc(ddmm(o.fecha)) : '—'}</span>
      <span class="vml-det-monto vml-mono">${money(o.monto)}</span>
    </div>`).join('');
  return `<div class="vml-det">
    <div class="vml-det-head">Orden ML <b>${esc(v.ml_order_id || '—')}</b> · operaciones del extracto (MP):</div>
    ${filas || '<div class="vml-det-empty">— sin operaciones vinculadas —</div>'}
  </div>`;
}

// Inserta/quita una fila de detalle justo debajo de la venta. Se reconstruye en
// cada render(), así que el estado abierto/cerrado es efímero (no persiste).
function toggleDetalle(btn) {
  const tr = btn.closest('tr');
  if (!tr) return;
  const next = tr.nextElementSibling;
  if (next && next.classList.contains('vml-detalle-row')) {
    next.remove(); btn.textContent = '▾'; return;
  }
  const v = VENTAS.find(x => String(x.id) === String(btn.dataset.venta));
  if (!v) return;
  const det = document.createElement('tr');
  det.className = 'vml-detalle-row';
  det.innerHTML = `<td colspan="${tr.children.length}">${detalleHTML(v)}</td>`;
  tr.after(det);
  btn.textContent = '▴';
}

function filaHTML(v, esMes, bonifMap) {
  const cl = clase(v);
  const est = ESTADO_LBL[cl];
  const rowCls = est.cls === 'canc' ? 'vml-row-canc' : est.cls === 'dev' ? 'vml-row-dev' : '';

  return `<tr class="vml-row-click ${rowCls}" data-venta="${v.id}" title="Ver detalle de la venta">
    ${esMes ? `<td class="vml-mono">${esc(ddmm(v.fecha))}</td>` : ''}
    <td class="vml-mono">${v.ml_order_id
      ? `<a class="vml-venta-id" href="https://www.mercadolibre.com.ar/ventas/${encodeURIComponent(v.ml_order_id)}/detalle" target="_blank" rel="noopener" title="Abrir la venta en Mercado Libre">${esc(v.ml_order_id)}</a>`
      : '—'}</td>
    <td>${esc(v.titulo || '—')}</td>
    <td class="vml-mono">${esc(v.sku || '—')}</td>
    <td style="text-align:right">${v.cantidad || 1}</td>
    <td style="text-align:right" class="vml-mono">${money(v.importe_bruto)}</td>
    ${celdaMonto(v.cargo_venta)}
    ${celdaMonto(v.cargo_envio)}
    ${celdaMonto(v.impuestos)}
    ${celdaMonto(v.costo_financiero)}
    <td style="text-align:right" class="vml-mono vml-fuerte">${money(v.por_cobrar)}</td>
    <td><span class="vml-est vml-est-${est.cls || 'ok'}">${esc(est.txt)}</span></td>
    <td>${cobroCell(v, bonifMap)}</td>
  </tr>`;
}

function onTablaClick(e) {
  const btn = e.target.closest('[data-accion]');
  if (btn) {
    if (btn.dataset.accion === 'conciliar') conciliar(btn.dataset.venta);
    else if (btn.dataset.accion === 'desvincular') desvincular(btn.dataset.venta);
    else if (btn.dataset.accion === 'toggle') toggleDetalle(btn);
    return;
  }
  if (e.target.closest('a')) return;                 // el N° de venta sigue abriendo ML
  const tr = e.target.closest('tr[data-venta]');
  if (tr) openVentaDetalle(tr.dataset.venta);
}

// Abre un modal con TODA la vida de plata de la venta: cobro + impuestos +
// devoluciones (endpoint /venta/:id/detalle). Solo lectura.
async function openVentaDetalle(ventaId) {
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `<div class="modal vml-detalle-modal"><div class="card-title">Detalle de la venta</div><div class="empty" style="margin-top:10px">Cargando…</div></div>`;
  document.body.appendChild(overlay);
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  const pintar = (inner) => {
    overlay.querySelector('.modal').innerHTML = inner +
      `<div class="modal-actions"><button class="btn btn-ghost" id="vd-close">Cerrar</button></div>`;
    overlay.querySelector('#vd-close').addEventListener('click', close);
  };
  try {
    const r = await fetch('/venta/' + encodeURIComponent(ventaId) + '/detalle');
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    pintar(detalleHTML(data));
  } catch (e) {
    pintar(`<div class="card-title">Detalle de la venta</div><div class="error">No se pudo cargar: ${esc(e.message)}</div>`);
  }
}

// Arma el HTML del detalle a partir de { venta, cobros, devoluciones, retenciones }.
function detalleHTML({ venta, cobros, devoluciones, retenciones }) {
  const sum = arr => arr.reduce((s, x) => s + (Number(x.monto) || 0), 0);
  const lineaMov = m => `<div class="vml-dev-linea">
      <span class="vml-det-fecha">${esc(ddmm(m.fecha))}</span>
      <span class="vml-dev-ldesc">${esc(m.descripcion || m.categoria || '—')}</span>
      <span class="vml-mono ${m.monto < 0 ? 'vml-neg' : m.monto > 0 ? 'vml-pos' : 'vml-cero'}">${m.monto < 0 ? '−' : ''}${money(m.monto)}</span>
    </div>`;
  const lineaRet = r => `<div class="vml-dev-linea">
      <span class="vml-det-fecha">${r.fecha ? esc(ddmm(r.fecha)) : '—'}</span>
      <span class="vml-dev-ldesc">${esc([r.tipo, r.jurisdiccion].filter(Boolean).join(' · ') || r.detail || '—')}</span>
      <span class="vml-mono ${r.monto < 0 ? 'vml-neg' : 'vml-pos'}">${r.monto < 0 ? '−' : ''}${money(r.monto)}</span>
    </div>`;
  const bloque = (titulo, total, rows, vacio) => `
    <div class="vml-det-bloque">
      <div class="vml-det-h"><b>${titulo}</b>${rows.length ? `<span class="vml-mono ${total < 0 ? 'vml-neg' : total > 0 ? 'vml-pos' : ''}">${total < 0 ? '−' : ''}${money(total)}</span>` : ''}</div>
      ${rows.length ? `<div class="vml-dev-lineas">${rows.join('')}</div>` : `<div class="vml-sub" style="margin:4px 0 0">${vacio}</div>`}
    </div>`;

  const est = ESTADO_LBL[clase(venta)] || { txt: '—', cls: 'ok' };
  const cab = `
    <div class="vml-det-cab">
      ${venta.ml_order_id
        ? `<a class="vml-venta-id" href="https://www.mercadolibre.com.ar/ventas/${encodeURIComponent(venta.ml_order_id)}/detalle" target="_blank" rel="noopener">${esc(venta.ml_order_id)}</a>`
        : '—'}
      <div class="vml-dev-prod">${esc(venta.titulo || '—')}${venta.sku ? ' · SKU ' + esc(venta.sku) : ''}</div>
      <div class="vml-sub" style="margin:4px 0 0">${esc(ddmm(venta.fecha))}/${(venta.fecha || '').slice(0, 4)} ·
        <span class="vml-est vml-est-${est.cls || 'ok'}">${esc(est.txt)}</span> ·
        Por cobrar <b class="vml-mono">${money(venta.por_cobrar)}</b>${venta.devuelta ? ' · <span class="vml-dev-claim">devuelta</span>' : ''}</div>
    </div>`;

  return `<div class="card-title">Detalle de la venta</div>${cab}
    ${bloque('Cobro', sum(cobros), cobros.map(lineaMov), 'Sin cobro conciliado todavía (puede faltar cargar el AS de ese mes).')}
    ${bloque('Impuestos / retenciones', sum(retenciones), retenciones.map(lineaRet), 'Sin retenciones cargadas para esta venta.')}
    ${bloque('Devoluciones', sum(devoluciones), devoluciones.map(lineaMov), 'Sin devoluciones.')}`;
}

// Vincula el pago principal y, si hace falta para llegar al por_cobrar, también
// la bonificación de envío. Cada vínculo imputa el monto de su propio movimiento.
async function conciliar(ventaId) {
  const v = VENTAS.find(x => String(x.id) === String(ventaId));
  if (!v) return;
  const cobros = matchCobros(v);
  if (!cobros.length) { window.toast('Esta venta no tiene cobro para conciliar', 'error'); return; }
  const sum = cobros.reduce((s, c) => s + (Number(c.monto) || 0), 0);
  const pc = objetivoDe(v);

  const aVincular = cobros.map(c => ({ id: c.id, monto: r2(Math.abs(Number(c.monto) || 0)) }));
  let conBonif = false;
  if (Math.abs(sum - pc) >= TOL) {
    const falta = r2(pc - sum);
    if (falta <= TOL) { window.toast('Esta venta no cierra con el por cobrar', 'error'); return; }
    const b = buscarBonif(falta, new Set(VINC_MOV_USADOS));
    if (!b) { window.toast('No encontré una bonificación disponible para completar el monto', 'error'); return; }
    aVincular.push({ id: b.id, monto: r2(Math.abs(Number(b.monto) || 0)) });
    conBonif = true;
  }

  window.toast('Conciliando…');
  try {
    for (const x of aVincular) {
      const r = await fetch('/vincular', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ movimiento_id: x.id, op_tipo: 'venta_ml', op_id: v.id, monto: x.monto })
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    }
    window.toast(conBonif ? 'Venta conciliada (cobros + bonificación)' : (cobros.length > 1 ? 'Venta conciliada (varios cobros)' : 'Venta conciliada'));
    await loadVentasML();
  } catch (e) {
    window.toast('Error al conciliar: ' + e.message, 'error');
  }
}

// Deshace TODOS los vínculos de la venta (pago y, si corresponde, bonificación).
async function desvincular(ventaId) {
  const vincs = VINC_BY_VENTA[ventaId] || [];
  if (!vincs.length) return;
  if (!confirm('¿Deshacer la conciliación de esta venta? El cobro y la venta vuelven a quedar pendientes.')) return;
  try {
    for (const vc of vincs) {
      const r = await fetch('/vincular/' + vc.id, { method: 'DELETE' });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    }
    window.toast('Conciliación deshecha');
    await loadVentasML();
  } catch (e) {
    window.toast('Error: ' + e.message, 'error');
  }
}

// Conjunto de ventas visible según el modo (día o mes). Lo usan render y "Conciliar todas".
function conjuntoActual() {
  if (MODO === 'mes') return VENTAS.filter(v => mesDe(v.fecha) === MES);
  return VENTAS.filter(v => v.fecha === FECHA);
}

// Arma la lista de vínculos de todas las ventas conciliables (pago, y bonificación
// cuando corresponde), reservando cada bonificación una sola vez.
function armarLoteConciliacion(ventas) {
  const usados = new Set(VINC_MOV_USADOS);
  const lote = [];
  for (const v of ventas) {
    if (estaConciliada(v) || v.ml_status === 'cancelled' || v.devuelta) continue;
    const cb = matchCobros(v);
    if (!cb.length) continue;
    const sum = cb.reduce((s, c) => s + (Number(c.monto) || 0), 0);
    const pc = objetivoDe(v);
    let bonif = null;
    if (Math.abs(sum - pc) >= TOL) {
      const falta = r2(pc - sum);
      if (falta <= TOL) continue;
      bonif = buscarBonif(falta, usados);
      if (!bonif) continue;
    }
    for (const c of cb) lote.push({ movimiento_id: c.id, op_tipo: 'venta_ml', op_id: v.id, monto: r2(Math.abs(Number(c.monto) || 0)) });
    if (bonif) {
      lote.push({ movimiento_id: bonif.id, op_tipo: 'venta_ml', op_id: v.id, monto: r2(Math.abs(Number(bonif.monto) || 0)) });
      usados.add(bonif.id);
    }
  }
  return lote;
}

// Concilia de una sola vez todas las ventas que cierran del período visible.
async function conciliarTodas() {
  const lote = armarLoteConciliacion(conjuntoActual());
  if (!lote.length) { window.toast('No hay ventas para conciliar'); return; }
  const nVentas = new Set(lote.map(x => x.op_id)).size;
  const periodo = MODO === 'mes' ? mesLargo(MES) : fechaLarga(FECHA);
  if (!confirm(`¿Conciliar ${nVentas} ventas de ${periodo}? Se vinculan con su cobro (y la bonificación de envío cuando corresponde). Las que no cierran no se tocan.`)) return;
  window.toast(`Conciliando ${nVentas} ventas…`);
  try {
    const r = await fetch('/vincular-lote', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ vinculos: lote })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    window.toast(`Listo: ${nVentas} ventas conciliadas`);
    await loadVentasML();
  } catch (e) {
    window.toast('Error al conciliar todas: ' + e.message, 'error');
  }
}

function openSyncModal() {
  const hoy = hoyISO();
  const hace7 = new Date(Date.now() - 7 * 86400000).toISOString().slice(0, 10);
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal">
      <div class="card-title">Sincronizar ventas de ML</div>
      <p class="vml-sub">Trae las ventas de Mercado Libre del rango elegido. Para rangos largos puede tardar
        un rato; si falla, probá de a un mes.</p>
      <div class="field"><label>Desde</label><input type="date" class="input" id="vml-desde" value="${hace7}"></div>
      <div class="field"><label>Hasta</label><input type="date" class="input" id="vml-hasta" value="${hoy}"></div>
      <p class="vml-sub" style="margin-top:8px">¿ML desconectado? <a href="/ml/auth" target="_blank">Reconectar Mercado Libre</a></p>
      <div class="modal-actions">
        <button class="btn btn-ghost" id="vml-cancel">Cancelar</button>
        <button class="btn btn-primary" id="vml-go">Traer ventas</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#vml-cancel').addEventListener('click', close);
  overlay.querySelector('#vml-go').addEventListener('click', () => {
    const desde = overlay.querySelector('#vml-desde').value;
    const hasta = overlay.querySelector('#vml-hasta').value;
    if (!desde || !hasta) { window.toast('Elegí ambas fechas', 'error'); return; }
    if (desde > hasta) { window.toast('La fecha "desde" no puede ser mayor que "hasta"', 'error'); return; }
    sincronizar(desde, hasta, overlay);
  });
}

async function sincronizar(desde, hasta, overlay) {
  const btn = overlay.querySelector('#vml-go');
  btn.disabled = true; btn.textContent = 'Sincronizando…';
  window.toast('Sincronizando ventas… puede tardar');
  try {
    const r = await fetch('/ml/sync', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ desde, hasta })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) {
      const msg = data.error || (r.status + ' ' + r.statusText);
      if (/autentic|token|auth/i.test(msg)) {
        window.toast('ML desconectado. Reconectalo con el link del modal.', 'error');
        btn.disabled = false; btn.textContent = 'Traer ventas'; return;
      }
      throw new Error(msg);
    }
    overlay.remove();
    window.toast(`Listo: ${data.insertados ?? 0} ventas sincronizadas`);
    await loadVentasML();
  } catch (e) {
    window.toast('Error al sincronizar: ' + e.message, 'error');
    btn.disabled = false; btn.textContent = 'Traer ventas';
  }
}

// ── SOLAPA DEVOLUCIONES (v2: bundles por op_id, vía /devoluciones/resolver) ──
// Cada "bundle" es el conjunto de líneas del Account Statement que comparten un
// op_id (id de liquidación). El backend resuelve a qué venta pertenece cada
// bundle por op_id (capas: mp_payment_id → retenciones → agregado → revisión) y
// acá pintamos + vinculamos TODAS las líneas del bundle a la venta de una sola
// vez. Match por op_id, NUNCA por monto. vinculos.monto = abs(monto) (>0).
// Ver ADARA-CANCELACIONES-DEVOLUCIONES.md §"Match en capas".

async function cargarBundlesDevol() {
  const r = await fetch('/devoluciones/resolver');
  const data = await r.json().catch(() => ({}));
  if (!r.ok || !data.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
  DEV_BUNDLES = data.bundles || [];
  DEV_RESUMEN = data.resumen || {};
}

const mmaa = ym => (ym || '').slice(5, 7) + '/' + (ym || '').slice(0, 4);
function mesesDeBundle(b) {
  return [...new Set(b.lineas.map(l => (l.fecha || '').slice(0, 7)).filter(Boolean))].sort();
}
// Badge para cuando, con un mes de referencia, el bundle también tiene líneas en
// otros meses (su impacto se reparte entre períodos: P3 imputa cada línea a su mes).
function multiMesBadge(b, refMes) {
  if (!refMes) return '';
  const otros = mesesDeBundle(b).filter(m => m !== refMes);
  if (!otros.length) return '';
  return ` <span class="vml-dev-mas" title="Este bundle también tiene líneas en: ${otros.join(', ')}">también ${otros.map(mmaa).join(', ')}</span>`;
}

function ventaLink(v) {
  return v && v.ml_order_id
    ? `<a class="vml-venta-id" href="https://www.mercadolibre.com.ar/ventas/${encodeURIComponent(v.ml_order_id)}/detalle" target="_blank" rel="noopener" title="Abrir la venta en Mercado Libre">${esc(v.ml_order_id)}</a>`
    : '—';
}

// Lista compacta de las líneas de un bundle (fecha · detalle · monto con signo).
function lineasHTML(b) {
  return `<div class="vml-dev-lineas">` + b.lineas.map(l => {
    const cls = l.monto < 0 ? 'vml-neg' : (l.monto > 0 ? 'vml-pos' : 'vml-cero');
    const chk = l.ya_vinculada ? '<span class="vml-dev-id" title="Ya vinculada">✓</span> ' : '';
    return `<div class="vml-dev-linea">
      <span class="vml-det-fecha">${esc(ddmm(l.fecha))}</span>
      <span class="vml-dev-ldesc">${chk}${esc(l.descripcion || '—')}</span>
      <span class="vml-mono ${cls}">${l.monto < 0 ? '−' : ''}${money(l.monto)}</span>
    </div>`;
  }).join('') + `</div>`;
}

// Celda del neto del bundle, con etiqueta de impacto de caja.
function netoCell(neto) {
  if (neto < 0) return `<span class="vml-mono vml-neg">−${money(neto)}</span><div class="vml-dev-mlbl">pérdida real</div>`;
  if (neto > 0) return `<span class="vml-mono vml-pos">${money(neto)}</span><div class="vml-dev-mlbl">a favor</div>`;
  return `<span class="vml-mono vml-cero">${money(0)}</span><div class="vml-dev-mlbl">neutro</div>`;
}

// Fila de un bundle accionable (pendiente / parcial).
function bundleRowHTML(b, refMes) {
  const v = b.venta;
  const ventaCell = v
    ? `<div class="vml-dev-sug">${ventaLink(v)}<div class="vml-dev-prod">cobro ${money(v.por_cobrar)}${v.devuelta ? ' · <span class="vml-dev-claim">marcada devuelta</span>' : ''}</div></div>`
    : `<span class="vml-dev-empty">—</span>`;
  const accion = v
    ? `<button class="btn btn-primary vml-mini" data-accion="vincular-bundle" data-op="${esc(b.op_id)}" data-venta="${v.id}">Vincular bundle</button>`
    : '—';
  const parcial = b.estado === 'parcial' ? `<span class="vml-rev">parcial</span> ` : '';
  return `<tr>
    <td class="vml-mono">${esc(b.op_id)}${multiMesBadge(b, refMes)}</td>
    <td>${ventaCell}</td>
    <td style="text-align:right">${netoCell(b.neto)}</td>
    <td>${parcial}${lineasHTML(b)}</td>
    <td>${accion}</td>
  </tr>`;
}

// Fila de bundle informativo (ya vinculada / cargo ML / revisión).
function bundleInfoRowHTML(b, conDesvincular, refMes) {
  const v = b.venta;
  const ventaCell = v
    ? `<div class="vml-dev-sug">${ventaLink(v)}<div class="vml-dev-prod">cobro ${money(v.por_cobrar)}</div></div>`
    : `<span class="vml-dev-empty">—</span>`;
  const accion = conDesvincular
    ? `<button class="btn btn-ghost vml-mini" data-accion="desvincular-bundle" data-op="${esc(b.op_id)}">Desvincular</button>`
    : '—';
  return `<tr class="vml-row-dev">
    <td class="vml-mono">${esc(b.op_id)}${multiMesBadge(b, refMes)}</td>
    <td>${ventaCell}</td>
    <td style="text-align:right">${netoCell(b.neto)}</td>
    <td>${lineasHTML(b)}</td>
    <td>${accion}</td>
  </tr>`;
}

function tablaBundles(rows, fn) {
  return `<div class="table-wrap" style="margin-top:10px"><table class="t vml-tabla-dev">
    <thead><tr>
      <th style="width:130px">Liquidación (op_id)</th>
      <th style="width:240px">Venta</th>
      <th style="width:130px;text-align:right">Neto del bundle</th>
      <th>Líneas del Account Statement</th>
      <th style="width:150px">Acción</th>
    </tr></thead>
    <tbody>${rows.map(fn).join('')}</tbody>
  </table></div>`;
}

// Bundles del período actual del navegador (Día/Mes), por fecha de la línea del AS.
// Devuelve null si los bundles no se pudieron cargar.
function bundlesPeriodo() {
  if (!Array.isArray(DEV_BUNDLES)) return null;
  const esMes = MODO === 'mes';
  const ref = esMes ? MES : FECHA;
  if (!ref) return DEV_BUNDLES.slice();
  const key = esMes ? (f => (f || '').slice(0, 7)) : (f => f || '');
  return DEV_BUNDLES.filter(b => b.lineas.some(l => key(l.fecha) === ref));
}

// Contenido de la herramienta de devoluciones (KPIs + tablas de bundles) para el
// chip "Devueltas". Se embebe dentro de renderVentas, usando el navegador Día/Mes
// de la página (no tiene selector propio). Devuelve HTML.
function devolucionesHTML() {
  const per = bundlesPeriodo();
  if (per === null) {
    return `<div class="error" style="margin-top:14px">No se pudieron cargar las devoluciones. Probá <b>Sincronizar</b> o recargar.</div>`;
  }
  const refMes = MODO === 'mes' ? MES : (FECHA || '').slice(0, 7);
  const pend = per.filter(b => b.estado === 'pendiente');
  const parc = per.filter(b => b.estado === 'parcial');
  const vinc = per.filter(b => b.estado === 'vinculada');
  const agg  = per.filter(b => b.estado === 'agregado');
  const rev  = per.filter(b => b.estado === 'revision');
  const accionables = [...pend, ...parc];

  const banner = `<div class="vml-dev-banner">
    Devoluciones del Account Statement del período elegido (por <b>fecha del movimiento</b>), agrupadas por
    <b>liquidación (op_id)</b>. <b>Vincular bundle</b> pega todas las líneas (neto + envío + impuestos) a su venta
    y, si el neto es negativo, la marca como devuelta. No toca <code>por_cobrar</code>.
  </div>`;

  const kpis = `<div class="kpi-grid" style="margin:14px 0">
    <div class="kpi"><div class="kpi-label">Para vincular</div><div class="kpi-value">${accionables.length}</div></div>
    <div class="kpi"><div class="kpi-label">Ya vinculadas</div><div class="kpi-value">${vinc.length}</div></div>
    <div class="kpi"><div class="kpi-label">Cargos ML (no venta)</div><div class="kpi-value">${agg.length}</div></div>
    <div class="kpi"><div class="kpi-label">Revisión</div><div class="kpi-value">${rev.length}</div></div>
  </div>`;

  let html = banner + kpis;
  if (accionables.length) {
    html += `<div class="card-title" style="margin-top:18px">Para vincular (${accionables.length})</div>`;
    html += tablaBundles(accionables, b => bundleRowHTML(b, refMes));
  } else {
    html += `<div class="empty" style="margin-top:14px">No hay devoluciones para vincular en este período.</div>`;
  }
  if (vinc.length) {
    html += `<div class="card-title" style="margin-top:26px">Ya vinculadas (${vinc.length})</div>`;
    html += tablaBundles(vinc, b => bundleInfoRowHTML(b, true, refMes));
  }
  if (agg.length) {
    html += `<div class="card-title" style="margin-top:26px">Cargos ML — no son devolución de venta (${agg.length})</div>`;
    html += `<div class="vml-sub">Facturas vencidas, reintegros batcheados y otros agregados de ML. No se imputan a una venta.</div>`;
    html += tablaBundles(agg, b => bundleInfoRowHTML(b, false, refMes));
  }
  if (rev.length) {
    html += `<div class="card-title" style="margin-top:26px">Revisión (${rev.length})</div>`;
    html += `<div class="vml-sub">No se pudo resolver la venta (típicamente anteriores al 31/12/2025). Impacto de caja casi siempre nulo.</div>`;
    html += tablaBundles(rev, b => bundleInfoRowHTML(b, false, refMes));
  }
  return html;
}

// Clics de las tablas de devoluciones (vincular / desvincular bundle).
function onDevClick(e) {
  const btn = e.target.closest('[data-accion]');
  if (!btn) return;
  const a = btn.dataset.accion;
  if (a === 'vincular-bundle') vincularBundle(btn.dataset.op, btn.dataset.venta, btn);
  else if (a === 'desvincular-bundle') desvincularBundle(btn.dataset.op);
}

// Vincula TODAS las líneas del bundle a la venta (endpoint /devoluciones/vincular).
// El backend recalcula montos desde movimientos y marca devuelta si neto < 0.
async function vincularBundle(opId, ventaId, btn) {
  if (btn) btn.disabled = true;
  window.toast('Vinculando bundle…');
  try {
    const r = await fetch('/devoluciones/vincular', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ op_id: String(opId), venta_id: Number(ventaId) })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    window.toast(`Bundle vinculado: ${data.vinculados} líneas${data.marcada_devuelta ? ' · venta marcada devuelta' : ''}`);
    DEV_BUNDLES = null;          // invalidar cache → recargar resolver
    await loadVentasML();
  } catch (e) {
    if (btn) btn.disabled = false;
    window.toast('Error al vincular: ' + e.message, 'error');
  }
}

// Deshace el bundle: borra el vínculo de cada línea (usa DEV_LINK_BY_MOV, que
// loadVentasML llena con los vínculos de devolución). No revierte el flag
// `devuelta` de la venta (acción manual si hiciera falta).
async function desvincularBundle(opId) {
  const b = (DEV_BUNDLES || []).find(x => String(x.op_id) === String(opId));
  if (!b) return;
  if (!confirm(`¿Desvincular las ${b.lineas.length} líneas de esta liquidación de su venta?`)) return;
  window.toast('Desvinculando…');
  try {
    for (const l of b.lineas) {
      const link = DEV_LINK_BY_MOV.get(l.id);
      if (!link) continue;
      const r = await fetch('/vincular/' + link.id, { method: 'DELETE' });
      if (!r.ok) throw new Error('No se pudo borrar el vínculo ' + link.id);
    }
    window.toast('Bundle desvinculado');
    DEV_BUNDLES = null;
    await loadVentasML();
  } catch (e) {
    window.toast('Error al desvincular: ' + e.message, 'error');
  }
}

function inyectarEstilo() {
  if (document.getElementById('vml-style')) return;
  const css = `
    .vml-tabs{display:flex;gap:4px;border-bottom:1px solid var(--border-strong);margin-bottom:14px}
    .vml-tab{border:0;background:transparent;color:#78716C;font-family:inherit;font-size:15px;font-weight:600;padding:9px 16px;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px}
    .vml-tab:hover{color:#0C447C}
    .vml-tab.active{color:#0C447C;border-bottom-color:#0C447C}
    .vml-tab-n{font-size:12px;background:#FAF1E1;color:#92500A;border-radius:8px;padding:1px 8px;margin-left:4px}
    .vml-dev-banner{font-size:13px;color:#57534E;background:#FBFAF6;border:1px solid #EAE6DD;border-radius:var(--r-sm);padding:10px 14px;line-height:1.5}
    .vml-dev-warn{display:inline-block;margin-left:6px;color:#92500A;background:#FAF1E1;border-radius:6px;padding:1px 8px;font-size:12px}
    .vml-dev-mlbl{font-size:11px;color:#A8A29E;font-weight:400}
    .vml-dev-sug{line-height:1.4}
    .vml-dev-prod{font-size:12px;color:#78716C;margin-top:2px}
    .vml-dev-lineas{display:flex;flex-direction:column;gap:2px}
    .vml-dev-linea{display:grid;grid-template-columns:42px 1fr auto;gap:10px;align-items:baseline;font-size:12px}
    .vml-dev-ldesc{color:#57534E;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .vml-row-click{cursor:pointer}
    .vml-row-click:hover td{background:#FAF7F2}
    .vml-detalle-modal{max-width:560px;width:92vw}
    .vml-det-cab{margin:10px 0 4px}
    .vml-det-bloque{margin-top:14px;border-top:1px solid #EFEAE3;padding-top:10px}
    .vml-det-h{display:flex;justify-content:space-between;align-items:baseline;font-size:14px;margin-bottom:6px}
    .vml-dev-claim{font-size:11px;color:#92500A;background:#FAF1E1;border-radius:6px;padding:1px 7px;margin-left:4px}
    .vml-dev-id{font-size:11px;color:#0F6E56;background:#E1F5EE;border-radius:6px;padding:1px 7px;font-weight:600}
    .vml-dev-pick{display:flex;gap:6px;align-items:center}
    .vml-dev-select{padding:4px 6px;font-size:12px;max-width:170px}
    .vml-dev-aprox{color:#A8A29E;margin-right:3px}
    .vml-dev-mas{font-size:11px;color:#0C447C;background:#E6F1FB;border-radius:6px;padding:1px 7px;margin-left:6px;cursor:help}
    .vml-dev-empty{color:#A8A29E;font-size:13px}
    .vml-bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
    .vml-modo{display:inline-flex;border:1px solid var(--border-strong);border-radius:var(--r-sm);overflow:hidden}
    .vml-modo button{border:0;background:var(--surface);color:var(--text-muted);font-family:inherit;font-size:14px;font-weight:600;padding:8px 16px;cursor:pointer}
    .vml-modo button.active{background:var(--acc-bg);color:var(--acc-dark)}
    .btn-conc{background:#0F6E56;color:#fff;border:0}
    .btn-conc:hover{background:#0C5A47}
    .vml-fechalbl{font-size:13px;color:#78716C;text-transform:capitalize;margin-left:4px}
    .vml-de{font-size:14px;color:#A8A29E;font-weight:400}
    .vml-sub{font-size:13px;color:#78716C;margin:-4px 0 12px}
    .vml-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .vml-fuerte{font-weight:600}
    .vml-pos{color:#15803D}
    .vml-neg{color:#B91C1C}
    .vml-cero{color:#C7C2BC}
    /* La tabla se ajusta al ancho del monitor: fuente y espaciado escalan según el ancho. */
    #vml-tabla{font-size:clamp(11px, 0.55vw + 4.5px, 16px)}
    #vml-tabla thead th{padding:clamp(6px,0.5vw,11px) clamp(6px,0.55vw,12px);font-size:clamp(10px, 0.4vw + 4.5px, 13px);white-space:nowrap}
    #vml-tabla tbody td{padding:clamp(6px,0.5vw,11px) clamp(6px,0.55vw,12px)}
    /* Montos y códigos nunca se parten en dos líneas */
    #vml-tabla tbody td.vml-mono{white-space:nowrap}
    .vml-venta-id{color:#0C447C;text-decoration:none;border-bottom:1px dashed #9DB6D4}
    .vml-venta-id:hover{color:#D97706;border-bottom-color:#D97706}
    .vml-est{font-size:12px;padding:2px 8px;border-radius:6px;background:#F1EFE8;color:#57534E}
    .vml-est-canc{background:#FBEAEA;color:#B42318}
    .vml-est-dev{background:#FAF1E1;color:#92500A}
    .vml-est-cob{background:#E6F1FB;color:#0C447C}
    .vml-est-conc{background:#E1F5EE;color:#0F6E56}
    .vml-row-canc{background:rgba(180,35,24,0.05)}
    .vml-row-dev{background:rgba(217,119,6,0.06)}
    .vml-cobro-ok{color:#0F6E56;font-weight:600;font-family:'JetBrains Mono',ui-monospace,monospace;margin-right:8px;white-space:nowrap}
    .vml-bonif{font-size:11px;color:#0C447C;background:#E6F1FB;border-radius:6px;padding:2px 7px;margin-right:8px;white-space:nowrap}
    .vml-cobro-no{font-size:12px;color:#857a5c;background:#FAF6EC;border:1px dashed #E3D9BE;border-radius:6px;padding:2px 8px}
    .vml-conc{color:#0F6E56;font-weight:600;margin-right:6px}
    .vml-rev{font-size:12px;color:#92500A;background:#FAF1E1;border-radius:6px;padding:2px 8px}
    .vml-mini{padding:4px 10px;font-size:13px}
    .vml-x{border:0;background:transparent;color:#A8A29E;cursor:pointer;font-size:13px;padding:0 2px}
    .vml-x:hover{color:#B91C1C}
    .vml-toggle{border:0;background:transparent;color:#0C447C;cursor:pointer;font-size:12px;padding:0 4px;margin-left:4px}
    .vml-toggle:hover{color:#D97706}
    .vml-detalle-row > td{background:#FBFAF6;border-top:0;padding:0!important}
    .vml-det{padding:8px 14px;font-size:12px;color:#57534E}
    .vml-det-head{margin-bottom:6px;color:#78716C}
    .vml-det-op{display:flex;gap:14px;align-items:center;padding:3px 0}
    .vml-det-num{font-family:'JetBrains Mono',ui-monospace,monospace;color:#0C447C;font-weight:600;user-select:all;cursor:text}
    .vml-det-fecha{color:#A8A29E;min-width:42px}
    .vml-det-monto{color:#0F6E56}
    .vml-det-empty{color:#A8A29E}
  `;
  const style = document.createElement('style');
  style.id = 'vml-style';
  style.textContent = css;
  document.head.appendChild(style);
}
