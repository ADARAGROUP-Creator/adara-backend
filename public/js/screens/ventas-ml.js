import { sbGet, sbPatch } from '../core/sb.js';
import { mlTabs } from '../core/mlTabs.js';
import { exportarVentasXLSX } from '../core/reporte-ventas-xlsx.js';

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
let MOV_MP = [];             // extracto completo de MP (origen='mp_account_statement'), todas las categorías
let SHIP_BY_VENTA = {};      // clave (v:venta_id | o:ml_order_id | p:pack_id) -> { liqIds:Set, retEnvio } — envío colecta O8 (retenciones SETTLEMENT_SHIPPING)
let VINC_BY_VENTA = {};      // ventas_ml.id -> [vínculos] (op_tipo='venta_ml')
let VINC_MOV_USADOS = new Set(); // movimiento_id ya vinculados a alguna venta
let DIAS = [];
let MESES = [];
let MODO = 'dia';            // dia | mes
let FECHA = '';              // YYYY-MM-DD (modo día)
let MES = '';                // YYYY-MM (modo mes)
let FILTRO = 'todas';        // todas | por_cobrar | cobradas | conciliadas | canceladas | devueltas
let SOLO_PEND_CANC = true;   // en el chip Canceladas, mostrar solo las pendientes (se van limpiando)
let COLV = { fecha: '', venta: '', prod: '', sku: '', cant: '', bruto: '', com: '', envio: '', imp: '', fin: '', cobrar: '', envest: '', estado: '' }; // filtros por columna
let BONIF_MAP_CUR = {};      // bonifs del período actual (para el repintado parcial de filtros)
let VISTA = 'control';       // control (lista) | conciliar (dos tablas Ventas ↔ Cobros)
let CONC_SEL = null;         // venta seleccionada en la vista de conciliación manual
let CONC_HIDE_DONE = false;  // ocultar de las tablas lo ya conciliado (los totales igual lo cuentan)

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
const nextPeriodo = ym => { const [y, m] = String(ym || '').split('-').map(Number); if (!y || !m) return ym; return m === 12 ? (y + 1) + '-01' : y + '-' + String(m + 1).padStart(2, '0'); };
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

// ¿La cancelada tiene movimientos del AS (cobro o devolución) SIN vincular a algo?
// Mira las dos categorías por el/los N° de operación de la venta. Si no hay
// ninguna línea, no hay nada pendiente. Esto evita marcar "resuelta" mientras
// quede plata del extracto suelta (principio: todo movimiento relacionado).
function cancMovPend(v) {
  const pids = new Set([v.mp_payment_id, ...String(v.mp_payment_ids || '').split(',')]
    .map(s => String(s || '').trim()).filter(Boolean));
  const cobroPend = matchCobros(v).some(c => !VINC_MOV_USADOS.has(c.id));
  const devPend = DEVOLS.some(d => pids.has(String(d.referencia || '').split('|')[1]) && !MOV_VINCULADOS.has(d.id));
  return cobroPend || devPend;
}

// Una cancelada está PENDIENTE si le falta resolver alguno de los dos ejes:
//  - plata: tiene movimientos del AS (cobro o devolución) sin vincular (cancMovPend)
//  - stock: salió del depósito (despachado/entregado) y no tiene recepción
function cancPendiente(v) {
  if (v.ml_status !== 'cancelled') return false;
  const salio = v.estado_envio === 'despachado' || v.estado_envio === 'entregado';
  const stockPend = salio && !v.recepcion_condicion;
  return stockPend || cancMovPend(v);
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
  VISTA = 'control';    // al (re)entrar a la pantalla, arrancar siempre en la lista de control
  CONC_SEL = null;
  DEV_BUNDLES = null;   // invalidar cache de bundles de devolución → se recarga al abrir la solapa
  try {
    VENTAS = await sbGet('ventas_ml', 'order=fecha.asc,hora_venta.asc,id.asc');
    const cobros = await sbGet('movimientos', 'categoria=eq.cobro_venta&order=fecha.asc,id.asc');
    COBROS_BY_REF = {};
    COBROS_BY_ID = {};
    BONIFS = [];
    for (const c of cobros) {
      const ref = String(c.referencia_externa || '').split('|')[1];
      if (ref) COBROS_BY_REF[ref.trim()] = c;
      COBROS_BY_ID[c.id] = c;
      if (/bonific/i.test(c.descripcion || '')) BONIFS.push(c);
    }
    // Extracto COMPLETO de MP del período (todas las categorías, no solo cobros): la
    // tabla derecha de la vista de conciliación muestra todo el movimiento de la cuenta.
    MOV_MP = await sbGet('movimientos', 'origen=eq.mp_account_statement&order=fecha.asc,id.asc');
    // Envío colecta (O8): índice de la retención de IIBB sobre la liquidación del envío.
    // Cada fila se indexa por venta_id, ml_order_id y pack_id; la clave guarda los liqIds
    // (mp_source_id del envío, para encontrar su cobro en COBROS_BY_REF) y la suma de la
    // retención. La venta usa UNA sola clave al consultar (shipDe), nunca las tres.
    SHIP_BY_VENTA = {};
    try {
      const retEnvio = await sbGet('retenciones',
        'transaction_type=eq.SETTLEMENT_SHIPPING&select=venta_id,order_id,pack_id,mp_source_id,monto&order=id.asc');
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

    const vinc = await sbGet('vinculos', 'op_tipo=eq.venta_ml&order=id.asc');
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
    const nuevos = await sbGet('movimientos', `categoria=in.${inFilter}&order=fecha.asc,id.asc`);
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
    const viejos = await sbGet('movimientos_mp', `categoria=in.${inFilter}&order=fecha.asc,id.asc`);
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
  if (VISTA === 'conciliar') return renderConciliar();
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
  const pendCancN = base.reduce((n, v) => n + (cancPendiente(v) ? 1 : 0), 0);

  const esDevueltas = FILTRO === 'devueltas';
  const perDev = bundlesPeriodo();   // bundles de devolución del período (por fecha de línea del AS)
  if (perDev) cont.devueltas = perDev.filter(b => b.estado === 'pendiente' || b.estado === 'parcial').length;

  const visibles = ventasVisiblesBase();
  const totCobrar = base.reduce((s, v) => s + (Number(v.por_cobrar) || 0), 0);

  // ── Filtros por columna (estilo Excel) ──
  const optsVEst = [...new Set(base.map(v => (ESTADO_LBL[clase(v)] && ESTADO_LBL[clase(v)].txt) || clase(v)))]
    .filter(Boolean).sort((a, b) => String(a).localeCompare(String(b), 'es'));
  const optsEnvest = [...new Set(base.map(v => v.estado_envio))].filter(Boolean).sort((a, b) => String(a).localeCompare(String(b), 'es'));
  const vIn = (col, ph) => `<input class="vmlf" data-col="${col}" type="text" placeholder="${ph}" value="${esc(COLV[col])}">`;
  const vSel = (col, opts) => `<select class="vmlf" data-col="${col}"><option value="">(todas)</option>${opts.map(o => `<option ${COLV[col] === o ? 'selected' : ''}>${esc(o)}</option>`).join('')}</select>`;
  const filtroRow = `<tr class="vml-filtros">
      ${esMes ? `<th>${vIn('fecha', 'dd/mm')}</th>` : ''}
      <th>${vIn('venta', '# venta')}</th>
      <th>${vIn('prod', 'buscar…')}</th>
      <th>${vIn('sku', 'sku')}</th>
      <th>${vIn('cant', 'cant')}</th>
      <th>${vIn('bruto', 'monto')}</th>
      <th>${vIn('com', 'monto')}</th>
      <th>${vIn('envio', 'monto')}</th>
      <th>${vIn('imp', 'monto')}</th>
      <th>${vIn('fin', 'monto')}</th>
      <th>${vIn('cobrar', 'monto')}</th>
      <th>${vSel('envest', optsEnvest)}</th>
      <th>${vSel('estado', optsVEst)}</th>
      <th></th>
    </tr>`;

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
    ${mlTabs('ventas_ml')}
    <div class="vml-bar">
      <button class="btn btn-primary" id="vml-sync">⟳ Sincronizar ventas</button>
      ${hayVentas ? modoHTML : ''}
      ${navHTML}
      ${!esDevueltas ? `<button class="btn btn-conc" id="vml-conc-manual">🔗 Conciliar a mano${conciliablesN > 0 ? ` (${conciliablesN} listas)` : ''}</button>` : ''}
      ${(hayVentas && FILTRO === 'canceladas') ? `<label class="vml-pendchk"><input type="checkbox" id="vml-solo-pend" ${SOLO_PEND_CANC ? 'checked' : ''}> Solo pendientes (${pendCancN})</label>` : ''}
      <button class="btn btn-ghost" id="vml-export-xlsx" style="margin-left:auto">📥 Exportar XLSX</button>
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
        : `<div class="vml-fbar"><span class="vml-count-lbl" id="vml-count"></span>
             <button class="vml-clear" id="vml-clear">Limpiar filtros</button></div>
           <div class="table-wrap"><table class="t" id="vml-tabla">
            <thead>
              <tr>
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
                <th style="width:104px">Estado envío</th>
                <th style="width:84px">Estado</th>
                <th style="width:230px">Cobro / Conciliación</th>
              </tr>
              ${filtroRow}
            </thead>
            <tbody id="vml-tbody"></tbody>
            <tfoot id="vml-tfoot"></tfoot>
          </table></div>
          <div class="empty" id="vml-empty" style="display:none;margin-top:10px">Sin resultados para los filtros aplicados.</div>`}
        `}
    ` : `<div class="empty">Todavía no hay ventas cargadas. Tocá <b>Sincronizar ventas</b> para traerlas de Mercado Libre.</div>`}
  `;

  document.getElementById('vml-sync').addEventListener('click', openSyncModal);
  document.getElementById('vml-export-xlsx').addEventListener('click', openExportModal);
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
    const soloPend = document.getElementById('vml-solo-pend');
    if (soloPend) soloPend.addEventListener('change', () => { SOLO_PEND_CANC = soloPend.checked; render(); });
    if (esDevueltas) {
      root.querySelectorAll('.vml-tabla-dev').forEach(t => t.addEventListener('click', onDevClick));
    } else {
      const tabla = document.getElementById('vml-tabla');
      if (tabla) tabla.addEventListener('click', onTablaClick);
      const cManual = document.getElementById('vml-conc-manual');
      if (cManual) cManual.addEventListener('click', () => { VISTA = 'conciliar'; CONC_SEL = null; render(); });
      // Filtros por columna: repintan SOLO el tbody/tfoot (no se pierde el foco).
      BONIF_MAP_CUR = BONIF_MAP;
      root.querySelectorAll('.vmlf').forEach(el => {
        const ev = el.tagName === 'SELECT' ? 'change' : 'input';
        el.addEventListener(ev, e => { COLV[e.target.dataset.col] = e.target.value; pintarVML(); });
      });
      const vclr = document.getElementById('vml-clear');
      if (vclr) vclr.addEventListener('click', () => {
        COLV = { fecha: '', venta: '', prod: '', sku: '', cant: '', bruto: '', com: '', envio: '', imp: '', fin: '', cobrar: '', envest: '', estado: '' };
        render();
      });
      pintarVML();
    }
  }
}

function celdaMonto(valor) {
  const n = Number(valor) || 0;
  if (n === 0) return `<td style="text-align:right" class="vml-mono vml-cero">—</td>`;
  return `<td style="text-align:right" class="vml-mono ${n < 0 ? 'vml-neg' : 'vml-pos'}">${money(n)}</td>`;
}

// Subrenglón del monto facturado en Tango (bajo el Bruto). Verde si coincide con
// el bruto; ámbar con la diferencia si no (esa diferencia suele ser el aporte de
// ML, que no se factura). Vacío si la venta todavía no tiene factura vinculada.
function factCell(v) {
  if (v.nro_factura == null || v.importe_facturado == null) return '';
  const fact = Number(v.importe_facturado) || 0;
  const dif = r2((Number(v.importe_bruto) || 0) - fact);
  if (Math.abs(dif) < 0.01)
    return `<div style="font-size:11px;color:#0F6E56;margin-top:2px" title="Facturado en Tango — coincide con el bruto">fact. ${money(fact)} ✓</div>`;
  const signo = dif > 0 ? '−' : '+';
  return `<div style="font-size:11px;color:#B45309;margin-top:2px" title="Facturado en Tango. La diferencia con el bruto suele ser el aporte de Mercado Libre, que no se factura.">fact. ${money(fact)} · ${signo}${money(dif)}</div>`;
}

// ── Vista de conciliación manual mensual (dos tablas: Ventas ↔ Extracto MP) ──
// Tablero bidireccional filtrado por mes. Izquierda: TODAS las ventas del mes
// (incluye conciliadas, canceladas que salieron y devoluciones; excluye solo las
// canceladas sin envío). Derecha: extracto COMPLETO de MP del mes con su estado.
// Clic en cualquiera resalta su contraparte sugerida (motor: matchCobros + bonif +
// colecta O8); la confirmación es venta por venta (reusa conciliar / conciliarMovimientos).
// Arriba, KPIs con progress-rings del % de avance. No se vincula nada en lote.
function infoMatch(v, bonifMap) {
  const cobros = matchCobros(v);
  const sum = r2(cobros.reduce((s, c) => s + (Number(c.monto) || 0), 0));
  const obj = objetivoDe(v);
  const bonif = bonifMap[v.id] || null;
  const cierra = conciliable(v, bonifMap);
  const falta = r2(obj - sum - (bonif ? (Number(bonif.monto) || 0) : 0));
  return { cobros, sum, obj, bonif, cierra, falta, sinCobro: cobros.length === 0 };
}

// Mueve una venta a otro "período de conciliación" (o la devuelve a su mes de venta
// con periodo=null). Persiste en ventas_ml.conciliacion_periodo. Requiere la columna
// (ver migración conciliacion_periodo) y policy UPDATE para authenticated (A17).
async function diferirVenta(ventaId, periodo, select = false) {
  try {
    await sbPatch('ventas_ml', `id=eq.${ventaId}`, { conciliacion_periodo: periodo });
    const v = VENTAS.find(x => String(x.id) === String(ventaId));
    if (v) v.conciliacion_periodo = periodo;
    if (select) CONC_SEL = String(ventaId);            // traída a este mes → queda seleccionada
    else if (String(CONC_SEL) === String(ventaId)) CONC_SEL = null;
    window.toast(periodo ? 'Venta movida a ' + periodo : 'Venta devuelta a su mes de venta');
    render();
  } catch (e) {
    window.toast('No se pudo mover la venta: ' + e.message, 'error');
  }
}

function renderConciliar() {
  const root = document.getElementById('app-screens');
  const esMes = MODO === 'mes';
  // La vista de conciliación usa el "período de conciliación" efectivo (override manual
  // conciliacion_periodo, o el mes de la venta si no se difirió), para poder mover
  // ventas entre meses con "Diferir →" y cerrar el mes actual al 100%.
  const efPeriodo = v => v.conciliacion_periodo || mesDe(v.fecha);
  const base = !VENTAS.length ? []
    : (esMes ? VENTAS.filter(v => efPeriodo(v) === MES) : VENTAS.filter(v => v.fecha === FECHA));
  const BONIF_MAP = asignarBonifs(base);

  // Ventas del período. Se INCLUYEN conciliadas (marcadas), canceladas que salieron
  // del depósito y devoluciones. Se EXCLUYEN solo las canceladas que NO salieron
  // (no_preparado/preparado): no mueven ni stock ni plata (S4).
  const noSalio = v => v.ml_status === 'cancelled' && (v.estado_envio === 'no_preparado' || v.estado_envio === 'preparado');
  const ventas = base.filter(v => !noSalio(v));

  const resuelta = v => (v.ml_status === 'cancelled' || v.devuelta) ? !cancMovPend(v) : estaConciliada(v);

  // Mapa movimiento_id -> venta_id de los vínculos existentes (op_tipo='venta_ml').
  const linkMov = {};
  for (const vid in VINC_BY_VENTA) for (const lk of VINC_BY_VENTA[vid]) linkMov[lk.movimiento_id] = vid;
  // Mapa movimiento -> venta para el clic desde el extracto (vínculo real o cobro sugerido).
  const mov2venta = Object.assign({}, linkMov);
  for (const v of ventas) {
    if (estaConciliada(v)) continue;
    for (const c of matchCobros(v)) if (mov2venta[c.id] == null) mov2venta[c.id] = String(v.id);
  }
  const ordById = {};
  VENTAS.forEach(v => { ordById[v.id] = v.ml_order_id || v.id; });
  // Para traer al mes una venta de OTRO mes cuando aparece su movimiento: mapa
  // referencia(mp_payment_id) -> venta sobre TODAS las ventas + set de las visibles.
  const ventaByRef = {};
  for (const v of VENTAS) for (const pid of paymentIds(v)) if (ventaByRef[pid] == null) ventaByRef[pid] = v;
  const curVentaIds = new Set(ventas.map(v => String(v.id)));

  // Selección (bidireccional). Default: primera pendiente que cierra, si no la primera.
  if (CONC_SEL == null || !ventas.some(v => String(v.id) === String(CONC_SEL))) {
    const f = ventas.find(v => !resuelta(v) && conciliable(v, BONIF_MAP)) || ventas.find(v => !resuelta(v)) || ventas[0];
    CONC_SEL = f ? f.id : null;
  }
  const sel = ventas.find(v => String(v.id) === String(CONC_SEL)) || null;

  // Movimientos resaltados para la venta seleccionada (sus cobros sugeridos, o sus
  // vínculos si ya está conciliada).
  const selMovs = new Set();
  let selI = null;
  if (sel) {
    if (estaConciliada(sel)) (VINC_BY_VENTA[sel.id] || []).forEach(lk => selMovs.add(lk.movimiento_id));
    else { selI = infoMatch(sel, BONIF_MAP); selI.cobros.forEach(c => selMovs.add(c.id)); if (selI.bonif) selMovs.add(selI.bonif.id); }
  }

  // Extracto de MP del período (todos los movimientos, con su estado).
  const movsMes = MOV_MP.filter(m => esMes ? mesDe(m.fecha) === MES : m.fecha === FECHA);

  // KPIs de avance.
  const ventasTot = ventas.length;
  const ventasConc = ventas.filter(resuelta).length;
  const ventasPct = ventasTot ? ventasConc / ventasTot : 0;
  const movTot = movsMes.length;
  const movVinc = movsMes.filter(m => linkMov[m.id] || m.conciliado_auto).length;
  const movPct = movTot ? movVinc / movTot : 0;

  const ring = (pct, color) => {
    const r = 26, c = 2 * Math.PI * r, off = c * (1 - Math.max(0, Math.min(1, pct)));
    return `<svg width="70" height="70" viewBox="0 0 70 70" class="vmlc-ring" aria-hidden="true">
      <circle cx="35" cy="35" r="${r}" fill="none" stroke="#ECEAE6" stroke-width="8"/>
      <circle cx="35" cy="35" r="${r}" fill="none" stroke="${color}" stroke-width="8" stroke-linecap="round"
        stroke-dasharray="${c.toFixed(1)}" stroke-dashoffset="${off.toFixed(1)}" transform="rotate(-90 35 35)"/>
      <text x="35" y="35" text-anchor="middle" dominant-baseline="central" class="vmlc-ring-pct">${Math.round(pct * 100)}%</text>
    </svg>`;
  };
  const kpi = `<div class="vmlc-kpi">
    <div class="vmlc-kpi-card">${ring(ventasPct, '#0F6E56')}
      <div><div class="vmlc-kpi-lbl">Ventas conciliadas</div>
        <div class="vmlc-kpi-num">${ventasConc} <span class="vmlc-kpi-de">de ${ventasTot}</span></div></div></div>
    <div class="vmlc-kpi-card">${ring(movPct, '#D97706')}
      <div><div class="vmlc-kpi-lbl">Movimientos MP vinculados</div>
        <div class="vmlc-kpi-num">${movVinc} <span class="vmlc-kpi-de">de ${movTot}</span></div></div></div>
    <div class="vmlc-kpi-card vmlc-kpi-plain">
      <div class="vmlc-kpi-lbl">Pendientes</div>
      <div class="vmlc-kpi-big">${ventasTot - ventasConc}</div>
      <div class="vmlc-kpi-sub">ventas por conciliar</div></div>
  </div>`;

  let navHTML = '';
  if (esMes) {
    const i = MESES.indexOf(MES);
    navHTML = `<button class="btn btn-ghost" id="vmlc-prev" ${i <= 0 ? 'disabled' : ''}>‹</button>
      <input type="month" class="input" id="vmlc-mes" value="${MES}" style="width:auto">
      <button class="btn btn-ghost" id="vmlc-next" ${i >= MESES.length - 1 ? 'disabled' : ''}>›</button>
      <span class="vml-fechalbl">${esc(mesLargo(MES))}</span>`;
  } else {
    const i = DIAS.indexOf(FECHA);
    navHTML = `<button class="btn btn-ghost" id="vmlc-prev" ${i <= 0 ? 'disabled' : ''}>‹</button>
      <input type="date" class="input" id="vmlc-fecha" value="${FECHA}" style="width:auto">
      <button class="btn btn-ghost" id="vmlc-next" ${i >= DIAS.length - 1 ? 'disabled' : ''}>›</button>
      <span class="vml-fechalbl">${esc(fechaLarga(FECHA))}</span>`;
  }

  // Estado + acción por venta.
  const ventaRow = v => {
    let chip = '', acc = '', done = '';
    const deferred = !!v.conciliacion_periodo;
    if (estaConciliada(v)) {
      chip = '<span class="vmlc-ok">✓ conciliada</span>'; done = 'vmlc-done';
      acc = `<button class="btn vml-mini vmlc-desv" data-accion="desv" data-venta="${v.id}">Deshacer</button>`;
    } else if (v.ml_status === 'cancelled' || v.devuelta) {
      const lbl = v.ml_status === 'cancelled' ? 'cancelada' : 'devuelta';
      if (cancMovPend(v)) { chip = `<span class="vmlc-chip-canc">${lbl}</span>`; acc = `<button class="btn btn-primary vml-mini" data-accion="concmov" data-venta="${v.id}">Conciliar mov.</button>`; }
      else { chip = `<span class="vmlc-chip-canc">${lbl}</span> <span class="vmlc-ok">✓</span>`; done = 'vmlc-done'; }
    } else {
      const I = infoMatch(v, BONIF_MAP);
      if (I.sinCobro) chip = '<span class="vmlc-no">sin cobro</span>';
      else if (I.cierra) { chip = '<span class="vmlc-ok">✓ cierra</span>'; acc = `<button class="btn btn-primary vml-mini" data-accion="conc" data-venta="${v.id}">Conciliar</button>`; }
      else chip = `<span class="vmlc-rev">${I.falta > 0 ? 'falta ' + money(I.falta) : 'sobra ' + money(I.falta)}</span>`;
      // Las que no cierran se pueden empujar al mes siguiente para cerrar el mes actual.
      if (!I.cierra) acc += `<button class="btn vml-mini vmlc-dif" data-accion="diferir" data-venta="${v.id}" title="Pasar esta venta al mes siguiente para conciliarla ahí">Diferir →</button>`;
    }
    if (deferred) {
      chip += ` <span class="vmlc-difbadge" title="Movida a ${esc(v.conciliacion_periodo)}">diferida</span>`;
      acc += ` <button class="btn vml-mini vmlc-traer" data-accion="traer" data-venta="${v.id}" title="Devolver al mes de la venta">↩</button>`;
    }
    return `<tr data-venta="${v.id}" class="${String(v.id) === String(CONC_SEL) ? 'vmlc-sel' : ''} ${done}">
        <td><a class="vml-venta-id" href="https://www.mercadolibre.com.ar/ventas/${esc(v.ml_order_id)}/detalle" target="_blank" rel="noopener">${esc(v.ml_order_id || v.id)}</a></td>
        <td class="vml-mono vmlc-fe">${v.fecha_entrega ? esc(ddmm(v.fecha_entrega)) : '—'}</td>
        <td class="vml-mono">${esc(v.sku || '—')}</td>
        <td class="vmlc-prod" title="${esc(v.titulo || '')}">${esc((v.titulo || '').slice(0, 30))}</td>
        <td style="text-align:right" class="vml-mono vml-fuerte">${money(objetivoDe(v))}</td>
        <td>${chip}</td>
        <td style="text-align:right">${acc}</td>
      </tr>`;
  };
  // Pendientes arriba (la seleccionada primero); las ya conciliadas bajan al fondo
  // bajo un separador, y se pueden ocultar con el check. Los KPIs igual las cuentan.
  const pendV0 = ventas.filter(v => !resuelta(v));
  const pendV = sel && pendV0.includes(sel) ? [sel, ...pendV0.filter(v => v !== sel)] : pendV0;
  const doneV = ventas.filter(v => resuelta(v));
  const divV = (doneV.length && !CONC_HIDE_DONE) ? `<tr class="vmlc-divider"><td colspan="7">— ya conciliadas (${doneV.length}) —</td></tr>` : '';
  const cuerpoV = pendV.map(ventaRow).join('') + divV + (CONC_HIDE_DONE ? '' : doneV.map(ventaRow).join(''));
  const filasVentas = !ventas.length
    ? `<tr><td colspan="7" class="vmlc-empty">No hay ventas en este período.</td></tr>`
    : (cuerpoV || `<tr><td colspan="7" class="vmlc-empty">✓ Todas las ventas del mes están conciliadas.</td></tr>`);

  // Fila del extracto de MP (todo el movimiento de la cuenta, con su estado).
  const movRow = m => {
    const ref = String(m.referencia_externa || '').split('|')[1] || '';
    const linkedV = linkMov[m.id];         // ya vinculado a una venta (cualquier mes)
    const inMonthV = mov2venta[m.id];       // venta visible en este mes (para seleccionar)
    const anyV = ventaByRef[ref];           // venta en cualquier mes (para traer)
    let estado, dataAttr = '';
    if (linkedV) estado = `<span class="vmlc-ok">→ #${esc(ordById[linkedV] || linkedV)}</span>`;
    else if (inMonthV != null && curVentaIds.has(String(inMonthV))) { estado = m.conciliado_auto ? '<span class="vmlc-auto">auto</span>' : '<span class="vmlc-pend">pendiente</span>'; dataAttr = ` data-venta="${inMonthV}"`; }
    else if (anyV) { estado = `<span class="vmlc-traible">venta de ${esc(efPeriodo(anyV))} · traer ↰</span>`; dataAttr = ` data-traer="${anyV.id}"`; }
    else estado = m.conciliado_auto ? '<span class="vmlc-auto">auto</span>' : '<span class="vmlc-pend">sin venta</span>';
    const isSel = selMovs.has(m.id);
    return `<tr class="${isSel ? 'vmlc-sug' : (linkedV ? 'vmlc-done' : '')}" data-mov="${m.id}"${dataAttr}>
        <td class="vml-mono">${esc(ddmm(m.fecha))}</td>
        <td class="vml-mono vmlc-ref">${esc(ref)}</td>
        <td class="vmlc-desc" title="${esc(m.descripcion || '')}">${esc((m.descripcion || '').slice(0, 38))}</td>
        <td style="text-align:right" class="vml-mono ${Number(m.monto) < 0 ? 'vml-neg' : 'vml-pos'}">${money(m.monto)}</td>
        <td>${estado}${isSel ? ' <span class="vmlc-sugbadge">sugerido</span>' : ''}</td>
      </tr>`;
  };
  // Orden: sugeridos (de la venta elegida) → pendientes sin vincular → ya vinculados/
  // automáticos (al fondo, ocultables). Así lo actionable queda arriba y no marea.
  const isDoneMov = m => !!(linkMov[m.id] || m.conciliado_auto);
  const movSug = movsMes.filter(m => selMovs.has(m.id));
  const movPend = movsMes.filter(m => !selMovs.has(m.id) && !isDoneMov(m));
  const movDone = movsMes.filter(m => !selMovs.has(m.id) && isDoneMov(m));
  const divPend = (movSug.length && movPend.length) ? `<tr class="vmlc-divider"><td colspan="5">— resto pendientes del extracto —</td></tr>` : '';
  const divDone = (movDone.length && !CONC_HIDE_DONE && (movSug.length || movPend.length)) ? `<tr class="vmlc-divider"><td colspan="5">— ya vinculados / automáticos (${movDone.length}) —</td></tr>` : '';
  const cuerpoM = movSug.map(movRow).join('') + divPend + movPend.map(movRow).join('') + divDone + (CONC_HIDE_DONE ? '' : movDone.map(movRow).join(''));
  const filasMovs = !movsMes.length
    ? `<tr><td colspan="5" class="vmlc-empty">No hay movimientos de MP en este período.</td></tr>`
    : (cuerpoM || `<tr><td colspan="5" class="vmlc-empty">✓ Todo el extracto del mes está conciliado.</td></tr>`);

  // Barra de selección (bidireccional).
  let selResumen;
  if (!sel) selResumen = `<div class="vmlc-selbar">Seleccioná una venta (izquierda) o un movimiento (derecha) para ver su contraparte.</div>`;
  else if (estaConciliada(sel)) selResumen = `<div class="vmlc-selbar">Venta <b>#${esc(sel.ml_order_id || sel.id)}</b> · <span class="vmlc-ok">conciliada ✓</span> — ${(VINC_BY_VENTA[sel.id] || []).length} movimiento(s) vinculado(s).</div>`;
  else if (sel.ml_status === 'cancelled' || sel.devuelta) selResumen = `<div class="vmlc-selbar">Venta <b>#${esc(sel.ml_order_id || sel.id)}</b> · <span class="vmlc-chip-canc">${sel.ml_status === 'cancelled' ? 'cancelada' : 'devuelta'}</span> — ${cancMovPend(sel) ? 'tiene movimientos del extracto sin conciliar' : 'movimientos resueltos ✓'}.</div>`;
  else selResumen = `<div class="vmlc-selbar">Venta <b>#${esc(sel.ml_order_id || sel.id)}</b> · objetivo <b>${money(selI.obj)}</b> · sugerido <b>${money(selI.sum)}</b>${selI.bonif ? ` + bonif. <b>${money(selI.bonif.monto)}</b>` : ''} · ${selI.cierra ? '<span class="vmlc-ok">cierra ✓</span>' : `<span class="vmlc-rev">${selI.sinCobro ? 'sin cobro' : (selI.falta > 0 ? 'falta ' + money(selI.falta) : 'sobra ' + money(selI.falta))}</span>`}</div>`;

  root.innerHTML = `
    ${mlTabs('ventas_ml')}
    <div class="vml-bar">
      <button class="btn btn-ghost" id="vmlc-back">‹ Volver a la lista</button>
      <div class="vml-modo">
        <button class="${!esMes ? 'active' : ''}" data-modo="dia">Día</button>
        <button class="${esMes ? 'active' : ''}" data-modo="mes">Mes</button>
      </div>
      ${navHTML}
      <label class="vmlc-hidechk"><input type="checkbox" id="vmlc-hide" ${CONC_HIDE_DONE ? 'checked' : ''}> Ocultar conciliadas</label>
    </div>
    ${kpi}
    <div class="vml-sub" style="margin-top:4px">Conciliación mensual bidireccional: clic en una venta (izq.) o en un movimiento (der.) y se resalta su contraparte sugerida. Confirmás vos, venta por venta.</div>
    ${selResumen}
    <div class="vmlc-grid">
      <div class="card vmlc-col" id="vmlc-left">
        <div class="card-title">Ventas del mes <span class="vmlc-hint">(clic para seleccionar)</span></div>
        <div class="table-wrap"><table class="t vmlc-tabla">
          <thead><tr><th># Venta</th><th>Entrega</th><th>SKU</th><th>Producto</th><th style="text-align:right">Objetivo</th><th>Estado</th><th style="text-align:right">Acción</th></tr></thead>
          <tbody>${filasVentas}</tbody>
        </table></div>
      </div>
      <div class="card vmlc-col" id="vmlc-right">
        <div class="card-title">Extracto de Mercado Pago del mes <span class="vmlc-hint">(resaltados = de la venta elegida)</span></div>
        <div class="table-wrap"><table class="t vmlc-tabla">
          <thead><tr><th>Fecha</th><th>N° op.</th><th>Descripción</th><th style="text-align:right">Monto</th><th>Estado</th></tr></thead>
          <tbody>${filasMovs}</tbody>
        </table></div>
      </div>
    </div>
  `;

  document.getElementById('vmlc-back').addEventListener('click', () => { VISTA = 'control'; render(); });
  root.querySelectorAll('.vml-modo button').forEach(b =>
    b.addEventListener('click', () => { MODO = b.dataset.modo; CONC_SEL = null; render(); }));
  const prev = document.getElementById('vmlc-prev');
  const next = document.getElementById('vmlc-next');
  if (prev) prev.addEventListener('click', () => { CONC_SEL = null; paso(-1); });
  if (next) next.addEventListener('click', () => { CONC_SEL = null; paso(1); });
  const inpF = document.getElementById('vmlc-fecha');
  const inpM = document.getElementById('vmlc-mes');
  if (inpF) inpF.addEventListener('change', e => { FECHA = e.target.value; CONC_SEL = null; render(); });
  if (inpM) inpM.addEventListener('change', e => { MES = e.target.value; CONC_SEL = null; render(); });
  const hideChk = document.getElementById('vmlc-hide');
  if (hideChk) hideChk.addEventListener('change', e => { CONC_HIDE_DONE = e.target.checked; render(); });
  document.getElementById('vmlc-left').addEventListener('click', e => {
    const btn = e.target.closest('button[data-accion]');
    if (btn) {
      e.stopPropagation();
      const id = btn.dataset.venta, a = btn.dataset.accion;
      if (a === 'conc') conciliar(id);
      else if (a === 'concmov') conciliarMovimientos(id);
      else if (a === 'desv') desvincular(id);
      else if (a === 'diferir') diferirVenta(id, nextPeriodo(MES));
      else if (a === 'traer') diferirVenta(id, null);
      return;
    }
    const row = e.target.closest('tr[data-venta]');
    if (row) { CONC_SEL = row.dataset.venta; render(); }
  });
  document.getElementById('vmlc-right').addEventListener('click', e => {
    const rowV = e.target.closest('tr[data-venta]');
    if (rowV) { CONC_SEL = rowV.dataset.venta; render(); return; }
    const rowT = e.target.closest('tr[data-traer]');
    if (rowT) {
      const vid = rowT.dataset.traer;
      const v = VENTAS.find(x => String(x.id) === String(vid));
      const mesV = v ? (v.conciliacion_periodo || mesDe(v.fecha)) : '?';
      if (confirm(`Esta venta (#${v ? (v.ml_order_id || v.id) : vid}) es de ${mesV}. ¿Traerla a ${MES} para conciliarla acá?`)) diferirVenta(vid, MES, true);
      return;
    }
    const m = e.target.closest('tr[data-mov]');
    if (m) window.toast('Ese movimiento no corresponde a una venta de ADARA (posible cargo de ML o venta 2025)');
  });
}

function cobroCellInner(v, bonifMap) {
  // Cancelada: dos ejes a resolver — plata (movimientos del AS vinculados) y, si
  // salió del depósito, stock (recepción). La acción se hace desde el detalle.
  // `cancelled` ≠ "no salió"; las despachadas/entregadas pueden sobrestimar stock
  // hasta saber si el producto volvió. Ver CANC2.
  if (v.ml_status === 'cancelled') {
    const salio = v.estado_envio === 'despachado' || v.estado_envio === 'entregado';
    const stockPend = salio && !v.recepcion_condicion;
    const movPend = cancMovPend(v);
    if (!stockPend && !movPend) {
      const r = v.recepcion_condicion ? (v.recepcion_condicion === 'ok' ? ' · 📦 reingresado' : v.recepcion_condicion === 'reacondicionar' ? ' · 🔧 reacondicionar' : ' · ❌ no disp.') : '';
      return `<span class="vml-conc">✓ resuelta${r}</span>`;
    }
    const falta = [];
    if (movPend)   falta.push('conciliar mov.');
    if (stockPend) falta.push('revisar recepción');
    return `<span class="vml-rec-pend" title="Abrí el detalle para resolver">⚠ ${falta.join(' · ')}</span>`;
  }
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
      : '—'}${v.nro_factura ? `<div style="font-size:11px;color:#6b7280;margin-top:2px;white-space:nowrap" title="Factura Tango">🧾 ${v.tipo_factura ? `<b>${esc(v.tipo_factura)}</b> ` : ''}${esc(v.nro_factura)}</div>` : ''}</td>
    <td>${esc(v.titulo || '—')}</td>
    <td class="vml-mono">${esc(v.sku || '—')}</td>
    <td style="text-align:right">${v.cantidad || 1}</td>
    <td style="text-align:right" class="vml-mono">${money(v.importe_bruto)}${factCell(v)}</td>
    ${celdaMonto(v.cargo_venta)}
    ${celdaMonto(v.cargo_envio)}
    ${celdaMonto(v.impuestos)}
    ${celdaMonto(v.costo_financiero)}
    <td style="text-align:right" class="vml-mono vml-fuerte">${money(v.por_cobrar)}</td>
    <td>${v.estado_envio ? `<span class="vml-env vml-env-${esc(v.estado_envio)}">${esc(v.estado_envio.replace(/_/g, ' '))}</span>` : '<span class="con-dash">—</span>'}</td>
    <td><span class="vml-est vml-est-${est.cls || 'ok'}">${esc(est.txt)}</span></td>
    <td>${cobroCell(v, bonifMap)}</td>
  </tr>`;
}

// Base del período tras el pill de estado (sin filtros de columna).
function ventasVisiblesBase() {
  const b = conjuntoActual();
  let r = FILTRO === 'todas' ? b : b.filter(v => clase(v) === FILTRO);
  if (FILTRO === 'canceladas' && SOLO_PEND_CANC) r = r.filter(cancPendiente);
  return r;
}

// Filtro por columna (estilo Excel) de la tabla de ventas.
function pasaColV(v) {
  const cl = clase(v);
  const est = (ESTADO_LBL[cl] && ESTADO_LBL[cl].txt) || cl;
  const num = (field, q) => { const x = q.replace(',', '.').replace(/[^\d.]/g, ''); return !x || Math.abs(Number(field) || 0).toFixed(2).includes(x); };
  if (COLV.estado && est !== COLV.estado) return false;
  if (COLV.fecha) { const h = ((v.fecha || '') + ' ' + ddmm(v.fecha)).toLowerCase(); if (!h.includes(COLV.fecha.toLowerCase())) return false; }
  if (COLV.venta && !String(v.ml_order_id || '').includes(COLV.venta)) return false;
  if (COLV.prod && !String(v.titulo || '').toLowerCase().includes(COLV.prod.toLowerCase())) return false;
  if (COLV.sku && !String(v.sku || '').toLowerCase().includes(COLV.sku.toLowerCase())) return false;
  if (COLV.cant) { const x = COLV.cant.replace(/[^\d]/g, ''); if (x && !String(v.cantidad || 1).includes(x)) return false; }
  if (COLV.bruto && !num(v.importe_bruto, COLV.bruto)) return false;
  if (COLV.com && !num(v.cargo_venta, COLV.com)) return false;
  if (COLV.envio && !num(v.cargo_envio, COLV.envio)) return false;
  if (COLV.imp && !num(v.impuestos, COLV.imp)) return false;
  if (COLV.fin && !num(v.costo_financiero, COLV.fin)) return false;
  if (COLV.cobrar && !num(v.por_cobrar, COLV.cobrar)) return false;
  if (COLV.envest && (v.estado_envio || '') !== COLV.envest) return false;
  return true;
}

// Fila de totales (suma de las columnas numéricas sobre lo filtrado).
function filaTotales(vis, esMes) {
  const sum = f => vis.reduce((s, v) => s + (Number(v[f]) || 0), 0);
  const cant = vis.reduce((s, v) => s + (Number(v.cantidad) || 1), 0);
  return `<tr class="vml-tot">
    ${esMes ? '<td></td>' : ''}
    <td class="vml-tot-lbl">Totales</td>
    <td>${vis.length.toLocaleString('es-AR')} ventas</td>
    <td></td>
    <td style="text-align:right">${cant.toLocaleString('es-AR')}</td>
    <td style="text-align:right" class="vml-mono">${money(sum('importe_bruto'))}</td>
    <td style="text-align:right" class="vml-mono">${money(sum('cargo_venta'))}</td>
    <td style="text-align:right" class="vml-mono">${money(sum('cargo_envio'))}</td>
    <td style="text-align:right" class="vml-mono">${money(sum('impuestos'))}</td>
    <td style="text-align:right" class="vml-mono">${money(sum('costo_financiero'))}</td>
    <td style="text-align:right" class="vml-mono vml-fuerte">${money(sum('por_cobrar'))}</td>
    <td></td><td></td><td></td>
  </tr>`;
}

// Repinta sólo el cuerpo y la fila de totales según los filtros de columna.
function pintarVML() {
  const tbody = document.getElementById('vml-tbody');
  if (!tbody) return;
  const esMes = MODO === 'mes';
  const base = ventasVisiblesBase();
  const vis = base.filter(pasaColV);
  tbody.innerHTML = vis.map(v => filaHTML(v, esMes, BONIF_MAP_CUR)).join('');
  const tfoot = document.getElementById('vml-tfoot');
  if (tfoot) tfoot.innerHTML = filaTotales(vis, esMes);
  const cnt = document.getElementById('vml-count');
  if (cnt) cnt.textContent = `Mostrando ${vis.length.toLocaleString('es-AR')} de ${base.length.toLocaleString('es-AR')}`;
  const empty = document.getElementById('vml-empty');
  if (empty) empty.style.display = vis.length ? 'none' : 'block';
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

  // Ejecuta la acción y cierra el modal: la acción recarga la lista (loadVentasML),
  // así la venta procesada se "limpia" y quedan a la vista las pendientes.
  const actuar = fn => { close(); fn(); };

  const pintar = (inner, actionsHTML = '', wire = null) => {
    overlay.querySelector('.modal').innerHTML = inner +
      `<div class="modal-actions">${actionsHTML}<button class="btn btn-ghost" id="vd-close">Cerrar</button></div>`;
    overlay.querySelector('#vd-close').addEventListener('click', close);
    if (wire) wire();
  };

  try {
    const r = await fetch('/venta/' + encodeURIComponent(ventaId) + '/detalle');
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));

    // Acciones contextuales según el estado (se decide con datos del cliente).
    const v = VENTAS.find(x => String(x.id) === String(ventaId));
    let actionsHTML = '', wire = null;
    if (v) {
      const wfns = [];
      const onClick = (id, fn) => wfns.push(() => overlay.querySelector('#' + id).addEventListener('click', () => actuar(fn)));

      if (v.ml_status === 'cancelled') {
        const acc = [];
        const salio = v.estado_envio === 'despachado' || v.estado_envio === 'entregado';
        const movPend = cancMovPend(v);
        const stockPend = salio && !v.recepcion_condicion;
        // Eje plata: vincular el bundle (cobro + devoluciones) por N° de operación.
        if (movPend) {
          acc.push(`<button class="btn btn-primary" id="vd-concmov" title="Vincula el cobro y la devolución (N° de operación) a esta venta">✓ Conciliar movimientos</button>`);
          onClick('vd-concmov', () => conciliarMovimientos(ventaId));
        }
        // Eje stock: solo si salió del depósito y no tiene recepción.
        if (stockPend) {
          acc.push(`<button class="btn ${movPend ? 'btn-ghost' : 'btn-primary'}" id="vd-rec-ok" title="Volvió y es vendible — no cambia el stock">📦 OK stock</button>`);
          acc.push(`<button class="btn btn-ghost" id="vd-rec-reac" title="Volvió pero no sano → va al depósito Reacondicionar">🔧 Reacondicionar</button>`);
          acc.push(`<button class="btn btn-ghost" id="vd-rec-no" title="No volvió → pérdida: descuenta stock">❌ No disp.</button>`);
          onClick('vd-rec-ok', () => recepcionCancelada(ventaId, 'ok'));
          onClick('vd-rec-reac', () => recepcionCancelada(ventaId, 'reacondicionar'));
          onClick('vd-rec-no', () => recepcionCancelada(ventaId, 'no_disponible'));
        }
        // Ya resuelta pero con movimientos vinculados → permitir deshacer.
        if (!acc.length && estaConciliada(v)) {
          acc.push(`<button class="btn btn-ghost" id="vd-desv">Deshacer vínculos</button>`);
          onClick('vd-desv', () => desvincular(ventaId));
        }
        actionsHTML = acc.join('');
      } else if (conciliable(v, asignarBonifs([v]))) {
        actionsHTML = `<button class="btn btn-primary" id="vd-conc">✓ Conciliar</button>`;
        onClick('vd-conc', () => conciliar(ventaId));
      } else if (estaConciliada(v)) {
        actionsHTML = `<button class="btn btn-ghost" id="vd-desv">Deshacer conciliación</button>`;
        onClick('vd-desv', () => desvincular(ventaId));
      }
      if (wfns.length) wire = () => wfns.forEach(f => f());
    }
    pintar(ventaDetalleHTML(data), actionsHTML, wire);
  } catch (e) {
    pintar(`<div class="card-title">Detalle de la venta</div><div class="error">No se pudo cargar: ${esc(e.message)}</div>`);
  }
}

// Arma el HTML del detalle a partir de { venta, cobros, devoluciones, retenciones }.
function ventaDetalleHTML({ venta, cobros, devoluciones, retenciones }) {
  const sum = arr => arr.reduce((s, x) => s + (Number(x.monto) || 0), 0);
  // Separa el N° de operación (≥8 dígitos al final, normalmente tras "·") para
  // mostrarlo completo y seleccionable, igual que en retenciones (la descripción
  // larga ya no lo recorta).
  const splitOpId = s => {
    s = String(s || '');
    const mm = s.match(/(\d{8,})\s*$/);
    if (!mm) return { desc: s || '—', op: '' };
    return { desc: s.slice(0, mm.index).replace(/[\s·\-]+$/, '') || '—', op: mm[1] };
  };
  const lineaMov = m => {
    const { desc, op } = splitOpId(m.descripcion || m.categoria || '—');
    return `<div class="vml-dev-linea">
      <span class="vml-det-fecha">${esc(ddmm(m.fecha))}</span>
      <span class="vml-dev-ldesc">${esc(desc)}${op ? ` <span class="vml-det-num">${esc(op)}</span>` : ''}</span>
      <span class="vml-mono ${m.monto < 0 ? 'vml-neg' : m.monto > 0 ? 'vml-pos' : 'vml-cero'}">${m.monto < 0 ? '−' : ''}${money(m.monto)}</span>
    </div>`;
  };
  const lineaRet = r => `<div class="vml-dev-linea">
      <span class="vml-det-fecha">${r.fecha ? esc(ddmm(r.fecha)) : '—'}</span>
      <span class="vml-dev-ldesc">${esc([r.tipo, r.jurisdiccion].filter(Boolean).join(' · ') || r.detail || '—')}${r.mp_source_id ? ` <span class="vml-det-num">${esc(String(r.mp_source_id))}</span>` : ''}</span>
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
      ${venta.nro_factura ? `<div class="vml-sub" style="margin:2px 0 0">🧾 Factura Tango ${venta.tipo_factura ? esc(venta.tipo_factura) + ' ' : ''}<b>${esc(venta.nro_factura)}</b>${venta.importe_facturado != null ? ` · facturado <b class="vml-mono">${money(venta.importe_facturado)}</b>` : ''}</div>` : ''}
      <div class="vml-sub" style="margin:4px 0 0">${esc(ddmm(venta.fecha))}/${(venta.fecha || '').slice(0, 4)} ·
        <span class="vml-est vml-est-${est.cls || 'ok'}">${esc(est.txt)}</span> ·
        Por cobrar <b class="vml-mono">${money(venta.por_cobrar)}</b>${venta.devuelta ? ' · <span class="vml-dev-claim">devuelta</span>' : ''}</div>
    </div>`;

  // Resultado neto de caja = cobros + devoluciones (los movimientos reales del
  // extracto). Las retenciones NO se suman: ya van embebidas en el cobro neto
  // liquidado (sumarlas duplicaría). Una cancelada con reintegro total cierra en 0.
  const neto = sum(cobros) + sum(devoluciones);
  const totalLinea = `
    <div class="vml-det-total">
      <span>Resultado neto<span class="vml-det-totsub">cobro + devoluciones · las retenciones ya van dentro del cobro</span></span>
      <span class="vml-mono ${neto < 0 ? 'vml-neg' : neto > 0 ? 'vml-pos' : ''}">${neto < 0 ? '−' : ''}${money(neto)}</span>
    </div>`;

  return `<div class="card-title">Detalle de la venta</div>${cab}
    ${bloque('Cobro', sum(cobros), cobros.map(lineaMov), 'Sin cobro conciliado todavía (puede faltar cargar el AS de ese mes).')}
    ${bloque('Impuestos / retenciones', sum(retenciones), retenciones.map(lineaRet), 'Sin retenciones cargadas para esta venta.')}
    ${bloque('Devoluciones', sum(devoluciones), devoluciones.map(lineaMov), 'Sin devoluciones.')}
    ${totalLinea}`;
}

// Refresco liviano tras una acción: re-baja SOLO los vínculos (las ventas, cobros,
// retenciones y devoluciones no cambian) y repinta. Mucho más rápido que loadVentasML.
async function refrescarVinculos() {
  try {
    const DEV_IDS = new Set(DEVOLS.map(d => d.id));
    const vinc = await sbGet('vinculos', 'op_tipo=eq.venta_ml&order=id.asc');
    VINC_BY_VENTA = {}; VINC_MOV_USADOS = new Set(); MOV_VINCULADOS = new Set(); DEV_LINK_BY_MOV = new Map();
    for (const v of vinc) {
      if (DEV_IDS.has(v.movimiento_id)) { MOV_VINCULADOS.add(v.movimiento_id); DEV_LINK_BY_MOV.set(v.movimiento_id, v); }
      else { (VINC_BY_VENTA[v.op_id] = VINC_BY_VENTA[v.op_id] || []).push(v); VINC_MOV_USADOS.add(v.movimiento_id); }
    }
    DEV_BUNDLES = null;
    render();
  } catch (e) {
    window.toast('No se pudo refrescar: ' + e.message, 'error');
  }
}

// Marca localmente (sin re-fetch) los movimientos de una venta como vinculados,
// para repintar al instante. Se re-sincroniza con el server al recargar la pantalla.
function vincularLocal(ventaId, cobroIds = [], devolIds = []) {
  for (const id of cobroIds) {
    if (id == null) continue;
    (VINC_BY_VENTA[ventaId] = VINC_BY_VENTA[ventaId] || []).push({ movimiento_id: id, op_id: ventaId, op_tipo: 'venta_ml' });
    VINC_MOV_USADOS.add(id);
  }
  for (const id of devolIds) if (id != null) MOV_VINCULADOS.add(id);
  DEV_BUNDLES = null;
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
    vincularLocal(v.id, aVincular.map(x => x.id));
    render();
  } catch (e) {
    window.toast('Error al conciliar: ' + e.message, 'error');
  }
}

// Registra la recepción física de una cancelada que salió del depósito.
//  - 'ok'           → el producto volvió y es vendible. NO cambia el stock
//                     (la cancelada nunca lo descontó: ADARA ya lo contaba).
//  - 'no_disponible'→ no volvió → pérdida: descuenta el stock por FIFO (faltante),
//                     corrigiendo la sobrestimación. Requiere nota.
// Semántica invertida vs devoluciones (ver server.js /ml/recepcion, CANC2).
async function recepcionCancelada(ventaId, condicion) {
  const v = VENTAS.find(x => String(x.id) === String(ventaId));
  const prod = v ? `${v.sku || ''} ${v.titulo || ''}`.trim() : ('venta #' + ventaId);
  let nota = null;
  if (condicion === 'no_disponible') {
    nota = prompt(`El producto NO volvió (pérdida). Se descontará del stock por FIFO.\n\n${prod}\n\nNota (obligatoria):`);
    if (nota === null) return;                 // canceló el prompt
    if (!nota.trim()) { window.toast('La nota es obligatoria', 'error'); return; }
  } else if (condicion === 'reacondicionar') {
    nota = prompt(`El producto volvió pero NO sano. Va al depósito Reacondicionar (recuperable con "Pasar a venta").\n\n${prod}\n\nNota (opcional, ej. qué tiene):`);
    if (nota === null) return;                 // canceló el prompt (nota no obligatoria)
  } else {
    if (!confirm(`Confirmás que el producto VOLVIÓ y es vendible?\n\n${prod}\n\nNo modifica el stock (ya estaba contado).`)) return;
  }

  window.toast('Registrando recepción…');
  try {
    const r = await fetch('/ml/recepcion', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ venta_id: ventaId, condicion, nota: nota?.trim() || undefined })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));

    if (condicion === 'ok') {
      window.toast('Recepción OK: producto reingresado');
    } else if (condicion === 'reacondicionar') {
      const rc = data.reac_stock || {};
      const u = Number(rc.unidades_a_reac) || 0;
      window.toast(u > 0 ? `A reacondicionar: ${u}u al depósito REAC` : 'Marcado a reacondicionar (sin stock que mover)');
    } else {
      const aj = data.ajuste_stock || {};
      const u = Number(aj.unidades_ajustadas) || 0;
      const falt = Number(aj.faltante) || 0;
      let msg = u > 0 ? `Marcado no disponible: −${u}u de stock (FIFO)` : 'Marcado no disponible';
      if (falt > 0) msg += ` · ⚠ ${falt}u sin lote para descontar`;
      window.toast(msg, falt > 0 ? 'error' : undefined);
    }
    if (v) v.recepcion_condicion = condicion;   // update local, sin re-fetch
    DEV_BUNDLES = null;
    render();
  } catch (e) {
    window.toast('Error al registrar la recepción: ' + e.message, 'error');
  }
}

// Concilia el bundle de movimientos del AS de una cancelada (cobro + devoluciones)
// vinculándolos a la venta por el N° de operación. Objetivo: que ninguna línea del
// extracto quede sin conciliar. No la mueve a "Conciliados" (sigue siendo cancelada);
// queda como "✓ resuelta" en Canceladas.
async function conciliarMovimientos(ventaId) {
  window.toast('Conciliando movimientos…');
  try {
    const r = await fetch('/venta/' + encodeURIComponent(ventaId) + '/conciliar-movimientos', { method: 'POST' });
    const data = await r.json().catch(() => ({}));
    if (!r.ok || !data.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    const n = data.vinculados || 0;
    const neto = Number(data.neto) || 0;
    window.toast(`Movimientos conciliados (${n} línea${n === 1 ? '' : 's'} · neto ${neto < 0 ? '−' : ''}${money(neto)})`);
    // Update local sin re-fetch: marcar el bundle (cobro + devoluciones) de la
    // venta como vinculado, según su(s) N° de operación.
    const v = VENTAS.find(x => String(x.id) === String(ventaId));
    if (v) {
      const pids = new Set([v.mp_payment_id, ...String(v.mp_payment_ids || '').split(',')]
        .map(s => String(s || '').trim()).filter(Boolean));
      const cobroIds = [...pids].map(pid => COBROS_BY_REF[pid]?.id).filter(x => x != null);
      const devolIds = DEVOLS.filter(d => pids.has(String(d.referencia || '').split('|')[1])).map(d => d.id);
      vincularLocal(v.id, cobroIds, devolIds);
      if (data.marcada_devuelta) v.devuelta = true;
    }
    render();
  } catch (e) {
    window.toast('Error al conciliar movimientos: ' + e.message, 'error');
  }
}

// Deshace TODOS los vínculos de la venta (cobro, bonificación y, en canceladas,
// las líneas de devolución del bundle). Trae los vínculos frescos del server
// (con su id) por si alguno se marcó localmente sin id.
async function desvincular(ventaId) {
  let vincs;
  try {
    vincs = await sbGet('vinculos', `op_tipo=eq.venta_ml&op_id=eq.${ventaId}&select=id`);
  } catch (e) { window.toast('Error: ' + e.message, 'error'); return; }
  if (!vincs.length) { window.toast('No hay vínculos para deshacer'); return; }
  if (!confirm('¿Deshacer la conciliación de esta venta? Los movimientos vuelven a quedar sin vincular.')) return;
  try {
    for (const vc of vincs) {
      const r = await fetch('/vincular/' + vc.id, { method: 'DELETE' });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    }
    window.toast('Conciliación deshecha');
    await refrescarVinculos();
  } catch (e) {
    window.toast('Error: ' + e.message, 'error');
  }
}

// Conjunto de ventas visible según el modo (día o mes). Lo usan render y "Conciliar todas".
function conjuntoActual() {
  if (MODO === 'mes') return VENTAS.filter(v => mesDe(v.fecha) === MES);
  return VENTAS.filter(v => v.fecha === FECHA);
}

// NOTA: el flujo automático "Conciliar todas" (funciones armarLoteConciliacion /
// conciliarTodas, que vinculaban en lote vía POST /vincular-lote) se retiró el
// 13/07/2026 por decisión de UX: la conciliación de ventas ML pasa a ser manual,
// venta por venta, desde la vista de dos tablas (renderConciliar → conciliar()).
// El endpoint /vincular-lote sigue existiendo en el backend por si se reusa.

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
    await refrescarVinculos();
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
    await refrescarVinculos();
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
    .vml-dev-ldesc{color:#57534E;overflow-wrap:anywhere;white-space:normal}
    .vml-det-total{display:flex;justify-content:space-between;align-items:baseline;gap:10px;margin-top:14px;border-top:2px solid #D6D3D1;padding-top:10px;font-weight:700;font-size:13px}
    .vml-det-totsub{display:block;font-size:11px;color:#A8A29E;font-weight:400;margin-top:2px}
    .vml-row-click{cursor:pointer}
    .vml-row-click:hover td{background:#FAF7F2}
    .vml-detalle-modal{max-width:560px;width:92vw}
    .vml-detalle-modal .modal-actions{display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap;align-items:center;margin-top:18px;padding-top:14px;border-top:1px solid #EFEAE3}
    .vml-detalle-modal .modal-actions .btn{border-radius:9px;padding:9px 16px;font-weight:600;font-size:13px;transition:filter .12s,background .12s}
    .vml-detalle-modal .modal-actions .btn:hover{filter:brightness(.96)}
    #vd-rec-ok{background:#0F6E56;border:1px solid #0F6E56;color:#fff}
    #vd-rec-no{background:#fff;border:1px solid #E7B7B0;color:#B91C1C}
    #vd-rec-no:hover{background:#FBEDEB;filter:none}
    #vd-rec-reac{background:#fff;border:1px solid #E3C48A;color:#9A6B00}
    #vd-rec-reac:hover{background:#FBF3DF;filter:none}
    #vd-conc,#vd-concmov{background:#0C447C;border:1px solid #0C447C;color:#fff}
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
    .vml-rec-pend{font-size:12px;font-weight:600;color:#9A3412;background:#FEEFE6;border:1px solid #F5C9AE;border-radius:6px;padding:2px 8px;white-space:nowrap}
    .vml-pendchk{display:inline-flex;align-items:center;gap:6px;font-size:13px;color:#57534E;cursor:pointer;user-select:none}
    .vml-pendchk input{cursor:pointer}
    .vml-mini{padding:5px 12px;font-size:13px;border-radius:8px;font-weight:600}
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
    .vml-fbar{display:flex;justify-content:space-between;align-items:center;gap:12px;margin:12px 0 6px}
    .vml-count-lbl{font-size:13px;color:#78716C}
    .vml-clear{border:1px solid #E7E5E4;background:#fff;color:#57534E;border-radius:6px;cursor:pointer;font-size:12px;padding:5px 11px}
    .vml-clear:hover{border-color:#B91C1C;color:#B91C1C}
    .vml-filtros th{padding:4px 6px;background:#FAFAF9;border-top:1px solid #E7E5E4}
    .vml-filtros .vmlf{width:100%;box-sizing:border-box;font-size:12px;padding:4px 6px;border:1px solid #E7E5E4;border-radius:6px;background:#fff;font-family:inherit;color:#1C1917}
    .vml-filtros .vmlf:focus{outline:none;border-color:#0F6E56;box-shadow:0 0 0 2px rgba(15,110,86,.13)}
    .vml-tot td{position:sticky;bottom:0;background:#F5F5F4;border-top:2px solid #D6D3D1;font-weight:600;padding:8px 6px}
    .vml-tot-lbl{color:#1C1917}
    .vml-env{font-size:11px;padding:2px 7px;border-radius:6px;white-space:nowrap;text-transform:capitalize}
    .vml-env-entregado{background:#E1F5EE;color:#0F6E56}
    .vml-env-despachado{background:#E6F1FB;color:#0C447C}
    .vml-env-no_preparado{background:#F1EFEC;color:#78716C}
    /* ── Vista de conciliación manual (dos tablas) ── */
    .vmlc-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:12px}
    @media(max-width:1000px){.vmlc-grid{grid-template-columns:1fr}}
    .vmlc-col{padding:12px}
    .vmlc-tabla{font-size:13px}
    .vmlc-tabla th{white-space:nowrap}
    .vmlc-hint{font-size:11px;color:#A8A29E;font-weight:400}
    .vmlc-prod{max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .vmlc-desc{max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#57534E}
    .vmlc-ref{color:#0C447C}
    .vmlc-tabla tbody tr[data-venta]{cursor:pointer}
    .vmlc-tabla tbody tr[data-venta]:hover{background:#FBFAF6}
    .vmlc-sel{background:#FEF6E7!important;box-shadow:inset 3px 0 0 #D97706}
    .vmlc-sug{background:#E1F5EE}
    .vmlc-sug:hover{background:#D3EFE5}
    .vmlc-sugbadge{font-size:11px;color:#0F6E56;background:#CDEDE1;border-radius:6px;padding:1px 7px;font-weight:600}
    .vmlc-sug td{border-top:1px solid #B7E3D3}
    .vmlc-divider td{background:#F5F5F4;color:#A8A29E;font-size:11px;text-align:center;padding:5px;text-transform:uppercase;letter-spacing:.04em}
    .vmlc-kpi{display:flex;gap:14px;flex-wrap:wrap;margin:12px 0 6px}
    .vmlc-kpi-card{display:flex;align-items:center;gap:12px;background:#fff;border:1px solid #EFEAE3;border-radius:12px;padding:12px 16px;min-width:220px}
    .vmlc-kpi-plain{flex-direction:column;align-items:flex-start;gap:1px;justify-content:center}
    .vmlc-ring{flex:none}
    .vmlc-ring-pct{font-family:'Manrope',system-ui,sans-serif;font-size:17px;font-weight:700;fill:#1C1917}
    .vmlc-kpi-lbl{font-size:12px;color:#78716C}
    .vmlc-kpi-num{font-size:22px;font-weight:700;color:#1C1917}
    .vmlc-kpi-de{font-size:13px;color:#A8A29E;font-weight:400}
    .vmlc-kpi-big{font-size:30px;font-weight:700;color:#D97706;line-height:1.1}
    .vmlc-kpi-sub{font-size:12px;color:#78716C}
    .vmlc-fe{color:#57534E}
    .vmlc-chip-canc{font-size:12px;color:#B42318;background:#FBEAEA;border-radius:6px;padding:2px 8px}
    .vmlc-auto{font-size:12px;color:#78716C;background:#F1EFEC;border-radius:6px;padding:2px 8px}
    .vmlc-pend{font-size:12px;color:#92500A;background:#FAF1E1;border-radius:6px;padding:2px 8px}
    .vmlc-done td{opacity:.6}
    .vmlc-sel.vmlc-done td{opacity:1}
    .vmlc-desv{color:#B91C1C}
    #vmlc-right tbody tr[data-mov]{cursor:pointer}
    #vmlc-right tbody tr[data-mov]:hover{background:#FBFAF6}
    .vmlc-hidechk{display:inline-flex;align-items:center;gap:6px;font-size:13px;color:#57534E;cursor:pointer;user-select:none}
    .vmlc-hidechk input{cursor:pointer}
    .vmlc-dif{color:#0C447C;margin-left:4px}
    .vmlc-traer{color:#78716C;padding:5px 8px;margin-left:2px}
    .vmlc-difbadge{font-size:11px;color:#0C447C;background:#E6F1FB;border-radius:6px;padding:1px 7px}
    .vmlc-traible{font-size:12px;color:#0C447C;background:#E6F1FB;border-radius:6px;padding:2px 8px;font-weight:600}
    .vmlc-ok{color:#0F6E56;font-weight:600}
    .vmlc-rev{font-size:12px;color:#92500A;background:#FAF1E1;border-radius:6px;padding:2px 8px}
    .vmlc-no{font-size:12px;color:#857a5c;background:#FAF6EC;border:1px dashed #E3D9BE;border-radius:6px;padding:2px 8px}
    .vmlc-empty{text-align:center;color:#A8A29E;padding:16px}
    .vmlc-selbar{margin:8px 0 4px;font-size:13px;color:#57534E;background:#FAFAF9;border:1px solid #EFEAE3;border-radius:8px;padding:8px 12px}
    .vmlc-cuenta{font-size:13px;color:#78716C;margin-left:4px}
  `;
  const style = document.createElement('style');
  style.id = 'vml-style';
  style.textContent = css;
  document.head.appendChild(style);
}

// ── Modal de exportación XLSX ──────────────────────────────────────────
function openExportModal() {
  const hoy = hoyISO();
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal" style="max-width:380px">
      <h3 style="margin:0 0 16px">Exportar reporte XLSX</h3>
      <p style="font-size:13px;color:#78716C;margin:0 0 14px">Genera un Excel con una hoja por SKU y hoja RESUMEN.</p>
      <div class="field">
        <label>Desde</label>
        <input type="date" class="input" id="exp-desde" value="${hoy}" style="width:100%">
      </div>
      <div class="field" style="margin-top:10px">
        <label>Hasta</label>
        <input type="date" class="input" id="exp-hasta" value="${hoy}" style="width:100%">
      </div>
      <div class="modal-actions" style="margin-top:18px">
        <button class="btn" id="exp-cancel">Cancelar</button>
        <button class="btn btn-primary" id="exp-go">📥 Descargar</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);

  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#exp-cancel').addEventListener('click', close);
  overlay.querySelector('#exp-go').addEventListener('click', async () => {
    const desde = overlay.querySelector('#exp-desde').value;
    const hasta = overlay.querySelector('#exp-hasta').value;
    if (!desde || !hasta) { window.toast('Seleccioná ambas fechas', 'error'); return; }
    if (desde > hasta) { window.toast('Desde no puede ser mayor que Hasta', 'error'); return; }
    const btn = overlay.querySelector('#exp-go');
    btn.disabled = true;
    btn.textContent = '⏳ Generando…';
    try {
      await exportarVentasXLSX(desde, hasta);
      close();
    } catch (err) {
      window.toast('Error: ' + err.message, 'error');
      btn.disabled = false;
      btn.textContent = '📥 Descargar';
    }
  });
}
