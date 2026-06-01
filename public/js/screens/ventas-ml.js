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
let BONIFS = [];             // movimientos cobro_venta de "bonificación por envío"
let VINC_BY_VENTA = {};      // ventas_ml.id -> [vínculos] (op_tipo='venta_ml')
let VINC_MOV_USADOS = new Set(); // movimiento_id ya vinculados a alguna venta
let DIAS = [];
let MESES = [];
let MODO = 'dia';            // dia | mes
let FECHA = '';              // YYYY-MM-DD (modo día)
let MES = '';                // YYYY-MM (modo mes)
let FILTRO = 'todas';        // todas | por_cobrar | cobradas | conciliadas | canceladas | devueltas

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
function matchCobros(v) {
  const vistos = new Set();
  const cobros = [];
  for (const id of paymentIds(v)) {
    const c = COBROS_BY_REF[id];
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
    const falta = r2((Number(v.por_cobrar) || 0) - sum);
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
  const pc = Number(v.por_cobrar) || 0;
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
  try {
    VENTAS = await sbGet('ventas_ml', 'order=fecha.asc,hora_venta.asc');
    const cobros = await sbGet('movimientos', 'categoria=eq.cobro_venta&order=fecha.asc');
    COBROS_BY_REF = {};
    BONIFS = [];
    for (const c of cobros) {
      const ref = String(c.referencia_externa || '').split('|')[1];
      if (ref) COBROS_BY_REF[ref.trim()] = c;
      if (/bonific/i.test(c.descripcion || '')) BONIFS.push(c);
    }
    const vinc = await sbGet('vinculos', 'op_tipo=eq.venta_ml');
    VINC_BY_VENTA = {};
    VINC_MOV_USADOS = new Set();
    for (const v of vinc) {
      (VINC_BY_VENTA[v.op_id] = VINC_BY_VENTA[v.op_id] || []).push(v);
      VINC_MOV_USADOS.add(v.movimiento_id);
    }
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudieron cargar las ventas: ${esc(e.message)}</div>`;
    return;
  }
  DIAS = [...new Set(VENTAS.map(v => v.fecha).filter(Boolean))].sort();
  MESES = [...new Set(VENTAS.map(v => mesDe(v.fecha)).filter(Boolean))].sort();
  if (!FECHA || !DIAS.includes(FECHA)) FECHA = DIAS.length ? DIAS[DIAS.length - 1] : '';
  if (!MES || !MESES.includes(MES)) MES = MESES.length ? MESES[MESES.length - 1] : '';
  inyectarEstilo();
  render();
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

function render() {
  const root = document.getElementById('app-screens');
  const hayVentas = VENTAS.length > 0;
  const esMes = MODO === 'mes';
  const base = !hayVentas ? [] : conjuntoActual();

  const BONIF_MAP = asignarBonifs(base);
  const conciliablesN = base.filter(v => conciliable(v, BONIF_MAP)).length;

  const cont = { todas: base.length, por_cobrar: 0, cobradas: 0, conciliadas: 0, canceladas: 0, devueltas: 0 };
  base.forEach(v => { cont[clase(v)]++; });

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
      ${conciliablesN > 0 ? `<button class="btn btn-conc" id="vml-conc-todas">✓ Conciliar todas (${conciliablesN})</button>` : ''}
    </div>

    ${hayVentas ? `
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
    const tabla = document.getElementById('vml-tabla');
    if (tabla) tabla.addEventListener('click', onTablaClick);
    const cTodas = document.getElementById('vml-conc-todas');
    if (cTodas) cTodas.addEventListener('click', conciliarTodas);
  }
}

function celdaMonto(valor) {
  const n = Number(valor) || 0;
  if (n === 0) return `<td style="text-align:right" class="vml-mono vml-cero">—</td>`;
  return `<td style="text-align:right" class="vml-mono ${n < 0 ? 'vml-neg' : 'vml-pos'}">${money(n)}</td>`;
}

function cobroCell(v, bonifMap) {
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

function filaHTML(v, esMes, bonifMap) {
  const cl = clase(v);
  const est = ESTADO_LBL[cl];
  const rowCls = est.cls === 'canc' ? 'vml-row-canc' : est.cls === 'dev' ? 'vml-row-dev' : '';

  return `<tr class="${rowCls}">
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
  if (!btn) return;
  if (btn.dataset.accion === 'conciliar') conciliar(btn.dataset.venta);
  else if (btn.dataset.accion === 'desvincular') desvincular(btn.dataset.venta);
}

// Vincula el pago principal y, si hace falta para llegar al por_cobrar, también
// la bonificación de envío. Cada vínculo imputa el monto de su propio movimiento.
async function conciliar(ventaId) {
  const v = VENTAS.find(x => String(x.id) === String(ventaId));
  if (!v) return;
  const cobros = matchCobros(v);
  if (!cobros.length) { window.toast('Esta venta no tiene cobro para conciliar', 'error'); return; }
  const sum = cobros.reduce((s, c) => s + (Number(c.monto) || 0), 0);
  const pc = Number(v.por_cobrar) || 0;

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
    const pc = Number(v.por_cobrar) || 0;
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

function inyectarEstilo() {
  if (document.getElementById('vml-style')) return;
  const css = `
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
  `;
  const style = document.createElement('style');
  style.id = 'vml-style';
  style.textContent = css;
  document.head.appendChild(style);
}
