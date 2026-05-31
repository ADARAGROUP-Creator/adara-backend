import { sbGet } from '../core/sb.js';

// ── Pantalla: Ventas ML (solo lectura + sincronización) ────────────────
// Lista las ventas de ML día por día con sus números reales (bruto, comisión,
// envío, por cobrar) y cruza cada venta contra los cobros del extracto de MP
// para mostrar cuáles ya se liquidaron y cuáles no.
//
// El botón "Sincronizar" dispara POST /ml/sync (trae las ventas de ML al rango
// elegido). Esta versión NO concilia nada — es para validar que el sync trajo
// bien los datos y que el cruce venta ↔ cobro funciona, antes de sumar el botón
// de conciliar. El cruce se hace por mp_payment_id (el REFERENCE_ID del extracto
// queda embebido en movimientos.referencia_externa como "fecha|REF|monto|saldo").

let VENTAS = [];          // todas las ventas_ml
let COBROS_BY_REF = {};   // REFERENCE_ID del extracto -> movimiento (cobro_venta)
let DIAS = [];            // días únicos con ventas (asc)
let FECHA = '';           // día seleccionado

const money = n => '$ ' + Math.abs(Number(n) || 0).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ddmm = f => `${(f || '').slice(8, 10)}/${(f || '').slice(5, 7)}`;
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const hoyISO = () => new Date().toISOString().slice(0, 10);
const fechaLarga = f => {
  if (!f) return '—';
  const [y, m, d] = f.split('-');
  return new Date(+y, +m - 1, +d).toLocaleDateString('es-AR', { weekday: 'long', day: 'numeric', month: 'long' });
};

function estadoVenta(v) {
  if (v.ml_status === 'cancelled') return { txt: 'Cancelada', cls: 'canc' };
  if (v.devuelta) return { txt: 'Devuelta', cls: 'dev' };
  const map = { entregado: 'Entregada', despachado: 'Despachada', preparado: 'Preparada', no_preparado: 'Sin preparar' };
  return { txt: map[v.estado_envio] || (v.estado_envio || '—'), cls: '' };
}

function paymentIds(v) {
  const ids = [];
  if (v.mp_payment_id) ids.push(String(v.mp_payment_id).trim());
  if (v.mp_payment_ids) String(v.mp_payment_ids).split(',').forEach(x => { const t = x.trim(); if (t) ids.push(t); });
  return [...new Set(ids)];
}

function matchCobros(v) {
  const cobros = [];
  for (const id of paymentIds(v)) if (COBROS_BY_REF[id]) cobros.push(COBROS_BY_REF[id]);
  return cobros;
}

export async function loadVentasML() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando ventas de ML…</div>`;
  try {
    VENTAS = await sbGet('ventas_ml', 'order=fecha.asc,hora_venta.asc');
    const cobros = await sbGet('movimientos', 'categoria=eq.cobro_venta&order=fecha.asc');
    COBROS_BY_REF = {};
    for (const c of cobros) {
      const ref = String(c.referencia_externa || '').split('|')[1];
      if (ref) COBROS_BY_REF[ref.trim()] = c;
    }
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudieron cargar las ventas: ${esc(e.message)}</div>`;
    return;
  }

  DIAS = [...new Set(VENTAS.map(v => v.fecha).filter(Boolean))].sort();
  if (!FECHA || !DIAS.includes(FECHA)) FECHA = DIAS.length ? DIAS[DIAS.length - 1] : '';

  inyectarEstilo();
  render();
}

function pasoDia(delta) {
  if (!DIAS.length) return;
  let i = DIAS.indexOf(FECHA);
  if (i === -1) i = DIAS.length - 1;
  FECHA = DIAS[Math.min(DIAS.length - 1, Math.max(0, i + delta))];
  render();
}

function render() {
  const root = document.getElementById('app-screens');
  const hayVentas = VENTAS.length > 0;
  const delDia = hayVentas ? VENTAS.filter(v => v.fecha === FECHA) : [];

  const totBruto = delDia.reduce((s, v) => s + (Number(v.importe_bruto) || 0), 0);
  const totCobrar = delDia.reduce((s, v) => s + (Number(v.por_cobrar) || 0), 0);
  const conCobro = delDia.filter(v => matchCobros(v).length > 0).length;

  const idx = DIAS.indexOf(FECHA);
  const prevDis = idx <= 0 ? 'disabled' : '';
  const nextDis = idx >= DIAS.length - 1 ? 'disabled' : '';

  const navHTML = hayVentas ? `
    <button class="btn btn-ghost" id="vml-prev" ${prevDis}>‹ Día anterior</button>
    <input type="date" class="input" id="vml-fecha" value="${FECHA}" style="width:auto">
    <button class="btn btn-ghost" id="vml-next" ${nextDis}>Día siguiente ›</button>
    <span class="vml-fechalbl">${esc(fechaLarga(FECHA))}</span>` : '';

  root.innerHTML = `
    <div class="vml-bar">
      <button class="btn btn-primary" id="vml-sync">⟳ Sincronizar ventas</button>
      ${navHTML}
    </div>

    ${hayVentas ? `
      <div class="kpi-grid" style="margin:14px 0">
        <div class="kpi"><div class="kpi-label">Ventas del día</div><div class="kpi-value">${delDia.length}</div></div>
        <div class="kpi"><div class="kpi-label">Bruto</div><div class="kpi-value">${money(totBruto)}</div></div>
        <div class="kpi"><div class="kpi-label">Por cobrar (neto)</div><div class="kpi-value">${money(totCobrar)}</div></div>
        <div class="kpi"><div class="kpi-label">Ya cobradas</div><div class="kpi-value">${conCobro} <span class="vml-de">de ${delDia.length}</span></div></div>
      </div>
      ${delDia.length === 0
        ? `<div class="empty">No hubo ventas el ${esc(ddmm(FECHA))}.</div>`
        : `<div class="table-wrap"><table class="t">
            <thead><tr>
              <th style="width:52px">Hora</th>
              <th>Producto</th>
              <th style="width:80px">SKU</th>
              <th style="width:42px;text-align:right">Cant</th>
              <th style="width:120px;text-align:right">Bruto</th>
              <th style="width:110px;text-align:right">Comisión</th>
              <th style="width:110px;text-align:right">Envío</th>
              <th style="width:130px;text-align:right">Por cobrar</th>
              <th style="width:100px">Estado</th>
              <th style="width:170px">Cobro</th>
            </tr></thead>
            <tbody>${delDia.map(filaHTML).join('')}</tbody>
          </table></div>`}
    ` : `<div class="empty">Todavía no hay ventas cargadas. Tocá <b>Sincronizar ventas</b> para traerlas de Mercado Libre.</div>`}
  `;

  document.getElementById('vml-sync').addEventListener('click', openSyncModal);
  if (hayVentas) {
    document.getElementById('vml-prev').addEventListener('click', () => pasoDia(-1));
    document.getElementById('vml-next').addEventListener('click', () => pasoDia(1));
    document.getElementById('vml-fecha').addEventListener('change', e => { FECHA = e.target.value; render(); });
  }
}

function filaHTML(v) {
  const est = estadoVenta(v);
  const cobros = matchCobros(v);
  const rowCls = est.cls === 'canc' ? 'vml-row-canc' : est.cls === 'dev' ? 'vml-row-dev' : '';

  let cobroCell;
  if (cobros.length) {
    const sum = cobros.reduce((s, c) => s + (Number(c.monto) || 0), 0);
    const difiere = Math.abs(sum - (Number(v.por_cobrar) || 0)) > 0.02;
    cobroCell = `<span class="vml-cobro-ok">✓ ${money(sum)}</span>`
      + `<span class="vml-cobro-fecha">${ddmm(cobros[0].fecha)}</span>`
      + (difiere ? `<span class="vml-cobro-dif" title="No coincide con el por cobrar">≠</span>` : '');
  } else {
    cobroCell = `<span class="vml-cobro-no">— sin cobro</span>`;
  }

  return `<tr class="${rowCls}">
    <td class="vml-mono">${esc(v.hora_venta ? v.hora_venta.slice(0, 5) : '—')}</td>
    <td>${esc(v.titulo || '—')}</td>
    <td class="vml-mono">${esc(v.sku || '—')}</td>
    <td style="text-align:right">${v.cantidad || 1}</td>
    <td style="text-align:right" class="vml-mono">${money(v.importe_bruto)}</td>
    <td style="text-align:right" class="vml-mono vml-neg">${money(v.cargo_venta)}</td>
    <td style="text-align:right" class="vml-mono ${Number(v.cargo_envio) < 0 ? 'vml-neg' : 'vml-pos'}">${money(v.cargo_envio)}</td>
    <td style="text-align:right" class="vml-mono vml-fuerte">${money(v.por_cobrar)}</td>
    <td><span class="vml-est vml-est-${est.cls || 'ok'}">${esc(est.txt)}</span></td>
    <td>${cobroCell}</td>
  </tr>`;
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
  btn.disabled = true;
  btn.textContent = 'Sincronizando…';
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
        btn.disabled = false; btn.textContent = 'Traer ventas';
        return;
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
    .vml-fechalbl{font-size:13px;color:#78716C;text-transform:capitalize;margin-left:4px}
    .vml-de{font-size:14px;color:#A8A29E;font-weight:400}
    .vml-sub{font-size:13px;color:#78716C;margin:-4px 0 12px}
    .vml-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .vml-fuerte{font-weight:600}
    .vml-pos{color:#15803D}
    .vml-neg{color:#B91C1C}
    .vml-est{font-size:12px;padding:2px 8px;border-radius:6px;background:#F1EFE8;color:#57534E}
    .vml-est-canc{background:#FBEAEA;color:#B42318}
    .vml-est-dev{background:#FAF1E1;color:#92500A}
    .vml-row-canc{background:rgba(180,35,24,0.05)}
    .vml-row-dev{background:rgba(217,119,6,0.06)}
    .vml-cobro-ok{color:#0F6E56;font-weight:600;font-family:'JetBrains Mono',ui-monospace,monospace}
    .vml-cobro-fecha{font-size:11px;color:#A8A29E;margin-left:6px}
    .vml-cobro-dif{color:#B45309;font-weight:700;margin-left:6px}
    .vml-cobro-no{font-size:12px;color:#857a5c;background:#FAF6EC;border:1px dashed #E3D9BE;border-radius:6px;padding:2px 8px}
  `;
  const style = document.createElement('style');
  style.id = 'vml-style';
  style.textContent = css;
  document.head.appendChild(style);
}
