import { sbGet, sbCount } from '../core/sb.js';

// ── Pantalla: Conciliación (capa 4) ────────────────────────────────────
// Vincula movimientos (salidas/pagos) con operaciones (gasto / compra), creando
// filas en `vinculos`. La conciliación universal es el principio #1 de ADARA.
//
// Parte 1 (esto): pagos pendientes → vincular a gasto o compra, con sugerencia
// por monto. Lo atado a ventas (cobros, liquidaciones de MP, devoluciones) queda
// "esperando el sync de ML" y se resume al pie, no se concilia a mano todavía.
//
// Convención de signo (ADARA-SCHEMA.md): `vinculos.monto` es la MAGNITUD POSITIVA
// imputada. v_movimientos_estado concilia con abs(monto) − Σ vínculos (tol. 0,02).

let CUENTAS = [], CUENTA_BY_ID = {};
let MOVS = [];        // v_movimientos_estado (pendiente / parcial)
let GASTOS = [];      // v_gastos_ap abiertos (pendiente / parcial)
let COMPRAS = [];     // v_compras_ap con saldo
let VINCULOS = [];    // vinculos (para mostrar y desvincular)
let CONCILIADOS = 0;  // count estado=conciliado
let FILTRO = { cuenta: '' };

const LABEL_CUENTA = {
  supervielle_ars: 'Supervielle ARS', mp_ars: 'MP ARS', caja_ars: 'Caja ARS', caja_usd: 'Caja USD',
};
const CAT_LABEL = {
  cobro_venta: 'Cobro de venta', pago_proveedor: 'Pago a proveedor', gasto: 'Gasto',
  comision_bancaria: 'Comisión bancaria', comision_marketplace: 'Comisión marketplace',
  impuesto: 'Impuesto', transferencia_interna: 'Transferencia interna', devolucion: 'Devolución',
  interes: 'Interés', ajuste_manual: 'Ajuste manual', sin_clasificar: 'Sin clasificar',
};

const cuentaLabel = id => { const c = CUENTA_BY_ID[id]; return c ? (LABEL_CUENTA[c.codigo] || c.codigo) : '—'; };
const money = n => '$ ' + Math.abs(Number(n) || 0).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ddmm = f => `${(f || '').slice(8, 10)}/${(f || '').slice(5, 7)}`;
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// "Espera ventas/sync ML": cobros (entradas) y lo atado a una venta.
// No se concilia a mano en Parte 1 (necesita las ventas del sync de ML).
function esEspera(m) {
  return Number(m.monto) > 0 || m.categoria === 'cobro_venta' || m.categoria === 'devolucion';
}

// Mejor sugerencia (gasto o compra) cuyo saldo coincida con el saldo del movimiento.
function sugerencia(saldoMov) {
  let best = null;
  for (const g of GASTOS) {
    const s = Number(g.saldo_pendiente_ars);
    if (!(s > 0)) continue;
    const diff = Math.abs(s - saldoMov);
    if (diff < 0.02 && (!best || diff < best.diff)) best = { tipo: 'gasto', id: g.id, monto: s, diff, label: `Gasto · ${g.descripcion || ('#' + g.id)}` };
  }
  for (const c of COMPRAS) {
    const s = Number(c.saldo_ap_ars);
    if (!(s > 0)) continue;
    const diff = Math.abs(s - saldoMov);
    if (diff < 0.02 && (!best || diff < best.diff)) best = { tipo: 'compra', id: c.compra_id, monto: s, diff, label: `Compra #${c.compra_id}` };
  }
  return best;
}

export async function loadConciliacion() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando conciliación…</div>`;
  try {
    CUENTAS = await sbGet('cuentas', 'order=id.asc');
    CUENTA_BY_ID = Object.fromEntries(CUENTAS.map(c => [c.id, c]));
    MOVS = await sbGet('v_movimientos_estado', 'estado=in.(pendiente,parcial)&order=fecha.desc');
    GASTOS = await sbGet('v_gastos_ap', 'estado_pago=in.(pendiente,parcial)&order=fecha.desc');
    COMPRAS = await sbGet('v_compras_ap', 'saldo_ap_ars=gt.0&order=fecha.desc');
    VINCULOS = await sbGet('vinculos', 'order=id.desc');
    CONCILIADOS = await sbCount('v_movimientos_estado', 'estado=eq.conciliado');
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudo cargar la conciliación: ${e.message}</div>`;
    return;
  }
  inyectarEstilo();
  render();
}

function render() {
  const root = document.getElementById('app-screens');

  const base = MOVS.filter(m => !FILTRO.cuenta || String(m.cuenta_id) === FILTRO.cuenta);
  const accionables = base.filter(m => !esEspera(m));
  const espera = base.filter(esEspera);
  const montoAConciliar = accionables.reduce((s, m) => s + Math.abs(Number(m.saldo_pendiente) || 0), 0);

  const opcionesCuenta = ['<option value="">Todas las cuentas</option>']
    .concat(CUENTAS.map(c => `<option value="${c.id}" ${FILTRO.cuenta === String(c.id) ? 'selected' : ''}>${cuentaLabel(c.id)}</option>`))
    .join('');

  root.innerHTML = `
    <div class="toolbar">
      <select class="select" id="c-cuenta" style="width:auto">${opcionesCuenta}</select>
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">A conciliar (pagos)</div><div class="kpi-value">${accionables.length}</div></div>
      <div class="kpi"><div class="kpi-label">Monto a conciliar</div><div class="kpi-value">${money(montoAConciliar)}</div></div>
      <div class="kpi"><div class="kpi-label">Conciliados</div><div class="kpi-value">${CONCILIADOS}</div></div>
    </div>

    ${accionables.length === 0
      ? `<div class="empty">No hay pagos pendientes de conciliar${FILTRO.cuenta ? ' en esta cuenta' : ''}. 🎉</div>`
      : `<div class="con-list">${accionables.map(filaHTML).join('')}</div>`}

    ${espera.length > 0
      ? `<div class="con-foot">
           <strong>${espera.length}</strong> movimiento/s esperan una venta (cobros y liquidaciones de ML).
           Se concilian con el <b>sync de ML</b>, no a mano. Los ves completos en <a href="#movimientos">Movimientos</a>.
         </div>` : ''}
  `;

  document.getElementById('c-cuenta').addEventListener('change', e => { FILTRO.cuenta = e.target.value; render(); });

  root.querySelectorAll('[data-accion="aceptar"]').forEach(b =>
    b.addEventListener('click', () => vincular(b.dataset.mov, b.dataset.tipo, b.dataset.op, Number(b.dataset.monto))));
  root.querySelectorAll('[data-accion="pick-gasto"]').forEach(b =>
    b.addEventListener('click', () => openPicker(b.dataset.mov, 'gasto')));
  root.querySelectorAll('[data-accion="pick-compra"]').forEach(b =>
    b.addEventListener('click', () => openPicker(b.dataset.mov, 'compra')));
  root.querySelectorAll('[data-accion="desvincular"]').forEach(b =>
    b.addEventListener('click', () => desvincular(b.dataset.id)));
}

function filaHTML(m) {
  const saldo = Math.abs(Number(m.saldo_pendiente) || 0);
  const sug = sugerencia(saldo);
  const vinc = VINCULOS.filter(v => String(v.movimiento_id) === String(m.id));

  const sugHTML = sug ? `
    <div class="con-sug">
      <span class="con-spark">✨</span> Coincide con <b>${esc(sug.label)}</b> · ${money(sug.monto)}
      <button class="btn btn-primary con-mini" data-accion="aceptar" data-mov="${m.id}" data-tipo="${sug.tipo}" data-op="${sug.id}" data-monto="${sug.monto}">Aceptar</button>
    </div>` : '';

  const vincHTML = vinc.length ? `
    <div class="con-vinc">${vinc.map(v => `
      <span class="con-chip">→ ${esc(CAT_LABEL[v.op_tipo] || v.op_tipo)} #${v.op_id} · ${money(v.monto)}
        <button class="con-x" data-accion="desvincular" data-id="${v.id}" title="Desvincular">✕</button>
      </span>`).join('')}</div>` : '';

  return `
    <div class="con-item">
      <div class="con-row">
        <div class="con-info">
          <div class="con-desc">${esc(m.descripcion || '—')}</div>
          <div class="con-meta">${ddmm(m.fecha)} · ${cuentaLabel(m.cuenta_id)} · ${esc(CAT_LABEL[m.categoria] || m.categoria)}</div>
        </div>
        <div class="con-monto con-neg">− ${money(saldo)}</div>
      </div>
      ${sugHTML}
      ${vincHTML}
      <div class="con-acts">
        <button class="btn btn-ghost con-mini" data-accion="pick-gasto" data-mov="${m.id}">Vincular a gasto</button>
        <button class="btn btn-ghost con-mini" data-accion="pick-compra" data-mov="${m.id}">Vincular a compra</button>
      </div>
    </div>`;
}

function openPicker(movId, tipo) {
  const m = MOVS.find(x => String(x.id) === String(movId));
  if (!m) return;
  const saldoMov = Math.abs(Number(m.saldo_pendiente) || 0);

  const candidatos = (tipo === 'gasto'
    ? GASTOS.map(g => ({ id: g.id, saldo: Number(g.saldo_pendiente_ars), label: g.descripcion || ('Gasto #' + g.id), sub: `${ddmm(g.fecha)} · ${g.categoria_codigo || ''}` }))
    : COMPRAS.map(c => ({ id: c.compra_id, saldo: Number(c.saldo_ap_ars), label: `Compra #${c.compra_id}`, sub: `${ddmm(c.fecha)} · ${c.tipo_compra || ''}` })))
    .filter(o => o.saldo > 0)
    .sort((a, b) => Math.abs(a.saldo - saldoMov) - Math.abs(b.saldo - saldoMov));

  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal">
      <div class="card-title">Vincular a ${tipo === 'gasto' ? 'gasto' : 'compra'}</div>
      <p class="con-sub">Movimiento: <b>${esc(m.descripcion || '')}</b> · saldo ${money(saldoMov)}.
        Se imputa el menor entre el saldo del movimiento y el de la operación.</p>
      ${candidatos.length === 0
        ? `<div class="empty">No hay ${tipo === 'gasto' ? 'gastos' : 'compras'} con saldo pendiente.</div>`
        : `<div class="con-pick">${candidatos.map(o => {
            const exacto = Math.abs(o.saldo - saldoMov) < 0.02;
            const imputa = Math.min(saldoMov, o.saldo);
            return `<div class="con-pick-row ${exacto ? 'exacto' : ''}">
              <div><div class="con-pick-lbl">${esc(o.label)} ${exacto ? '<span class="con-tag">coincide</span>' : ''}</div>
                <div class="con-pick-sub">${esc(o.sub)} · saldo ${money(o.saldo)}</div></div>
              <button class="btn btn-primary con-mini" data-op="${o.id}" data-monto="${imputa}">Imputar ${money(imputa)}</button>
            </div>`;
          }).join('')}</div>`}
      <div class="modal-actions"><button class="btn btn-ghost" id="c-cancel">Cancelar</button></div>
    </div>`;
  document.body.appendChild(overlay);
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#c-cancel').addEventListener('click', close);
  overlay.querySelectorAll('.con-pick-row button').forEach(b =>
    b.addEventListener('click', () => { close(); vincular(movId, tipo, b.dataset.op, Number(b.dataset.monto)); }));
}

async function vincular(movimiento_id, op_tipo, op_id, monto) {
  window.toast('Vinculando…');
  try {
    const r = await fetch('/vincular', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ movimiento_id, op_tipo, op_id, monto })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    window.toast('Vinculado');
    await loadConciliacion();
  } catch (e) {
    window.toast('Error al vincular: ' + e.message, 'error');
  }
}

async function desvincular(id) {
  if (!confirm('¿Desvincular? El movimiento y la operación vuelven a quedar pendientes.')) return;
  try {
    const r = await fetch('/vincular/' + id, { method: 'DELETE' });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    window.toast('Desvinculado');
    await loadConciliacion();
  } catch (e) {
    window.toast('Error al desvincular: ' + e.message, 'error');
  }
}

function inyectarEstilo() {
  if (document.getElementById('con-style')) return;
  const css = `
    .con-list{display:flex;flex-direction:column;gap:10px}
    .con-item{border:1px solid #E7E5E4;border-radius:10px;padding:12px 14px;background:#fff}
    .con-row{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
    .con-desc{font-size:14px;font-weight:500;line-height:1.3}
    .con-meta{font-size:12px;color:#78716C;margin-top:3px}
    .con-monto{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums;font-weight:500;white-space:nowrap}
    .con-neg{color:#B91C1C}
    .con-sug{margin-top:10px;padding:8px 10px;background:#E1F5EE;border-radius:8px;font-size:13px;color:#0F6E56;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
    .con-spark{font-size:14px}
    .con-acts{margin-top:10px;display:flex;gap:8px;flex-wrap:wrap}
    .con-mini{padding:6px 12px;font-size:13px}
    .con-vinc{margin-top:8px;display:flex;gap:6px;flex-wrap:wrap}
    .con-chip{font-size:12px;background:#F1EFE8;color:#44403C;border-radius:6px;padding:3px 8px;display:inline-flex;align-items:center;gap:6px}
    .con-x{border:0;background:transparent;color:#A8A29E;cursor:pointer;font-size:13px;line-height:1;padding:0}
    .con-x:hover{color:#B91C1C}
    .con-foot{margin-top:18px;padding:12px 14px;background:#FAFAF9;border:1px solid #E7E5E4;border-radius:8px;font-size:13px;color:#57534E}
    .con-foot a{color:#0C447C;text-decoration:none}
    .con-sub{font-size:13px;color:#78716C;margin:-4px 0 12px}
    .con-pick{display:flex;flex-direction:column;gap:6px;max-height:340px;overflow:auto}
    .con-pick-row{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:8px 10px;border:1px solid #E7E5E4;border-radius:8px}
    .con-pick-row.exacto{border-color:#0F6E56;background:#E1F5EE}
    .con-pick-lbl{font-size:13px;font-weight:500}
    .con-pick-sub{font-size:12px;color:#78716C;margin-top:2px}
    .con-tag{font-size:11px;color:#0F6E56;background:#fff;border:1px solid #0F6E56;border-radius:6px;padding:0 6px;font-weight:400;margin-left:4px}
  `;
  const style = document.createElement('style');
  style.id = 'con-style';
  style.textContent = css;
  document.head.appendChild(style);
}
