import { sbGet } from '../core/sb.js';

// ── Pantalla: Conciliación (capa 4) ────────────────────────────────────
// Muestra TODOS los movimientos con su estado de conciliación y permite
// vincular los pagos a operaciones (gasto / compra). Principio #1 de ADARA:
// conciliación universal, nada se pierde, todo visible.
//
// Lo atado a ventas (cobros, liquidaciones de MP, devoluciones) se muestra con
// la etiqueta "espera venta" — se concilia con el sync de ML, no a mano todavía.
// Convención: vinculos.monto = MAGNITUD POSITIVA imputada (ADARA-SCHEMA.md);
// v_movimientos_estado concilia con abs(monto) − Σ vínculos (tol. 0,02).

let CUENTAS = [], CUENTA_BY_ID = {};
let MOVS = [];           // v_movimientos_estado (todos)
let GASTOS = [];         // v_gastos_ap abiertos
let COMPRAS = [];        // v_compras_ap con saldo
let VINC_BY_MOV = {};    // movimiento_id -> [vinculos]
let FILTRO = { cuenta: '', estado: 'por_conciliar' };

const LABEL_CUENTA = {
  supervielle_ars: 'Supervielle ARS', mp_ars: 'MP ARS', caja_ars: 'Caja ARS', caja_usd: 'Caja USD',
};
const CAT_LABEL = {
  cobro_venta: 'Cobro de venta', pago_proveedor: 'Pago a proveedor', gasto: 'Gasto',
  comision_bancaria: 'Comisión bancaria', comision_marketplace: 'Comisión marketplace',
  impuesto: 'Impuesto', transferencia_interna: 'Transferencia interna', devolucion: 'Devolución',
  interes: 'Interés', ajuste_manual: 'Ajuste manual', sin_clasificar: 'Sin clasificar',
};
const LABEL_ESTADO = { auto: 'auto', pendiente: 'pendiente', parcial: 'parcial', conciliado: 'conciliado' };

const cuentaLabel = id => { const c = CUENTA_BY_ID[id]; return c ? (LABEL_CUENTA[c.codigo] || c.codigo) : '—'; };
const monedaDe = id => { const c = CUENTA_BY_ID[id]; return c ? c.moneda : 'ARS'; };
const money = n => '$ ' + Math.abs(Number(n) || 0).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ddmm = f => `${(f || '').slice(8, 10)}/${(f || '').slice(5, 7)}`;
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

function fmtMonto(valor, moneda) {
  const n = Number(valor) || 0;
  const abs = Math.abs(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return `${n < 0 ? '−' : '+'}${moneda === 'USD' ? 'US$ ' : '$ '}${abs}`;
}

// "Espera venta/sync ML": cobros (entradas) y lo atado a una venta.
function esEspera(m) {
  return Number(m.monto) > 0 || m.categoria === 'cobro_venta' || m.categoria === 'devolucion';
}
// Accionable hoy: pago (salida) pendiente/parcial que no espera venta → gasto/compra.
function esAccionable(m) {
  return (m.estado === 'pendiente' || m.estado === 'parcial') && !esEspera(m);
}

// Mejor sugerencia (gasto o compra) cuyo saldo coincida con el del movimiento.
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
    MOVS = await sbGet('v_movimientos_estado', 'order=fecha.desc,id.desc');
    GASTOS = await sbGet('v_gastos_ap', 'estado_pago=in.(pendiente,parcial)&order=fecha.desc');
    COMPRAS = await sbGet('v_compras_ap', 'saldo_ap_ars=gt.0&order=fecha.desc');
    const vinc = await sbGet('vinculos', 'order=id.desc');
    VINC_BY_MOV = {};
    for (const v of vinc) (VINC_BY_MOV[v.movimiento_id] = VINC_BY_MOV[v.movimiento_id] || []).push(v);
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudo cargar la conciliación: ${e.message}<br><br>
      Si dice algo de permisos sobre <b>vinculos</b>, corré en Supabase: <code>grant select on vinculos to anon, authenticated;</code></div>`;
    return;
  }
  inyectarEstilo();
  render();
}

function pasaEstado(m) {
  if (FILTRO.estado === 'por_conciliar') return m.estado === 'pendiente' || m.estado === 'parcial';
  if (FILTRO.estado === 'todos') return true;
  return m.estado === FILTRO.estado;
}

function render() {
  const root = document.getElementById('app-screens');
  const base = MOVS.filter(m => !FILTRO.cuenta || String(m.cuenta_id) === FILTRO.cuenta);

  const nPorConciliar = base.filter(m => m.estado === 'pendiente' || m.estado === 'parcial').length;
  const nConciliados = base.filter(m => m.estado === 'conciliado').length;
  const nAuto = base.filter(m => m.estado === 'auto').length;
  const montoAConciliar = base.filter(esAccionable).reduce((s, m) => s + Math.abs(Number(m.saldo_pendiente) || 0), 0);

  const filtrados = base.filter(pasaEstado);

  const opcionesCuenta = ['<option value="">Todas las cuentas</option>']
    .concat(CUENTAS.map(c => `<option value="${c.id}" ${FILTRO.cuenta === String(c.id) ? 'selected' : ''}>${cuentaLabel(c.id)}</option>`))
    .join('');

  const pill = (val, label, n) =>
    `<button class="pill ${FILTRO.estado === val ? 'active' : ''}" data-estado="${val}">${label} <span class="num">${n}</span></button>`;
  const pills = [
    pill('por_conciliar', 'Por conciliar', nPorConciliar),
    pill('conciliado', 'Conciliados', nConciliados),
    pill('auto', 'Auto', nAuto),
    pill('todos', 'Todos', base.length),
  ].join('');

  root.innerHTML = `
    <div class="toolbar">
      <select class="select" id="c-cuenta" style="width:auto">${opcionesCuenta}</select>
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">Por conciliar</div><div class="kpi-value">${nPorConciliar}</div></div>
      <div class="kpi"><div class="kpi-label">Monto a conciliar (pagos)</div><div class="kpi-value">${money(montoAConciliar)}</div></div>
      <div class="kpi"><div class="kpi-label">Conciliados</div><div class="kpi-value">${nConciliados}</div></div>
    </div>

    <div class="pills">${pills}</div>

    ${filtrados.length === 0
      ? `<div class="empty">No hay movimientos para mostrar.</div>`
      : `<div class="table-wrap"><table class="t" id="c-tabla">
          <thead><tr>
            <th style="width:62px">Fecha</th>
            <th style="width:110px">Cuenta</th>
            <th>Descripción</th>
            <th style="width:130px">Categoría</th>
            <th style="width:150px;text-align:right">Monto</th>
            <th style="width:96px">Estado</th>
            <th style="width:260px">Conciliación</th>
          </tr></thead>
          <tbody>${filtrados.map(filaHTML).join('')}</tbody>
        </table></div>`}
  `;

  document.getElementById('c-cuenta').addEventListener('change', e => { FILTRO.cuenta = e.target.value; render(); });
  root.querySelectorAll('.pill').forEach(p => p.addEventListener('click', () => { FILTRO.estado = p.dataset.estado; render(); }));

  // Delegación de eventos (la tabla puede tener miles de filas)
  const tabla = document.getElementById('c-tabla');
  if (tabla) tabla.addEventListener('click', onTablaClick);
}

function filaHTML(m) {
  const moneda = monedaDe(m.cuenta_id);
  const neg = Number(m.monto) < 0;
  const est = m.estado;
  return `<tr>
    <td>${ddmm(m.fecha)}</td>
    <td>${cuentaLabel(m.cuenta_id)}</td>
    <td>${esc(m.descripcion || '—')}</td>
    <td><span class="con-cat">${esc(CAT_LABEL[m.categoria] || m.categoria)}</span></td>
    <td style="text-align:right" class="con-mono ${neg ? 'con-neg' : 'con-pos'}">${fmtMonto(m.monto, moneda)}</td>
    <td><span class="con-badge con-badge-${est}">${LABEL_ESTADO[est] || est}</span></td>
    <td>${accionHTML(m)}</td>
  </tr>`;
}

function chipLabel(v, m) {
  if (v.op_tipo === 'transferencia') {
    return String(v.op_id) === String(m.id) ? 'Transf. interna (sin par)' : `↔ Transf. mov #${v.op_id}`;
  }
  return `${CAT_LABEL[v.op_tipo] || v.op_tipo} #${v.op_id}`;
}

function accionHTML(m) {
  const vinc = VINC_BY_MOV[m.id] || [];
  const chips = vinc.map(v => `<span class="con-chip">${esc(chipLabel(v, m))} · ${money(v.monto)}
      <button class="con-x" data-accion="desvincular" data-id="${v.id}" title="Desvincular">✕</button></span>`).join('');

  const transfBtn = `<button class="btn btn-ghost con-mini" data-accion="transf" data-mov="${m.id}">Transf. interna</button>`;

  if (esAccionable(m)) {
    const saldo = Math.abs(Number(m.saldo_pendiente) || 0);
    const sug = sugerencia(saldo);
    const spark = sug
      ? `<button class="con-spark-btn" data-accion="aceptar" data-mov="${m.id}" data-tipo="${sug.tipo}" data-op="${sug.id}" data-monto="${sug.monto}" title="Aceptar: ${esc(sug.label)} · ${money(sug.monto)}">✨</button>`
      : '';
    return `${chips}<div class="con-acts">${spark}<button class="btn btn-ghost con-mini" data-accion="pick" data-mov="${m.id}">Vincular</button>${transfBtn}</div>`;
  }
  if ((m.estado === 'pendiente' || m.estado === 'parcial') && esEspera(m)) {
    return `${chips}<div class="con-acts"><span class="con-wait">espera venta</span>${transfBtn}</div>`;
  }
  if (m.estado === 'conciliado') return chips || '<span class="con-ok">✓</span>';
  return chips || '<span class="con-dash">—</span>';
}

function onTablaClick(e) {
  const btn = e.target.closest('[data-accion]');
  if (!btn) return;
  const a = btn.dataset.accion;
  if (a === 'aceptar') vincular(btn.dataset.mov, btn.dataset.tipo, btn.dataset.op, Number(btn.dataset.monto));
  else if (a === 'pick') openPicker(btn.dataset.mov);
  else if (a === 'transf') openTransferencia(btn.dataset.mov);
  else if (a === 'desvincular') desvincular(btn.dataset.id);
}

function openPicker(movId) {
  const m = MOVS.find(x => String(x.id) === String(movId));
  if (!m) return;
  const saldoMov = Math.abs(Number(m.saldo_pendiente) || 0);

  const candidatos = [
    ...GASTOS.map(g => ({ tipo: 'gasto', id: g.id, saldo: Number(g.saldo_pendiente_ars), label: g.descripcion || ('Gasto #' + g.id), sub: `Gasto · ${ddmm(g.fecha)} · ${g.categoria_codigo || ''}` })),
    ...COMPRAS.map(c => ({ tipo: 'compra', id: c.compra_id, saldo: Number(c.saldo_ap_ars), label: `Compra #${c.compra_id}`, sub: `Compra · ${ddmm(c.fecha)} · ${c.tipo_compra || ''}` })),
  ].filter(o => o.saldo > 0).sort((a, b) => Math.abs(a.saldo - saldoMov) - Math.abs(b.saldo - saldoMov));

  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal">
      <div class="card-title">Vincular movimiento</div>
      <p class="con-sub"><b>${esc(m.descripcion || '')}</b> · saldo ${money(saldoMov)}.
        Se imputa el menor entre el saldo del movimiento y el de la operación.</p>
      ${candidatos.length === 0
        ? `<div class="empty">No hay gastos ni compras con saldo pendiente.</div>`
        : `<div class="con-pick">${candidatos.map(o => {
            const exacto = Math.abs(o.saldo - saldoMov) < 0.02;
            const imputa = Math.min(saldoMov, o.saldo);
            return `<div class="con-pick-row ${exacto ? 'exacto' : ''}">
              <div><div class="con-pick-lbl">${esc(o.label)} ${exacto ? '<span class="con-tag">coincide</span>' : ''}</div>
                <div class="con-pick-sub">${esc(o.sub)} · saldo ${money(o.saldo)}</div></div>
              <button class="btn btn-primary con-mini" data-op="${o.id}" data-tipo="${o.tipo}" data-monto="${imputa}">Imputar ${money(imputa)}</button>
            </div>`;
          }).join('')}</div>`}
      <div class="modal-actions"><button class="btn btn-ghost" id="c-cancel">Cancelar</button></div>
    </div>`;
  document.body.appendChild(overlay);
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#c-cancel').addEventListener('click', close);
  overlay.querySelectorAll('.con-pick-row button').forEach(b =>
    b.addEventListener('click', () => { close(); vincular(movId, b.dataset.tipo, b.dataset.op, Number(b.dataset.monto)); }));
}

function openTransferencia(movId) {
  const m = MOVS.find(x => String(x.id) === String(movId));
  if (!m) return;
  const mag = Math.abs(Number(m.monto) || 0);
  const signo = Math.sign(Number(m.monto) || 0);

  // Contrapartes: otra cuenta, signo opuesto, aún sin conciliar. Más cercanas primero.
  const cands = MOVS
    .filter(x => String(x.id) !== String(m.id)
      && String(x.cuenta_id) !== String(m.cuenta_id)
      && Math.sign(Number(x.monto) || 0) === -signo
      && (x.estado === 'pendiente' || x.estado === 'parcial'))
    .map(x => ({ x, diff: Math.abs(Math.abs(Number(x.monto) || 0) - mag) }))
    .sort((a, b) => a.diff - b.diff)
    .slice(0, 50)
    .map(o => o.x);

  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal">
      <div class="card-title">Transferencia interna</div>
      <p class="con-sub"><b>${esc(m.descripcion || '')}</b> · ${cuentaLabel(m.cuenta_id)} · ${fmtMonto(m.monto, monedaDe(m.cuenta_id))}<br>
        Elegí el movimiento contrario (la otra pata de la transferencia). Ambos quedan conciliados y no impactan el P&amp;L.</p>
      ${cands.length === 0
        ? `<div class="empty">No hay movimientos de otra cuenta con signo opuesto sin conciliar.</div>`
        : `<div class="con-pick">${cands.map(o => {
            const exacto = Math.abs(Math.abs(Number(o.monto) || 0) - mag) < 0.02;
            return `<div class="con-pick-row ${exacto ? 'exacto' : ''}">
              <div><div class="con-pick-lbl">${esc(o.descripcion || ('#' + o.id))} ${exacto ? '<span class="con-tag">coincide</span>' : ''}</div>
                <div class="con-pick-sub">${cuentaLabel(o.cuenta_id)} · ${ddmm(o.fecha)} · ${fmtMonto(o.monto, monedaDe(o.cuenta_id))}</div></div>
              <button class="btn btn-primary con-mini" data-emp="${o.id}">Emparejar</button>
            </div>`;
          }).join('')}</div>`}
      <div class="modal-actions">
        <button class="btn btn-ghost" id="t-sinpar">Marcar interna sin contrapartida</button>
        <button class="btn btn-ghost" id="t-cancel">Cancelar</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#t-cancel').addEventListener('click', close);
  overlay.querySelector('#t-sinpar').addEventListener('click', () => {
    if (!confirm('¿Marcar como transferencia interna sin contrapartida en el sistema? Quedará conciliada sola.')) return;
    close(); transfInterna(m.id, null);
  });
  overlay.querySelectorAll('.con-pick-row button').forEach(b =>
    b.addEventListener('click', () => { close(); transfInterna(m.id, b.dataset.emp); }));
}

async function transfInterna(movimiento_a, movimiento_b) {
  window.toast('Emparejando…');
  try {
    const r = await fetch('/transferencia-interna', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(movimiento_b ? { movimiento_a, movimiento_b } : { movimiento_a })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    window.toast('Transferencia interna conciliada');
    await loadConciliacion();
  } catch (e) {
    window.toast('Error: ' + e.message, 'error');
  }
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
    .con-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .con-pos{color:#15803D}
    .con-neg{color:#B91C1C}
    .con-cat{font-size:12px;color:#57534E;background:#F5F5F4;padding:2px 8px;border-radius:6px}
    .con-badge{font-size:12px;padding:2px 8px;border-radius:6px}
    .con-badge-pendiente{background:#FAEEDA;color:#854F0B}
    .con-badge-parcial{background:#E6F1FB;color:#0C447C}
    .con-badge-conciliado{background:#E1F5EE;color:#0F6E56}
    .con-badge-auto{background:#F1EFE8;color:#5F5E5A}
    .con-acts{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
    .con-mini{padding:5px 11px;font-size:13px}
    .con-spark-btn{border:1px solid #0F6E56;background:#E1F5EE;color:#0F6E56;border-radius:6px;cursor:pointer;font-size:14px;padding:3px 8px;line-height:1}
    .con-spark-btn:hover{background:#0F6E56;color:#fff}
    .con-wait{font-size:12px;color:#857a5c;background:#FAF6EC;border:1px dashed #E3D9BE;border-radius:6px;padding:2px 8px}
    .con-ok{color:#0F6E56;font-weight:600}
    .con-dash{color:#A8A29E}
    .con-chip{font-size:12px;background:#F1EFE8;color:#44403C;border-radius:6px;padding:3px 8px;display:inline-flex;align-items:center;gap:6px;margin:0 4px 4px 0}
    .con-x{border:0;background:transparent;color:#A8A29E;cursor:pointer;font-size:13px;line-height:1;padding:0}
    .con-x:hover{color:#B91C1C}
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
