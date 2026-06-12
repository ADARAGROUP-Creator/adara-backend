import { sbGet } from '../core/sb.js';

// ── Pantalla: Cuadre — control de conciliación ────────────────────────────
// Tablero gerencial (NO la lista detallada, eso es #conciliacion):
//  1) Semáforo por cuenta: % conciliado por MONTO + cuánto falta.
//  2) Cola priorizada por monto: lo accionable a mano, lo grande arriba, con
//     vínculo de 1 clic cuando hay sugerencia exacta (reusa POST /vincular).
//  3) Resumen "espera motor ML": los cobros/devoluciones de venta (volumen
//     chico) que se conciliarán solos con el motor venta↔cobro (próximo paso),
//     no son trabajo manual.
//
// Criterios reusados de conciliacion.js (mismo lenguaje):
//  - esEspera: entrada (monto>0) o cobro_venta/devolucion → espera sync ML.
//  - esAccionable: pendiente/parcial que NO espera venta → gasto/compra a mano.

let CUENTAS = [], CUENTA_BY_ID = {};
let MOVS = [];          // v_movimientos_estado (todos)
let GASTOS = [];        // v_gastos_ap abiertos
let COMPRAS = [];       // v_compras_ap con saldo

const CAT_LABEL = {
  cobro_venta: 'Cobro de venta', pago_proveedor: 'Pago a proveedor', gasto: 'Gasto',
  comision_bancaria: 'Comisión bancaria', comision_marketplace: 'Comisión marketplace',
  impuesto: 'Impuesto', transferencia_interna: 'Transferencia interna', devolucion: 'Devolución',
  interes: 'Interés', ajuste_manual: 'Ajuste manual', sin_clasificar: 'Sin clasificar',
};

// ── Helpers ──────────────────────────────────────────────────────────────────
const cuentaLabel = id => { const c = CUENTA_BY_ID[id]; return c ? c.nombre : '—'; };
const monedaDe = id => { const c = CUENTA_BY_ID[id]; return c ? c.moneda : 'ARS'; };
const ddmm = f => `${(f || '').slice(8, 10)}/${(f || '').slice(5, 7)}`;
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
function num(v) { return Number(v) || 0; }
function money(n, moneda = 'ARS') {
  return (moneda === 'USD' ? 'US$ ' : '$ ') + Math.abs(num(n)).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function fmtMonto(valor, moneda) {
  const n = num(valor);
  return `${n < 0 ? '−' : '+'}${moneda === 'USD' ? 'US$ ' : '$ '}${Math.abs(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function esEspera(m) {
  return num(m.monto) > 0 || m.categoria === 'cobro_venta' || m.categoria === 'devolucion';
}
function esAccionable(m) {
  return (m.estado === 'pendiente' || m.estado === 'parcial') && !esEspera(m);
}

function sugerencia(saldoMov) {
  let best = null;
  for (const g of GASTOS) {
    const s = num(g.saldo_pendiente_ars);
    if (!(s > 0)) continue;
    const diff = Math.abs(s - saldoMov);
    if (diff < 0.02 && (!best || diff < best.diff)) best = { tipo: 'gasto', id: g.id, monto: s, diff, label: `Gasto · ${g.descripcion || ('#' + g.id)}` };
  }
  for (const c of COMPRAS) {
    const s = num(c.saldo_ap_ars);
    if (!(s > 0)) continue;
    const diff = Math.abs(s - saldoMov);
    if (diff < 0.02 && (!best || diff < best.diff)) best = { tipo: 'compra', id: c.compra_id, monto: s, diff, label: `Compra #${c.compra_id}` };
  }
  return best;
}

// ── Carga ──────────────────────────────────────────────────────────────────
export async function loadCuadre() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando cuadre…</div>`;
  try {
    CUENTAS = await sbGet('cuentas', 'order=id.asc');
    CUENTA_BY_ID = {};
    for (const c of CUENTAS) CUENTA_BY_ID[c.id] = c;
    MOVS = await sbGet('v_movimientos_estado', 'order=fecha.desc,id.desc');
    GASTOS = await sbGet('v_gastos_ap', 'estado_pago=in.(pendiente,parcial)&order=fecha.desc');
    COMPRAS = await sbGet('v_compras_ap', 'saldo_ap_ars=gt.0&order=fecha.desc');
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudo cargar el cuadre: ${esc(e.message)}</div>`;
    return;
  }
  inyectarEstilo();
  render();
}

// ── Render ───────────────────────────────────────────────────────────────────
function render() {
  const root = document.getElementById('app-screens');

  // Semáforo por cuenta (solo cuentas con movimientos).
  const porCuenta = {};
  for (const m of MOVS) {
    const k = m.cuenta_id;
    if (!porCuenta[k]) porCuenta[k] = { cuenta_id: k, total: 0, conc: 0, pend: 0, movsPend: 0 };
    const abs = Math.abs(num(m.monto));
    porCuenta[k].total += abs;
    if (m.estado === 'conciliado' || m.estado === 'auto') porCuenta[k].conc += abs;
    else { porCuenta[k].pend += abs; porCuenta[k].movsPend++; }
  }
  const filasCuenta = Object.values(porCuenta).filter(c => c.total > 0)
    .sort((a, b) => b.pend - a.pend);

  // Cola accionable (a mano) y resumen "espera motor".
  const accionables = MOVS.filter(esAccionable).sort((a, b) => Math.abs(num(b.monto)) - Math.abs(num(a.monto)));
  const espera = MOVS.filter(m => esEspera(m) && (m.estado === 'pendiente' || m.estado === 'parcial'));

  const montoAccionARS = accionables.filter(m => monedaDe(m.cuenta_id) === 'ARS').reduce((s, m) => s + Math.abs(num(m.monto)), 0);
  const montoEsperaARS = espera.filter(m => monedaDe(m.cuenta_id) === 'ARS').reduce((s, m) => s + Math.abs(num(m.monto)), 0);

  root.innerHTML = `
    <div class="cq-intro">
      Tablero de control: cuánto de cada cuenta está conciliado y qué falta.
      La <strong>cola</strong> ordena lo accionable por monto — atacá de arriba hacia abajo.
      Lo de "espera motor" se concilia solo con el motor venta↔cobro (próximo paso), no es trabajo manual.
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">🟠 A conciliar a mano</div><div class="kpi-value">${accionables.length}</div><div class="kpi-sub">${money(montoAccionARS)}</div></div>
      <div class="kpi"><div class="kpi-label">⏳ Espera motor ML</div><div class="kpi-value">${espera.length}</div><div class="kpi-sub">${money(montoEsperaARS)}</div></div>
      <div class="kpi"><div class="kpi-label">Cuentas</div><div class="kpi-value">${filasCuenta.length}</div></div>
    </div>

    <h3 class="cq-h">Semáforo por cuenta</h3>
    <div class="cq-cuentas">${filasCuenta.map(semaforoHTML).join('')}</div>

    <h3 class="cq-h">Cola — lo accionable a mano (por monto)</h3>
    ${accionables.length === 0
      ? `<div class="empty">No hay movimientos accionables pendientes. 🎉</div>`
      : `<div class="table-wrap"><table class="t cq-t">
          <thead><tr>
            <th>Fecha</th><th>Cuenta</th><th>Tipo</th><th>Descripción</th>
            <th class="cq-r">Monto</th><th>Acción</th>
          </tr></thead>
          <tbody id="cq-tbody">${accionables.map(filaHTML).join('')}</tbody>
        </table></div>`}

    <h3 class="cq-h">Espera motor (venta ↔ cobro)</h3>
    <div class="cq-motor">
      <strong>${espera.length}</strong> movimientos · ${money(montoEsperaARS)} en ARS son cobros y devoluciones de ventas.
      Se conciliarán automáticamente cuando armemos el <em>motor venta↔cobro</em>. No los toques a mano.
    </div>
  `;

  const tb = document.getElementById('cq-tbody');
  if (tb) tb.addEventListener('click', onColaClick);
}

function semaforoHTML(c) {
  const moneda = monedaDe(c.cuenta_id);
  const pct = c.total > 0 ? (c.conc / c.total) * 100 : 100;
  const color = pct >= 95 ? 'verde' : pct >= 70 ? 'amarillo' : 'rojo';
  return `
    <div class="cq-card">
      <div class="cq-card-top">
        <span class="cq-cuenta">${esc(cuentaLabel(c.cuenta_id))}</span>
        <span class="cq-pct cq-${color}">${pct.toFixed(1)}%</span>
      </div>
      <div class="cq-bar"><div class="cq-bar-fill cq-bg-${color}" style="width:${Math.max(2, Math.min(100, pct)).toFixed(1)}%"></div></div>
      <div class="cq-card-foot">
        ${c.movsPend > 0
          ? `Falta: <strong>${c.movsPend}</strong> movs · ${money(c.pend, moneda)}`
          : `<span class="cq-ok">Todo conciliado ✓</span>`}
      </div>
    </div>`;
}

function filaHTML(m) {
  const moneda = monedaDe(m.cuenta_id);
  const saldo = Math.abs(num(m.saldo_pendiente));
  const sug = sugerencia(saldo);
  let accion;
  if (sug) {
    accion = `<button class="btn btn-primary cq-mini" data-act="vincular" data-mov="${m.id}" data-tipo="${sug.tipo}" data-op="${sug.id}" data-monto="${sug.monto}" title="Vincular con ${esc(sug.label)}">✨ ${esc(sug.label)}</button>`;
  } else {
    accion = `<a class="cq-link" href="#conciliacion" title="Abrir en Conciliación para vincular en detalle">Conciliar →</a>`;
  }
  return `<tr>
    <td class="cq-mono">${ddmm(m.fecha)}</td>
    <td>${esc(cuentaLabel(m.cuenta_id))}</td>
    <td>${esc(CAT_LABEL[m.categoria] || m.categoria || '—')}</td>
    <td class="cq-desc" title="${esc(m.descripcion)}">${esc(m.descripcion || '—')}</td>
    <td class="cq-r cq-mono ${num(m.monto) < 0 ? 'cq-neg' : 'cq-pos'}">${fmtMonto(m.monto, moneda)}</td>
    <td>${accion}</td>
  </tr>`;
}

// ── Acción: aplicar sugerencia (reusa POST /vincular del backend) ─────────────
function onColaClick(e) {
  const btn = e.target.closest('[data-act="vincular"]');
  if (!btn) return;
  aplicarVinculo(btn);
}

async function aplicarVinculo(btn) {
  const movId = Number(btn.dataset.mov);
  const op_tipo = btn.dataset.tipo;
  const op_id = Number(btn.dataset.op);
  const monto = Number(btn.dataset.monto);
  btn.disabled = true; const txt = btn.textContent; btn.textContent = 'Vinculando…';
  try {
    const r = await fetch('/vincular', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ movimiento_id: movId, op_tipo, op_id, monto })
    });
    if (!r.ok) throw new Error('status ' + r.status);
    window.toast('Conciliado', 'ok');
    await loadCuadre();
  } catch (err) {
    btn.disabled = false; btn.textContent = txt;
    window.toast('No se pudo vincular: ' + err.message, 'error');
  }
}

// ── Estilo ─────────────────────────────────────────────────────────────────
function inyectarEstilo() {
  if (document.getElementById('cq-style')) return;
  const css = `
    .cq-intro{font-size:13px;line-height:1.5;color:#57534E;background:#F5F5F4;border:1px solid #E7E5E4;border-radius:10px;padding:11px 14px}
    .cq-intro em{font-style:normal;font-weight:600}
    .kpi-sub{font-size:12px;color:#A8A29E;margin-top:2px;font-variant-numeric:tabular-nums}
    .cq-h{font-size:14px;font-weight:700;color:#1C1917;margin:22px 0 10px}
    .cq-cuentas{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px}
    .cq-card{border:1px solid #E7E5E4;border-radius:12px;padding:14px;background:#fff}
    .cq-card-top{display:flex;justify-content:space-between;align-items:baseline;gap:8px}
    .cq-cuenta{font-size:14px;font-weight:600;color:#1C1917}
    .cq-pct{font-size:16px;font-weight:700;font-variant-numeric:tabular-nums}
    .cq-bar{height:8px;border-radius:6px;background:#F5F5F4;overflow:hidden;margin:8px 0}
    .cq-bar-fill{height:100%;border-radius:6px}
    .cq-bg-verde{background:#16A34A}
    .cq-bg-amarillo{background:#CA8A04}
    .cq-bg-rojo{background:#DC2626}
    .cq-verde{color:#15803D}
    .cq-amarillo{color:#A16207}
    .cq-rojo{color:#B91C1C}
    .cq-card-foot{font-size:12px;color:#78716C}
    .cq-card-foot strong{color:#44403C;font-variant-numeric:tabular-nums}
    .cq-ok{color:#15803D;font-weight:600}
    .cq-t th,.cq-t td{white-space:nowrap}
    .cq-r{text-align:right}
    .cq-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .cq-desc{max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .cq-neg{color:#B91C1C}
    .cq-pos{color:#15803D}
    .cq-mini{padding:5px 10px;font-size:12px}
    .cq-link{color:#D97706;font-weight:600;font-size:13px;text-decoration:none}
    .cq-link:hover{text-decoration:underline}
    .cq-motor{font-size:13px;line-height:1.5;color:#57534E;background:#FAFAF9;border:1px dashed #D6D3D1;border-radius:10px;padding:11px 14px}
    .cq-motor em{font-style:normal;font-weight:600;color:#44403C}
  `;
  const style = document.createElement('style');
  style.id = 'cq-style';
  style.textContent = css;
  document.head.appendChild(style);
}
