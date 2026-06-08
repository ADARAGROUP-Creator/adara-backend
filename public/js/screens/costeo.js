import { sbGet } from '../core/sb.js';

// ── Pantalla: Costeo — CMV, Valorización y Margen ──────────────────────────
// Explota las 3 vistas creadas el 8/6/2026 sobre el circuito de costeo FIFO:
//  - v_valorizacion_stock : inventario a costo por familia × depósito.
//  - v_cmv_mensual        : COGS devengado por período × familia (consumo + reverso).
//  - v_margen_ventas      : margen bruto por ítem (ingreso_neto − cmv), flag `costeada`.
//
// Notas de lectura (ver ADARA-COSTEO-FIFO.md / ADARA-PNL.md):
//  - El FIFO costea de 8/6 en adelante: el CMV/margen histórico NO está (capa 6).
//    Por eso el margen se calcula SOLO sobre ítems `costeada=true`.
//  - El margen es BRUTO (ingreso neto s/IVA − CMV). NO descuenta comisión ML,
//    IIBB ni envío (eso es contribución/margen neto, pendiente del P&L).
//  - `unidades_sin_costo` marca el stock con costo $0 todavía sin cargar:
//    la valorización y el CMV de esas familias quedan subestimados.

function num(v) { return Number(v) || 0; }
function fmtNum(n, dec = 0) {
  return num(n).toLocaleString('es-AR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}
function fmtMoney(n) { return '$ ' + fmtNum(n, 2); }
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// ── Carga ──────────────────────────────────────────────────────────────────
export async function loadCosteo() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando costeo…</div>`;

  try {
    const [val, cmv, margenItems] = await Promise.all([
      sbGet('v_valorizacion_stock', 'select=*&order=valorizado.desc.nullslast'),
      sbGet('v_cmv_mensual', 'select=*&order=periodo.asc,familia.asc'),
      // sólo ítems costeados (margen real); evita arrastrar las ~14k ventas históricas sin CMV
      sbGet('v_margen_ventas', 'select=periodo,familia,ingreso_neto,cmv,margen_bruto&costeada=eq.true')
    ]);
    render(val, cmv, margenItems);
  } catch (e) {
    root.innerHTML = `<div class="error"><strong>Error al cargar Costeo.</strong><br>${esc(e.message)}</div>`;
  }
}

// ── Render ───────────────────────────────────────────────────────────────
function render(val, cmv, margenItems) {
  const root = document.getElementById('app-screens');

  // KPIs
  const valorizadoTotal = val.reduce((s, r) => s + num(r.valorizado), 0);
  const udsSinCosto = val.reduce((s, r) => s + num(r.unidades_sin_costo), 0);

  // Margen agregado por período × familia (sobre ítems costeados)
  const mg = {};
  for (const r of margenItems) {
    const k = (r.periodo || '—') + '|' + (r.familia || '—');
    if (!mg[k]) mg[k] = { periodo: r.periodo || '—', familia: r.familia || '—', ingreso: 0, cmv: 0, margen: 0 };
    mg[k].ingreso += num(r.ingreso_neto);
    mg[k].cmv += num(r.cmv);
    mg[k].margen += num(r.margen_bruto);
  }
  const margenRows = Object.values(mg).sort((a, b) =>
    (a.periodo).localeCompare(b.periodo) || (a.familia).localeCompare(b.familia));

  const ingresoCost = margenRows.reduce((s, r) => s + r.ingreso, 0);
  const cmvCost = margenRows.reduce((s, r) => s + r.cmv, 0);
  const margenCost = margenRows.reduce((s, r) => s + r.margen, 0);
  const pctMargen = ingresoCost > 0 ? (margenCost / ingresoCost * 100) : 0;

  const aviso = udsSinCosto > 0
    ? `<div class="psi-aviso">⚠ <strong>${fmtNum(udsSinCosto)} unidades sin costo cargado.</strong>
        La valorización y el CMV de esas familias quedan subestimados hasta cargar esos costos en SKUs.</div>`
    : '';

  // Tabla valorización
  const valBody = val.length
    ? val.map(r => `<tr>
        <td>${esc(r.familia)}</td>
        <td>${esc(r.deposito ?? '—')}</td>
        <td class="num">${fmtNum(r.unidades)}</td>
        <td class="num">${fmtMoney(r.valorizado)}</td>
        <td class="num">${num(r.unidades_sin_costo) > 0 ? fmtNum(r.unidades_sin_costo) : '—'}</td>
      </tr>`).join('')
    : `<tr><td colspan="5" class="empty">Sin stock.</td></tr>`;

  // Tabla CMV mensual
  const cmvBody = cmv.length
    ? cmv.map(r => `<tr>
        <td>${esc(r.periodo)}</td>
        <td>${esc(r.familia)}</td>
        <td class="num">${fmtNum(r.unidades_netas)}</td>
        <td class="num">${fmtMoney(r.cmv_consumo)}</td>
        <td class="num">${num(r.cmv_reverso) !== 0 ? fmtMoney(r.cmv_reverso) : '—'}</td>
        <td class="num"><strong>${fmtMoney(r.cmv_neto)}</strong></td>
      </tr>`).join('')
    : `<tr><td colspan="6" class="empty">Sin CMV devengado todavía.</td></tr>`;

  // Tabla margen
  const mgBody = margenRows.length
    ? margenRows.map(r => `<tr>
        <td>${esc(r.periodo)}</td>
        <td>${esc(r.familia)}</td>
        <td class="num">${fmtMoney(r.ingreso)}</td>
        <td class="num">${fmtMoney(r.cmv)}</td>
        <td class="num"><strong>${fmtMoney(r.margen)}</strong></td>
        <td class="num">${r.ingreso > 0 ? fmtNum(r.margen / r.ingreso * 100, 1) + '%' : '—'}</td>
      </tr>`).join('')
    : `<tr><td colspan="6" class="empty">Todavía no hay ventas costeadas (FIFO arranca 8/6).</td></tr>`;

  root.innerHTML = `
    <div class="toolbar">
      <span class="grow"></span>
      <button class="btn btn-ghost" id="cost-reload">Actualizar</button>
    </div>

    ${aviso}

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">Valorización stock</div><div class="kpi-value">${fmtMoney(valorizadoTotal)}</div></div>
      <div class="kpi"><div class="kpi-label">Uds sin costo</div><div class="kpi-value">${fmtNum(udsSinCosto)}</div></div>
      <div class="kpi"><div class="kpi-label">Ingreso costeado</div><div class="kpi-value">${fmtMoney(ingresoCost)}</div></div>
      <div class="kpi"><div class="kpi-label">CMV costeado</div><div class="kpi-value">${fmtMoney(cmvCost)}</div></div>
      <div class="kpi"><div class="kpi-label">Margen bruto</div><div class="kpi-value">${fmtMoney(margenCost)}</div></div>
      <div class="kpi"><div class="kpi-label">% Margen</div><div class="kpi-value">${fmtNum(pctMargen, 1)}%</div></div>
    </div>

    <h3 style="margin:22px 0 8px 0;font-size:15px;font-weight:600">Valorización de stock</h3>
    <div class="table-wrap"><table class="t">
      <thead><tr>
        <th>Familia</th><th>Depósito</th>
        <th class="num">Unidades</th><th class="num">Valorizado</th><th class="num">Uds s/costo</th>
      </tr></thead>
      <tbody>${valBody}</tbody>
    </table></div>

    <h3 style="margin:22px 0 8px 0;font-size:15px;font-weight:600">CMV mensual (devengado)</h3>
    <div class="table-wrap"><table class="t">
      <thead><tr>
        <th>Período</th><th>Familia</th>
        <th class="num">Uds netas</th><th class="num">CMV consumo</th>
        <th class="num">CMV reverso</th><th class="num">CMV neto</th>
      </tr></thead>
      <tbody>${cmvBody}</tbody>
    </table></div>

    <h3 style="margin:22px 0 8px 0;font-size:15px;font-weight:600">Margen bruto (sólo ventas costeadas)</h3>
    <div class="table-wrap"><table class="t">
      <thead><tr>
        <th>Período</th><th>Familia</th>
        <th class="num">Ingreso neto</th><th class="num">CMV</th>
        <th class="num">Margen bruto</th><th class="num">%</th>
      </tr></thead>
      <tbody>${mgBody}</tbody>
    </table></div>

    <div class="psi-aviso" style="margin-top:16px">
      Margen <strong>bruto</strong>: ingreso neto s/IVA − CMV. No descuenta comisión ML, IIBB ni envío
      (eso es contribución/margen neto, pendiente del P&L). Las ventas anteriores al 8/6 no tienen CMV
      todavía (se incorporan con el costeo histórico de Tango).
    </div>
  `;

  document.getElementById('cost-reload').addEventListener('click', loadCosteo);
}
