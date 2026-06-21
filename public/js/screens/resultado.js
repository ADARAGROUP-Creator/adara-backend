import { sbGet } from '../core/sb.js';

// ── Pantalla: Resultado — Estado de resultado mensual por línea ─────────────
// Lee v_resultado_mensual (período × línea), canal ML. Baja la cascada hasta el
// MARGEN DE CONTRIBUCIÓN ANTES DE CMV: ventas netas − comisión − envío −
// financiero − retenciones/IIBB. Montos netos de IVA, criterio devengado.
//
// Límites actuales (ver ADARA-PNL.md / ADARA-COSTEO-FIFO.md):
//  - El CMV todavía NO está cargado para casi ninguna venta (solo las costeadas
//    ≥ 8/6). Se muestra en su columna marcado como pendiente; NO se resta del
//    margen de contribución, que por eso es "antes de CMV".
//  - Faltan gastos de estructura + Ganancias (pantalla Gastos) → no hay
//    resultado operativo ni neto todavía.
//  - Solo hay datos de canal ML (en 2026, todo ML Electrónica). El resto de las
//    líneas entra con el sync de ventas Tango (capa 6).

function num(v) { return Number(v) || 0; }
function fmtNum(n, dec = 0) {
  return num(n).toLocaleString('es-AR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}
function fmtMoney(n) { return '$ ' + fmtNum(n, 2); }
function fmtPct(n) { return fmtNum(n, 1) + '%'; }
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
// Período '2026-03' → 'Mar 2026'
const MESES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];
function fmtPeriodo(p) {
  const [y, m] = String(p || '').split('-');
  const i = parseInt(m, 10) - 1;
  return (MESES[i] || m) + ' ' + y;
}

let DATA = [];        // filas crudas de v_resultado_mensual
let GASTOS = [];      // filas de v_gastos_mensual (período × línea, neto ARS)
let GASTOS_CAT = [];  // filas de v_gastos_categoria_mensual (drill-down por categoría)
let LINEAS = [];      // todas las líneas de negocio (para el selector, incluso sin datos)
let LINEA_SEL = '__all__';
let CANAL_SEL = '__all__';
let CONTROL = {};     // v_control_mensual indexado por periodo (drill-down)
let EXPANDED = null;  // periodo desplegado

const CANAL_NOMBRE = {
  ml: 'Mercado Libre',
  tienda_nube: 'Tienda Nube',
  whatsapp_efectivo: 'WhatsApp / Efectivo',
  b2b: 'B2B',
};
const canalNombre = c => CANAL_NOMBRE[c] || c || '—';

// ── Carga ──────────────────────────────────────────────────────────────────
export async function loadResultado() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando resultado…</div>`;
  try {
    const [data, lineas, gastos, control, gastosCat] = await Promise.all([
      sbGet('v_resultado_mensual', 'select=*&order=periodo.asc'),
      sbGet('lineas_negocio', 'select=id,nombre&order=id.asc'),
      sbGet('v_gastos_mensual', 'select=*&order=periodo.asc'),
      sbGet('v_control_mensual', 'select=*&order=periodo.asc'),
      sbGet('v_gastos_categoria_mensual', 'select=*&order=periodo.asc')
    ]);
    DATA = data;
    LINEAS = lineas;
    GASTOS = gastos;
    GASTOS_CAT = gastosCat || [];
    CONTROL = {};
    (control || []).forEach(r => { CONTROL[r.periodo] = r; });
    render();
  } catch (e) {
    root.innerHTML = `<div class="error"><strong>Error al cargar Resultado.</strong><br>${esc(e.message)}</div>`;
  }
}

// ── Estilos scopeados (.res) ────────────────────────────────────────────────
const STYLE = `
<style>
.res .res-bar{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:14px}
.res .res-bar label{font-size:13px;color:var(--text-muted);font-weight:500}

.res .cost-aviso{display:flex;gap:9px;align-items:flex-start;background:var(--acc-bg);border:1px solid #F3D6A3;color:var(--acc-dark);border-radius:var(--r);padding:12px 14px;font-size:13.5px;line-height:1.5;margin:0 0 14px}
.res .cost-aviso .ic{flex-shrink:0;font-size:15px}
.res .cost-aviso b{font-weight:700}

.res .res-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px;margin:0 0 18px}
.res .res-kpi{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:16px 18px;min-width:0}
.res .res-kpi .l{font-size:11.5px;color:var(--text-muted);font-weight:600;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px}
.res .res-kpi .v{font-weight:700;font-size:22px;line-height:1.15;color:var(--text);letter-spacing:-0.02em;font-variant-numeric:tabular-nums;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.res .res-kpi.acc .v{color:var(--acc)}

.res .tbl{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);overflow-x:auto}
.res table{width:100%;border-collapse:collapse;font-size:14px;min-width:1040px}
.res thead th{text-align:right;padding:11px 14px;font-weight:600;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em;background:var(--surface-alt);border-bottom:1px solid var(--border);white-space:nowrap}
.res thead th:first-child{text-align:left}
.res tbody td{padding:11px 14px;border-top:1px solid var(--border);text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;color:var(--text)}
.res tbody td:first-child{text-align:left;font-weight:500}
.res tbody tr:hover{background:var(--surface-alt)}
.res .neg{color:#9A3412}
.res .contrib{font-weight:600}
.res .pend{color:var(--text-soft)}
.res .chip{display:inline-block;font-size:10.5px;font-weight:600;padding:1px 7px;border-radius:var(--r-sm);background:var(--acc-bg);color:var(--acc-dark);margin-left:6px;vertical-align:middle}
.res tr.tot td{border-top:2px solid var(--border-strong);font-weight:700;background:var(--surface-alt)}
.res tr.tot:hover td{background:var(--surface-alt)}

.res .cost-note{margin-top:16px;background:var(--surface-alt);border:1px solid var(--border);border-radius:var(--r);padding:12px 14px;font-size:13px;line-height:1.6;color:var(--text-muted)}
.res .cost-note b{color:var(--text);font-weight:600}
.res .empty{padding:34px;text-align:center;color:var(--text-muted)}
.res .res-row{cursor:pointer}
.res .res-row:hover td{background:#FAFAF9}
.res .res-row.open td{background:#FFF7ED}
.res .res-panel > td{background:#FAFAF9;padding:14px}
.res .rp-wrap{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}
.res .rp-card{background:#fff;border:1px solid var(--border,#E7E5E4);border-radius:10px;padding:12px 14px}
.res .rp-h{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.03em;color:var(--text-muted,#78716C);margin-bottom:8px}
.res .rp-row{display:flex;justify-content:space-between;gap:10px;font-size:13px;padding:3px 0}
.res .rp-row > span{color:var(--text-muted,#78716C)}
.res .rp-row > b{font-variant-numeric:tabular-nums;white-space:nowrap}
.res .rp-strong{border-top:1px solid #F5F5F4;margin-top:4px;padding-top:6px;font-weight:700}
.res .rp-sub{font-size:11px;color:var(--text-muted,#78716C);margin-top:1px;line-height:1.35}
.res .rp-wrap-2{margin-top:12px}
.res .gcat{display:flex;flex-direction:column;gap:7px;margin-bottom:6px}
.res .gcat-row{display:grid;grid-template-columns:118px 1fr auto;gap:8px;align-items:center;font-size:12.5px}
.res .gcat-name{color:var(--text-muted,#78716C);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.res .gcat-bar{height:8px;border-radius:4px;background:#F0EEEC;overflow:hidden}
.res .gcat-bar > i{display:block;height:100%;background:#B45309;border-radius:4px}
.res .gcat-val{font-variant-numeric:tabular-nums;white-space:nowrap;color:var(--text)}
.res .gcat-empty{font-size:12.5px;color:var(--text-soft,#A8A29E);padding:6px 0}
.res .comp-bar{display:flex;height:22px;border-radius:6px;overflow:hidden;margin-bottom:10px;background:#F0EEEC}
.res .comp-bar > span{display:block;height:100%}
.res .comp-leg{display:flex;flex-direction:column;gap:5px}
.res .comp-leg-row{display:grid;grid-template-columns:12px 1fr auto auto;gap:8px;align-items:center;font-size:12.5px}
.res .comp-dot{width:11px;height:11px;border-radius:3px}
.res .comp-leg-name{color:var(--text-muted,#78716C)}
.res .comp-pct{color:var(--text-muted,#78716C);font-variant-numeric:tabular-nums;text-align:right}
.res .comp-amt{font-variant-numeric:tabular-nums;white-space:nowrap;text-align:right}
</style>`;

// ── Render ───────────────────────────────────────────────────────────────
function render() {
  const root = document.getElementById('app-screens');

  // Líneas con datos cargados (para marcar las vacías en el selector)
  const conDatos = new Set(DATA.map(r => r.linea).filter(Boolean));
  // Catálogo completo de líneas (incluye las que todavía no tienen ventas)
  const lineas = (LINEAS.length ? LINEAS.map(l => l.nombre) : [...conDatos])
    .filter(Boolean);

  // Canales presentes en los datos (para el selector de canal)
  const canalesData = [...new Set(DATA.map(r => r.canal).filter(Boolean))].sort();

  // Filtrar por línea y canal seleccionados, consolidar por período
  const filt = DATA.filter(r =>
    (LINEA_SEL === '__all__' || r.linea === LINEA_SEL) &&
    (CANAL_SEL === '__all__' || r.canal === CANAL_SEL));
  const perMap = {};
  for (const r of filt) {
    const p = r.periodo;
    if (!perMap[p]) perMap[p] = {
      periodo: p, ventas: 0, costeadas: 0,
      ingreso: 0, cmv: 0, cmv_real: 0, cmv_estimado: 0, comision: 0, envio: 0, financiero: 0, impuestos: 0
    };
    const a = perMap[p];
    a.ventas    += num(r.ventas);
    a.costeadas += num(r.ventas_costeadas);
    a.ingreso   += num(r.ingreso_neto);
    a.cmv       += num(r.cmv);
    a.cmv_real  += num(r.cmv_real);
    a.cmv_estimado += num(r.cmv_estimado);
    a.comision  += num(r.comision);
    a.envio     += num(r.envio);
    a.financiero+= num(r.costo_financiero);
    a.impuestos += num(r.impuestos);
  }
  const filas = Object.values(perMap).sort((a, b) => a.periodo.localeCompare(b.periodo));
  // Margen de contribución ANTES de CMV (deducciones vienen con signo negativo)
  for (const f of filas) {
    f.contrib = f.ingreso + f.comision + f.envio + f.financiero + f.impuestos;
  }

  // Gastos operativos por período (son por LÍNEA, no por canal). Se filtran por la
  // línea elegida; con un canal puntual el margen es de ese canal pero los gastos
  // siguen siendo de la línea completa (ver nota al pie; multicanal real es TBD).
  const gastoPer = {};
  for (const g of GASTOS) {
    if (LINEA_SEL !== '__all__' && g.linea !== LINEA_SEL) continue;
    gastoPer[g.periodo] = (gastoPer[g.periodo] || 0) + num(g.gastos_neto_ars);
  }
  for (const f of filas) {
    f.gastos = gastoPer[f.periodo] || 0;
    f.resultado_op = f.contrib - f.cmv - f.gastos;  // cmv y gastos positivos: se restan
    f.pct = f.ingreso > 0 ? f.resultado_op / f.ingreso * 100 : 0;  // margen operativo real (s/ventas)
  }

  // Totales
  const T = filas.reduce((a, f) => ({
    ventas: a.ventas + f.ventas, costeadas: a.costeadas + f.costeadas,
    ingreso: a.ingreso + f.ingreso, cmv: a.cmv + f.cmv, cmv_real: a.cmv_real + f.cmv_real, cmv_estimado: a.cmv_estimado + f.cmv_estimado, comision: a.comision + f.comision,
    envio: a.envio + f.envio, financiero: a.financiero + f.financiero,
    impuestos: a.impuestos + f.impuestos, contrib: a.contrib + f.contrib,
    gastos: a.gastos + f.gastos, resultado_op: a.resultado_op + f.resultado_op,
  }), { ventas: 0, costeadas: 0, ingreso: 0, cmv: 0, cmv_real: 0, cmv_estimado: 0, comision: 0, envio: 0, financiero: 0, impuestos: 0, contrib: 0, gastos: 0, resultado_op: 0 });
  const Tpct = T.ingreso > 0 ? T.resultado_op / T.ingreso * 100 : 0;

  const moneyNeg = n => `<span class="neg">${fmtMoney(n)}</span>`;
  const cmvCell = (cmv, est) => num(cmv) === 0
    ? `<span class="pend">pendiente</span>`
    : num(est) > 0
      ? `${fmtMoney(-cmv)} <span class="pend" title="CMV estimado a costo actual del SKU. Las ventas nuevas costean por lote (FIFO real).">est.</span>`
      : fmtMoney(-cmv);

  const selector = `<div class="res-bar">
        <label for="res-linea">Línea de negocio</label>
        <select class="select" id="res-linea" style="max-width:280px">
          <option value="__all__"${LINEA_SEL === '__all__' ? ' selected' : ''}>Todas las líneas</option>
          ${lineas.map(l => `<option value="${esc(l)}"${LINEA_SEL === l ? ' selected' : ''}>${esc(l)}${conDatos.has(l) ? '' : ' — sin ventas'}</option>`).join('')}
        </select>
        <label for="res-canal" style="margin-left:8px">Canal</label>
        <select class="select" id="res-canal" style="max-width:220px">
          <option value="__all__"${CANAL_SEL === '__all__' ? ' selected' : ''}>Todos los canales</option>
          ${canalesData.map(c => `<option value="${esc(c)}"${CANAL_SEL === c ? ' selected' : ''}>${esc(canalNombre(c))}</option>`).join('')}
        </select>
      </div>`;

  const body = filas.length
    ? filas.map(f => `<tr class="res-row${EXPANDED === f.periodo ? ' open' : ''}" data-periodo="${esc(f.periodo)}">
        <td>${EXPANDED === f.periodo ? '▾' : '▸'} ${esc(fmtPeriodo(f.periodo))}</td>
        <td>${fmtMoney(f.ingreso)}</td>
        <td>${moneyNeg(f.comision)}</td>
        <td>${moneyNeg(f.envio)}</td>
        <td>${moneyNeg(f.financiero)}</td>
        <td>${moneyNeg(f.impuestos)}</td>
        <td class="contrib">${fmtMoney(f.contrib)}</td>
        <td>${cmvCell(f.cmv, f.cmv_estimado)}</td>
        <td>${f.gastos ? moneyNeg(-f.gastos) : '<span class="pend">—</span>'}</td>
        <td class="contrib">${fmtMoney(f.resultado_op)}</td>
        <td>${fmtPct(f.pct)}</td>
      </tr>${EXPANDED === f.periodo ? panelRow(f) : ''}`).join('')
    : `<tr><td colspan="11" class="empty">${
        LINEA_SEL === '__all__'
          ? 'Todavía no hay ventas cargadas.'
          : `«${esc(LINEA_SEL)}» todavía no tiene ventas cargadas. Las ventas que no son de Mercado Libre (Tienda Nube, B2B, sindicatos) entran con el sync de Tango.`
      }</td></tr>`;

  const totRow = filas.length ? `<tr class="tot">
      <td>Total</td>
      <td>${fmtMoney(T.ingreso)}</td>
      <td>${fmtMoney(T.comision)}</td>
      <td>${fmtMoney(T.envio)}</td>
      <td>${fmtMoney(T.financiero)}</td>
      <td>${fmtMoney(T.impuestos)}</td>
      <td class="contrib">${fmtMoney(T.contrib)}</td>
      <td>${cmvCell(T.cmv, T.cmv_estimado)}</td>
      <td>${T.gastos ? moneyNeg(-T.gastos) : '<span class="pend">—</span>'}</td>
      <td class="contrib">${fmtMoney(T.resultado_op)}</td>
      <td>${fmtPct(Tpct)}</td>
    </tr>` : '';

  root.innerHTML = `${STYLE}
  <div class="res">
    <div class="toolbar"><span class="grow"></span>
      <button class="btn btn-ghost" id="res-reload">Actualizar</button>
    </div>

    <div class="cost-aviso"><span class="ic">⚠</span><div>
      Este estado baja hasta el <b>resultado operativo</b>: a las ventas netas les resta las deducciones de
      Mercado Libre (comisión, envío, financiero, IIBB), el <b>CMV</b> y los <b>gastos operativos</b> de la línea.
      El CMV de las ventas históricas está <b>estimado a costo actual</b> del SKU (marcado <i>est.</i>); las ventas
      nuevas costean por <b>lote real (FIFO)</b>. Falta <b>Ganancias</b> para llegar al resultado neto.
    </div></div>

    ${selector}

    <div class="res-kpis">
      <div class="res-kpi acc"><div class="l">Ventas netas</div><div class="v">${fmtMoney(T.ingreso)}</div></div>
      <div class="res-kpi"><div class="l">Margen contribución (antes CMV)</div><div class="v">${fmtMoney(T.contrib)}</div></div>
      <div class="res-kpi"><div class="l">Margen operativo</div><div class="v">${fmtPct(Tpct)}</div></div>
      <div class="res-kpi"><div class="l">CMV (estimado)</div><div class="v">${fmtMoney(T.cmv)}</div></div>
      <div class="res-kpi"><div class="l">Gastos operativos</div><div class="v">${fmtMoney(T.gastos)}</div></div>
      <div class="res-kpi acc"><div class="l">Resultado operativo</div><div class="v">${fmtMoney(T.resultado_op)}</div></div>
    </div>

    <div class="tbl"><table>
      <thead><tr>
        <th>Período</th>
        <th>Ventas netas</th>
        <th>Comisión ML</th>
        <th>Envío</th>
        <th>Financiero</th>
        <th>Retenc./IIBB</th>
        <th>Contribución</th>
        <th>CMV</th>
        <th>Gastos</th>
        <th>Resultado op.</th>
        <th>Margen %</th>
      </tr></thead>
      <tbody>${body}${totRow}</tbody>
    </table></div>

    <div class="cost-note">
      <b>Cómo leer esta tabla.</b> Cada fila es un mes (criterio devengado, montos sin IVA). De las ventas netas
      se restan las deducciones de Mercado Libre (→ <b>contribución</b>), después el <b>CMV</b> y los <b>gastos</b>
      operativos, para llegar al <b>resultado operativo</b>. El CMV marcado <i>est.</i> está <b>estimado a costo
      actual</b> del SKU (no al costo histórico exacto de la unidad vendida), porque las ventas previas al stock de
      apertura no consumen lote; las ventas nuevas costean por <b>lote real (FIFO)</b>. Por la inflación, ese costo
      a valor de hoy puede achicar un poco el margen de los meses viejos. Los <b>gastos son por línea</b> (no por
      canal): con un canal puntual filtrado, el margen es de ese canal pero los gastos siguen siendo de la línea
      completa. Falta Ganancias para llegar al resultado neto.
    </div>
  </div>`;

  document.getElementById('res-reload').addEventListener('click', loadResultado);
  const sel = document.getElementById('res-linea');
  if (sel) sel.addEventListener('change', e => { LINEA_SEL = e.target.value; render(); });
  const selC = document.getElementById('res-canal');
  if (selC) selC.addEventListener('change', e => { CANAL_SEL = e.target.value; render(); });
  root.querySelectorAll('.res-row').forEach(tr => tr.addEventListener('click', () => {
    const p = tr.dataset.periodo;
    EXPANDED = (EXPANDED === p) ? null : p;
    render();
  }));
}

// Panel de control desplegable por mes: reconcilia con ML, cascada a resultado, IVA y cobranza.
// Gastos del período abiertos por categoría (suma sobre canal, filtra por línea
// elegida — consistente con la columna "Gastos" de la tabla, que ignora canal).
function gastosCatDe(periodo) {
  const map = {};
  for (const g of GASTOS_CAT) {
    if (g.periodo !== periodo) continue;
    if (LINEA_SEL !== '__all__' && g.linea !== LINEA_SEL) continue;
    const k = g.categoria || g.categoria_codigo || '—';
    map[k] = (map[k] || 0) + num(g.gastos_neto_ars);
  }
  return Object.entries(map)
    .map(([categoria, monto]) => ({ categoria, monto }))
    .filter(x => x.monto > 0)
    .sort((a, b) => b.monto - a.monto);
}

function cardGastosCategoria(f) {
  const rows = gastosCatDe(f.periodo);
  const total = rows.reduce((s, x) => s + x.monto, 0);
  const body = rows.length
    ? rows.map(x => {
        const pct = total > 0 ? x.monto / total * 100 : 0;
        return `<div class="gcat-row">
          <span class="gcat-name" title="${esc(x.categoria)}">${esc(x.categoria)}</span>
          <span class="gcat-bar"><i style="width:${pct.toFixed(1)}%"></i></span>
          <span class="gcat-val">${fmtMoney(x.monto)}</span>
        </div>`;
      }).join('')
    : `<div class="gcat-empty">Sin gastos imputados este mes${LINEA_SEL !== '__all__' ? ' en ' + esc(LINEA_SEL) : ''}.</div>`;
  return `<div class="rp-card">
      <div class="rp-h">Gastos por categoría</div>
      <div class="gcat">${body}</div>
      ${rows.length ? `<div class="rp-row rp-strong"><span>Total gastos</span><b>${fmtMoney(total)}</b></div>` : ''}
    </div>`;
}

// Cómo se reparte cada peso de venta neta: deducciones ML + CMV + gastos + resultado.
const COMP_COLORS = { ded: '#B45309', cmv: '#78716C', gastos: '#9A3412', resultado: '#0F6E56' };
function cardComposicion(f) {
  const ingreso = num(f.ingreso);
  if (ingreso <= 0) {
    return `<div class="rp-card"><div class="rp-h">Composición de la venta</div>
      <div class="gcat-empty">Sin ventas netas este mes.</div></div>`;
  }
  const dedMag = -(num(f.comision) + num(f.envio) + num(f.financiero) + num(f.impuestos)); // vienen negativas → magnitud
  const segs = [
    { k: 'ded', name: 'Deducciones ML', val: dedMag },
    { k: 'cmv', name: 'CMV', val: num(f.cmv) },
    { k: 'gastos', name: 'Gastos', val: num(f.gastos) },
    { k: 'resultado', name: 'Resultado op.', val: num(f.resultado_op) },
  ];
  const bar = segs
    .map(s => ({ s, w: Math.max(0, s.val) / ingreso * 100 }))
    .filter(x => x.w > 0)
    .map(x => `<span style="width:${x.w.toFixed(2)}%;background:${COMP_COLORS[x.s.k]}" title="${esc(x.s.name)}"></span>`)
    .join('');
  const leg = segs.map(s => `<div class="comp-leg-row">
      <span class="comp-dot" style="background:${COMP_COLORS[s.k]}"></span>
      <span class="comp-leg-name">${esc(s.name)}</span>
      <span class="comp-pct">${fmtPct(s.val / ingreso * 100)}</span>
      <span class="comp-amt">${fmtMoney(s.val)}</span>
    </div>`).join('');
  return `<div class="rp-card">
      <div class="rp-h">Composición de la venta</div>
      <div class="comp-bar">${bar}</div>
      <div class="comp-leg">${leg}</div>
      <div class="rp-sub">Cómo se reparte cada peso de venta neta (${fmtMoney(ingreso)}).</div>
    </div>`;
}

function panelRow(f) {
  const c = CONTROL[f.periodo] || {};
  const ded = num(f.comision) + num(f.envio) + num(f.financiero) + num(f.impuestos);
  const ivaCred = num(c.iva_cred_compras) + num(c.iva_cred_gastos);
  return `<tr class="res-panel"><td colspan="11"><div class="rp-wrap">
    <div class="rp-card">
      <div class="rp-h">Cuadre con Mercado Libre</div>
      <div class="rp-row"><span>Órdenes</span><b>${fmtNum(c.ordenes_ml)}</b></div>
      <div class="rp-sub">válidas ${fmtNum(c.ordenes_validas)} · canceladas/excluidas ${fmtNum(c.ordenes_excluidas)}</div>
      <div class="rp-row"><span>Unidades</span><b>${fmtNum(c.unidades_ml)}</b></div>
      <div class="rp-row rp-strong"><span>Bruto facturado (c/IVA)</span><b>${fmtMoney(c.bruto_ml)}</b></div>
      <div class="rp-sub">= "ventas brutas" de ML · válidas ${fmtMoney(c.bruto_validas)} · excluido ${fmtMoney(c.bruto_excluido)}</div>
    </div>
    <div class="rp-card">
      <div class="rp-h">Cascada a resultado</div>
      <div class="rp-row"><span>Neto (sin IVA)</span><b>${fmtMoney(c.neto)}</b></div>
      <div class="rp-row"><span>− Deducciones ML</span><b>${fmtMoney(ded)}</b></div>
      <div class="rp-row"><span>Contribución</span><b>${fmtMoney(f.contrib)}</b></div>
      <div class="rp-row"><span>− CMV ${num(f.cmv_estimado) > 0 ? '(est.)' : ''}</span><b>${fmtMoney(-num(f.cmv))}</b></div>
      <div class="rp-row"><span>− Gastos</span><b>${f.gastos ? fmtMoney(-num(f.gastos)) : '—'}</b></div>
      <div class="rp-row rp-strong"><span>Resultado op.</span><b>${fmtMoney(f.resultado_op)} · ${fmtPct(f.pct)}</b></div>
    </div>
    <div class="rp-card">
      <div class="rp-h">IVA del mes</div>
      <div class="rp-row"><span>IVA débito (ventas)</span><b>${fmtMoney(c.iva_debito)}</b></div>
      <div class="rp-row"><span>IVA crédito (compras/gastos)</span><b>${fmtMoney(ivaCred)}</b></div>
      <div class="rp-row rp-strong"><span>IVA a pagar</span><b>${fmtMoney(c.iva_a_pagar)}</b></div>
      <div class="rp-sub">crédito incompleto hasta cargar todas las compras</div>
    </div>
    <div class="rp-card">
      <div class="rp-h">Cobranza</div>
      <div class="rp-row rp-strong"><span>Por cobrar (neto)</span><b>${fmtMoney(c.por_cobrar)}</b></div>
      <div class="rp-sub">= "por cobrar" de Ventas ML</div>
    </div>
  </div>
  <div class="rp-wrap rp-wrap-2">
    ${cardGastosCategoria(f)}
    ${cardComposicion(f)}
  </div></td></tr>`;
}
