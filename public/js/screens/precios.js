import { sbGet, sbPost, sbPatch } from '../core/sb.js';
import { calculatePriceSummary } from '../core/pricing.js';
import { configHTML, bindConfig } from './precios-config.js?v=1';

// ── Pantalla: Precios ───────────────────────────────────────────────────
// Simulador de precios por canal, migrado de pricing-adara-online (ver ADARA-PRICING.md).
// Es SIMULACIÓN (PRC1): usa el costo y las tasas propios de pricing (tablas pricing_*),
// no el FIFO ni el CM03. Nunca escribe en skus, lotes ni iibb_parametros (PRC2), y
// todavía no publica nada en Mercado Libre (paso 3).
// El costo FIFO se muestra al lado sólo como referencia.

let SKUS = [];          // skus activos
let PROD = {};          // sku_id -> pricing_producto
let CANALES = [];       // pricing_canal (todos, ordenados)
let TASAS = null;       // pricing_tasas (fila única)
let CATS = [];          // pricing_comision_categoria
let MARG = {};          // `${sku_id}|${canal}` -> pricing_margen
let FIFO = {};          // sku_id -> costo unitario FIFO (referencia)
let BUSQ = '';
let FILTRO = 'todos';   // 'todos' | 'con' | 'sin'
let VERCONFIG = false;

const MARGEN_DEFAULT = 5;   // pricing usa 5 % cuando el SKU no tiene margen cargado para el canal

// ── Formato y parseo ────────────────────────────────────────────────────
function fmt$(v) {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return '—';
  return '$ ' + Math.round(Number(v)).toLocaleString('es-AR');
}
function fmtPct(v) {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return '—';
  return Number(v).toLocaleString('es-AR', { maximumFractionDigits: 1 }) + '%';
}
const fmtTasa = v => Number(v || 0).toLocaleString('es-AR', { maximumFractionDigits: 4 }) + '%';
// Acepta "342000", "342.000", "342.000,50" y "8,4" / "8.4".
function num(str) {
  const s = String(str ?? '').trim().replace(/\s|\$|%/g, '');
  if (s === '') return null;
  let n;
  if (s.includes(',')) n = Number(s.replace(/\./g, '').replace(',', '.'));
  else if (/^\d{1,3}(\.\d{3})+$/.test(s)) n = Number(s.replace(/\./g, ''));
  else n = Number(s);
  return Number.isFinite(n) ? n : NaN;
}
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}
const UTIL = { fmt$, fmtPct, num, esc };

// ── Carga ───────────────────────────────────────────────────────────────
export async function loadPrecios() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando precios…</div>`;
  try {
    await recargar();
  } catch (e) {
    root.innerHTML = `<div class="error"><strong>No se pudo cargar Precios:</strong><br>${esc(e.message)}</div>`;
    return;
  }
  inyectarEstilo();
  render();
}

async function recargar() {
  const [skus, prod, canales, tasas, cats, marg, fifo] = await Promise.all([
    sbGet('skus', 'select=id,codigo,descripcion,familia&activo=eq.true&order=codigo.asc,id.asc'),
    sbGet('pricing_producto', 'order=sku_id.asc'),
    sbGet('pricing_canal', 'order=orden.asc,codigo.asc'),
    sbGet('pricing_tasas', 'id=eq.1'),
    sbGet('pricing_comision_categoria', 'order=categoria.asc,id.asc'),
    sbGet('pricing_margen', 'order=id.asc'),
    sbGet('v_costo_sku_actual', 'select=sku_id,costo_unit'),
  ]);
  SKUS = skus;
  PROD = Object.fromEntries(prod.map(p => [p.sku_id, p]));
  CANALES = canales;
  TASAS = tasas[0] || { iibb_pct: 0, idc_pct: 0, iigg_pct: 0 };
  CATS = cats;
  MARG = Object.fromEntries(marg.map(m => [`${m.sku_id}|${m.canal_codigo}`, m]));
  FIFO = Object.fromEntries(fifo.map(f => [f.sku_id, f.costo_unit]));
}

// ── Cálculo (adaptador tablas pricing_* → core/pricing.js) ─────────────
function opcionDe(canal) {
  return {
    code: canal.codigo,
    name: canal.nombre,
    channel_type: canal.tipo,
    installment_count: canal.cuotas,
    financing_fee_rate: Number(canal.costo_financiacion_pct || 0),
    applies_marketplace_fee: canal.aplica_comision_ml,
    applies_shipping: canal.aplica_envio,
    applies_iibb: canal.aplica_iibb,
    applies_idc: canal.aplica_idc,
    applies_iigg: canal.aplica_iigg,
    applies_structure: canal.aplica_estructura,
    applies_vat: canal.aplica_iva,
  };
}

// overrides: valores del modal todavía sin guardar ({ margen, prod }).
function calcular(skuId, canal, overrides = {}) {
  const p = overrides.prod || PROD[skuId];
  if (!p || !(Number(p.costo_sin_iva) > 0)) return null;
  const m = overrides.margen || MARG[`${skuId}|${canal.codigo}`] || {};
  const cat = CATS.find(c => c.activo && (c.categoria || '').toLowerCase() === (p.categoria || '').toLowerCase());
  return calculatePriceSummary(
    { cost_without_vat: Number(p.costo_sin_iva), vat_rate: Number(p.iva_pct) },
    opcionDe(canal),
    cat ? { marketplace_fee_rate: Number(cat.comision_pct) } : null,
    { iibb_rate: Number(TASAS.iibb_pct), idc_rate: Number(TASAS.idc_pct), iigg_rate: Number(TASAS.iigg_pct) },
    { fixed_fee_amount: Number(p.cargo_fijo_ml_monto || 0), shipping_cost_amount: Number(p.envio_ml_monto || 0) },
    {
      desiredMarginRate: m.margen_pct != null ? Number(m.margen_pct) : MARGEN_DEFAULT,
      desiredNetProfit: m.utilidad_neta != null ? Number(m.utilidad_neta) : null,
      salePrice: m.pvp_manual != null ? Number(m.pvp_manual) : null,
      structureAmount: Number(m.estructura_monto || 0),
      manualShippingAmount: Number(m.envio_manual_monto || 0),
      salesCommissionRate: Number(m.comision_venta_pct || 0),
      saleAppliesVat: m.venta_con_iva ?? undefined,
      costVatRate: Number(m.iva_costo_pct || 0),
      roundTo: canal.redondeo_a,
      roundingMode: canal.modo_redondeo,
    },
  );
}

// ── Render ──────────────────────────────────────────────────────────────
function visibles() {
  const q = BUSQ.trim().toLowerCase();
  return SKUS.filter(s => {
    const con = Number(PROD[s.id]?.costo_sin_iva) > 0;
    if (FILTRO === 'con' && !con) return false;
    if (FILTRO === 'sin' && con) return false;
    return !q || s.codigo.toLowerCase().includes(q) || (s.descripcion || '').toLowerCase().includes(q);
  });
}

function render() {
  const root = document.getElementById('app-screens');
  const conCosto = SKUS.filter(s => Number(PROD[s.id]?.costo_sin_iva) > 0).length;
  const mc = CANALES.find(c => c.codigo === 'MC');
  const margenes = mc ? SKUS.map(s => calcular(s.id, mc)).filter(r => r && r.valid).map(r => r.marginOnNetSale) : [];
  const promedio = margenes.length ? margenes.reduce((a, b) => a + b, 0) / margenes.length : null;
  const pill = (val, label, n) => `<button class="pill ${FILTRO === val ? 'active' : ''}" data-filtro="${val}">${label} <span class="num">${n}</span></button>`;

  root.innerHTML = `
    <div id="prc">
      <div class="prc-aviso">Simulador de precios. Usa el costo y las tasas propios de pricing, no el FIFO ni el CM03 (PRC1). No publica nada en Mercado Libre.</div>
      <div class="kpi-grid" style="margin:0 0 14px">
        <div class="kpi"><div class="kpi-label">SKUs con costo de simulación</div><div class="kpi-value">${conCosto} / ${SKUS.length}</div><div class="kpi-sub">${SKUS.length - conCosto} sin costo cargado</div></div>
        <div class="kpi"><div class="kpi-label">Margen neto promedio · ML Clásica</div><div class="kpi-value">${fmtPct(promedio)}</div><div class="kpi-sub">sobre venta neta, después de Ganancias</div></div>
        <div class="kpi"><div class="kpi-label">Tasas</div><div class="kpi-value" style="font-size:22px">IIBB ${fmtTasa(TASAS.iibb_pct)}</div><div class="kpi-sub">IDC ${fmtTasa(TASAS.idc_pct)} · Ganancias ${fmtTasa(TASAS.iigg_pct)}</div></div>
      </div>
      <div class="toolbar">
        <div class="grow"><input class="input" id="prc-busq" placeholder="Buscar por código o descripción…" value="${esc(BUSQ)}"></div>
        <button class="btn btn-ghost" data-accion="config">${VERCONFIG ? 'Ocultar configuración' : 'Tasas, canales y comisiones'}</button>
      </div>
      <div class="pills">
        ${pill('todos', 'Todos', SKUS.length)}${pill('con', 'Con costo', conCosto)}${pill('sin', 'Sin costo', SKUS.length - conCosto)}
      </div>
      <div id="prc-config">${VERCONFIG ? configHTML({ TASAS, CANALES, CATS }, UTIL) : ''}</div>
      <div class="table-wrap" id="prc-tabla"></div>
      <div class="prc-muted" id="prc-cuenta"></div>
    </div>`;

  bindDelegation(document.getElementById('prc'));
  if (VERCONFIG) bindConfig(document.getElementById("prc-config"), { TASAS, CANALES, CATS }, recargarYRender, UTIL);
  renderTabla();
}

function renderTabla() {
  const filas = visibles();
  const activos = CANALES.filter(c => c.activo);
  const opcCat = cat => ['<option value="">—</option>',
    ...CATS.map(c => `<option value="${esc(c.categoria)}" ${(cat || '').toLowerCase() === c.categoria.toLowerCase() ? 'selected' : ''}>${esc(c.categoria)}</option>`),
    ...(cat && !CATS.some(c => c.categoria.toLowerCase() === cat.toLowerCase()) ? [`<option selected>${esc(cat)}</option>`] : []),
  ].join('');

  const cuerpo = filas.map(s => {
    const p = PROD[s.id] || {};
    const celdas = activos.map(c => {
      const r = calcular(s.id, c);
      if (!r) return '<td class="prc-num prc-muted">·</td>';
      if (!r.valid) return `<td class="prc-num prc-err" title="${esc(r.error)}">error</td>`;
      const manual = MARG[`${s.id}|${c.codigo}`]?.pvp_manual != null;
      return `<td class="prc-num"><div class="prc-precio">${fmt$(r.roundedPrice)}${manual ? ' ✎' : ''}</div><div class="prc-mg ${r.netProfit < 0 ? 'neg' : ''}">${fmtPct(r.marginOnNetSale)}</div></td>`;
    }).join('');
    return `<tr data-sku="${s.id}">
      <td class="prc-code">${esc(s.codigo)}</td>
      <td class="prc-desc" title="${esc(s.descripcion)}">${esc(s.descripcion)}</td>
      <td><input class="inline-input prc-in" data-campo="costo_sin_iva" inputmode="decimal" value="${p.costo_sin_iva != null && Number(p.costo_sin_iva) > 0 ? Number(p.costo_sin_iva) : ''}" placeholder="0"></td>
      <td><select class="inline-select" data-campo="iva_pct">${[21, 10.5].map(v => `<option value="${v}" ${Number(p.iva_pct ?? 21) === v ? 'selected' : ''}>${v === 21 ? '21%' : '10,5%'}</option>`).join('')}</select></td>
      <td><select class="inline-select" data-campo="categoria">${opcCat(p.categoria)}</select></td>
      <td class="prc-num prc-muted">${fmt$(FIFO[s.id])}</td>
      ${celdas}
      <td><button class="btn btn-ghost prc-btn" data-accion="detalle">Detalle</button></td>
    </tr>`;
  }).join('');

  document.getElementById('prc-tabla').innerHTML = filas.length === 0 ? '<div class="empty">No hay SKUs que coincidan con el filtro</div>' : `
    <table class="t prc-t">
      <thead><tr>
        <th>Código</th><th>Descripción</th><th>Costo sim. s/IVA</th><th>IVA</th><th>Categoría ML</th><th class="prc-num">FIFO (ref.)</th>
        ${activos.map(c => `<th class="prc-num" title="${esc(c.nombre)}">${esc(c.codigo)}</th>`).join('')}<th></th>
      </tr></thead>
      <tbody>${cuerpo}</tbody>
    </table>`;
  document.getElementById('prc-cuenta').textContent = `Mostrando ${filas.length} de ${SKUS.length} · precio redondeado con IVA y margen neto sobre venta · ✎ = PVP manual`;
}

// ── Eventos (delegación sobre #prc, que se recrea en cada render) ───────
function bindDelegation(host) {
  host.addEventListener('input', e => {
    if (e.target.id === 'prc-busq') { BUSQ = e.target.value; renderTabla(); }
  });
  host.addEventListener('click', e => {
    const b = e.target.closest('[data-filtro],[data-accion]');
    if (!b || !host.contains(b)) return;
    if (b.dataset.filtro) { FILTRO = b.dataset.filtro; render(); return; }
    if (b.dataset.accion === 'config') { VERCONFIG = !VERCONFIG; render(); return; }
    if (b.dataset.accion === 'detalle') abrirDetalle(Number(b.closest('tr').dataset.sku));
  });
  host.addEventListener('change', e => {
    const el = e.target;
    const tr = el.closest('#prc-tabla tr[data-sku]');
    if (!tr || !el.dataset.campo) return;
    let valor = el.value;
    if (el.dataset.campo === 'costo_sin_iva') {
      valor = num(valor) ?? 0;
      if (Number.isNaN(valor) || valor < 0) { window.toast('Costo inválido', 'error'); return; }
    } else if (el.dataset.campo === 'iva_pct') valor = Number(valor);
    else valor = valor || null;
    guardarProducto(Number(tr.dataset.sku), { [el.dataset.campo]: valor });
  });
}

// Alta o actualización de pricing_producto. El trigger registra el historial de costo.
async function guardarProducto(skuId, cambios) {
  try {
    if (PROD[skuId]) {
      const [row] = await sbPatch('pricing_producto', `sku_id=eq.${skuId}`, cambios);
      PROD[skuId] = row || { ...PROD[skuId], ...cambios };
    } else {
      const [row] = await sbPost('pricing_producto', { sku_id: skuId, ...cambios });
      PROD[skuId] = row;
    }
    render();
    window.toast('Guardado');
  } catch (e) { window.toast('No se pudo guardar: ' + e.message, 'error'); }
}

async function recargarYRender() {
  await recargar();
  render();
}

// El modal de detalle vive en su propio módulo para no pasar las 500 líneas.
async function abrirDetalle(skuId) {
  const { abrirDetallePrecio } = await import('./precios-detalle.js?v=1');
  abrirDetallePrecio({
    sku: SKUS.find(s => s.id === skuId), prod: PROD[skuId], canales: CANALES.filter(c => c.activo),
    margenes: MARG, calcular, alGuardar: recargarYRender, util: UTIL,
  });
}

// ── Estilos ─────────────────────────────────────────────────────────────
function inyectarEstilo() {
  if (document.getElementById('prc-style')) return;
  const s = document.createElement('style');
  s.id = 'prc-style';
  s.textContent = `
    .prc-aviso{background:#FEF3C7;border:1px solid #FDE68A;color:#854F0B;border-radius:var(--r-sm);padding:8px 12px;font-size:13px;margin-bottom:14px}
    .prc-t td,.prc-t th{white-space:nowrap}
    .prc-t .prc-desc{max-width:240px;overflow:hidden;text-overflow:ellipsis}
    .prc-code{font-family:'JetBrains Mono',monospace;font-size:13px}
    .prc-num{text-align:right}
    .prc-precio{font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600}
    .prc-mg{font-size:11px;color:#15803D}
    .prc-mg.neg,.prc-err{color:#B91C1C}
    .prc-muted{color:var(--text-muted);font-size:13px}
    #prc-cuenta{margin-top:10px;text-align:right}
    .prc-in{width:110px;text-align:right}
    .prc-t .inline-select[data-campo="iva_pct"]{min-width:76px}
    .prc-t .inline-select[data-campo="categoria"]{min-width:150px}
    .prc-btn{padding:4px 10px;font-size:13px}
    .prc-cfg{background:#fff;border:1px solid #E7E5E4;border-radius:12px;padding:16px;margin-bottom:16px}
    .prc-cfg h4{margin:0 0 10px;font-size:15px}
    .prc-cfg-grid{display:grid;grid-template-columns:200px minmax(0,1.3fr) minmax(0,1fr);gap:20px}
    .prc-cfg-grid>div{min-width:0;overflow-x:auto}
    .prc-cfg td,.prc-cfg th{white-space:nowrap;padding:6px 8px}
    .prc-cfg td.prc-wrap{white-space:normal;min-width:120px}
    .prc-cfg .inline-input{width:90px;text-align:right}
    .prc-det td,.prc-det th{white-space:nowrap;padding:6px 8px}
    .prc-det .inline-input{width:100px;text-align:right}
    @media (max-width:1100px){.prc-cfg-grid{grid-template-columns:1fr}}`;
  document.head.appendChild(s);
}
