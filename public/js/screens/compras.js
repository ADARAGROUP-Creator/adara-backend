import { sbGet, sbPost } from '../core/sb.js';

// ── Pantalla: Compras (capa 2) ──────────────────────────────────────────
// Facturas de compra de MERCADERÍA (local). A diferencia de Gastos, una compra
// se vuelve STOCK (lotes) y recién impacta el P&L como CMV al vender (FIFO).
// Dos pestañas: Facturas (lista + alta) y Cuenta corriente por proveedor
// (suma compras + gastos del proveedor = lo que le debés / te debe).
// Importaciones (USD, prorrateo de nacionalización) son otro flujo, pendiente.

let LINEAS = [], LINEA_LABEL = {};
let PROVEEDORES = [], PROV_BY_ID = {};
let SKUS = [], SKU_BY_ID = {};
let COMPRAS = [];     // v_compras_ap
let GASTOS_AP = [];   // v_gastos_ap (para cuenta corriente)
let ADJ = {};         // adjuntos por compra: compra_id -> [{id, nombre}]
let TAB = 'facturas';
let FILTRO = { periodo: '', q: '' };
let DETALLE_ID = null;   // compra con el detalle abierto (una por vez)
let DETALLE = {};        // cache compra_id -> { comps, lotes, gastosCap }

const hoyISO = () => new Date().toISOString().slice(0, 10);
const lineaLabel = l => l.nombre || l.descripcion || l.codigo || ('Línea ' + l.id);
const provLabel = p => p && (p.nombre || p.cuit || ('Proveedor #' + p.id)) || '— Sin proveedor —';
const skuLabel = s => `${s.codigo}${s.descripcion ? ' — ' + s.descripcion : ''}`;
const num = n => Number(n) || 0;
const money = n => '$ ' + num(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ddmm = f => `${(f || '').slice(8, 10)}/${(f || '').slice(5, 7)}`;
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// Jurisdicciones de Convenio Multilateral. El value (snake_case) es el MISMO que
// usan las retenciones de IIBB de ventas (tabla retenciones / v_retenciones_iibb),
// para que percepciones de compra y retenciones de venta crucen por provincia.
const JURISDICCIONES = [
  ['', '— Sin jurisdicción / Nacional'],
  ['buenos_aires', 'Buenos Aires'],
  ['caba', 'CABA'],
  ['catamarca', 'Catamarca'],
  ['chaco', 'Chaco'],
  ['chubut', 'Chubut'],
  ['cordoba', 'Córdoba'],
  ['corrientes', 'Corrientes'],
  ['entre_rios', 'Entre Ríos'],
  ['formosa', 'Formosa'],
  ['jujuy', 'Jujuy'],
  ['la_pampa', 'La Pampa'],
  ['la_rioja', 'La Rioja'],
  ['mendoza', 'Mendoza'],
  ['misiones', 'Misiones'],
  ['neuquen', 'Neuquén'],
  ['rio_negro', 'Río Negro'],
  ['salta', 'Salta'],
  ['san_juan', 'San Juan'],
  ['san_luis', 'San Luis'],
  ['santa_cruz', 'Santa Cruz'],
  ['santa_fe', 'Santa Fe'],
  ['santiago_del_estero', 'Santiago del Estero'],
  ['tierra_del_fuego', 'Tierra del Fuego'],
  ['tucuman', 'Tucumán'],
];

// Familias (igual que la pantalla SKUs) para el alta rápida de SKU en la compra.
const FAMILIAS = [
  { val: '', label: 'Sin clasificar' },
  { val: 'electronica', label: 'Electrónica' },
  { val: 'luminaria', label: 'Luminaria' },
  { val: 'mochila_sindical', label: 'Mochila sindical' },
  { val: 'mochila_individual', label: 'Mochila individual' },
];

export async function loadCompras() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando compras…</div>`;
  try {
    LINEAS = await sbGet('lineas_negocio', 'order=id.asc');
    LINEA_LABEL = Object.fromEntries(LINEAS.map(l => [l.id, lineaLabel(l)]));
    SKUS = await sbGet('skus', 'activo=eq.true&order=codigo.asc&select=id,codigo,descripcion,alicuota_iva');
    SKU_BY_ID = Object.fromEntries(SKUS.map(s => [s.id, s]));
    await recargar();
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudo cargar Compras: ${e.message}</div>`;
    return;
  }
  inyectarEstilo();
  render();
}

async function recargar() {
  PROVEEDORES = await sbGet('proveedores', 'order=nombre.asc').catch(() => []);
  PROV_BY_ID = Object.fromEntries(PROVEEDORES.map(p => [p.id, p]));
  COMPRAS = await sbGet('v_compras_ap', 'order=fecha.desc,compra_id.desc');
  GASTOS_AP = await sbGet('v_gastos_ap', 'select=proveedor_id,a_pagar_ars,vinculado_ars,saldo_pendiente_ars').catch(() => []);
  ADJ = {};
  try {
    const adj = await sbGet('adjuntos', 'op_tipo=eq.compra&select=id,op_id,nombre&order=id.desc');
    for (const a of (adj || [])) (ADJ[a.op_id] = ADJ[a.op_id] || []).push(a);
  } catch { ADJ = {}; }
}

async function verAdjunto(id) {
  try {
    const r = await fetch('/adjuntos/' + id + '/url');
    const d = await r.json().catch(() => ({}));
    if (!r.ok || !d.url) throw new Error(d.error || 'No se pudo abrir');
    window.open(d.url, '_blank');
  } catch (e) { window.toast('Error al abrir el adjunto: ' + e.message, 'error'); }
}

function estadoPago(c) {
  const saldo = num(c.saldo_ap_ars);
  if (saldo < 0.02) return 'pagado';
  if (num(c.pagado_ars) > 0) return 'parcial';
  return 'pendiente';
}

function render() {
  const root = document.getElementById('app-screens');
  const tab = (val, label) => `<button class="pill ${TAB === val ? 'active' : ''}" data-tab="${val}">${label}</button>`;
  root.innerHTML = `
    <div class="pills" style="margin-bottom:14px">
      ${tab('facturas', 'Facturas')}
      ${tab('cc', 'Cuenta corriente')}
    </div>
    <div id="com-body"></div>`;
  root.querySelectorAll('.pill[data-tab]').forEach(p =>
    p.addEventListener('click', () => { TAB = p.dataset.tab; render(); }));
  if (TAB === 'facturas') renderFacturas();
  else renderCC();
}

// ── Pestaña Facturas ────────────────────────────────────────────────────
function renderFacturas() {
  const body = document.getElementById('com-body');
  const activas = COMPRAS.filter(c => c.estado_compra !== 'anulada' && c.tipo_compra !== 'inicial');

  const periodos = [...new Set(activas.map(c => c.periodo).filter(Boolean))].sort().reverse();
  const optPeriodo = '<option value="">Todos los períodos</option>' +
    periodos.map(p => `<option value="${p}" ${FILTRO.periodo === p ? 'selected' : ''}>${p}</option>`).join('');

  const filtradas = activas.filter(c => {
    if (FILTRO.periodo && c.periodo !== FILTRO.periodo) return false;
    if (FILTRO.q) {
      const txt = `${provLabel(PROV_BY_ID[c.proveedor_id])} ${LINEA_LABEL[c.linea_id] || ''}`.toLowerCase();
      if (!txt.includes(FILTRO.q.toLowerCase())) return false;
    }
    return true;
  });

  // Los KPI miran lo FILTRADO, no el total: con un período elegido, decir "7 compras" arriba
  // de una tabla que muestra 4 es confuso.
  const comprado = filtradas.reduce((s, c) => s + num(c.total_facturado_ars), 0);
  const pendiente = filtradas.reduce((s, c) => s + num(c.saldo_ap_ars), 0);
  const sufijo = FILTRO.periodo ? ` · ${FILTRO.periodo}` : ' · todos';

  body.innerHTML = `
    <div class="toolbar" style="justify-content:space-between">
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <select class="select" id="cf-periodo" style="width:auto">${optPeriodo}</select>
        <input class="input" id="cf-q" placeholder="Buscar proveedor / línea…" value="${esc(FILTRO.q)}" style="width:220px">
      </div>
      <button class="btn btn-primary" id="cf-nueva">+ Nueva compra</button>
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">Compras${sufijo}</div><div class="kpi-value">${filtradas.length}</div></div>
      <div class="kpi"><div class="kpi-label">Comprado${sufijo}</div><div class="kpi-value">${money(comprado)}</div></div>
      <div class="kpi"><div class="kpi-label">Pendiente de pago${sufijo}</div><div class="kpi-value">${money(pendiente)}</div></div>
    </div>

    ${filtradas.length === 0
      ? `<div class="empty">No hay compras cargadas todavía.</div>`
      : `<div class="table-wrap"><table class="t">
          <thead><tr>
            <th style="width:62px">Fecha</th><th>Proveedor</th><th style="width:160px">Factura</th><th style="width:150px">Línea</th>
            <th style="width:140px;text-align:right">Total</th>
            <th style="width:140px;text-align:right">Pagado</th>
            <th style="width:140px;text-align:right">Saldo</th>
            <th style="width:96px">Pago</th>
            <th style="width:64px"></th>
          </tr></thead>
          <tbody>${filtradas.map(filaCompra).join('')}</tbody>
        </table></div>`}
  `;

  document.getElementById('cf-periodo').addEventListener('change', e => { FILTRO.periodo = e.target.value; renderFacturas(); });
  document.getElementById('cf-q').addEventListener('input', e => { FILTRO.q = e.target.value; renderFacturas(); });
  document.getElementById('cf-nueva').addEventListener('click', openAlta);
  document.querySelectorAll('.com-anular').forEach(b => b.addEventListener('click', e => { e.stopPropagation(); anularCompra(+b.dataset.id); }));
  document.querySelectorAll('.com-pdf').forEach(b => b.addEventListener('click', e => { e.stopPropagation(); verAdjunto(+b.dataset.adj); }));
  document.querySelectorAll('.com-asignar-fac').forEach(b => b.addEventListener('click', e => { e.stopPropagation(); asignarFactura(+b.dataset.id); }));
  document.querySelectorAll('tr.com-fila').forEach(tr => tr.addEventListener('click', () => toggleDetalle(+tr.dataset.id)));
  document.querySelectorAll('.com-adj-dl').forEach(b => b.addEventListener('click', e => { e.stopPropagation(); verAdjunto(+b.dataset.adj); }));
}

// ── Detalle expandible ──────────────────────────────────────────────────
// Se carga a demanda al abrir la fila (una compra por vez) y queda cacheado.
async function toggleDetalle(compraId) {
  if (DETALLE_ID === compraId) { DETALLE_ID = null; renderFacturas(); return; }
  DETALLE_ID = compraId;
  if (!DETALLE[compraId]) {
    renderFacturas();   // pinta el "Cargando…" mientras trae los datos
    try {
      const [comps, lotes, gastosCap] = await Promise.all([
        sbGet('compra_componentes', `compra_id=eq.${compraId}&order=id.asc`),
        sbGet('lotes', `compra_id=eq.${compraId}&order=id.asc`),
        sbGet('v_gastos_ap', `capitaliza_compra_id=eq.${compraId}&order=fecha.asc`).catch(() => [])
      ]);
      DETALLE[compraId] = { comps, lotes, gastosCap };
    } catch (e) {
      DETALLE[compraId] = { error: e.message };
    }
  }
  renderFacturas();
}

const TIPO_LABEL = {
  producto: 'Producto', flete: 'Flete / cargo del proveedor', seguro: 'Seguro',
  arancel: 'Derechos', tasa_estadistica: 'Tasa estadística', despacho: 'Despacho',
  otro_costo: 'Otro costo', gasto_prorrateable: 'Gasto de tercero (prorrateado)',
  extra_directo: 'Extra directo', sin_factura: 'Sin factura',
  iva: 'IVA', iibb_percepcion: 'Percepción IIBB',
  ganancias_percepcion: 'Percepción Ganancias', otro_impuesto: 'Otro impuesto'
};

function filaDetalle(c) {
  const d = DETALLE[c.compra_id];
  const cols = 9;
  const wrap = inner => `<tr class="com-det"><td colspan="${cols}"><div class="com-det-box">${inner}</div></td></tr>`;
  if (!d) return wrap(`<div class="com-det-load">Cargando detalle…</div>`);
  if (d.error) return wrap(`<div class="com-det-err">No se pudo cargar el detalle: ${esc(d.error)}</div>`);

  const skuDe = id => (SKU_BY_ID[id] && SKU_BY_ID[id].codigo) || (id ? '#' + id : '—');
  const desc = id => (SKU_BY_ID[id] || {}).descripcion || '';
  const esUSD = c.moneda === 'USD';
  const sim = esUSD ? 'US$ ' : '$ ';
  const n2 = n => num(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const m = n => sim + n2(n);        // moneda de la factura
  const a = n => '$ ' + n2(n);       // ARS (lotes siempre en ARS)

  const prod   = d.comps.filter(x => x.tipo === 'producto');
  const costos = d.comps.filter(x => x.clase === 'costo' && x.tipo !== 'producto');
  const ivas   = d.comps.filter(x => x.tipo === 'iva');
  const percs  = d.comps.filter(x => x.clase === 'fiscal' && x.tipo !== 'iva');

  const totProd   = prod.reduce((s, x) => s + num(x.monto), 0);
  const totCostos = costos.reduce((s, x) => s + num(x.monto), 0);
  const totIva    = ivas.reduce((s, x) => s + num(x.monto), 0);
  const totPerc   = percs.reduce((s, x) => s + num(x.monto), 0);

  const uIni = d.lotes.reduce((s, l) => s + num(l.cantidad_inicial), 0);
  const uAct = d.lotes.reduce((s, l) => s + num(l.cantidad_actual), 0);
  const valorStock = d.lotes.reduce((s, l) => s + num(l.cantidad_actual) * num(l.costo_unitario), 0);
  const vendidas = uIni - uAct;
  const pctVend = uIni > 0 ? Math.round((vendidas / uIni) * 100) : 0;

  const prov = PROV_BY_ID[c.proveedor_id] || {};
  const chip = (k, v) => `<div class="com-chip"><span>${k}</span><b>${v}</b></div>`;

  const cabecera = `
    <div class="com-det-head">
      <div class="com-det-title">
        ${esc(provLabel(prov))}
        ${prov.cuit ? `<span class="com-det-cuit">CUIT ${esc(prov.cuit)}</span>` : ''}
      </div>
      <div class="com-chips">
        ${chip('Factura', c.nro_factura ? esc(c.nro_factura) : '<i>pendiente</i>')}
        ${chip('Fecha', esc(c.fecha || ''))}
        ${chip('Línea', esc(LINEA_LABEL[c.linea_id] || '—'))}
        ${chip('Moneda', esc(c.moneda) + (esUSD && c.tc_blue ? ` · TC ${n2(c.tc_blue)}` : ''))}
      </div>
    </div>`;

  const tabla = (titulo, nota, filas) => !filas.length ? '' : `
    <div class="com-card">
      <div class="com-card-h">${titulo}${nota ? `<span class="com-card-n">${nota}</span>` : ''}</div>
      <table class="com-card-t"><tbody>${filas.join('')}</tbody></table>
    </div>`;

  const filasProd = prod.map(x => `<tr>
      <td class="com-c-sku">${esc(skuDe(x.sku_id))}</td>
      <td class="com-c-desc" title="${esc(desc(x.sku_id))}">${esc(desc(x.sku_id))}</td>
      <td class="com-c-num">${num(x.cantidad)} u</td>
      <td class="com-c-num com-mono">${m(num(x.monto) / (num(x.cantidad) || 1))}</td>
      <td class="com-c-num com-mono com-c-tot">${m(x.monto)}</td>
    </tr>`);

  const filasCostos = costos.map(x => `<tr>
      <td colspan="4"><span class="com-tag com-tag-costo">${esc(TIPO_LABEL[x.tipo] || x.tipo)}</span>
        ${x.descripcion ? `<span class="com-c-desc">${esc(x.descripcion)}</span>` : ''}</td>
      <td class="com-c-num com-mono com-c-tot">${m(x.monto)}</td>
    </tr>`);

  const filasFisc = [...ivas, ...percs].map(x => `<tr>
      <td colspan="4"><span class="com-tag com-tag-fisc">${esc(TIPO_LABEL[x.tipo] || x.tipo)}</span>
        ${x.descripcion ? `<span class="com-c-desc">${esc(x.descripcion)}</span>` : ''}</td>
      <td class="com-c-num com-mono com-c-tot">${m(x.monto)}</td>
    </tr>`);

  const filasLotes = d.lotes.map(l => {
    const v = num(l.cantidad_inicial) - num(l.cantidad_actual);
    const pct = num(l.cantidad_inicial) > 0 ? Math.round((v / num(l.cantidad_inicial)) * 100) : 0;
    return `<tr>
      <td class="com-c-sku">${esc(skuDe(l.sku_id))}</td>
      <td class="com-c-desc">
        <div class="com-bar"><div class="com-bar-f" style="width:${pct}%"></div></div>
        <span class="com-bar-l">${num(l.cantidad_actual)} de ${num(l.cantidad_inicial)} en stock${v > 0 ? ` · ${v} vendida${v === 1 ? '' : 's'}` : ''}</span>
      </td>
      <td class="com-c-num com-mono">${a(l.costo_unitario)}<span class="com-c-u">/u</span></td>
      <td class="com-c-num com-mono com-muted">${a(num(l.cantidad_actual) * num(l.costo_unitario))}</td>
      <td class="com-c-num com-mono com-c-tot">${a(num(l.cantidad_inicial) * num(l.costo_unitario))}</td>
    </tr>`;
  });

  const filasGastos = (d.gastosCap || []).map(g => `<tr>
      <td class="com-c-sku">${esc((g.fecha || '').slice(8, 10))}/${esc((g.fecha || '').slice(5, 7))}</td>
      <td class="com-c-desc">${esc(g.descripcion || '')}${g.nro_comprobante ? ` · ${esc(g.nro_comprobante)}` : ''}</td>
      <td colspan="2" class="com-c-num"><span class="com-badge com-badge-${esc(g.estado_pago)}">${esc(g.estado_pago)}</span></td>
      <td class="com-c-num com-mono com-c-tot">$ ${n2(g.monto_neto)}</td>
    </tr>`);

  const linea = (k, v, cls = '') => `<div class="com-tot-r ${cls}"><span>${k}</span><b class="com-mono">${v}</b></div>`;
  const totales = `
    <div class="com-card com-card-tot">
      <div class="com-card-h">Totales</div>
      <div class="com-tot">
        ${linea('Productos (neto)', m(totProd))}
        ${totCostos ? linea('+ otros costos al lote', m(totCostos)) : ''}
        ${linea('= Costo de la mercadería', m(totProd + totCostos), 'com-tot-mid')}
        ${totIva ? linea('IVA (crédito fiscal)', m(totIva), 'com-tot-fisc') : ''}
        ${totPerc ? linea('Percepciones (crédito fiscal)', m(totPerc), 'com-tot-fisc') : ''}
        ${linea('Total factura', money(c.total_facturado_ars), 'com-tot-big')}
        <div class="com-tot-sep"></div>
        ${linea('Pagado', money(c.pagado_ars))}
        ${linea('Saldo pendiente', money(c.saldo_ap_ars), num(c.saldo_ap_ars) > 0 ? 'com-tot-deuda' : 'com-tot-ok')}
        ${uIni > 0 ? `<div class="com-tot-sep"></div>
          ${linea('Unidades', `${uAct} de ${uIni} en stock (${pctVend}% vendido)`)}
          ${linea('Stock valorizado', a(valorStock))}` : ''}
      </div>
    </div>`;

  const adjs = ADJ[c.compra_id] || [];
  const comprobante = `
    <div class="com-card">
      <div class="com-card-h">Comprobante</div>
      ${adjs.length
        ? `<div class="com-det-adj">${adjs.map(x =>
            `<button class="com-adj-dl" data-adj="${x.id}">
               <span class="com-adj-ic">PDF</span>
               <span class="com-adj-n">${esc(x.nombre)}</span>
               <span class="com-adj-go">abrir ↗</span>
             </button>`).join('')}</div>`
        : `<div class="com-det-vacio">Esta compra no tiene comprobante adjunto.</div>`}
    </div>`;

  return wrap(`
    ${cabecera}
    <div class="com-det-cols">
      <div class="com-det-main">
        ${tabla('Productos', `${prod.length} renglón${prod.length === 1 ? '' : 'es'}`, filasProd)}
        ${tabla('Otros costos', 'se reparten en el costo de los lotes', filasCostos)}
        ${tabla('Impuestos', 'crédito fiscal — no son costo', filasFisc)}
        ${d.lotes.length
          ? tabla('Lotes generados', 'costo unitario final en ARS', filasLotes)
          : `<div class="com-card"><div class="com-card-h">Lotes</div><div class="com-det-vacio">Esta compra no generó stock.</div></div>`}
        ${tabla('Gastos de terceros al costo de esta compra', 'cargados desde Gastos', filasGastos)}
      </div>
      <div class="com-det-side">
        ${totales}
        ${comprobante}
      </div>
    </div>`);
}

function filaCompra(c) {
  const est = estadoPago(c);
  const pendiente = c.tipo_compra === 'local' && c.estado_compra === 'activa' && !c.nro_factura;
  const facturaCell = c.nro_factura
    ? esc(c.nro_factura)
    : (pendiente
        ? `<span class="com-badge com-badge-pendiente">factura pendiente</span> <button class="com-asignar-fac" data-id="${c.compra_id}" title="Asignar N° de factura" style="font-size:12px;color:#2563EB;background:none;border:0;cursor:pointer;font:inherit;padding:2px 4px">asignar</button>`
        : '<span class="com-muted">—</span>');
  const abierta = DETALLE_ID === c.compra_id;
  return `<tr class="com-fila ${abierta ? 'com-fila-open' : ''}" data-id="${c.compra_id}" title="Ver detalle">
    <td>${ddmm(c.fecha)}</td>
    <td><span class="com-caret">${abierta ? '▾' : '▸'}</span> ${esc(provLabel(PROV_BY_ID[c.proveedor_id]))}</td>
    <td>${facturaCell}</td>
    <td class="com-muted">${esc(LINEA_LABEL[c.linea_id] || '—')}</td>
    <td style="text-align:right" class="com-mono">${money(c.total_facturado_ars)}</td>
    <td style="text-align:right" class="com-mono com-muted">${money(c.pagado_ars)}</td>
    <td style="text-align:right" class="com-mono">${money(c.saldo_ap_ars)}</td>
    <td><span class="com-badge com-badge-${est}">${est}</span></td>
    <td class="com-acc">
      ${(ADJ[c.compra_id] && ADJ[c.compra_id].length)
        ? `<button class="com-pdf" data-adj="${ADJ[c.compra_id][0].id}" title="Abrir ${esc(ADJ[c.compra_id][0].nombre)}"><span class="com-pdf-i">PDF</span></button>`
        : `<span class="com-pdf-no" title="Sin comprobante adjunto">—</span>`}
      ${c.tipo_compra === 'inicial' ? '' : `<button class="com-anular" data-id="${c.compra_id}" title="Anular compra">Anular</button>`}
    </td>
  </tr>${abierta ? filaDetalle(c) : ''}`;
}

async function asignarFactura(id) {
  const nro = prompt('N° de factura del proveedor (ej: A 0001-00001234):');
  if (nro == null) return;
  if (!nro.trim()) { window.toast('Poné el número de factura', 'error'); return; }
  try {
    const r = await fetch('/compras/' + id + '/factura', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ nro_factura: nro.trim() })
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.error || (r.status + ' ' + r.statusText));
    window.toast('Factura asignada');
    await recargar();
    renderFacturas();
  } catch (e) { window.toast('Error: ' + e.message, 'error'); }
}

async function anularCompra(id) {
  const motivo = prompt('Anular esta compra: se quita su stock (lotes) y deja de contar para cuentas por pagar.\n\nMotivo:');
  if (motivo == null) return;
  if (!motivo.trim()) { window.toast('Necesitás un motivo para anular', 'error'); return; }
  try {
    const r = await fetch('/compras/' + id + '/anular', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ motivo: motivo.trim() })
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.error || (r.status + ' ' + r.statusText));
    window.toast('Compra anulada');
    await recargar();
    renderFacturas();
  } catch (e) { window.toast('Error: ' + e.message, 'error'); }
}

// ── Pestaña Cuenta corriente ──────────────────────────────────────────────
function renderCC() {
  const body = document.getElementById('com-body');
  const acc = {}; // proveedor_id -> {fact, pag, saldo}
  const add = (pid, fact, pag, saldo) => {
    if (pid == null) return;
    const a = acc[pid] || (acc[pid] = { fact: 0, pag: 0, saldo: 0 });
    a.fact += fact; a.pag += pag; a.saldo += saldo;
  };
  for (const c of COMPRAS) {
    if (c.estado_compra === 'anulada' || c.tipo_compra === 'inicial') continue;
    add(c.proveedor_id, num(c.total_facturado_ars), num(c.pagado_ars), num(c.saldo_ap_ars));
  }
  for (const g of GASTOS_AP) {
    add(g.proveedor_id, num(g.a_pagar_ars), num(g.vinculado_ars), num(g.saldo_pendiente_ars));
  }

  const filas = Object.entries(acc)
    .map(([pid, a]) => ({ pid, ...a }))
    .sort((x, y) => Math.abs(y.saldo) - Math.abs(x.saldo));

  const totalDeuda = filas.reduce((s, f) => s + Math.max(0, f.saldo), 0);

  body.innerHTML = `
    <div class="kpi-grid" style="margin:0 0 14px">
      <div class="kpi"><div class="kpi-label">Proveedores con saldo</div><div class="kpi-value">${filas.filter(f => Math.abs(f.saldo) >= 0.02).length}</div></div>
      <div class="kpi"><div class="kpi-label">Total que debés</div><div class="kpi-value">${money(totalDeuda)}</div></div>
    </div>
    <p class="com-sub">Suma compras + gastos por proveedor. Saldo positivo = le debés; negativo = te debe.</p>
    ${filas.length === 0
      ? `<div class="empty">Todavía no hay operaciones con proveedores.</div>`
      : `<div class="table-wrap"><table class="t">
          <thead><tr>
            <th>Proveedor</th>
            <th style="width:150px;text-align:right">Facturado</th>
            <th style="width:150px;text-align:right">Pagado</th>
            <th style="width:200px;text-align:right">Saldo</th>
          </tr></thead>
          <tbody>${filas.map(filaCC).join('')}</tbody>
        </table></div>`}
  `;
}

function filaCC(f) {
  const saldo = f.saldo;
  let etiqueta = '<span class="com-muted">saldada</span>';
  if (saldo >= 0.02) etiqueta = `<span class="com-debe">le debés ${money(saldo)}</span>`;
  else if (saldo <= -0.02) etiqueta = `<span class="com-favor">te debe ${money(-saldo)}</span>`;
  return `<tr>
    <td>${esc(provLabel(PROV_BY_ID[f.pid]))}</td>
    <td style="text-align:right" class="com-mono com-muted">${money(f.fact)}</td>
    <td style="text-align:right" class="com-mono com-muted">${money(f.pag)}</td>
    <td style="text-align:right" class="com-mono">${etiqueta}</td>
  </tr>`;
}

// ── Alta de compra local ──────────────────────────────────────────────────
let ITEMS = [];
let PERCEPS = [];   // percepciones de la factura: { tipo:'iibb'|'ganancias', jurisdiccion, monto }
let GASTOS = [];    // gastos prorrateables (flete/comisión/despacho a terceros): { concepto, monto }

function openAlta() {
  ITEMS = [{ sku_id: '', cantidad: '', costo: '', iva: 0.21, extra: '' }];
  PERCEPS = [];
  GASTOS = [];
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal" style="max-width:680px">
      <div class="card-title">Nueva compra local</div>

      <div class="field">
        <label>Proveedor</label>
        <div style="display:flex;gap:8px;align-items:center">
          <select class="select" id="c-prov" style="flex:1">${optProv()}</select>
          <button class="btn btn-ghost com-mini" id="c-prov-add" type="button">+ Nuevo</button>
        </div>
        <div id="c-prov-new" style="display:none;flex-direction:column;gap:8px;margin-top:8px">
          <div style="display:flex;gap:8px;align-items:center">
            <input class="input" id="c-prov-cuit" placeholder="CUIT (opcional, sin guiones)" style="width:200px">
            <span id="c-prov-status" style="font-size:12px;color:#78716C"></span>
          </div>
          <div style="display:flex;gap:8px;align-items:center">
            <input class="input" id="c-prov-nombre" placeholder="Nombre (con o sin CUIT)" style="flex:1">
            <button class="btn btn-primary com-mini" id="c-prov-crear" type="button">Crear</button>
          </div>
        </div>
      </div>

      <div class="com-row3">
        <div class="field"><label>Fecha</label><input class="input" type="date" id="c-fecha" value="${hoyISO()}"></div>
        <div class="field"><label>Nº factura (opcional)</label><input class="input" id="c-factura" placeholder="A 0001-00001234"></div>
        <div class="field"><label>Línea de negocio</label><select class="select" id="c-linea"><option value="">Elegí línea…</option>${LINEAS.map(l => `<option value="${l.id}">${esc(LINEA_LABEL[l.id])}</option>`).join('')}</select></div>
      </div>

      <div class="com-row3">
        <div class="field"><label>Moneda</label>
          <select class="select" id="c-moneda"><option value="ARS">ARS</option><option value="USD">USD</option></select>
        </div>
        <div class="field" id="c-tc-wrap" style="display:none"><label>TC (USD→ARS)</label>
          <input class="input" id="c-tc" inputmode="decimal" placeholder="0,00"></div>
        <div class="field"><label>Factura / comprobante (PDF o imagen)</label>
          <input class="input" id="c-adjunto" type="file" accept="application/pdf,image/*"></div>
      </div>

      <div class="com-block">
        <div class="com-block-h"><span>Productos (mercadería)</span><div style="display:flex;gap:6px"><button class="btn btn-ghost com-mini" id="c-new-sku" type="button">+ Nuevo SKU</button><button class="btn btn-ghost com-mini" id="c-add-item" type="button">+ Agregar</button></div></div>
        <div id="c-items"></div>
      </div>

      <div class="com-block">
        <div class="com-block-h"><span>Impuestos de la factura (crédito fiscal — no son costo)</span></div>
        <div class="field" style="max-width:240px"><label>IVA (automático)</label><input class="input" id="c-iva-disp" readonly value="$ 0,00"></div>
        <div class="com-block-h" style="margin-top:10px"><span>Percepciones (una por jurisdicción)</span><button class="btn btn-ghost com-mini" id="c-add-perc" type="button">+ percepción</button></div>
        <div id="c-perceps"></div>
      </div>

      <div class="com-block">
        <div class="com-block-h"><span>Gastos prorrateables (flete, comisión, despacho — van al costo, no a la cuenta del proveedor)</span><button class="btn btn-ghost com-mini" id="c-add-gasto" type="button">+ gasto</button></div>
        <div class="field" style="max-width:240px"><label>Criterio de reparto</label>
          <select class="select" id="c-criterio"><option value="costo">Por costo neto</option><option value="unidades">Por unidades</option></select>
        </div>
        <div id="c-gastos" style="margin-top:8px"></div>
      </div>

      <div class="com-resumen" id="c-resumen"></div>

      <div class="modal-actions">
        <button class="btn btn-ghost" id="c-cancel" type="button">Cancelar</button>
        <button class="btn btn-primary" id="c-guardar" type="button">Guardar compra</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  const $ = s => overlay.querySelector(s);
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  $('#c-cancel').addEventListener('click', close);

  // Alta rápida de proveedor
  $('#c-prov-add').addEventListener('click', () => {
    const box = $('#c-prov-new');
    box.style.display = box.style.display === 'none' ? 'flex' : 'none';
  });
  async function buscarPadronProv() {
    const cuit = $('#c-prov-cuit').value.replace(/\D/g, '');
    const st = $('#c-prov-status');
    if (cuit.length !== 11) { st.textContent = cuit.length ? '⚠ El CUIT debe tener 11 dígitos' : ''; return null; }
    st.textContent = 'Buscando en ARCA…'; st.style.color = '#78716C';
    try {
      const r = await fetch('/padron/' + cuit);
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || 'no encontrado');
      if (d.nombre) $('#c-prov-nombre').value = d.nombre;
      st.textContent = '✓ ' + d.nombre + (d.estado ? ' · ' + d.estado : ''); st.style.color = '#0F6E56';
      return d.nombre;
    } catch (e) {
      st.textContent = '⚠ ' + e.message; st.style.color = '#B91C1C';
      return null;
    }
  }
  $('#c-prov-cuit').addEventListener('blur', buscarPadronProv);

  // Crea (o reutiliza) el proveedor del alta rápida. Con CUIT: dedup por CUIT (+ ARCA). Sin CUIT: dedup por nombre. Devuelve el proveedor o null.
  async function crearProveedorRapido() {
    const cuit = $('#c-prov-cuit').value.replace(/\D/g, '');
    if (cuit && cuit.length !== 11) { window.toast('Si cargás CUIT, debe tener 11 dígitos', 'error'); return null; }
    if (cuit.length === 11 && !$('#c-prov-nombre').value.trim()) await buscarPadronProv();
    const nombre = $('#c-prov-nombre').value.trim();
    if (!nombre) { window.toast(cuit.length === 11 ? 'No se pudo traer el nombre desde ARCA; escribilo a mano' : 'Cargá el nombre del proveedor', 'error'); return null; }
    try {
      const body = cuit.length === 11 ? { nombre, cuit } : { nombre };
      const r = await fetch('/proveedores', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      const p = await r.json();
      if (!r.ok) throw new Error(p.error || r.statusText);
      if (!PROV_BY_ID[p.id]) { PROVEEDORES.push(p); PROV_BY_ID[p.id] = p; PROVEEDORES.sort((a, b) => provLabel(a).localeCompare(provLabel(b))); }
      $('#c-prov').innerHTML = optProv(p.id);
      $('#c-prov-new').style.display = 'none';
      $('#c-prov-nombre').value = ''; $('#c-prov-cuit').value = ''; $('#c-prov-status').textContent = '';
      return p;
    } catch (e) { window.toast('Error: ' + e.message, 'error'); return null; }
  }
  $('#c-prov-crear').addEventListener('click', async () => {
    const p = await crearProveedorRapido();
    if (p) window.toast('Proveedor listo');
  });

  // Ítems
  const itemsBox = $('#c-items');
  const pintarItems = () => {
    itemsBox.innerHTML = ITEMS.map((it, i) => `
      <div class="com-item" data-i="${i}">
        <select class="select com-it-sku"><option value="">SKU…</option>${SKUS.map(s => `<option value="${s.id}" ${String(it.sku_id) === String(s.id) ? 'selected' : ''}>${esc(skuLabel(s))}</option>`).join('')}</select>
        <input class="input com-it-cant" inputmode="decimal" placeholder="Cant." value="${esc(it.cantidad)}">
        <input class="input com-it-costo" inputmode="decimal" placeholder="Costo unit." value="${esc(it.costo)}">
        <input class="input com-it-extra" inputmode="decimal" placeholder="Extra (flete/comis.)" value="${esc(it.extra || '')}">
        <select class="select com-it-iva">
          <option value="0.21" ${num(it.iva) === 0.21 ? 'selected' : ''}>IVA 21%</option>
          <option value="0.105" ${num(it.iva) === 0.105 ? 'selected' : ''}>IVA 10,5%</option>
          <option value="0.27" ${num(it.iva) === 0.27 ? 'selected' : ''}>IVA 27%</option>
          <option value="0" ${num(it.iva) === 0 ? 'selected' : ''}>Exento</option>
        </select>
        <span class="com-it-sub com-mono">${money(num(it.cantidad) * num(it.costo))}</span>
        <button class="com-it-del" type="button" title="Quitar" ${ITEMS.length === 1 ? 'style="visibility:hidden"' : ''}>✕</button>
      </div>`).join('');
    pintarResumen();
  };
  const leerItems = () => {
    itemsBox.querySelectorAll('.com-item').forEach(row => {
      const i = +row.dataset.i;
      ITEMS[i].sku_id = row.querySelector('.com-it-sku').value;
      ITEMS[i].cantidad = row.querySelector('.com-it-cant').value;
      ITEMS[i].costo = row.querySelector('.com-it-costo').value;
      ITEMS[i].extra = row.querySelector('.com-it-extra').value;
      ITEMS[i].iva = Number(row.querySelector('.com-it-iva').value);
    });
  };
  itemsBox.addEventListener('input', e => {
    if (e.target.matches('.com-it-cant, .com-it-costo, .com-it-extra')) {
      const row = e.target.closest('.com-item'); const i = +row.dataset.i;
      ITEMS[i].cantidad = row.querySelector('.com-it-cant').value;
      ITEMS[i].costo = row.querySelector('.com-it-costo').value;
      ITEMS[i].extra = row.querySelector('.com-it-extra').value;
      row.querySelector('.com-it-sub').textContent = money(num(ITEMS[i].cantidad) * num(ITEMS[i].costo));
      pintarResumen();
    }
  });
  itemsBox.addEventListener('change', e => {
    const row = e.target.closest('.com-item'); if (!row) return;
    const i = +row.dataset.i;
    if (e.target.matches('.com-it-sku')) {
      leerItems();
      ITEMS[i].sku_id = e.target.value;
      const s = SKU_BY_ID[e.target.value];
      if (s && s.alicuota_iva != null) ITEMS[i].iva = Number(s.alicuota_iva);
      pintarItems(); // refresca el % de IVA de la fila según el SKU
    } else if (e.target.matches('.com-it-iva')) {
      ITEMS[i].iva = Number(e.target.value);
      pintarResumen();
    }
  });
  itemsBox.addEventListener('click', e => {
    if (e.target.matches('.com-it-del')) { leerItems(); ITEMS.splice(+e.target.closest('.com-item').dataset.i, 1); pintarItems(); }
  });
  $('#c-add-item').addEventListener('click', () => { leerItems(); ITEMS.push({ sku_id: '', cantidad: '', costo: '', iva: 0.21, extra: '' }); pintarItems(); });

  // + Nuevo SKU: alta al vuelo sin salir de la compra (reusa sbPost('skus') como la pantalla SKUs).
  function openNuevoSku() {
    const ov = document.createElement('div');
    ov.className = 'modal-overlay';
    ov.innerHTML = `
      <div class="modal" style="max-width:460px">
        <div class="card-title">Nuevo SKU</div>
        <div class="field"><label>Código *</label><input class="input" id="ns-cod" placeholder="ej: TA012G"></div>
        <div class="field"><label>Descripción *</label><input class="input" id="ns-desc" placeholder="ej: Tablet Samsung Galaxy Tab A9"></div>
        <div class="com-row3">
          <div class="field" style="grid-column:span 2"><label>Familia</label><select class="select" id="ns-fam">${FAMILIAS.map(f => `<option value="${f.val}" ${f.val === 'electronica' ? 'selected' : ''}>${esc(f.label)}</option>`).join('')}</select></div>
          <div class="field"><label>Alícuota IVA</label>
            <select class="select" id="ns-iva">
              <option value="21" selected>21% — general</option>
              <option value="10.5">10,5% — informática</option>
              <option value="27">27% — servicios</option>
              <option value="0">Exento (0%)</option>
            </select>
          </div>
        </div>
        <div class="modal-actions">
          <button class="btn btn-ghost" id="ns-cancel" type="button">Cancelar</button>
          <button class="btn btn-primary" id="ns-save" type="button">Crear SKU</button>
        </div>
      </div>`;
    document.body.appendChild(ov);
    const q = s => ov.querySelector(s);
    const cerrar = () => ov.remove();
    ov.addEventListener('click', e => { if (e.target === ov) cerrar(); });
    q('#ns-cancel').addEventListener('click', cerrar);
    q('#ns-cod').focus();
    q('#ns-save').addEventListener('click', async () => {
      const codigo = q('#ns-cod').value.trim();
      const desc = q('#ns-desc').value.trim();
      const fam = q('#ns-fam').value || null;
      const ivaNum = parseFloat(q('#ns-iva').value);   // el select ya entrega un número limpio
      if (!codigo) { window.toast('Falta el código', 'error'); return; }
      if (!desc) { window.toast('Falta la descripción', 'error'); return; }
      if (isNaN(ivaNum) || ivaNum < 0 || ivaNum > 100) { window.toast('IVA inválido', 'error'); return; }
      if (SKUS.some(s => (s.codigo || '').toLowerCase() === codigo.toLowerCase())) { window.toast('Ya existe un SKU con ese código', 'error'); return; }
      const btn = q('#ns-save'); btn.disabled = true;
      try {
        const created = await sbPost('skus', { codigo, descripcion: desc, familia: fam, alicuota_iva: +(ivaNum / 100).toFixed(4), activo: true });
        const s = (created && created[0]) ? created[0] : { id: null, codigo, descripcion: desc, alicuota_iva: +(ivaNum / 100).toFixed(4) };
        SKUS.push(s); SKUS.sort((a, b) => (a.codigo || '').localeCompare(b.codigo || ''));
        SKU_BY_ID[s.id] = s;
        // Lo dejo seleccionado en la primera fila sin SKU (o creo una nueva).
        leerItems();
        let idx = ITEMS.findIndex(it => !it.sku_id);
        if (idx === -1) { ITEMS.push({ sku_id: '', cantidad: '', costo: '', iva: 0.21, extra: '' }); idx = ITEMS.length - 1; }
        ITEMS[idx].sku_id = String(s.id);
        if (s.alicuota_iva != null) ITEMS[idx].iva = Number(s.alicuota_iva);
        pintarItems();
        window.toast('SKU creado: ' + codigo);
        cerrar();
      } catch (e) { window.toast('Error: ' + e.message, 'error'); btn.disabled = false; }
    });
  }
  $('#c-new-sku').addEventListener('click', openNuevoSku);

  // Moneda / TC
  const monedaSel = $('#c-moneda');
  const tcInp = $('#c-tc');
  const monActual = () => monedaSel.value === 'USD' ? 'USD' : 'ARS';
  const simb = () => monActual() === 'USD' ? 'US$ ' : '$ ';
  const mon = n => simb() + num(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  // IVA = el de cada producto por su alícuota + el de los gastos que vienen en ESTA factura.
  // Una misma factura puede tener varias alícuotas (ej: productos al 10,5% + un cargo al 21%).
  const ivaTotal = () => ITEMS.reduce((s, it) => s + num(it.cantidad) * num(it.costo) * num(it.iva), 0)
                       + GASTOS.reduce((s, g) => s + num(g.monto) * num(g.iva), 0);
  const percepTotal = () => PERCEPS.reduce((s, p) => s + num(p.monto), 0);
  const gastosTotal = () => GASTOS.reduce((s, g) => s + num(g.monto), 0);
  // Sólo los gastos de esta factura son deuda con este proveedor. Los de terceros van al costo
  // del lote igual, pero se le deben al tercero (y v_compras_ap los excluye del saldo).
  const gastosProvTotal = () => GASTOS.reduce((s, g) => s + (g.delProv ? num(g.monto) : 0), 0);
  const extrasTotal = () => ITEMS.reduce((s, it) => s + num(it.extra), 0);

  const pintarResumen = () => {
    const neto = ITEMS.reduce((s, it) => s + num(it.cantidad) * num(it.costo), 0);
    const iva = ivaTotal();
    const perc = percepTotal();
    const extras = extrasTotal();
    const gastos = gastosTotal();
    const costoMerc = neto + extras + gastos;       // entra al stock (costo del lote)
    // Deuda con ESTE proveedor: productos + los gastos que vienen en su factura + IVA + percepciones.
    // Coincide con v_compras_ap, que excluye extra_directo y gasto_prorrateable (son de terceros).
    const totalFactura = neto + gastosProvTotal() + iva + perc;
    const disp = $('#c-iva-disp'); if (disp) disp.value = mon(iva);
    const tc = num(tcInp.value);
    const equivArs = (monActual() === 'USD' && tc > 0)
      ? `<div class="com-res-r com-muted"><span>Total factura en ARS (TC ${num(tc).toLocaleString('es-AR')})</span><span class="com-mono">$ ${(totalFactura * tc).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span></div>`
      : '';
    const lineaExtras = (extras + gastos > 0)
      ? `<div class="com-res-r com-muted"><span>+ extras por producto + gastos prorrateables (al costo)</span><span class="com-mono">${mon(extras + gastos)}</span></div>
      <div class="com-res-r"><span>= Costo total de la mercadería (entra al stock)</span><span class="com-mono">${mon(costoMerc)}</span></div>`
      : '';
    $('#c-resumen').innerHTML = `
      <div class="com-res-r"><span>Productos (neto)</span><span class="com-mono">${mon(neto)}</span></div>
      ${lineaExtras}
      <div class="com-res-r com-muted"><span>+ IVA (auto) + percepciones (crédito fiscal)</span><span class="com-mono">${mon(iva + perc)}</span></div>
      <div class="com-res-r com-res-strong"><span>Total factura — lo que le debés al proveedor</span><span class="com-mono">${mon(totalFactura)}</span></div>
      ${equivArs}`;
  };

  const aplicarMoneda = () => {
    $('#c-tc-wrap').style.display = monActual() === 'USD' ? '' : 'none';
    pintarResumen();
  };
  monedaSel.addEventListener('change', aplicarMoneda);
  tcInp.addEventListener('input', pintarResumen);

  // Repeater de percepciones (una por jurisdicción → una fila en compra_componentes)
  const percBox = $('#c-perceps');
  const pintarPerceps = () => {
    percBox.innerHTML = PERCEPS.length ? PERCEPS.map((p, i) => `
      <div class="com-item com-perc-item" data-i="${i}">
        <select class="select com-pc-tipo">
          <option value="iibb" ${p.tipo === 'iibb' ? 'selected' : ''}>Percep. IIBB</option>
          <option value="ganancias" ${p.tipo === 'ganancias' ? 'selected' : ''}>Percep. Ganancias</option>
        </select>
        <select class="select com-pc-jur">${JURISDICCIONES.map(([v, l]) => `<option value="${v}" ${p.jurisdiccion === v ? 'selected' : ''}>${esc(l)}</option>`).join('')}</select>
        <input class="input com-pc-monto" inputmode="decimal" placeholder="Monto" value="${esc(p.monto)}">
        <button class="com-it-del" type="button" title="Quitar">✕</button>
      </div>`).join('')
      : `<div class="com-muted" style="padding:4px 0;font-size:13px">Sin percepciones. Agregá una por cada jurisdicción.</div>`;
    pintarResumen();
  };
  const leerPerceps = () => {
    percBox.querySelectorAll('.com-item').forEach(row => {
      const i = +row.dataset.i;
      PERCEPS[i].tipo = row.querySelector('.com-pc-tipo').value;
      PERCEPS[i].jurisdiccion = row.querySelector('.com-pc-jur').value;
      PERCEPS[i].monto = row.querySelector('.com-pc-monto').value;
    });
  };
  percBox.addEventListener('input', e => {
    if (e.target.matches('.com-pc-monto')) { leerPerceps(); pintarResumen(); }
  });
  percBox.addEventListener('change', e => {
    if (e.target.matches('.com-pc-tipo, .com-pc-jur')) { leerPerceps(); pintarResumen(); }
  });
  percBox.addEventListener('click', e => {
    if (e.target.matches('.com-it-del')) { leerPerceps(); PERCEPS.splice(+e.target.closest('.com-item').dataset.i, 1); pintarPerceps(); }
  });
  $('#c-add-perc').addEventListener('click', () => { leerPerceps(); PERCEPS.push({ tipo: 'iibb', jurisdiccion: '', monto: '' }); pintarPerceps(); });

  // Repeater de gastos prorrateables (flete/comisión/despacho a terceros → costo del lote)
  const gastoBox = $('#c-gastos');
  const pintarGastos = () => {
    gastoBox.innerHTML = GASTOS.length ? GASTOS.map((g, i) => `
      <div class="com-item com-gasto-item" data-i="${i}">
        <input class="input com-ga-conc" placeholder="Concepto (flete, comisión…)" value="${esc(g.concepto || '')}">
        <label class="com-ga-prov-lbl" title="Tildá si el cargo viene en ESTA factura. Si es de un tercero con factura aparte, dejalo sin tildar.">
          <input type="checkbox" class="com-ga-prov" ${g.delProv ? 'checked' : ''}>
          <span>de esta factura</span>
        </label>
        <select class="select com-ga-iva" ${g.delProv ? '' : 'disabled'}>
          <option value="0" ${num(g.iva) === 0 ? 'selected' : ''}>Sin IVA</option>
          <option value="0.21" ${num(g.iva) === 0.21 ? 'selected' : ''}>IVA 21%</option>
          <option value="0.105" ${num(g.iva) === 0.105 ? 'selected' : ''}>IVA 10,5%</option>
          <option value="0.27" ${num(g.iva) === 0.27 ? 'selected' : ''}>IVA 27%</option>
        </select>
        <input class="input com-ga-monto" inputmode="decimal" placeholder="Monto neto" value="${esc(g.monto)}">
        <button class="com-it-del" type="button" title="Quitar">✕</button>
      </div>`).join('')
      : `<div class="com-muted" style="padding:4px 0;font-size:13px">Sin gastos prorrateables. Agregá flete, comisión o despacho a repartir.</div>`;
    pintarResumen();
  };
  const leerGastos = () => {
    gastoBox.querySelectorAll('.com-item').forEach(row => {
      const i = +row.dataset.i;
      GASTOS[i].concepto = row.querySelector('.com-ga-conc').value;
      GASTOS[i].monto = row.querySelector('.com-ga-monto').value;
      GASTOS[i].delProv = row.querySelector('.com-ga-prov').checked;
      // El IVA sólo aplica si el cargo está en esta factura. Si es de un tercero, su crédito
      // entra con la factura de ese tercero, no acá — si no, se lo estaríamos debiendo al
      // proveedor equivocado.
      GASTOS[i].iva = GASTOS[i].delProv ? Number(row.querySelector('.com-ga-iva').value) : 0;
    });
  };
  gastoBox.addEventListener('input', e => {
    if (e.target.matches('.com-ga-conc, .com-ga-monto')) { leerGastos(); pintarResumen(); }
  });
  gastoBox.addEventListener('change', e => {
    if (e.target.matches('.com-ga-prov')) { leerGastos(); pintarGastos(); }
    else if (e.target.matches('.com-ga-iva')) { leerGastos(); pintarResumen(); }
  });
  gastoBox.addEventListener('click', e => {
    if (e.target.matches('.com-it-del')) { leerGastos(); GASTOS.splice(+e.target.closest('.com-item').dataset.i, 1); pintarGastos(); }
  });
  $('#c-add-gasto').addEventListener('click', () => { leerGastos(); GASTOS.push({ concepto: '', monto: '', iva: 0, delProv: false }); pintarGastos(); });

  aplicarMoneda();
  pintarPerceps();
  pintarGastos();
  pintarItems();

  // Guardar
  $('#c-guardar').addEventListener('click', async () => {
    leerItems();
    leerPerceps();
    leerGastos();
    const fecha = $('#c-fecha').value;
    const linea_id = $('#c-linea').value;
    if (!fecha) { window.toast('Falta la fecha', 'error'); return; }
    if (!linea_id) { window.toast('Elegí la línea de negocio', 'error'); return; }
    if (monedaSel.value === 'USD' && !(num(tcInp.value) > 0)) { window.toast('Compra en USD: cargá el TC', 'error'); return; }
    const items = ITEMS
      .filter(it => it.sku_id && num(it.cantidad) > 0 && num(it.costo) >= 0)
      .map(it => ({ sku_id: +it.sku_id, cantidad: num(it.cantidad), costo_unitario: num(it.costo), extra_directo: num(it.extra) }));
    if (!items.length) { window.toast('Cargá al menos un producto con SKU, cantidad y costo', 'error'); return; }

    // Si quedó un proveedor a medio cargar en el alta rápida (CUIT o nombre) y no se creó, lo creamos ahora
    let provId = $('#c-prov').value ? +$('#c-prov').value : null;
    if (!provId && ($('#c-prov-cuit').value.replace(/\D/g, '').length === 11 || $('#c-prov-nombre').value.trim())) {
      const np = await crearProveedorRapido();
      if (np) provId = np.id;
    }

    const payload = {
      compra: {
        proveedor_id: provId,
        linea_id: +linea_id,
        fecha,
        moneda: monedaSel.value === 'USD' ? 'USD' : 'ARS',
        tc_blue: monedaSel.value === 'USD' ? num(tcInp.value) : null,
        nro_factura: $('#c-factura').value.trim() || null
      },
      items,
      fiscales: {
        iva: ivaTotal(),
        percepciones: PERCEPS
          .map(p => ({ tipo: p.tipo, jurisdiccion: (p.jurisdiccion || '').trim(), monto: num(p.monto) }))
          .filter(p => p.monto !== 0)
      },
      gastos: {
        criterio: $('#c-criterio').value,
        prorrateables: GASTOS
          .map(g => ({ concepto: (g.concepto || '').trim(), monto: num(g.monto), del_proveedor: !!g.delProv }))
          .filter(g => g.monto > 0)
      }
    };

    const btn = $('#c-guardar'); btn.disabled = true;
    try {
      const r = await fetch('/compras', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));

      // Subir la factura adjunta (si se eligió un archivo)
      const fileEl = $('#c-adjunto');
      const file = fileEl && fileEl.files && fileEl.files[0];
      if (file && data.compra_id) {
        try {
          const fd = new FormData();
          fd.append('op_tipo', 'compra');
          fd.append('op_id', String(data.compra_id));
          fd.append('file', file);
          const ra = await fetch('/adjuntos', { method: 'POST', body: fd });
          if (!ra.ok) { const da = await ra.json().catch(() => ({})); throw new Error(da.error || (ra.status + '')); }
        } catch (eAdj) {
          window.toast('Compra guardada, pero la factura no se subió: ' + eAdj.message, 'error');
          close(); await recargar(); render(); return;
        }
      }

      window.toast('Compra cargada');
      close();
      await recargar();
      render();
    } catch (e) {
      window.toast('Error al guardar: ' + e.message, 'error');
      btn.disabled = false;
    }
  });
}

function optProv(selId) {
  return '<option value="">— Sin proveedor —</option>' +
    PROVEEDORES.map(p => `<option value="${p.id}" ${String(selId) === String(p.id) ? 'selected' : ''}>${esc(provLabel(p))}</option>`).join('');
}

function inyectarEstilo() {
  if (document.getElementById('com-style')) return;
  const css = `
    /* fila clickeable + acciones */
    .com-fila{cursor:pointer;transition:background .12s}
    .com-fila:hover{background:#FAFAF9}
    .com-fila-open{background:#FEF9F3;box-shadow:inset 3px 0 0 #D97706}
    .com-caret{display:inline-block;width:12px;color:#A8A29E;font-size:11px}
    .com-acc{text-align:right;white-space:nowrap}
    .com-pdf{background:#fff;border:1px solid #E7E5E4;border-radius:5px;padding:2px 7px;cursor:pointer;font:inherit;margin-right:6px;line-height:1.5;vertical-align:middle}
    .com-pdf:hover{border-color:#D97706;background:#FFFBF5}
    .com-pdf-i{font-size:10px;font-weight:700;letter-spacing:.04em;color:#B45309}
    .com-pdf-no{display:inline-block;width:34px;color:#D6D3D1;text-align:center;margin-right:6px}
    .com-anular{font-size:12px;color:#B91C1C;background:none;border:0;cursor:pointer;font-family:inherit;padding:2px 4px}
    .com-anular:hover{text-decoration:underline}

    /* panel de detalle */
    .com-det > td{padding:0 !important;background:#F7F6F4;border-bottom:2px solid #E7E5E4}
    .com-det-box{padding:16px 20px 20px}
    .com-det-load,.com-det-err,.com-det-vacio{padding:10px 2px;color:#78716C}
    .com-det-err{color:#B42318}
    .com-det-head{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;margin-bottom:14px}
    .com-det-title{font-size:16px;font-weight:700;color:#1C1917;display:flex;align-items:baseline;gap:10px;flex-wrap:wrap}
    .com-det-cuit{font-size:12px;font-weight:500;color:#78716C;font-family:'JetBrains Mono',ui-monospace,monospace}
    .com-chips{display:flex;gap:8px;flex-wrap:wrap}
    .com-chip{background:#fff;border:1px solid #E7E5E4;border-radius:6px;padding:4px 10px;font-size:12px;line-height:1.4}
    .com-chip span{color:#A8A29E;display:block;font-size:10px;text-transform:uppercase;letter-spacing:.05em}
    .com-chip b{color:#1C1917;font-weight:600}
    .com-det-cols{display:grid;grid-template-columns:1fr 320px;gap:16px;align-items:start}
    @media (max-width:1100px){.com-det-cols{grid-template-columns:1fr}}
    .com-det-main{display:flex;flex-direction:column;gap:12px;min-width:0}
    .com-det-side{display:flex;flex-direction:column;gap:12px}

    /* cards */
    .com-card{background:#fff;border:1px solid #E7E5E4;border-radius:10px;overflow:hidden}
    .com-card-h{padding:9px 14px;background:#FCFBFA;border-bottom:1px solid #F0EEEC;font-size:12px;font-weight:700;
      text-transform:uppercase;letter-spacing:.05em;color:#57534E;display:flex;justify-content:space-between;gap:10px;align-items:baseline}
    .com-card-n{font-size:11px;font-weight:500;text-transform:none;letter-spacing:0;color:#A8A29E}
    .com-card-t{width:100%;border-collapse:collapse;font-size:13px}
    .com-card-t td{padding:8px 14px;border-bottom:1px solid #F5F4F2;vertical-align:middle}
    .com-card-t tr:last-child td{border-bottom:0}
    .com-card-t tr:hover{background:#FCFCFB}
    .com-c-sku{font-family:'JetBrains Mono',ui-monospace,monospace;font-weight:600;color:#1C1917;white-space:nowrap;width:1%}
    .com-c-desc{color:#78716C;max-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .com-c-num{text-align:right;white-space:nowrap;width:1%;color:#57534E}
    .com-c-tot{color:#1C1917;font-weight:600}
    .com-c-u{color:#A8A29E;font-size:11px;margin-left:1px}
    .com-tag{display:inline-block;font-size:12px;font-weight:600;padding:1px 8px;border-radius:4px;margin-right:8px}
    .com-tag-costo{background:#FEF3E2;color:#B45309}
    .com-tag-fisc{background:#EFF6FF;color:#1D4ED8}

    /* barra de consumo del lote */
    .com-bar{height:5px;background:#F0EEEC;border-radius:3px;overflow:hidden;margin-bottom:4px;max-width:220px}
    .com-bar-f{height:100%;background:#D97706;border-radius:3px}
    .com-bar-l{font-size:11px;color:#A8A29E}

    /* totales */
    .com-card-tot .com-tot{padding:10px 14px 12px}
    .com-tot-r{display:flex;justify-content:space-between;gap:12px;padding:4px 0;font-size:13px;color:#57534E}
    .com-tot-r b{color:#1C1917;font-weight:600;white-space:nowrap}
    .com-tot-mid{border-top:1px solid #F0EEEC;margin-top:4px;padding-top:8px}
    .com-tot-fisc{color:#1D4ED8}.com-tot-fisc b{color:#1D4ED8;font-weight:500}
    .com-tot-big{border-top:2px solid #E7E5E4;margin-top:6px;padding-top:9px;font-size:14px;font-weight:700;color:#1C1917}
    .com-tot-big b{font-size:15px}
    .com-tot-sep{height:1px;background:#F0EEEC;margin:9px 0}
    .com-tot-deuda b{color:#B45309}
    .com-tot-ok b{color:#0F6E56}

    /* comprobante */
    .com-det-adj{display:flex;flex-direction:column;gap:6px;padding:10px 12px}
    .com-adj-dl{display:flex;align-items:center;gap:9px;width:100%;text-align:left;background:#fff;border:1px solid #E7E5E4;
      border-radius:7px;padding:9px 11px;cursor:pointer;font:inherit;font-size:13px;color:#1C1917;transition:all .12s}
    .com-adj-dl:hover{border-color:#D97706;background:#FFFBF5}
    .com-adj-ic{font-size:10px;font-weight:700;letter-spacing:.04em;color:#B45309;background:#FEF3E2;border-radius:4px;padding:3px 6px;flex:0 0 auto}
    .com-adj-n{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .com-adj-go{font-size:11px;color:#A8A29E;flex:0 0 auto}
    .com-adj-dl:hover .com-adj-go{color:#B45309}
    .com-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .com-muted{color:#78716C}
    .com-sub{font-size:13px;color:#78716C;margin:-4px 0 12px}
    .com-badge{font-size:12px;padding:2px 8px;border-radius:6px}
    .com-badge-pendiente{background:#FAEEDA;color:#854F0B}
    .com-badge-parcial{background:#E6F1FB;color:#0C447C}
    .com-badge-pagado{background:#E1F5EE;color:#0F6E56}
    .com-debe{color:#B91C1C;font-weight:600}
    .com-favor{color:#0F6E56;font-weight:600}
    .com-mini{padding:6px 12px;font-size:13px}
    .com-row3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}
    .com-block{border:1px solid #E7E5E4;border-radius:10px;padding:12px;margin:12px 0}
    .com-block-h{display:flex;justify-content:space-between;align-items:center;font-size:13px;font-weight:600;color:#44403C;margin-bottom:8px}
    .com-item{display:grid;grid-template-columns:1fr 58px 92px 92px 84px 80px 24px;gap:7px;align-items:center;margin-bottom:8px}
    .com-it-sub{text-align:right;font-size:13px;color:#57534E}
    .com-it-del{border:0;background:transparent;color:#A8A29E;cursor:pointer;font-size:14px}
    .com-it-del:hover{color:#B91C1C}
    .com-resumen{border-top:1px solid #E7E5E4;margin-top:12px;padding-top:12px}
    .com-res-r{display:flex;justify-content:space-between;font-size:14px;padding:3px 0}
    .com-res-strong{font-weight:700;font-size:15px;border-top:1px dashed #E7E5E4;margin-top:4px;padding-top:8px}
    .com-perc-item{grid-template-columns:140px 1fr 120px 26px}
    .com-gasto-item{grid-template-columns:1fr 128px 108px 120px 26px}
    .com-ga-prov-lbl{display:flex;align-items:center;gap:5px;font-size:12px;color:var(--muted);cursor:pointer;user-select:none}
    .com-ga-prov-lbl input{cursor:pointer;margin:0}
    #com-body .table-wrap{border:1px solid #E7E5E4;border-radius:12px;overflow:hidden;background:#fff}
    #com-body table.t{width:100%;border-collapse:collapse;font-size:14px}
    #com-body table.t thead th{background:#FAFAF9;color:#78716C;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.04em;padding:11px 14px;border-bottom:1px solid #E7E5E4;text-align:left;white-space:nowrap}
    #com-body table.t tbody td{padding:13px 14px;border-bottom:1px solid #F5F5F4;vertical-align:middle;color:#1C1917}
    #com-body table.t tbody tr:last-child td{border-bottom:0}
    #com-body table.t tbody tr:hover td{background:#FAFAF9}
    #com-body table.t .com-mono{white-space:nowrap}
  `;
  const style = document.createElement('style');
  style.id = 'com-style';
  style.textContent = css;
  document.head.appendChild(style);
}
