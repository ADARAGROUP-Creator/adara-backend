import { sbGet } from '../core/sb.js';

// ── Pantalla: PSI — Planificación de Recompra ──────────────────────────
// Porte a v22 de la pantalla PSI del v21 (loadPSI/exportPSI).
//
// Qué hace: por cada SKU con ventas en el rango, calcula la velocidad de
// venta (uds/día), los días de stock que quedan y la cantidad sugerida a
// reponer para cubrir `diasObj` días. Pinta una matriz de ventas por semana
// + métricas + alerta por color.
//
// Fuentes de datos en v22 (decididas el 5/6/2026, ver ADARA-STOCK.md):
//  - Velocidad de venta  ← `ventas_ml` (sku = código, cantidad, fecha, ml_status).
//    Hoy SOLO canal ML (Tango sync no corre): subestima SKUs que venden fuerte
//    off-ML. Se ampliará cuando entre el sync de Tango (tabla `ventas`).
//  - Stock disponible     ← Σ `lotes.cantidad_actual` por `sku_id`, mapeado a
//    `skus.codigo`. `skus` NO tiene columna stock (invariante S7): se deriva de
//    lotes. `v_stock_check` queda como chequeo de drift, no como fuente acá.
//  - Producto/título      ← `skus.descripcion` (fallback al `titulo` de la venta).
//
// Diferencias respecto del v21 (intencionales):
//  - NO hay columna "🚚 En tránsito": v22 no modela órdenes de compra en viaje
//    (`compras.estado` ∈ {activa, anulada}, sin `en_viaje`). Vuelve cuando la
//    pantalla Compras modele OC pendientes.
//  - Escala de 5 colores de ADARA-STOCK.md (no las 3 del v21):
//    verde >30 / amarillo 15-30 / naranja 7-15 / rojo <7 / negro =0 (quebrado).
//  - Ventana por defecto 4 semanas (28 d) + diasObj 30, ambos configurables
//    (alineado a la spec de STOCK.md; el v21 default era 8 sem).
//
// ⚠ Estado de datos (5/6/2026): `lotes` está casi vacío (carga inicial 31/12
// pendiente). Mientras tanto la mitad "stock/días/recompra" sale en 0 para casi
// todos los SKUs; se muestra un aviso. La matriz de ventas/velocidad ya es útil.

let VENTAS = [];            // ventas_ml del rango (sku, titulo, cantidad, fecha)
let STOCK_BY_COD = {};      // codigo -> unidades disponibles (Σ lotes.cantidad_actual)
let DESC_BY_COD = {};       // codigo -> descripcion del catálogo skus
let ROWS = [];              // filas calculadas (estado del módulo, para exportar)
let SEMANAS = [];           // [{desde, hasta, label}]
let PARAMS = { desde: '', hasta: '', diasObj: 30, ignorarQuiebres: true, leadTime: 15, colchon: 7 };
let BUSQ = '';

const ALERTAS = ['negro', 'rojo', 'naranja', 'amarillo', 'verde'];
const ALERT_ORD = { negro: 0, rojo: 1, naranja: 2, amarillo: 3, verde: 4 };
const ALERT_DOT = { negro: '⚫', rojo: '🔴', naranja: '🟠', amarillo: '🟡', verde: '🟢' };
const ALERT_LABEL = { negro: 'Quebrado', rojo: 'Crítico', naranja: 'Bajo', amarillo: 'Atención', verde: 'OK' };

// ── Helpers de fecha (locales, sin pasar por UTC para no correr un día) ──
function isoLocal(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}
function parseISO(f) { return new Date(f + 'T00:00:00'); }
function ddmm(d) { return `${String(d.getDate()).padStart(2, '0')}/${String(d.getMonth() + 1).padStart(2, '0')}`; }

function num(v) { return Number(v) || 0; }
function fmtNum(n, dec = 0) {
  return num(n).toLocaleString('es-AR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

// ── Carga ────────────────────────────────────────────────────────────────
export async function loadPSI() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando PSI…</div>`;

  // Defaults: últimas 4 semanas (spec STOCK.md).
  if (!PARAMS.desde) {
    const d = new Date(); d.setDate(d.getDate() - 28);
    PARAMS.desde = isoLocal(d);
  }
  if (!PARAMS.hasta) PARAMS.hasta = isoLocal(new Date());

  try {
    // 1) Ventas del rango. Excluimos canceladas en cliente (incluye null como
    //    no-cancelada → no perdemos ventas con status vacío en la velocidad).
    const ventas = await sbGet('ventas_ml',
      `select=sku,titulo,cantidad,fecha,ml_status` +
      `&fecha=gte.${PARAMS.desde}&fecha=lte.${PARAMS.hasta}&order=fecha.asc`);
    VENTAS = ventas.filter(v => v.ml_status !== 'cancelled');

    // 2) Catálogo: id -> codigo, codigo -> descripcion.
    const skus = await sbGet('skus', 'select=id,codigo,descripcion');
    const codById = {};
    DESC_BY_COD = {};
    for (const s of skus) {
      codById[s.id] = s.codigo;
      DESC_BY_COD[s.codigo] = s.descripcion || '';
    }

    // 3) Stock disponible por código = Σ lotes.cantidad_actual (mapeado por sku_id).
    const lotes = await sbGet('lotes', 'select=sku_id,cantidad_actual');
    STOCK_BY_COD = {};
    for (const l of lotes) {
      const cod = codById[l.sku_id];
      if (!cod) continue;
      STOCK_BY_COD[cod] = (STOCK_BY_COD[cod] || 0) + num(l.cantidad_actual);
    }
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudo cargar el PSI: ${esc(e.message)}</div>`;
    return;
  }

  inyectarEstilo();
  calcular();
  render();
}

// ── Cálculo ────────────────────────────────────────────────────────────────
function calcular() {
  // Semanas del rango, alineadas al lunes.
  SEMANAS = [];
  const dDesde = parseISO(PARAMS.desde);
  const dHasta = parseISO(PARAMS.hasta);
  const startMon = new Date(dDesde);
  startMon.setDate(startMon.getDate() - ((startMon.getDay() + 6) % 7)); // lunes
  let cur = new Date(startMon);
  while (cur <= dHasta) {
    const fin = new Date(cur); fin.setDate(fin.getDate() + 6);
    SEMANAS.push({ desde: new Date(cur), hasta: fin, label: ddmm(cur) });
    cur.setDate(cur.getDate() + 7);
  }
  const nSem = SEMANAS.length || 1;

  // Agrupar ventas por SKU + semana.
  const map = {};
  for (const v of VENTAS) {
    const cod = (v.sku && String(v.sku).trim()) ? String(v.sku).trim() : 'SIN_SKU';
    if (!map[cod]) map[cod] = { cod, titulo: v.titulo || '—', semanas: new Array(SEMANAS.length).fill(0), total: 0 };
    const fv = parseISO(v.fecha);
    const q = num(v.cantidad) || 1;
    map[cod].total += q;
    for (let i = 0; i < SEMANAS.length; i++) {
      if (fv >= SEMANAS[i].desde && fv <= SEMANAS[i].hasta) { map[cod].semanas[i] += q; break; }
    }
  }

  // Punto de reorden (en días): hay que recomprar cuando los días de stock que
  // quedan ya no alcanzan a cubrir el tránsito (lead time) + un colchón de margen.
  // Si esperás a que el stock llegue a 0, quebrás durante el viaje de la compra.
  const ropDias = num(PARAMS.leadTime) + num(PARAMS.colchon);

  // Métricas por SKU.
  ROWS = Object.values(map).map(r => {
    const enCatalogo = r.cod !== 'SIN_SKU' && (r.cod in DESC_BY_COD);
    const producto = (DESC_BY_COD[r.cod] || r.titulo || r.cod);
    const stock = num(STOCK_BY_COD[r.cod]);
    // Velocidad: por defecto ignora las semanas sin venta (asumidas quiebre de stock) y usa la
    // MEDIANA de las semanas con venta, para que el quiebre no subestime la demanda ni el número
    // dependa de cuántas semanas se tomen. Sin el toggle, promedio simple sobre todas las semanas.
    // (Lo fino —velocidad sobre días con stock real— queda para cuando haya histórico de stock.)
    const semConVenta = r.semanas.filter(x => x > 0);
    const velSem = (PARAMS.ignorarQuiebres && semConVenta.length) ? mediana(semConVenta) : (r.total / nSem);
    const promSem = velSem;
    const velDiaria = velSem / 7;
    const diasStock = velDiaria > 0 ? Math.round(stock / velDiaria) : (stock > 0 ? 999 : 0);
    const necesario = Math.ceil(velDiaria * PARAMS.diasObj);
    const recompra = Math.max(0, necesario - stock);
    const alerta = alertaDe(stock, diasStock);
    // "Recomprar YA": hay algo para reponer Y el stock que queda ya no cubre lead+colchón.
    // Los quebrados (diasStock 0) con recompra>0 caen acá naturalmente.
    const recomprarYa = recompra > 0 && diasStock <= ropDias;
    return { ...r, producto, enCatalogo, stock, promSem, velDiaria, diasStock, recompra, alerta, recomprarYa };
  });

  // Orden: primero los "Recomprar YA", después peor alerta (negro→rojo→…), y dentro por días asc.
  ROWS.sort((a, b) =>
    (Number(b.recomprarYa) - Number(a.recomprarYa)) ||
    (ALERT_ORD[a.alerta] - ALERT_ORD[b.alerta]) ||
    (a.diasStock - b.diasStock));
}

// Escala de 5 colores (ADARA-STOCK.md). Toda fila tiene ventas en el rango
// (velDiaria>0), así que stock 0 = quebrado.
function alertaDe(stock, diasStock) {
  if (stock <= 0) return 'negro';
  if (diasStock < 7) return 'rojo';
  if (diasStock < 15) return 'naranja';
  if (diasStock < 30) return 'amarillo';
  return 'verde';
}

// ── Render ────────────────────────────────────────────────────────────────
function render() {
  const root = document.getElementById('app-screens');

  const totalUds = ROWS.reduce((s, r) => s + r.total, 0);
  const quebrados = ROWS.filter(r => r.alerta === 'negro').length;
  const criticos = ROWS.filter(r => r.alerta === 'rojo').length;
  const aReponer = ROWS.filter(r => r.recompra > 0).length;
  const recomprarYa = ROWS.filter(r => r.recomprarYa).length;

  // Aviso si el stock de apertura no está cargado (lotes casi vacío).
  const sinStock = ROWS.filter(r => r.stock <= 0).length;
  const avisoStock = (ROWS.length && sinStock / ROWS.length > 0.5)
    ? `<div class="psi-aviso">⚠ <strong>Stock de apertura sin cargar:</strong> ${sinStock} de ${ROWS.length} SKUs con venta no tienen lotes.
       Las columnas <em>Stock · Días · Recompra</em> se activan al cargar los lotes del 31/12. La matriz de ventas y la velocidad ya son válidas.</div>`
    : '';

  const theadSem = SEMANAS.map(s => `<th class="psi-c psi-wk">${esc(s.label)}</th>`).join('');

  root.innerHTML = `
    <div class="toolbar">
      <label class="psi-lbl">Desde <input class="input psi-date" id="psi-desde" type="date" value="${PARAMS.desde}"></label>
      <label class="psi-lbl">Hasta <input class="input psi-date" id="psi-hasta" type="date" value="${PARAMS.hasta}"></label>
      <label class="psi-lbl">Cobertura objetivo
        <input class="input psi-dias" id="psi-dias" type="number" min="1" step="1" value="${PARAMS.diasObj}"> días
      </label>
      <label class="psi-lbl">Lead time
        <input class="input psi-dias" id="psi-lead" type="number" min="0" step="1" value="${PARAMS.leadTime}"> días
      </label>
      <label class="psi-lbl">Colchón
        <input class="input psi-dias" id="psi-colchon" type="number" min="0" step="1" value="${PARAMS.colchon}"> días
      </label>
      <label class="psi-lbl psi-chk"><input type="checkbox" id="psi-quiebres" ${PARAMS.ignorarQuiebres ? 'checked' : ''}> Ignorar semanas sin venta (quiebres)</label>
      <button class="btn btn-primary" id="psi-calc">Recalcular</button>
      <span class="grow"></span>
      <input class="input" id="psi-busq" type="search" placeholder="Buscar SKU o producto…" style="min-width:200px" value="${esc(BUSQ)}">
      <button class="btn btn-ghost psi-clear" id="psi-busq-clear" title="Limpiar búsqueda"${BUSQ ? '' : ' style="display:none"'}>✕</button>
      <button class="btn btn-ghost" id="psi-export">Exportar Excel</button>
    </div>

    ${avisoStock}

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">SKUs con venta</div><div class="kpi-value">${ROWS.length}</div></div>
      <div class="kpi"><div class="kpi-label">Uds vendidas</div><div class="kpi-value">${fmtNum(totalUds)}</div></div>
      <div class="kpi"><div class="kpi-label">⚫ Quebrados</div><div class="kpi-value">${quebrados}</div></div>
      <div class="kpi"><div class="kpi-label">🔴 Críticos (&lt;7d)</div><div class="kpi-value">${criticos}</div></div>
      <div class="kpi psi-kpi-ya"><div class="kpi-label">🛒 Recomprar YA</div><div class="kpi-value">${recomprarYa}</div></div>
      <div class="kpi"><div class="kpi-label">A reponer</div><div class="kpi-value">${aReponer}</div></div>
    </div>

    ${ROWS.length === 0
      ? `<div class="empty">No hay ventas en el rango seleccionado.</div>`
      : `<div class="table-wrap psi-wrap"><table class="t psi-t">
          <thead><tr>
            <th class="psi-sticky">SKU</th>
            <th class="psi-sticky2">Producto</th>
            ${theadSem}
            <th class="psi-c">Prom/sem</th>
            <th class="psi-c">Stock</th>
            <th class="psi-c">Días</th>
            <th class="psi-c">Recompra</th>
            <th class="psi-c">⚠</th>
          </tr></thead>
          <tbody id="psi-tbody">${filasFiltradas().map(filaHTML).join('')}</tbody>
        </table></div>`}
  `;

  document.getElementById('psi-calc').addEventListener('click', aplicarParams);
  document.getElementById('psi-export').addEventListener('click', exportarXLSX);
  // Enter en cualquier control recalcula.
  ['psi-desde', 'psi-hasta', 'psi-dias', 'psi-lead', 'psi-colchon'].forEach(id => {
    document.getElementById(id).addEventListener('keydown', e => { if (e.key === 'Enter') aplicarParams(); });
  });
  document.getElementById('psi-quiebres')?.addEventListener('change', aplicarParams);
  document.getElementById('psi-busq')?.addEventListener('input', e => {
    BUSQ = e.target.value;
    const clr = document.getElementById('psi-busq-clear');
    if (clr) clr.style.display = BUSQ ? '' : 'none';
    pintarFilas();
  });
  document.getElementById('psi-busq-clear')?.addEventListener('click', () => {
    BUSQ = '';
    const inp = document.getElementById('psi-busq');
    if (inp) inp.value = '';
    document.getElementById('psi-busq-clear').style.display = 'none';
    pintarFilas();
    inp?.focus();
  });
}

function mediana(arr) {
  const a = [...arr].sort((x, y) => x - y);
  const n = a.length;
  if (!n) return 0;
  return n % 2 ? a[(n - 1) / 2] : (a[n / 2 - 1] + a[n / 2]) / 2;
}

function filaHTML(r) {
  const diasTxt = r.stock <= 0 ? '0' : (r.diasStock >= 999 ? '∞' : r.diasStock);
  const sem = r.semanas.map(s => `<td class="psi-c psi-mono ${s === 0 ? 'psi-muted' : ''}">${s || '—'}</td>`).join('');
  let recompraCell;
  if (r.recompra <= 0) {
    recompraCell = `<td class="psi-c psi-mono psi-muted">—</td>`;
  } else if (r.recomprarYa) {
    recompraCell = `<td class="psi-c psi-mono psi-recompra psi-ya" title="Recomprar YA: quedan ≤ lead+colchón días de stock">🛒 ${fmtNum(r.recompra)}</td>`;
  } else {
    recompraCell = `<td class="psi-c psi-mono psi-recompra-soft" title="Recompra sugerida (todavía hay margen de días)">${fmtNum(r.recompra)}</td>`;
  }
  const codCell = r.cod === 'SIN_SKU'
    ? `<span class="psi-tag-empty" title="Venta sin SKU asignado">sin SKU</span>`
    : `<span class="psi-cod">${esc(r.cod)}</span>${!r.enCatalogo ? ' <span class="psi-tag-empty" title="No está en el catálogo skus">?</span>' : ''}`;
  return `<tr class="${r.recomprarYa ? 'psi-row-ya' : ''}">
    <td class="psi-sticky">${codCell}</td>
    <td class="psi-prod psi-sticky2" title="${esc(r.producto)}">${esc(r.producto)}</td>
    ${sem}
    <td class="psi-c psi-mono">${fmtNum(r.promSem, 1)}</td>
    <td class="psi-c psi-mono">${r.stock > 0 ? fmtNum(r.stock) : '<span class="psi-muted">0</span>'}</td>
    <td class="psi-c psi-mono psi-dias-${r.alerta}">${diasTxt}</td>
    ${recompraCell}
    <td class="psi-c" title="${ALERT_LABEL[r.alerta]}">${ALERT_DOT[r.alerta]}</td>
  </tr>`;
}

function filasFiltradas() {
  const q = BUSQ.trim().toLowerCase();
  if (!q) return ROWS;
  return ROWS.filter(r => (r.cod || '').toLowerCase().includes(q) || (r.producto || '').toLowerCase().includes(q));
}

function pintarFilas() {
  const tb = document.getElementById('psi-tbody');
  if (!tb) return;
  const f = filasFiltradas();
  tb.innerHTML = f.length
    ? f.map(filaHTML).join('')
    : `<tr><td class="psi-muted" colspan="${SEMANAS.length + 7}" style="padding:14px">Sin resultados para “${esc(BUSQ)}”.</td></tr>`;
}

function aplicarParams() {
  const desde = document.getElementById('psi-desde').value;
  const hasta = document.getElementById('psi-hasta').value;
  const dias = parseInt(document.getElementById('psi-dias').value, 10);
  const lead = parseInt(document.getElementById('psi-lead').value, 10);
  const colchon = parseInt(document.getElementById('psi-colchon').value, 10);
  if (!desde || !hasta) { window.toast('Elegí desde y hasta', 'error'); return; }
  if (desde > hasta) { window.toast('"Desde" no puede ser posterior a "Hasta"', 'error'); return; }
  PARAMS.desde = desde;
  PARAMS.hasta = hasta;
  PARAMS.diasObj = (dias && dias > 0) ? dias : 30;
  PARAMS.leadTime = (Number.isFinite(lead) && lead >= 0) ? lead : 15;
  PARAMS.colchon = (Number.isFinite(colchon) && colchon >= 0) ? colchon : 7;
  const chk = document.getElementById('psi-quiebres');
  PARAMS.ignorarQuiebres = chk ? chk.checked : true;
  loadPSI();
}

// ── Exportar a Excel (SheetJS lazy desde CDN; index.html no lo carga) ──────
let _xlsxPromise = null;
function cargarXLSX() {
  if (window.XLSX) return Promise.resolve(window.XLSX);
  if (_xlsxPromise) return _xlsxPromise;
  _xlsxPromise = new Promise((resolve, reject) => {
    const s = document.createElement('script');
    s.src = 'https://cdn.sheetjs.com/xlsx-0.20.3/package/dist/xlsx.full.min.js';
    s.onload = () => window.XLSX ? resolve(window.XLSX) : reject(new Error('XLSX no disponible'));
    s.onerror = () => reject(new Error('No se pudo cargar la librería de Excel'));
    document.head.appendChild(s);
  });
  return _xlsxPromise;
}

async function exportarXLSX() {
  const filas = filasFiltradas();
  if (!filas.length) { window.toast('No hay datos para exportar', 'error'); return; }
  let XLSX;
  try { XLSX = await cargarXLSX(); }
  catch (e) { window.toast(e.message, 'error'); return; }

  const data = filas.map(r => {
    const row = { 'SKU': r.cod, 'Producto': r.producto };
    r.semanas.forEach((s, i) => { row[`Sem ${SEMANAS[i]?.label || (i + 1)}`] = s; });
    row['Prom/sem'] = Math.round(r.promSem * 10) / 10;
    row['Vel/día'] = Math.round(r.velDiaria * 100) / 100;
    row['Stock'] = r.stock;
    row['Días stock'] = r.stock <= 0 ? 0 : (r.diasStock >= 999 ? '∞' : r.diasStock);
    row['Recompra'] = r.recompra;
    row['Recomprar YA'] = r.recomprarYa ? 'Sí' : '';
    row['Alerta'] = ALERT_LABEL[r.alerta];
    return row;
  });

  const ws = XLSX.utils.json_to_sheet(data);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'PSI');
  const sufijo = BUSQ.trim() ? '_filtrado' : '';
  XLSX.writeFile(wb, `PSI_${PARAMS.desde}_${PARAMS.hasta}${sufijo}.xlsx`);
}

// ── Utilidades ─────────────────────────────────────────────────────────────
function esc(s) {
  return String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

function inyectarEstilo() {
  if (document.getElementById('psi-style')) return;
  const css = `
    .psi-lbl{display:inline-flex;align-items:center;gap:6px;font-size:13px;color:#78716C;white-space:nowrap}
    .psi-date{width:auto}
    .psi-dias{width:74px;text-align:right}
    .psi-aviso{margin:4px 0 0;padding:9px 12px;font-size:13px;line-height:1.45;color:#854F0B;background:#FAEEDA;border:1px solid #F0DCB8;border-radius:8px}
    .psi-aviso em{font-style:normal;font-weight:600}
    .psi-t th,.psi-t td{white-space:nowrap}
    .psi-c{text-align:center}
    .psi-wk{font-weight:500;color:#78716C;min-width:46px}
    .psi-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .psi-muted{color:#A8A29E}
    .psi-wrap{overflow-x:auto;max-width:100%}
    .psi-sticky{position:sticky;left:0;background:#fff;z-index:2;width:80px;min-width:80px}
    .psi-sticky2{position:sticky;left:80px;background:#fff;z-index:2}
    .psi-cod{font-family:'JetBrains Mono',ui-monospace,monospace;font-size:13px;color:#D97706;font-weight:600}
    .psi-prod{min-width:260px}
    .psi-recompra{color:#B91C1C;font-weight:700}
    .psi-recompra-soft{color:#A16207;font-weight:600}
    .psi-ya{background:#FEF2F2}
    .psi-row-ya{background:#FEF2F2}
    .psi-row-ya .psi-sticky,.psi-row-ya .psi-sticky2{background:#FEF2F2}
    .psi-kpi-ya{border-color:#FCA5A5;background:#FEF2F2}
    .psi-clear{padding:6px 10px;color:#78716C}
    .psi-tag-empty{font-size:11px;color:#A8A29E;border:1px dashed #D6D3D1;padding:1px 6px;border-radius:6px}
    .psi-dias-verde{color:#15803D;font-weight:600}
    .psi-dias-amarillo{color:#A16207;font-weight:600}
    .psi-dias-naranja{color:#C2410C;font-weight:700}
    .psi-dias-rojo{color:#B91C1C;font-weight:700}
    .psi-dias-negro{color:#1C1917;font-weight:700}
  `;
  const style = document.createElement('style');
  style.id = 'psi-style';
  style.textContent = css;
  document.head.appendChild(style);
}
