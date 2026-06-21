import { sbGet, sbPatch } from '../core/sb.js';

// ── Pantalla: Gastos ───────────────────────────────────────────────────
// Gastos operativos (capa 5). Lista desde la vista v_gastos_ap (AP dinámico,
// CB6) + KPIs + filtros + alta por modal + anular con motivo.
//
// Notas de diseño (ver ADARA-GASTOS.md / ADARA-DECISIONES.md / ADARA-SCHEMA.md):
// - El alta SIEMPRE pasa por el endpoint POST /gastos (Opción A): inserta el
//   gasto + sus renglones fiscales (gasto_fiscal) y, si es sin_factura+efectivo,
//   dispara el movimiento de caja + vínculo, todo del lado del backend.
// - El gasto al P&L es el NETO (P2). Retenciones/percepciones NO cambian el P&L:
//   cambian "a pagar" (bruto + percepciones − retenciones) y la posición fiscal.
// - IVA sólo en factura A (G5 / chk_iva_solo_factura_a). En el resto, monto_iva=0.
// - USD: tc opcional al alta (G9 / B1). Mientras esté NULL traba el cierre del mes.
// - estado_pago lo deriva la vista: usd_sin_tc | pendiente | parcial | pagado.

let DATA = [];            // filas de v_gastos_ap
let ADJ = {};             // adjuntos por gasto: op_id -> [{id, nombre}]
let LINEAS = [];          // lineas_negocio
let LINEA_LABEL = {};     // id -> label
let CANALES = [];         // canales (maestro, FK de gasto_imputacion.canal)
let CUENTAS = [];         // cuentas (para "cuenta origen intención")
let PROVEEDORES = [];     // proveedores (opcional, puede estar vacío)
let FILTRO = { periodo: '', categoria: '', linea: '', estado: '', q: '' };

const CATEGORIAS = [
  ['servicios_basicos', 'Servicios básicos'],
  ['alquiler', 'Alquiler'],
  ['honorarios', 'Honorarios profesionales'],
  ['logistica', 'Logística'],
  ['publicidad', 'Publicidad y marketing'],
  ['insumos', 'Insumos y papelería'],
  ['comunicaciones', 'Comunicaciones'],
  ['software', 'Software / SaaS'],
  ['mantenimiento', 'Mantenimiento'],
  ['combustible', 'Combustible'],
  ['transporte_viajes', 'Transporte y viajes'],
  ['comidas', 'Comidas y representación'],
  ['comisiones_bancarias', 'Comisiones bancarias'],
  ['comisiones_financieras', 'Comisiones financieras'],
  ['cargas_sociales', 'Cargas sociales'],
  ['capacitacion', 'Capacitación'],
  ['otros', 'Otros'],
];
const CAT_LABEL = Object.fromEntries(CATEGORIAS);

const COMPROBANTES = [
  ['factura_a', 'Factura A'],
  ['factura_b', 'Factura B'],
  ['factura_c', 'Factura C'],
  ['ticket', 'Ticket'],
  ['sin_factura', 'Sin factura'],
];
const COMP_CORTO = { factura_a: 'Fra A', factura_b: 'Fra B', factura_c: 'Fra C', ticket: 'Ticket', sin_factura: 'Sin fra' };

// Selector único de "Pago": combina forma_pago + cuenta_origen en una sola
// elección (antes eran dos selects que el usuario tenía que llenar redundante).
// Por dentro sigue guardando forma_pago + cuenta_origen_intencion derivados, así
// que NO cambia ni la tabla ni el matching (ambos son "intención", la verdad del
// pago vive en vinculos + movimientos.cuenta_id).
// Notas:
// - Tarjeta: cuenta vacía. El gasto se concilia vía el desglose del resumen
//   (aparece un único débito agregado en Supervielle, no el cargo individual).
// - USDT/Trust Wallet: forma_pago='transferencia' (el CHECK de gastos.forma_pago
//   no admite 'usdt'); la cuenta trust_wallet es la que lo distingue como cripto.
//   Trust Wallet está creada como caja USD (decisión: simple > tipo cripto propio).
// [value, label, forma_pago, cuenta_origen, moneda sugerida]
const PAGO_OPCIONES = [
  ['efectivo_caja_ars',  'Efectivo — Caja ARS',             'efectivo',          'caja_ars',        'ARS'],
  ['efectivo_caja_usd',  'Efectivo — Caja USD',             'efectivo',          'caja_usd',        'USD'],
  ['transferencia_svl',  'Transferencia — Supervielle',     'transferencia',     'supervielle_ars', 'ARS'],
  ['debito_svl',         'Débito automático — Supervielle', 'debito_automatico', 'supervielle_ars', 'ARS'],
  ['mp',                 'Mercado Pago',                    'mp',                'mp_ars',          'ARS'],
  ['tarjeta',            'Tarjeta',                         'tarjeta',           '',                'ARS'],
  ['usdt_trust',         'USDT — Trust Wallet',             'transferencia',     'trust_wallet',    'USD'],
];
const PAGO_MAP = Object.fromEntries(
  PAGO_OPCIONES.map(([v, , fp, co, mon]) => [v, { forma_pago: fp, cuenta_origen: co || null, moneda: mon }])
);

const FISCAL_TIPOS = [
  ['ret_ganancias', 'Retención Ganancias'],
  ['ret_iva', 'Retención IVA'],
  ['ret_iibb', 'Retención IIBB'],
  ['ret_suss', 'Retención SUSS'],
  ['perc_iva', 'Percepción IVA'],
  ['perc_iibb', 'Percepción IIBB'],
  ['otro_ret', 'Otra retención'],
  ['otro_perc', 'Otra percepción'],
];
// Mismo criterio que la columna generada `clase` en gasto_fiscal
const RET_SET = new Set(['ret_ganancias', 'ret_iva', 'ret_iibb', 'ret_suss', 'otro_ret']);

const LABEL_ESTADO = { usd_sin_tc: 'USD sin TC', pendiente: 'Pendiente', parcial: 'Parcial', pagado: 'Pagado' };
const LABEL_CUENTA = { supervielle_ars: 'Supervielle ARS', mp_ars: 'MP ARS', caja_ars: 'Caja ARS', caja_usd: 'Caja USD', trust_wallet: 'Trust Wallet (USDT)' };

const hoyISO = () => new Date().toISOString().slice(0, 10);

function lineaLabel(l) { return l.nombre || l.descripcion || l.codigo || ('Línea ' + l.id); }
function provLabel(p) { return p.nombre || p.razon_social || p.descripcion || p.cuit || ('Proveedor #' + p.id); }

function fmtMonto(valor, moneda = 'ARS') {
  const n = Number(valor) || 0;
  const abs = Math.abs(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const simbolo = moneda === 'USD' ? 'US$ ' : '$ ';
  return `${n < 0 ? '−' : ''}${simbolo}${abs}`;
}

// ── Carga ──────────────────────────────────────────────────────────────
export async function loadGastos() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando gastos…</div>`;
  try {
    LINEAS = await sbGet('lineas_negocio', 'order=id.asc');
    LINEA_LABEL = Object.fromEntries(LINEAS.map(l => [l.id, lineaLabel(l)]));
    CANALES = await sbGet('canales', 'order=codigo.asc').catch(() => []);
    CUENTAS = await sbGet('cuentas', 'order=id.asc');
    PROVEEDORES = await sbGet('proveedores', 'order=id.asc').catch(() => []);
    await recargar();
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudieron cargar los gastos: ${e.message}</div>`;
    return;
  }
  inyectarEstilo();
  render();
}

async function recargar() {
  DATA = await sbGet('v_gastos_ap', 'order=fecha.desc,id.desc');
  ADJ = {};
  try {
    const adj = await sbGet('adjuntos', 'op_tipo=eq.gasto&select=id,op_id,nombre&order=id.desc');
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

function periodosPresentes() {
  return [...new Set(DATA.map(g => g.periodo).filter(Boolean))].sort().reverse();
}

// ── Render principal ───────────────────────────────────────────────────
function render() {
  const root = document.getElementById('app-screens');
  const periodos = periodosPresentes();

  // KPIs sobre el período seleccionado (o todos)
  const delPeriodo = DATA.filter(g => !FILTRO.periodo || g.periodo === FILTRO.periodo);
  const cantidad = delPeriodo.length;
  const totalArs = delPeriodo.reduce((s, g) => s + (Number(g.bruto_ars) || 0), 0);
  const apItems = delPeriodo.filter(g => g.estado_pago === 'pendiente' || g.estado_pago === 'parcial');
  const apTotal = apItems.reduce((s, g) => s + (Number(g.saldo_pendiente_ars) || 0), 0);
  const usdSinTc = delPeriodo.filter(g => g.estado_pago === 'usd_sin_tc').length;
  const creditoIva = delPeriodo.filter(g => g.genera_credito_iva).reduce((s, g) => s + (Number(g.monto_iva) || 0), 0);

  // Tabla: período + resto de filtros
  const filtrados = delPeriodo.filter(g => {
    if (FILTRO.categoria && g.categoria_codigo !== FILTRO.categoria) return false;
    if (FILTRO.linea && !(Array.isArray(g.imputaciones) && g.imputaciones.some(i => String(i.linea_id) === FILTRO.linea))) return false;
    if (FILTRO.estado && g.estado_pago !== FILTRO.estado) return false;
    if (FILTRO.q) {
      const txt = `${g.descripcion || ''} ${CAT_LABEL[g.categoria_codigo] || ''} ${g.nro_comprobante || ''}`.toLowerCase();
      if (!txt.includes(FILTRO.q.toLowerCase())) return false;
    }
    return true;
  });

  const estadosPresentes = ['pendiente', 'parcial', 'pagado', 'usd_sin_tc'].filter(es => delPeriodo.some(g => g.estado_pago === es));
  const pills = [`<button class="pill ${FILTRO.estado === '' ? 'active' : ''}" data-estado="">Todos <span class="num">${cantidad}</span></button>`]
    .concat(estadosPresentes.map(es => {
      const n = delPeriodo.filter(g => g.estado_pago === es).length;
      return `<button class="pill ${FILTRO.estado === es ? 'active' : ''}" data-estado="${es}">${LABEL_ESTADO[es]} <span class="num">${n}</span></button>`;
    })).join('');

  const optPeriodo = ['<option value="">Todos los meses</option>']
    .concat(periodos.map(p => `<option value="${p}" ${FILTRO.periodo === p ? 'selected' : ''}>${p}</option>`)).join('');
  const optCat = ['<option value="">Todas las categorías</option>']
    .concat(CATEGORIAS.map(([v, l]) => `<option value="${v}" ${FILTRO.categoria === v ? 'selected' : ''}>${l}</option>`)).join('');
  const optLinea = ['<option value="">Todas las líneas</option>']
    .concat(LINEAS.map(l => `<option value="${l.id}" ${FILTRO.linea === String(l.id) ? 'selected' : ''}>${esc(LINEA_LABEL[l.id])}</option>`)).join('');

  root.innerHTML = `
    <div class="toolbar">
      <select class="select" id="f-periodo" style="width:auto">${optPeriodo}</select>
      <select class="select" id="f-cat" style="width:auto">${optCat}</select>
      <select class="select" id="f-linea" style="width:auto">${optLinea}</select>
      <input class="input grow" id="f-q" type="text" placeholder="Buscar descripción…" value="${FILTRO.q.replace(/"/g, '&quot;')}">
      <button class="btn btn-primary" id="btn-nuevo">+ Cargar gasto</button>
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">Gastos del período</div><div class="kpi-value">${fmtMonto(totalArs)}</div><div class="kpi-sub">${cantidad} ${cantidad === 1 ? 'gasto' : 'gastos'}</div></div>
      <div class="kpi"><div class="kpi-label">Pendiente de pago</div><div class="kpi-value">${fmtMonto(apTotal)}</div><div class="kpi-sub">AP · ${apItems.length}</div></div>
      <div class="kpi"><div class="kpi-label">USD sin TC</div><div class="kpi-value ${usdSinTc ? 'gas-warn' : ''}">${usdSinTc}</div><div class="kpi-sub">${usdSinTc ? 'traba el cierre' : '—'}</div></div>
      <div class="kpi"><div class="kpi-label">Crédito IVA</div><div class="kpi-value">${fmtMonto(creditoIva)}</div><div class="kpi-sub">sólo factura A</div></div>
    </div>

    <div class="pills">${pills}</div>

    ${filtrados.length === 0
      ? `<div class="empty">No hay gastos para mostrar. Cargá el primero con “+ Cargar gasto”.</div>`
      : `<div class="table-wrap"><table class="t">
          <thead><tr>
            <th style="width:74px">Fecha</th>
            <th>Descripción</th>
            <th style="width:160px">Categoría</th>
            <th style="width:130px">Línea</th>
            <th style="width:140px;text-align:right">Total</th>
            <th style="width:110px">Estado</th>
            <th style="width:70px"></th>
          </tr></thead>
          <tbody>${filtrados.map(filaHTML).join('')}</tbody>
        </table></div>`}
  `;

  document.getElementById('f-periodo').addEventListener('change', e => { FILTRO.periodo = e.target.value; render(); });
  document.getElementById('f-cat').addEventListener('change', e => { FILTRO.categoria = e.target.value; render(); });
  document.getElementById('f-linea').addEventListener('change', e => { FILTRO.linea = e.target.value; render(); });
  document.getElementById('f-q').addEventListener('input', e => { FILTRO.q = e.target.value; render(); });
  document.getElementById('btn-nuevo').addEventListener('click', openModalNuevo);
  document.querySelectorAll('.pill').forEach(p => p.addEventListener('click', () => { FILTRO.estado = p.dataset.estado; render(); }));
  document.querySelectorAll('.gas-anular').forEach(b => b.addEventListener('click', () => anularGasto(+b.dataset.id)));
  document.querySelectorAll('.gas-clip').forEach(b => b.addEventListener('click', () => verAdjunto(+b.dataset.adj)));
}

function filaHTML(g) {
  const totalOrigen = Number(g.total_factura_origen) || 0;
  const comp = COMP_CORTO[g.tipo_comprobante] || g.tipo_comprobante;
  const estado = g.estado_pago;
  const rowCls = estado === 'usd_sin_tc' ? 'gas-row-warn' : '';
  return `<tr class="${rowCls}">
    <td>${(g.fecha || '').slice(8, 10)}/${(g.fecha || '').slice(5, 7)}</td>
    <td>${esc(g.descripcion || '—')} <span class="gas-comp">· ${comp}</span></td>
    <td><span class="gas-tag">${esc(CAT_LABEL[g.categoria_codigo] || g.categoria_codigo)}</span></td>
    <td class="gas-muted">${esc(g.lineas_resumen || '—')}</td>
    <td style="text-align:right" class="gas-mono">${fmtMonto(totalOrigen, g.moneda)}</td>
    <td><span class="gas-badge gas-badge-${estado}">${LABEL_ESTADO[estado] || estado}</span></td>
    <td style="text-align:right">${(ADJ[g.id] && ADJ[g.id].length) ? `<button class="gas-clip" data-adj="${ADJ[g.id][0].id}" title="Ver factura: ${esc(ADJ[g.id][0].nombre)}">📎</button>` : ''}<button class="gas-anular" data-id="${g.id}" title="Anular">Anular</button></td>
  </tr>`;
}

async function anularGasto(id) {
  const g = DATA.find(x => x.id === id);
  const motivo = prompt('Motivo de la anulación:');
  if (motivo == null) return;
  if (!motivo.trim()) { window.toast('Necesitás un motivo para anular', 'error'); return; }
  // Aviso: el mov de caja automático de sin_factura efectivo NO se revierte solo en v1.
  if (g && g.tipo_comprobante === 'sin_factura') {
    const ok = confirm('Si este gasto generó un movimiento de caja automático, ese movimiento NO se revierte solo todavía. ¿Anular igual?');
    if (!ok) return;
  }
  try {
    await sbPatch('gastos', `id=eq.${id}`, { estado: 'anulado', motivo_anulacion: motivo.trim() });
    window.toast('Gasto anulado');
    await recargar();
    render();
  } catch (e) {
    window.toast('Error al anular: ' + e.message, 'error');
  }
}

// ── Modal de alta ──────────────────────────────────────────────────────
function openModalNuevo() {
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';

  const optCat = '<option value="">Elegí categoría…</option>' + CATEGORIAS.map(([v, l]) => `<option value="${v}">${l}</option>`).join('');
  const optLinea = '<option value="">Elegí línea…</option>' + LINEAS.map(l => `<option value="${l.id}">${esc(LINEA_LABEL[l.id])}</option>`).join('');
  const optProv = '<option value="">— Sin proveedor —</option>' + PROVEEDORES.map(p => `<option value="${p.id}">${esc(provLabel(p))}</option>`).join('');
  const optComp = COMPROBANTES.map(([v, l]) => `<option value="${v}">${l}</option>`).join('');
  const optPago = '<option value="">— Sin especificar —</option>' + PAGO_OPCIONES.map(([v, l]) => `<option value="${v}">${l}</option>`).join('');
  const optFiscal = FISCAL_TIPOS.map(([v, l]) => `<option value="${v}">${l}</option>`).join('');

  overlay.innerHTML = `
    <div class="modal gas-modal">
      <div class="card-title">Cargar gasto</div>

      <div class="gas-grid2">
        <div class="field"><label>Fecha</label><input class="input" id="g-fecha" type="date" value="${hoyISO()}"></div>
        <div class="field"><label>Comprobante</label><select class="select" id="g-comp">${optComp}</select></div>
      </div>

      <div class="field"><label>Categoría</label><select class="select" id="g-cat">${optCat}</select></div>

      <div class="gas-imp">
        <div class="gas-imp-h">
          <span>Imputación por línea / canal</span>
          <div>
            <button type="button" class="gas-imp-eq" id="g-imp-eq">repartir equitativo</button>
            <button type="button" class="gas-addfisc" id="g-imp-add">+ agregar</button>
          </div>
        </div>
        <div id="g-imp-rows"></div>
        <div class="gas-imp-total" id="g-imp-total"></div>
      </div>

      <div class="field"><label>Descripción</label><input class="input" id="g-desc" type="text" placeholder="Ej: alquiler depósito mayo"></div>

      <div class="gas-grid2">
        <div class="field"><label>Proveedor (opcional)</label>
          <div style="display:flex;gap:8px;align-items:center">
            <select class="select" id="g-prov" style="flex:1">${optProv}</select>
            <button class="btn btn-ghost" id="g-prov-add" type="button" style="padding:6px 12px;font-size:13px">+ Nuevo</button>
          </div>
          <div id="g-prov-new" style="display:none;flex-direction:column;gap:8px;margin-top:8px">
            <div style="display:flex;gap:8px;align-items:center">
              <input class="input" id="g-prov-cuit" placeholder="CUIT (sin guiones)" style="width:200px">
              <span id="g-prov-status" style="font-size:12px;color:#78716C"></span>
            </div>
            <div style="display:flex;gap:8px;align-items:center">
              <input class="input" id="g-prov-nombre" placeholder="Nombre (se completa con el CUIT)" style="flex:1">
              <button class="btn btn-primary" id="g-prov-crear" type="button" style="padding:6px 12px;font-size:13px">Crear</button>
            </div>
          </div>
        </div>
        <div class="field"><label>N° comprobante (opcional)</label><input class="input" id="g-nro" type="text" placeholder="0001-00001234"></div>
      </div>

      <div class="gas-grid2">
        <div class="field"><label>Moneda</label>
          <select class="select" id="g-moneda"><option value="ARS">ARS</option><option value="USD">USD</option></select>
        </div>
        <div class="field" id="g-tc-wrap" style="display:none"><label>TC (USD→ARS)</label>
          <input class="input" id="g-tc" type="number" min="0" step="0.01" placeholder="se puede completar al pagar">
        </div>
      </div>

      <div class="gas-grid2">
        <div class="field"><label id="g-neto-lbl">Neto (sin IVA)</label><input class="input" id="g-neto" type="number" min="0" step="0.01" placeholder="0,00"></div>
        <div class="field" id="g-iva-wrap"><label>IVA</label><input class="input" id="g-iva" type="number" min="0" step="0.01" placeholder="0,00"></div>
      </div>

      <div class="gas-fiscal">
        <div class="gas-fiscal-h">
          <span>Retenciones y percepciones</span>
          <button type="button" class="gas-addfisc" id="g-addfisc">+ agregar</button>
        </div>
        <div id="g-fisc-rows"></div>
      </div>

      <div class="gas-resumen" id="g-resumen"></div>

      <div class="field"><label>Pago</label><select class="select" id="g-pago">${optPago}</select></div>

      <div class="field"><label>Factura / comprobante (PDF o imagen)</label>
        <input class="input" id="g-factura" type="file" accept="application/pdf,image/*">
        <div class="gas-comp" style="margin-top:4px">Opcional. Se guarda el archivo y queda asociado al gasto.</div>
      </div>

      <div class="modal-actions">
        <button class="btn btn-ghost" id="g-cancel">Cancelar</button>
        <button class="btn btn-primary" id="g-guardar">Guardar</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);

  const $ = sel => overlay.querySelector(sel);
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  $('#g-cancel').addEventListener('click', close);

  // Alta rápida de proveedor (mismo patrón que Compras)
  $('#g-prov-add').addEventListener('click', () => {
    const box = $('#g-prov-new');
    box.style.display = box.style.display === 'none' ? 'flex' : 'none';
  });
  async function buscarPadronProv() {
    const cuit = $('#g-prov-cuit').value.replace(/\D/g, '');
    const st = $('#g-prov-status');
    if (cuit.length !== 11) { st.textContent = cuit.length ? '⚠ El CUIT debe tener 11 dígitos' : ''; return null; }
    st.textContent = 'Buscando en ARCA…'; st.style.color = '#78716C';
    try {
      const r = await fetch('/padron/' + cuit);
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || 'no encontrado');
      if (d.nombre) $('#g-prov-nombre').value = d.nombre;
      st.textContent = '✓ ' + d.nombre + (d.estado ? ' · ' + d.estado : ''); st.style.color = '#0F6E56';
      return d.nombre;
    } catch (e) {
      st.textContent = '⚠ ' + e.message; st.style.color = '#B91C1C';
      return null;
    }
  }
  $('#g-prov-cuit').addEventListener('blur', buscarPadronProv);

  $('#g-prov-crear').addEventListener('click', async () => {
    const cuit = $('#g-prov-cuit').value.replace(/\D/g, '');
    if (cuit.length !== 11) { window.toast('El CUIT es obligatorio (11 dígitos)', 'error'); return; }
    if (!$('#g-prov-nombre').value.trim()) await buscarPadronProv();
    const nombre = $('#g-prov-nombre').value.trim();
    if (!nombre) { window.toast('No se pudo traer el nombre desde ARCA; revisá el CUIT o escribilo a mano', 'error'); return; }
    try {
      const r = await fetch('/proveedores', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nombre, cuit })
      });
      const p = await r.json();
      if (!r.ok) throw new Error(p.error || r.statusText);
      PROVEEDORES.push(p);
      PROVEEDORES.sort((a, b) => provLabel(a).localeCompare(provLabel(b)));
      $('#g-prov').innerHTML = '<option value="">— Sin proveedor —</option>' +
        PROVEEDORES.map(x => `<option value="${x.id}" ${x.id === p.id ? 'selected' : ''}>${esc(provLabel(x))}</option>`).join('');
      $('#g-prov-new').style.display = 'none';
      $('#g-prov-nombre').value = ''; $('#g-prov-cuit').value = ''; $('#g-prov-status').textContent = '';
      window.toast('Proveedor creado');
    } catch (e) { window.toast('Error: ' + e.message, 'error'); }
  });

  const compSel = $('#g-comp');
  const monedaSel = $('#g-moneda');
  const netoInp = $('#g-neto');
  const ivaInp = $('#g-iva');

  function aplicarComprobante() {
    const esA = compSel.value === 'factura_a';
    $('#g-iva-wrap').style.display = esA ? '' : 'none';
    $('#g-neto-lbl').textContent = esA ? 'Neto (sin IVA)' : 'Monto';
    if (!esA) ivaInp.value = '';
    recalc();
  }
  function aplicarMoneda() {
    $('#g-tc-wrap').style.display = monedaSel.value === 'USD' ? '' : 'none';
    recalc();
  }

  // Auto-sugerir IVA 21% al tipear neto en factura A (editable)
  netoInp.addEventListener('input', () => {
    if (compSel.value === 'factura_a') {
      const neto = parseFloat(netoInp.value);
      if (neto > 0 && (!ivaInp.value || parseFloat(ivaInp.value) === 0)) {
        ivaInp.value = Math.round(neto * 0.21 * 100) / 100;
      }
    }
    recalc();
  });
  ivaInp.addEventListener('input', recalc);
  compSel.addEventListener('change', aplicarComprobante);
  monedaSel.addEventListener('change', aplicarMoneda);

  // Al elegir el pago, auto-sugiere la moneda (USD para Caja USD / Trust Wallet).
  // Queda editable por si hay un caso atípico (ej: gasto USD pagado en ARS).
  const pagoEl = $('#g-pago');
  pagoEl.addEventListener('change', () => {
    const m = PAGO_MAP[pagoEl.value];
    if (m && m.moneda) { monedaSel.value = m.moneda; aplicarMoneda(); }
  });

  // Renglones fiscales (repeater)
  function addFiscRow() {
    const row = document.createElement('div');
    row.className = 'gas-fisc-row';
    row.innerHTML = `
      <select class="select gas-f-tipo"><option value="">Tipo…</option>${optFiscal}</select>
      <input class="input gas-f-monto" type="number" min="0" step="0.01" placeholder="0,00">
      <button type="button" class="gas-f-del" title="Quitar">✕</button>`;
    $('#g-fisc-rows').appendChild(row);
    row.querySelector('.gas-f-tipo').addEventListener('change', recalc);
    row.querySelector('.gas-f-monto').addEventListener('input', recalc);
    row.querySelector('.gas-f-del').addEventListener('click', () => { row.remove(); recalc(); });
  }
  $('#g-addfisc').addEventListener('click', addFiscRow);

  function leerFiscal() {
    return [...overlay.querySelectorAll('.gas-fisc-row')].map(r => ({
      tipo: r.querySelector('.gas-f-tipo').value,
      monto: parseFloat(r.querySelector('.gas-f-monto').value)
    })).filter(x => x.tipo && x.monto > 0);
  }

  function recalc() {
    const esA = compSel.value === 'factura_a';
    const neto = parseFloat(netoInp.value) || 0;
    const iva = esA ? (parseFloat(ivaInp.value) || 0) : 0;
    const bruto = neto + iva;
    const fisc = leerFiscal();
    const perc = fisc.filter(x => !RET_SET.has(x.tipo)).reduce((s, x) => s + x.monto, 0);
    const ret = fisc.filter(x => RET_SET.has(x.tipo)).reduce((s, x) => s + x.monto, 0);
    const totalFactura = bruto + perc;
    const aPagar = totalFactura - ret;
    const sym = monedaSel.value === 'USD' ? 'US$ ' : '$ ';
    const f = n => sym + (Math.round(n * 100) / 100).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    $('#g-resumen').innerHTML = `
      <div class="gas-res-r"><span>Total factura</span><span class="gas-mono">${f(totalFactura)}</span></div>
      <div class="gas-res-r gas-res-strong"><span>A pagar al proveedor</span><span class="gas-mono">${f(aPagar)}</span></div>`;
  }

  aplicarComprobante();
  aplicarMoneda();
  recalc();

  // Imputaciones (línea + canal opcional + %). El % reparte el gasto completo:
  // neto al P&L por línea/canal; IVA y perc/ret al ledger fiscal por línea.
  const optCanal = '<option value="">Toda la línea</option>' + CANALES.map(c => `<option value="${c.codigo}">${esc(c.nombre)}</option>`).join('');
  const optLineaImp = '<option value="">Línea…</option>' + LINEAS.map(l => `<option value="${l.id}">${esc(LINEA_LABEL[l.id])}</option>`).join('');

  function leerImput() {
    return [...overlay.querySelectorAll('.gas-imp-row')].map(r => ({
      linea_id: r.querySelector('.gas-i-linea').value ? +r.querySelector('.gas-i-linea').value : null,
      canal: r.querySelector('.gas-i-canal').value || null,
      porcentaje: parseFloat(r.querySelector('.gas-i-pct').value)
    }));
  }
  function recalcImp() {
    const suma = Math.round(leerImput().reduce((s, x) => s + (x.porcentaje || 0), 0) * 100) / 100;
    const ok = Math.abs(suma - 100) < 0.01;
    $('#g-imp-total').innerHTML = `Σ ${suma.toLocaleString('es-AR', { maximumFractionDigits: 2 })}% `
      + (ok ? '<span class="gas-i-ok">✓</span>' : '<span class="gas-i-bad">debe sumar 100%</span>');
  }
  function addImpRow(pct) {
    const row = document.createElement('div');
    row.className = 'gas-imp-row';
    row.innerHTML = `
      <select class="select gas-i-linea">${optLineaImp}</select>
      <select class="select gas-i-canal">${optCanal}</select>
      <input class="input gas-i-pct" type="number" min="0" max="100" step="0.01" placeholder="%" value="${pct != null ? pct : ''}">
      <button type="button" class="gas-i-del" title="Quitar">✕</button>`;
    $('#g-imp-rows').appendChild(row);
    row.querySelector('.gas-i-pct').addEventListener('input', recalcImp);
    row.querySelector('.gas-i-del').addEventListener('click', () => { row.remove(); recalcImp(); });
    recalcImp();
  }
  function repartirEquitativo() {
    const inps = [...overlay.querySelectorAll('.gas-imp-row .gas-i-pct')];
    const n = inps.length;
    if (!n) return;
    const base = Math.floor((100 / n) * 100) / 100;
    let acc = 0;
    inps.forEach((inp, i) => {
      if (i === n - 1) { inp.value = Math.round((100 - acc) * 100) / 100; }
      else { inp.value = base; acc = Math.round((acc + base) * 100) / 100; }
    });
    recalcImp();
  }
  $('#g-imp-add').addEventListener('click', () => addImpRow());
  $('#g-imp-eq').addEventListener('click', repartirEquitativo);
  addImpRow(100);   // arranca con un renglón al 100%

  // Guardar
  $('#g-guardar').addEventListener('click', async () => {
    const tipo_comprobante = compSel.value;
    const esA = tipo_comprobante === 'factura_a';
    const fecha = $('#g-fecha').value;
    const categoria_codigo = $('#g-cat').value;
    const descripcion = $('#g-desc').value.trim();
    const moneda = monedaSel.value;
    const neto = parseFloat(netoInp.value);
    const iva = esA ? (parseFloat(ivaInp.value) || 0) : 0;
    const tcVal = $('#g-tc').value;
    const pagoVal = $('#g-pago').value;
    const pagoMap = PAGO_MAP[pagoVal] || {};
    const forma_pago = pagoMap.forma_pago || null;
    const cuenta_origen_intencion = pagoMap.cuenta_origen || null;
    const proveedor_id = $('#g-prov').value ? +$('#g-prov').value : null;
    const nro_comprobante = $('#g-nro').value.trim() || null;
    const fiscal = leerFiscal();

    const imputacionesRaw = leerImput();
    const imputaciones = imputacionesRaw.filter(x => x.linea_id && x.porcentaje > 0);

    if (!fecha) { window.toast('Falta la fecha', 'error'); return; }
    if (!tipo_comprobante) { window.toast('Elegí el comprobante', 'error'); return; }
    if (!categoria_codigo) { window.toast('Elegí una categoría', 'error'); return; }
    if (imputacionesRaw.some(x => (x.porcentaje > 0 && !x.linea_id) || (x.linea_id && !(x.porcentaje > 0)))) {
      window.toast('Cada imputación necesita línea y %', 'error'); return;
    }
    if (!imputaciones.length) { window.toast('Cargá al menos una imputación (línea + %)', 'error'); return; }
    const sumaPct = Math.round(imputaciones.reduce((s, x) => s + x.porcentaje, 0) * 100) / 100;
    if (Math.abs(sumaPct - 100) > 0.01) { window.toast(`Las imputaciones deben sumar 100% (suman ${sumaPct}%)`, 'error'); return; }
    if (!descripcion) { window.toast('Poné una descripción', 'error'); return; }
    if (!(neto > 0)) { window.toast('El monto tiene que ser mayor a 0', 'error'); return; }
    if (moneda === 'USD' && tipo_comprobante === 'sin_factura' && forma_pago === 'efectivo' && !(parseFloat(tcVal) > 0)) {
      window.toast('Efectivo sin factura en USD: cargá el TC', 'error'); return;
    }

    const gasto = {
      fecha,
      categoria_codigo,
      proveedor_id,
      descripcion,
      tipo_comprobante,
      nro_comprobante,
      moneda,
      tc: (moneda === 'USD' && parseFloat(tcVal) > 0) ? parseFloat(tcVal) : null,
      monto_neto: neto,
      monto_iva: iva,
      cuenta_origen_intencion,
      forma_pago,
    };

    const btn = $('#g-guardar');
    btn.disabled = true;
    try {
      const r = await fetch('/gastos', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ gasto, fiscal, imputaciones })
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));

      // Subir la factura adjunta (si se eligió un archivo)
      const fileEl = $('#g-factura');
      const file = fileEl && fileEl.files && fileEl.files[0];
      if (file && data.id) {
        try {
          const fd = new FormData();
          fd.append('op_tipo', 'gasto');
          fd.append('op_id', String(data.id));
          fd.append('file', file);
          const ra = await fetch('/adjuntos', { method: 'POST', body: fd });
          if (!ra.ok) { const da = await ra.json().catch(() => ({})); throw new Error(da.error || (ra.status + '')); }
        } catch (eAdj) {
          window.toast('Gasto guardado, pero la factura no se subió: ' + eAdj.message, 'error');
          close(); await recargar(); render(); return;
        }
      }

      window.toast('Gasto cargado');
      close();
      await recargar();
      render();
    } catch (e) {
      window.toast('Error al guardar: ' + e.message, 'error');
      btn.disabled = false;
    }
  });
}

// ── Utilidades ─────────────────────────────────────────────────────────
function esc(s) {
  return String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

// Estilos propios de la pantalla (los componentes generales viven en base.css).
function inyectarEstilo() {
  if (document.getElementById('gas-style')) return;
  const css = `
    .gas-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .gas-muted{color:#78716C;font-size:13px}
    .gas-comp{color:#A8A29E;font-size:12px}
    .gas-warn{color:#854F0B}
    .gas-tag{font-size:12px;color:#57534E;background:#F5F5F4;padding:2px 8px;border-radius:6px}
    .gas-badge{font-size:12px;padding:2px 8px;border-radius:6px}
    .gas-badge-pendiente{background:#E6F1FB;color:#0C447C}
    .gas-badge-parcial{background:#E6F1FB;color:#0C447C}
    .gas-badge-pagado{background:#E1F5EE;color:#0F6E56}
    .gas-badge-usd_sin_tc{background:#FAEEDA;color:#854F0B}
    .gas-row-warn{background:rgba(217,119,6,0.06)}
    .gas-anular{font-size:12px;color:#B91C1C;background:none;border:0;cursor:pointer;font:inherit;padding:2px 4px}
    .gas-clip{font-size:14px;background:none;border:0;cursor:pointer;padding:2px 6px;margin-right:2px}
    .gas-clip:hover{opacity:.65}
    .gas-anular:hover{text-decoration:underline}
    .gas-modal{max-width:560px}
    .gas-grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .gas-fiscal{margin:10px 0;padding:10px 12px;background:#FAFAF9;border:1px solid #E7E5E4;border-radius:8px}
    .gas-fiscal-h{display:flex;justify-content:space-between;align-items:center;font-size:13px;color:#57534E;margin-bottom:6px}
    .gas-addfisc{font-size:12px;color:#D97706;background:none;border:0;cursor:pointer;font:inherit}
    .gas-fisc-row{display:grid;grid-template-columns:1fr 120px 28px;gap:8px;margin-bottom:6px;align-items:center}
    .gas-f-del{background:none;border:0;color:#A8A29E;cursor:pointer;font-size:14px}
    .gas-imp{margin:10px 0;padding:10px 12px;background:#FAFAF9;border:1px solid #E7E5E4;border-radius:8px}
    .gas-imp-h{display:flex;justify-content:space-between;align-items:center;font-size:13px;color:#57534E;margin-bottom:6px}
    .gas-imp-eq{font-size:12px;color:#0C447C;background:none;border:0;cursor:pointer;font:inherit;margin-right:10px}
    .gas-imp-row{display:grid;grid-template-columns:1fr 1fr 90px 28px;gap:8px;margin-bottom:6px;align-items:center}
    .gas-i-del{background:none;border:0;color:#A8A29E;cursor:pointer;font-size:14px}
    .gas-imp-total{font-size:13px;color:#57534E;text-align:right;margin-top:4px}
    .gas-i-ok{color:#0F6E56;font-weight:600}
    .gas-i-bad{color:#B91C1C;font-weight:600}
    .gas-resumen{margin:10px 0;padding:8px 12px;border:1px dashed #D6D3D1;border-radius:8px}
    .gas-res-r{display:flex;justify-content:space-between;font-size:13px;padding:2px 0;color:#57534E}
    .gas-res-strong{font-weight:600;color:#1C1917;border-top:1px solid #E7E5E4;margin-top:4px;padding-top:6px}
  `;
  const style = document.createElement('style');
  style.id = 'gas-style';
  style.textContent = css;
  document.head.appendChild(style);
}
