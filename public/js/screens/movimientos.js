import { sbGet, sbPost, sbPatch } from '../core/sb.js';
import { crearSkuPicker } from '../core/skuPicker.js';

// ── Pantalla: Movimientos ──────────────────────────────────────────────
// Extracto unificado de cuentas (tabla `movimientos`, capa 4).
// v1: carga MANUAL. La importación de CSV/extracto (Supervielle/MP) es etapa 2.
//
// Notas de diseño (ver ADARA-DECISIONES.md / ADARA-SCHEMA.md):
// - `monto` es signed: + entrada, − salida. Una sola columna, sin `tipo`.
// - En cuentas USD (caja_usd) el monto se guarda en USD nativo; la valuación
//   a pesos es dinámica (TI5). Acá NO se convierte.
// - `referencia_externa` es NOT NULL + UNIQUE(origen, referencia_externa).
//   En carga manual NO se deduplica por contenido (dos movimientos iguales pueden
//   ser reales): cada fila lleva una referencia ÚNICA propia (surrogate). El seguro
//   contra el doble-cargado por error es un AVISO al guardar (mira cuenta+fecha+monto),
//   no un bloqueo por constraint.
// - La línea de negocio NO vive en el movimiento (opción A / CB6): se hereda de la
//   operación vinculada al conciliar. Por eso no hay campo de línea acá.
// - `estado` en v1 se deriva de conciliado_auto (auto | pendiente). Cuando exista
//   Conciliación, cambiar la lectura a la vista v_movimientos_estado para traer
//   parcial/conciliado en vivo.

let CUENTAS = [];          // [{id, codigo, moneda, ...}]
let CUENTA_BY_ID = {};     // id -> cuenta
let DATA = [];             // movimientos cargados
let FILTRO = { cuenta: '', estado: '', q: '' };
let MES = '';              // YYYY-MM seleccionado (mes del extracto)
let MESES = [];            // meses disponibles, desc
let ESTADO_BY_ID = {};     // movimiento_id -> estado (ÚNICA fuente de verdad: v_movimientos_estado)
let VINC_OPS = {};         // movimiento_id -> [{op_tipo, op_id}]
let VENTA_NUM = {};        // ventas_ml.id -> ml_order_id (para mostrar el N° de venta)
let OP_SIBLINGS = {};      // N° de operación MP -> movimientos que lo comparten (en el mes)
let LINEAS = [];           // catálogo de líneas de negocio
let LINEA_LABEL = {};      // linea_id -> nombre
let VENTA_LINEA = {};      // ventas_ml.id -> linea_negocio_id (línea heredada)
let GASTO_LINEA = {};      // gasto id -> linea_id
let COMPRA_LINEA = {};     // compra_id -> linea_id

// ── Cheques (pestaña) ──────────────────────────────────────────────────
// Los cheques NO se mezclan en la lista del extracto: van en pestaña aparte.
// Si un cheque emitido apareciera como movimiento, la misma plata se vería dos
// veces (al emitirlo y al debitarse). Emitir no mueve plata: cambia el acreedor.
let TAB = 'extracto';      // 'extracto' | 'cheques'
let CHEQUES = [];          // v_cheques
let CHQ_IMPUT = {};        // cheque_id -> [{op_tipo, op_id, monto}]
let AP_COMPRAS = [];       // compras con saldo, para imputar
let PROVEEDORES = [];      // catálogo
let COMPRA_INFO = {};      // compra_id -> {nro_factura, proveedor_id}

// Etiqueta linda por código de cuenta (fallback al código si no está mapeado)
const LABEL_CUENTA = {
  supervielle_ars: 'Supervielle ARS',
  mp_ars: 'MP ARS',
  caja_ars: 'Caja ARS',
  caja_usd: 'Caja USD',
};

// Categorías operativas convencionales (ADARA-SCHEMA.md, capa 4)
const CATEGORIAS = [
  ['cobro_venta', 'Cobro de venta'],
  ['pago_proveedor', 'Pago a proveedor'],
  ['gasto', 'Gasto'],
  ['comision_bancaria', 'Comisión bancaria'],
  ['comision_marketplace', 'Comisión marketplace'],
  ['impuesto', 'Impuesto'],
  ['transferencia_interna', 'Transferencia interna'],
  ['devolucion', 'Devolución'],
  ['interes', 'Interés'],
  ['ajuste_manual', 'Ajuste manual'],
];

const hoyISO = () => new Date().toISOString().slice(0, 10);
const mesDe = f => (f || '').slice(0, 7);
const mesLargo = ym => {
  if (!ym) return '—';
  const [y, m] = ym.split('-');
  return new Date(+y, +m - 1, 1).toLocaleDateString('es-AR', { month: 'long', year: 'numeric' });
};

// Referencia única para cargas manuales (cumple NOT NULL + UNIQUE sin deduplicar
// por contenido: dos movimientos iguales son válidos, cada uno con su referencia).
function nuevaRef() {
  const rnd = (window.crypto && crypto.randomUUID)
    ? crypto.randomUUID()
    : Date.now() + '-' + Math.random().toString(36).slice(2);
  return 'manual-' + rnd;
}

function cuentaLabel(id) {
  const c = CUENTA_BY_ID[id];
  if (!c) return '—';
  return LABEL_CUENTA[c.codigo] || c.codigo;
}

function monedaDe(id) {
  const c = CUENTA_BY_ID[id];
  return c ? c.moneda : 'ARS';
}

// Formato de monto con signo y símbolo según moneda
function fmtMonto(valor, moneda) {
  const n = Number(valor) || 0;
  const abs = Math.abs(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const signo = n < 0 ? '−' : '+';
  const simbolo = moneda === 'USD' ? 'US$ ' : '$ ';
  return `${signo}${simbolo}${abs}`;
}

// Estado: ÚNICA fuente de verdad = la vista v_movimientos_estado de la base
// (la misma que usa la pantalla Conciliación), para que las dos pantallas nunca
// muestren estados distintos para el mismo movimiento. Fallback solo para filas
// recién cargadas a mano que todavía no pasaron por la vista.
function estadoDe(m) {
  return ESTADO_BY_ID[m.id] || (m.conciliado_auto ? 'auto' : 'pendiente');
}

// Texto de "con qué está vinculado": el N° de venta (ml_order_id) para enganches
// a ventas de ML; el tipo de operación para los demás (gasto/compra/…). El N° de
// factura de Tango se sumará cuando se integre Tango.
function vinculadoA(m) {
  const ops = VINC_OPS[m.id] || [];
  if (!ops.length) return '';
  const nums = [];
  for (const op of ops) {
    if (op.op_tipo === 'venta_ml') nums.push(VENTA_NUM[op.op_id] || ('venta #' + op.op_id));
    else if (op.op_tipo === 'venta') nums.push('venta #' + op.op_id);
    else nums.push(op.op_tipo);
  }
  const uniq = [...new Set(nums)];
  const extra = uniq.length > 1 ? ` <span class="mov-muted">+${uniq.length - 1}</span>` : '';
  return esc(uniq[0]) + extra;
}

const LABEL_ESTADO = { auto: 'auto', pendiente: 'pendiente', parcial: 'parcial', conciliado: 'Vinculado' };

// ¿El movimiento tiene contraparte (está vinculado a una operación)?
function tieneContraparte(m) {
  return (VINC_OPS[m.id] || []).length > 0;
}

// Línea HEREDADA de la operación vinculada (opción A). Devuelve el linea_id o null.
function lineaHeredada(m) {
  for (const op of (VINC_OPS[m.id] || [])) {
    if (op.op_tipo === 'venta_ml' && VENTA_LINEA[op.op_id] != null) return VENTA_LINEA[op.op_id];
    if (op.op_tipo === 'gasto' && GASTO_LINEA[op.op_id] != null) return GASTO_LINEA[op.op_id];
    if (op.op_tipo === 'compra' && COMPRA_LINEA[op.op_id] != null) return COMPRA_LINEA[op.op_id];
  }
  return null;
}

// Etiqueta de categoría legible.
function catLabelDe(m) {
  return (CATEGORIAS.find(c => c[0] === (m.categoria || ''))?.[1]) || m.categoria || '';
}

// Columna "Vinculado a": si tiene enganche real, el N° de venta. Si no, pero
// comparte N° de operación de MP con otros (ej. impuesto por extracción ↔ su
// transferencia), muestra con qué va y deja hacer clic para verlos juntos.
function vinculadoCol(m) {
  const v = vinculadoA(m);
  if (v) return v;
  const op = nroOperacion(m);
  if (!op) return '';
  const sibs = (OP_SIBLINGS[op] || []).filter(x => x.id !== m.id);
  if (!sibs.length) return '';
  const cats = [...new Set(sibs.map(catLabelDe))].filter(Boolean);
  const etiqueta = cats.length ? cats.slice(0, 2).join(', ') : `${sibs.length} mov`;
  const extra = sibs.length > 1 ? ` <span class="mov-muted">·${sibs.length}</span>` : '';
  return `<span class="mov-op-link" data-op="${esc(op)}" title="Misma operación ${esc(op)} — clic para verlos juntos">${esc(etiqueta)}${extra}</span>`;
}

// Celda "Línea" por movimiento. Huérfano (sin contraparte) → etiqueta editable
// (clic para elegir; se aplica a toda la operación si comparte N°). Con
// contraparte → línea heredada de su operación, bloqueada.
function celdaLinea(m) {
  if (tieneContraparte(m)) {
    const id = lineaHeredada(m);
    const txt = id != null ? (LINEA_LABEL[id] || ('#' + id)) : '(heredada)';
    return `<span class="mov-linea-ro" title="La toma de la operación vinculada (no se edita acá)">${esc(txt)}</span>`;
  }
  const actual = m.linea_id;
  const nroOp = nroOperacion(m);
  const tieneLinea = actual != null && actual !== '';
  const label = tieneLinea ? (LINEA_LABEL[actual] || ('#' + actual)) : '+ asignar línea';
  return `<span class="mov-linea-edit ${tieneLinea ? 'asignada' : 'vacia'}" data-mov="${m.id}" data-op="${esc(nroOp)}" data-current="${tieneLinea ? actual : ''}" title="${tieneLinea ? 'Cambiar línea' : 'Asignar línea'}">${esc(label)}</span>`;
}

// Al hacer clic en la etiqueta, la cambia por un selector para elegir la línea.
function editarLinea(span) {
  const td = span.closest('td');
  if (!td) return;
  const mov = span.dataset.mov, op = span.dataset.op || '', cur = span.dataset.current || '';
  const opts = ['<option value="">— Sin asignar —</option>']
    .concat(LINEAS.map(l => `<option value="${l.id}" ${String(cur) === String(l.id) ? 'selected' : ''}>${esc(LINEA_LABEL[l.id])}</option>`))
    .join('');
  td.innerHTML = `<select class="select mov-linea-sel">${opts}</select>`;
  const sel = td.querySelector('select');
  let cambiado = false;
  sel.addEventListener('change', () => { cambiado = true; asignarLinea(mov, op, sel.value); });
  sel.addEventListener('blur', () => { if (!cambiado) render(); });
  sel.focus();
}

// N° de operación: SOLO para Mercado Pago (origen mp_account_statement), donde
// el 2° campo de referencia_externa es el REFERENCE_ID que comparten las líneas
// de una misma operación. En el banco (Supervielle) ese campo es la HORA y NO
// agrupa nada; en cargas manuales no hay número. Para esos devuelve ''.
function nroOperacion(m) {
  if (m.origen !== 'mp_account_statement') return '';
  const f = String(m.referencia_externa || '').split('|');
  return f.length > 1 ? f[1].trim() : '';
}

// ── Render principal ───────────────────────────────────────────────────
export async function loadMovimientos() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando movimientos…</div>`;

  try {
    CUENTAS = await sbGet('cuentas', 'order=id.asc');
    CUENTA_BY_ID = Object.fromEntries(CUENTAS.map(c => [c.id, c]));
    DATA = await sbGet('movimientos', 'order=fecha.desc,id.desc');
    // Estado de conciliación: lo trae la VISTA de la base (misma fuente que
    // Conciliación). Pedimos solo id + estado (payload chico).
    const estados = await sbGet('v_movimientos_estado', 'select=id,estado');
    ESTADO_BY_ID = Object.fromEntries(estados.map(e => [e.id, e.estado]));
    // Enganches: para mostrar CON QUÉ está vinculado cada movimiento.
    const vincs = await sbGet('vinculos', '');
    VINC_OPS = {};
    for (const v of vincs) {
      (VINC_OPS[v.movimiento_id] = VINC_OPS[v.movimiento_id] || []).push({ op_tipo: v.op_tipo, op_id: v.op_id });
    }
    // N° de venta de ML por id + línea heredada de la venta.
    const ventas = await sbGet('ventas_ml', 'select=id,ml_order_id,linea_negocio_id');
    VENTA_NUM = Object.fromEntries(ventas.map(v => [v.id, v.ml_order_id]));
    VENTA_LINEA = Object.fromEntries(ventas.map(v => [v.id, v.linea_negocio_id]));
    // Catálogo de líneas de negocio (para el dropdown y las etiquetas).
    LINEAS = await sbGet('lineas_negocio', 'order=id.asc');
    LINEA_LABEL = Object.fromEntries(LINEAS.map(l => [l.id, l.nombre || l.descripcion || l.codigo || ('Línea ' + l.id)]));
    // Líneas heredadas de gastos / compras (para mostrarlas bloqueadas).
    const gastos = await sbGet('v_gastos_ap', 'select=id,linea_id').catch(() => []);
    GASTO_LINEA = Object.fromEntries(gastos.map(g => [g.id, g.linea_id]));
    const compras = await sbGet('v_compras_ap', 'select=compra_id,linea_id,nro_factura,proveedor_id').catch(() => []);
    COMPRA_LINEA = Object.fromEntries(compras.map(c => [c.compra_id, c.linea_id]));
    // Etiquetas de TODAS las compras (no solo las que tienen saldo): una factura
    // ya cancelada por cheque sale del AP pendiente pero el chip la sigue nombrando.
    COMPRA_INFO = Object.fromEntries(compras.map(c => [c.compra_id, c]));
    // Cheques: pestaña propia. Tolerante a que las tablas todavía no existan.
    CHEQUES = await sbGet('v_cheques', 'order=fecha_pago.asc,id.asc').catch(() => []);
    const imps = await sbGet('cheque_imputaciones', 'order=id.asc').catch(() => []);
    CHQ_IMPUT = {};
    for (const i of imps) (CHQ_IMPUT[i.cheque_id] = CHQ_IMPUT[i.cheque_id] || []).push(i);
    AP_COMPRAS = await sbGet('v_compras_ap', 'saldo_ap_ars=gt.0.02&order=fecha.desc,compra_id.desc').catch(() => []);
    PROVEEDORES = await sbGet('proveedores', 'order=nombre.asc').catch(() => []);
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudieron cargar los movimientos: ${e.message}</div>`;
    return;
  }

  MESES = [...new Set(DATA.map(m => mesDe(m.fecha)).filter(Boolean))].sort().reverse();
  if (!MES || !MESES.includes(MES)) MES = MESES.length ? MESES[0] : '';

  inyectarEstilo();
  render();
}

function tabsHTML() {
  const t = (id, label, n) => `<button class="mov-tab ${TAB === id ? 'is-on' : ''}" data-tab="${id}">${label}${n != null ? ` <span class="mov-tab-n">${n}</span>` : ''}</button>`;
  const aVencer = CHEQUES.filter(c => c.estado === 'emitido').length;
  return `<div class="mov-tabs">${t('extracto', 'Extracto')}${t('cheques', 'Cheques', aVencer)}</div>`;
}

function bindTabs() {
  document.querySelectorAll('.mov-tab').forEach(b => {
    b.addEventListener('click', () => { TAB = b.dataset.tab; render(); });
  });
}

function render() {
  if (TAB === 'cheques') return renderCheques();
  const root = document.getElementById('app-screens');

  // Base: respeta MES (extracto) + cuenta + búsqueda (NO el estado, que es la pill)
  const base = DATA.filter(m => {
    if (MES && mesDe(m.fecha) !== MES) return false;
    if (FILTRO.cuenta && String(m.cuenta_id) !== FILTRO.cuenta) return false;
    if (FILTRO.q) {
      const txt = `${m.descripcion || ''} ${m.categoria || ''} ${nroOperacion(m)}`.toLowerCase();
      if (!txt.includes(FILTRO.q.toLowerCase())) return false;
    }
    return true;
  });

  // Lista: base + filtro de estado (la pill activa)
  const filtrados = base.filter(m => !FILTRO.estado || estadoDe(m) === FILTRO.estado);

  // Hermanos por operación MP (mismo N° de operación dentro del mes), para
  // mostrar con quién va cada movimiento (ej. impuesto por extracción ↔ su
  // transferencia). Se calcula sobre el mes, no sobre la búsqueda.
  OP_SIBLINGS = {};
  for (const m of DATA) {
    if (MES && mesDe(m.fecha) !== MES) continue;
    const op = nroOperacion(m);
    if (op) (OP_SIBLINGS[op] = OP_SIBLINGS[op] || []).push(m);
  }

  // KPIs (sobre la base filtrada por mes + cuenta + búsqueda)
  const total = base.length;
  const pendientes = base.filter(m => estadoDe(m) === 'pendiente').length;
  const vinculados = base.filter(m => estadoDe(m) === 'conciliado').length;
  const netoArs = base
    .filter(m => monedaDe(m.cuenta_id) === 'ARS')
    .reduce((s, m) => s + (Number(m.monto) || 0), 0);

  // Pills dinámicas por estado presente (sobre la base)
  const estadosPresentes = [...new Set(base.map(estadoDe))];
  const pills = [`<button class="pill ${FILTRO.estado === '' ? 'active' : ''}" data-estado="">Todos <span class="num">${total}</span></button>`]
    .concat(estadosPresentes.map(es => {
      const n = base.filter(m => estadoDe(m) === es).length;
      return `<button class="pill ${FILTRO.estado === es ? 'active' : ''}" data-estado="${es}">${LABEL_ESTADO[es] || es} <span class="num">${n}</span></button>`;
    }))
    .join('');

  const opcionesCuenta = ['<option value="">Todas las cuentas</option>']
    .concat(CUENTAS.map(c => `<option value="${c.id}" ${FILTRO.cuenta === String(c.id) ? 'selected' : ''}>${cuentaLabel(c.id)}</option>`))
    .join('');

  const opcionesMes = MESES.length
    ? MESES.map(ym => `<option value="${ym}" ${MES === ym ? 'selected' : ''}>${esc(mesLargo(ym))}</option>`).join('')
    : '<option value="">—</option>';

  root.innerHTML = `
    ${tabsHTML()}
    <div class="toolbar">
      <select class="select" id="f-mes" style="width:auto;text-transform:capitalize">${opcionesMes}</select>
      <select class="select" id="f-cuenta" style="width:auto">${opcionesCuenta}</select>
      <input class="input grow" id="f-q" type="text" placeholder="Buscar descripción…" value="${FILTRO.q.replace(/"/g, '&quot;')}">
      <button class="btn btn-ghost" id="btn-importar">Importar</button>
      <button class="btn btn-ghost" id="btn-venta-efvo">+ Venta en efectivo</button>
      <button class="btn btn-primary" id="btn-nuevo">+ Movimiento de caja</button>
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">Movimientos del mes</div><div class="kpi-value">${total}</div></div>
      <div class="kpi"><div class="kpi-label">Vinculados</div><div class="kpi-value">${vinculados}</div></div>
      <div class="kpi"><div class="kpi-label">Pendientes</div><div class="kpi-value">${pendientes}</div></div>
      <div class="kpi"><div class="kpi-label">Neto ARS</div><div class="kpi-value">${fmtMonto(netoArs, 'ARS')}</div></div>
    </div>

    <div class="pills">${pills}</div>

    ${filtrados.length === 0
      ? `<div class="empty">No hay movimientos para este mes/filtro.</div>`
      : `<div class="table-wrap"><table class="t">
          <thead><tr>
            <th style="width:74px">Fecha</th>
            <th style="width:110px">Cuenta</th>
            <th>Descripción</th>
            <th style="width:128px">N° operación</th>
            <th style="width:130px">Categoría</th>
            <th style="width:150px">Línea</th>
            <th style="width:140px;text-align:right">Monto</th>
            <th style="width:150px">Vinculado a</th>
            <th style="width:104px">Estado</th>
            <th style="width:44px"></th>
          </tr></thead>
          <tbody>${filtrados.map(filaHTML).join('')}</tbody>
        </table></div>`}
  `;

  // Bindings
  bindTabs();
  document.getElementById('f-mes').addEventListener('change', e => { MES = e.target.value; FILTRO.estado = ''; render(); });
  document.getElementById('f-cuenta').addEventListener('change', e => { FILTRO.cuenta = e.target.value; render(); });
  document.getElementById('f-q').addEventListener('input', e => { FILTRO.q = e.target.value; render(); });
  document.getElementById('btn-nuevo').addEventListener('click', openModalNuevo);
  document.getElementById('btn-importar').addEventListener('click', openModalImportar);
  document.getElementById('btn-venta-efvo').addEventListener('click', openModalVentaEfectivo);
  document.querySelectorAll('.pill').forEach(p => {
    p.addEventListener('click', () => { FILTRO.estado = p.dataset.estado; render(); });
  });
  document.querySelectorAll('.mov-del').forEach(b => {
    b.addEventListener('click', () => borrarMovimiento(b.dataset.id));
  });
  document.querySelectorAll('.mov-op-num, .mov-op-link').forEach(b => {
    b.addEventListener('click', () => {
      FILTRO.q = b.dataset.op;
      const inp = document.getElementById('f-q');
      if (inp) inp.value = b.dataset.op;
      render();
    });
  });
  document.querySelectorAll('.mov-linea-edit').forEach(s => {
    s.addEventListener('click', () => editarLinea(s));
  });
}

// Una fila por movimiento, con su monto real. El N° de operación es clicable
// cuando comparte operación con otros (para verlos juntos), y "Vinculado a"
// muestra con qué va.
function filaHTML(m) {
  const moneda = monedaDe(m.cuenta_id);
  const neg = Number(m.monto) < 0;
  const est = estadoDe(m);
  const catLabel = catLabelDe(m);
  const catSinClasif = !m.categoria || m.categoria === 'sin_clasificar';
  const nroOp = nroOperacion(m);
  const nSibs = nroOp ? (OP_SIBLINGS[nroOp] || []).filter(x => x.id !== m.id).length : 0;
  const opCell = nroOp
    ? `<span class="mov-op-num" data-op="${esc(nroOp)}" title="${nSibs ? `Operación con ${nSibs + 1} movimientos — clic para verlos juntos` : 'N° de operación de Mercado Pago'}">${esc(nroOp)}</span>${nSibs ? ` <span class="mov-op-n">·${nSibs + 1}</span>` : ''}`
    : '<span class="mov-muted">—</span>';
  const vinc = vinculadoCol(m);
  return `<tr>
    <td>${(m.fecha || '').slice(8, 10)}/${(m.fecha || '').slice(5, 7)}</td>
    <td>${cuentaLabel(m.cuenta_id)}</td>
    <td>${m.descripcion ? esc(m.descripcion) : '<span class="mov-muted">—</span>'}</td>
    <td class="mov-mono mov-op">${opCell}</td>
    <td>${catSinClasif ? '<span class="mov-tag-empty">sin clasificar</span>' : `<span class="mov-tag">${esc(catLabel)}</span>`}</td>
    <td>${celdaLinea(m)}</td>
    <td style="text-align:right" class="mov-mono ${neg ? 'mov-neg' : 'mov-pos'}">${fmtMonto(m.monto, moneda)}</td>
    <td class="mov-mono mov-vinc">${vinc || '<span class="mov-muted">—</span>'}</td>
    <td><span class="mov-badge mov-badge-${est}">${LABEL_ESTADO[est] || est}</span></td>
    <td style="text-align:center">${m.origen === 'manual' ? `<button class="mov-del" data-id="${m.id}" title="Borrar movimiento">✕</button>` : ''}</td>
  </tr>`;
}

// ── Modal de carga manual ──────────────────────────────────────────────
function openModalNuevo() {
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';

  const cajas = CUENTAS.filter(c => c.tipo === 'caja');
  const opcionesCuenta = '<option value="">Elegí caja…</option>' + cajas.map(c => `<option value="${c.id}">${cuentaLabel(c.id)}</option>`).join('');
  const opcionesCat = '<option value="">Elegí categoría…</option>' + CATEGORIAS.map(([v, l]) => `<option value="${v}">${l}</option>`).join('');

  overlay.innerHTML = `
    <div class="modal">
      <div class="card-title">Movimiento de caja</div>

      <div class="field"><label>Cuenta</label>
        <select class="select" id="m-cuenta">${opcionesCuenta}</select>
      </div>

      <div class="field"><label>Fecha</label>
        <input class="input" id="m-fecha" type="date" value="${hoyISO()}">
      </div>

      <div class="field"><label>Tipo</label>
        <div class="mov-toggle">
          <button type="button" class="mov-tg active" id="tg-in" data-signo="1">Entrada</button>
          <button type="button" class="mov-tg" id="tg-out" data-signo="-1">Salida</button>
        </div>
      </div>

      <div class="field"><label>Monto <span class="mov-cur" id="m-cur">$</span></label>
        <input class="input" id="m-monto" type="number" min="0" step="0.01" placeholder="0,00">
      </div>

      <div class="field"><label>Categoría</label>
        <select class="select" id="m-cat">${opcionesCat}</select>
      </div>

      <div class="field"><label>Descripción</label>
        <input class="input" id="m-desc" type="text" placeholder="Ej: compra galletitas">
      </div>

      <div id="m-aviso"></div>
      <div id="m-yahoy"></div>

      <div class="modal-actions">
        <button class="btn btn-ghost" id="m-cancel">Cancelar</button>
        <button class="btn btn-primary" id="m-guardar">Guardar</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);

  let signo = 1; // 1 entrada, -1 salida

  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#m-cancel').addEventListener('click', close);

  const tgIn = overlay.querySelector('#tg-in');
  const tgOut = overlay.querySelector('#tg-out');
  const setSigno = (s) => {
    signo = s;
    tgIn.classList.toggle('active', s === 1);
    tgOut.classList.toggle('active', s === -1);
  };
  tgIn.addEventListener('click', () => setSigno(1));
  tgOut.addEventListener('click', () => setSigno(-1));

  // Símbolo de moneda según cuenta + refresco de "ya cargados hoy"
  const cuentaSel = overlay.querySelector('#m-cuenta');
  const fechaSel = overlay.querySelector('#m-fecha');
  const refrescarMoneda = () => {
    overlay.querySelector('#m-cur').textContent = monedaDe(+cuentaSel.value) === 'USD' ? 'US$' : '$';
  };
  const refrescarYaHoy = async () => {
    const cid = +cuentaSel.value;
    const fecha = fechaSel.value;
    const cont = overlay.querySelector('#m-yahoy');
    if (!cid || !fecha) { cont.innerHTML = ''; return; }
    try {
      const ya = await sbGet('movimientos', `cuenta_id=eq.${cid}&fecha=eq.${fecha}&order=creado_en.desc`);
      if (!ya.length) { cont.innerHTML = ''; return; }
      const mon = monedaDe(cid);
      cont.innerHTML = `<div class="mov-yahoy">
        <div class="mov-yahoy-t">Ya cargados ese día en ${cuentaLabel(cid)}</div>
        ${ya.slice(0, 6).map(m => `<div class="mov-yahoy-r">
          <span>${m.descripcion ? esc(m.descripcion) : '—'}</span>
          <span class="mov-mono ${Number(m.monto) < 0 ? 'mov-neg' : 'mov-pos'}">${fmtMonto(m.monto, mon)}</span>
        </div>`).join('')}
      </div>`;
    } catch { cont.innerHTML = ''; }
  };
  cuentaSel.addEventListener('change', () => { refrescarMoneda(); refrescarYaHoy(); });
  fechaSel.addEventListener('change', refrescarYaHoy);
  refrescarMoneda();
  refrescarYaHoy();

  // Guardar (con chequeo de posible duplicado)
  overlay.querySelector('#m-guardar').addEventListener('click', async () => {
    const cuenta_id = +cuentaSel.value;
    const fecha = fechaSel.value;
    const montoAbs = parseFloat(overlay.querySelector('#m-monto').value);
    const categoria = overlay.querySelector('#m-cat').value;
    const descripcion = overlay.querySelector('#m-desc').value.trim();

    if (!cuenta_id) { window.toast('Elegí una cuenta', 'error'); return; }
    if (!fecha) { window.toast('Falta la fecha', 'error'); return; }
    if (!(montoAbs > 0)) { window.toast('El monto tiene que ser mayor a 0', 'error'); return; }
    if (!categoria) { window.toast('Elegí una categoría', 'error'); return; }
    if (!descripcion) { window.toast('Poné una descripción', 'error'); return; }

    const monto = +(signo * montoAbs).toFixed(2);

    // Aviso de posible duplicado: misma cuenta + fecha + monto
    try {
      const iguales = await sbGet('movimientos', `cuenta_id=eq.${cuenta_id}&fecha=eq.${fecha}&monto=eq.${monto}`);
      if (iguales.length) {
        const ok = confirm(
          `Ya cargaste un movimiento igual el ${fecha} en ${cuentaLabel(cuenta_id)} ` +
          `(${fmtMonto(monto, monedaDe(cuenta_id))}).\n\n¿Es otro distinto y querés cargarlo igual?`
        );
        if (!ok) return;
      }
    } catch { /* si falla el chequeo, seguimos: el insert no rompe nada */ }

    try {
      const [nuevo] = await sbPost('movimientos', {
        cuenta_id, fecha, monto, categoria, descripcion,
        origen: 'manual',
        referencia_externa: nuevaRef(),  // NOT NULL + UNIQUE: referencia única por carga manual
        conciliado_auto: false,
      });
      DATA.unshift(nuevo);
      MESES = [...new Set(DATA.map(m => mesDe(m.fecha)).filter(Boolean))].sort().reverse();
      if (!MESES.includes(MES)) MES = MESES[0] || '';
      window.toast('Movimiento cargado');
      close();
      render();
    } catch (e) {
      window.toast('Error al guardar: ' + e.message, 'error');
    }
  });
}

// ── Venta en efectivo sin comprobante ──────────────────────────────────
// Da de alta una VENTA (no un movimiento suelto): descuenta stock por FIFO, suma a
// Caja ARS y entra al Resultado. NO genera IVA débito porque no se factura
// (ventas.tipo_comprobante='sin_comprobante' → es_gravada=false).
// Todo el trabajo lo hace POST /ventas/efectivo, que es atómico. Ver ADARA-VENTAS-EFECTIVO.md.
async function openModalVentaEfectivo() {
  // Los SKUs se traen CADA VEZ que se abre el modal, a propósito. Cachearlos en una
  // variable de módulo hacía que un SKU dado de alta después de entrar a Movimientos
  // no apareciera nunca: la app es SPA, no recarga la página y el caché no expiraba.
  let skus;
  try {
    skus = await sbGet('skus', 'activo=eq.true&select=id,codigo,descripcion&order=codigo.asc');
  } catch (e) { window.toast('No se pudieron cargar los SKUs: ' + e.message, 'error'); return; }
  if (!skus.length) { window.toast('No hay SKUs activos para vender', 'error'); return; }

  const optLinea = '<option value="">Elegí línea…</option>' + LINEAS.map(l => `<option value="${l.id}">${LINEA_LABEL[l.id]}</option>`).join('');

  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal" style="max-width:720px">
      <div class="card-title">Venta en efectivo (sin comprobante)</div>

      <div class="vef-nota">
        Descuenta stock por FIFO, suma a <strong>Caja ARS</strong> y entra al Resultado.
        <strong>No genera IVA débito</strong> porque no se factura — el precio que cargues
        es lo que cobrás, sin desglosar IVA.
      </div>

      <div class="vef-row2">
        <div class="field"><label>Fecha</label>
          <input class="input" id="vef-fecha" type="date" value="${hoyISO()}">
        </div>
        <div class="field"><label>Línea de negocio</label>
          <select class="select" id="vef-linea">${optLinea}</select>
        </div>
      </div>

      <div class="field"><label>Ítems</label>
        <table class="t vef-items"><thead><tr>
          <th>SKU</th>
          <th style="width:90px">Cant.</th>
          <th style="width:140px">Precio unit.</th>
          <th style="width:120px;text-align:right">Subtotal</th>
          <th style="width:36px"></th>
        </tr></thead><tbody id="vef-tbody"></tbody></table>
        <button type="button" class="btn btn-ghost" id="vef-add" style="margin-top:8px">+ Agregar ítem</button>
      </div>

      <div class="vef-total">Total a cobrar: <strong id="vef-total">$ 0,00</strong></div>

      <div class="field"><label>Cliente / referencia <span style="opacity:.6">(opcional)</span></label>
        <input class="input" id="vef-cliente" type="text" placeholder="Ej: Juan, mostrador">
      </div>

      <div class="modal-actions">
        <button class="btn btn-ghost" id="vef-cancel">Cancelar</button>
        <button class="btn btn-primary" id="vef-guardar">Registrar venta</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  inyectarEstiloVef();

  const tbody = overlay.querySelector('#vef-tbody');
  const fmt = n => '$ ' + Number(n || 0).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  const recalc = () => {
    let total = 0;
    tbody.querySelectorAll('tr').forEach(tr => {
      const c = parseFloat(tr.querySelector('.vef-cant').value) || 0;
      const p = parseFloat(tr.querySelector('.vef-precio').value) || 0;
      const sub = c * p;
      total += sub;
      tr.querySelector('.vef-sub').textContent = fmt(sub);
    });
    overlay.querySelector('#vef-total').textContent = fmt(total);
  };

  const pickers = [];
  const addFila = () => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><div class="vef-skuhost"></div></td>
      <td><input class="input vef-cant" type="number" min="1" step="1" value="1"></td>
      <td><input class="input vef-precio" type="number" min="0" step="0.01" placeholder="0,00"></td>
      <td style="text-align:right" class="vef-sub mov-mono">$ 0,00</td>
      <td><button type="button" class="btn btn-ghost vef-del" title="Quitar">×</button></td>
    `;
    tbody.appendChild(tr);
    // El picker deja un <input type="hidden" class="vef-sku">: el resto del código
    // lo lee igual que cuando era un <select>.
    const p = crearSkuPicker(tr.querySelector('.vef-skuhost'), { skus, hiddenClass: 'vef-sku' });
    pickers.push(p);
    tr.querySelector('.vef-cant').addEventListener('input', recalc);
    tr.querySelector('.vef-precio').addEventListener('input', recalc);
    tr.querySelector('.vef-del').addEventListener('click', () => {
      if (tbody.querySelectorAll('tr').length === 1) { window.toast('La venta necesita al menos un ítem', 'error'); return; }
      const i = pickers.indexOf(p);
      if (i >= 0) { pickers[i].destroy(); pickers.splice(i, 1); }
      tr.remove(); recalc();
    });
    recalc();
    return p;
  };
  addFila();

  const close = () => { pickers.forEach(p => p.destroy()); overlay.remove(); };
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#vef-cancel').addEventListener('click', close);
  overlay.querySelector('#vef-add').addEventListener('click', () => addFila().focus());

  overlay.querySelector('#vef-guardar').addEventListener('click', async () => {
    const fecha = overlay.querySelector('#vef-fecha').value;
    const linea_id = overlay.querySelector('#vef-linea').value;
    const cliente_nombre = overlay.querySelector('#vef-cliente').value.trim();

    if (!fecha) { window.toast('Falta la fecha', 'error'); return; }
    if (!linea_id) { window.toast('Elegí la línea de negocio', 'error'); return; }

    const items = [];
    for (const tr of tbody.querySelectorAll('tr')) {
      const sku_id = tr.querySelector('.vef-sku').value;
      const cantidad = parseFloat(tr.querySelector('.vef-cant').value);
      const precio_unitario = parseFloat(tr.querySelector('.vef-precio').value);
      if (!sku_id) { window.toast('Hay un ítem sin SKU', 'error'); return; }
      if (!(cantidad > 0)) { window.toast('Cantidad inválida', 'error'); return; }
      if (!(precio_unitario >= 0)) { window.toast('Precio inválido', 'error'); return; }
      items.push({ sku_id: +sku_id, cantidad, precio_unitario });
    }
    if (!items.length) { window.toast('La venta no tiene ítems', 'error'); return; }

    const btn = overlay.querySelector('#vef-guardar');
    btn.disabled = true;

    const enviar = async (confirmar) => {
      const r = await fetch('/ventas/efectivo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ fecha, linea_id: +linea_id, items, cliente_nombre: cliente_nombre || null, confirmar_mes_anterior: !!confirmar })
      });
      return { status: r.status, data: await r.json().catch(() => ({})) };
    };

    try {
      let { status, data } = await enviar(false);

      // Guardarriel de fecha: el backend frena si cae en un mes anterior.
      if (status === 409 && data.error === 'fecha_mes_anterior') {
        if (!confirm(data.mensaje)) { btn.disabled = false; return; }
        ({ status, data } = await enviar(true));
      }
      if (status !== 200 || !data.ok) throw new Error(data.mensaje || data.error || 'Error desconocido');

      window.toast(`Venta ${data.referencia} registrada · ${fmt(data.total)}`);
      if (data.aviso_stock) window.toast(data.aviso_stock, 'error');
      if (data.fifo_error) window.toast('La venta quedó cargada pero el costeo FIFO falló: ' + data.fifo_error, 'error');
      close();
      await loadMovimientos();   // recarga movimientos + vínculos + estados
    } catch (e) {
      window.toast('Error: ' + e.message, 'error');
      btn.disabled = false;
    }
  });
}

function inyectarEstiloVef() {
  if (document.getElementById('vef-style')) return;
  const s = document.createElement('style');
  s.id = 'vef-style';
  s.textContent = `
    .vef-nota{background:#F0F9FF;border-left:3px solid #0284C7;padding:10px 12px;border-radius:6px;font-size:12.5px;line-height:1.6;margin-bottom:14px}
    .vef-row2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .vef-items th{font-size:11px}
    .vef-items td{padding:4px}
    .vef-items .input,.vef-items .select{margin:0}
    .vef-total{text-align:right;font-size:14px;margin:10px 0 4px}
    .vef-total strong{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
  `;
  document.head.appendChild(s);
}

// ── Borrar movimiento manual ───────────────────────────────────────────
async function borrarMovimiento(id) {
  if (!confirm('¿Borrar este movimiento cargado a mano? No se puede deshacer.')) return;
  try {
    const r = await fetch('/movimientos/' + id, { method: 'DELETE' });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    DATA = DATA.filter(m => String(m.id) !== String(id));
    render();
    window.toast('Movimiento borrado');
  } catch (e) {
    window.toast('Error al borrar: ' + e.message, 'error');
  }
}

// ── Asignar línea de negocio a un movimiento huérfano (y a toda su operación) ──
// Se aplica a TODAS las líneas sin contraparte que comparten el número de
// operación (ej: la transferencia y su impuesto a la vez). Las que ya tienen
// contraparte no se tocan (heredan su línea de la operación).
async function asignarLinea(movId, opNum, valor) {
  const linea_id = valor ? Number(valor) : null;
  // ¿A qué movimientos? Los huérfanos de la misma operación; si no hay número, solo este.
  let objetivos;
  if (opNum) {
    objetivos = DATA.filter(m => nroOperacion(m) === opNum && !tieneContraparte(m));
  } else {
    objetivos = DATA.filter(m => String(m.id) === String(movId));
  }
  const ids = objetivos.map(m => m.id);
  if (!ids.length) return;
  try {
    await sbPatch('movimientos', `id=in.(${ids.join(',')})`, { linea_id });
    for (const m of objetivos) m.linea_id = linea_id;  // reflejar local
    const nombre = linea_id ? (LINEA_LABEL[linea_id] || ('#' + linea_id)) : 'Sin asignar';
    window.toast(ids.length > 1 ? `Línea asignada a la operación (${ids.length} líneas): ${nombre}` : `Línea asignada: ${nombre}`);
    render();
  } catch (e) {
    if (/linea_id/.test(e.message) || /column/.test(e.message) || /42703/.test(e.message)) {
      window.toast('Falta correr la migración (agregar columna linea_id) en Supabase', 'error');
    } else {
      window.toast('Error al asignar línea: ' + e.message, 'error');
    }
  }
}


function openModalImportar() {
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal">
      <div class="card-title">Importar extracto</div>
      <p class="imp-sub">Elegí la fuente y subí el archivo descargado del homebanking.</p>

      <div class="imp-src">
        <div class="imp-src-h">Supervielle <span class="imp-tag">banco</span></div>
        <div class="imp-src-fmt">Export de <b>Movimientos</b> (.xlsx o .csv). Columnas: Fecha · Hora · Concepto · Detalle · Débito · Crédito · Saldo.</div>
        <button class="btn btn-ghost imp-pick" data-src="supervielle">Elegir archivo…</button>
      </div>

      <div class="imp-src">
        <div class="imp-src-h">Mercado Pago <span class="imp-tag">mp</span></div>
        <div class="imp-src-fmt">Resumen de cuenta (.xlsx). Columnas: RELEASE_DATE · TRANSACTION_TYPE · REFERENCE_ID · TRANSACTION_NET_AMOUNT · PARTIAL_BALANCE.</div>
        <div class="imp-note">Las liquidaciones de ventas entran <b>sin conciliar</b> hasta el sync de ML.</div>
        <button class="btn btn-ghost imp-pick" data-src="mp">Elegir archivo…</button>
      </div>

      <input type="file" id="imp-file" accept=".xlsx,.xls,.csv" style="display:none">

      <div class="modal-actions">
        <button class="btn btn-ghost" id="imp-cancel">Cerrar</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);

  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#imp-cancel').addEventListener('click', close);

  const fileInput = overlay.querySelector('#imp-file');
  let src = 'supervielle';
  overlay.querySelectorAll('.imp-pick').forEach(b => {
    b.addEventListener('click', () => { src = b.dataset.src; fileInput.click(); });
  });
  fileInput.addEventListener('change', e => {
    const f = e.target.files[0];
    e.target.value = '';
    if (!f) return;
    close();
    importarExtracto(f, src);
  });
}

async function importarExtracto(file, source) {
  const cfg = source === 'mp'
    ? { url: '/mp/import', label: 'Mercado Pago' }
    : { url: '/supervielle/import', label: 'Supervielle' };
  window.toast(`Importando ${cfg.label}…`);
  try {
    const fd = new FormData();
    fd.append('file', file);
    const r = await fetch(cfg.url, { method: 'POST', body: fd });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));

    if (data.saldo_check && !data.saldo_check.ok) {
      alert(
        `⚠ El saldo no encadena (${data.saldo_check.total_breaks} salto/s) en ${cfg.label}. ` +
        `Puede que falten movimientos en el archivo.\n\n` +
        `Se importaron igual ${data.importados}, pero conviene revisar/rebajar el extracto completo.`
      );
    }
    window.toast(`${cfg.label}: importados ${data.importados} · pendientes ${data.pendientes} · auto ${data.auto}`);

    await loadMovimientos();
  } catch (e) {
    window.toast('Error al importar: ' + e.message, 'error');
  }
}

// ── Pestaña Cheques ────────────────────────────────────────────────────
// Un cheque entregado cancela la deuda con el proveedor aunque la plata todavía
// no haya salido. Por eso `v_compras_ap.en_cheque_ars` descuenta del saldo AP y
// el débito real aparece recién en el extracto, vinculado con op_tipo='cheque'.

const ESTADO_CHQ = {
  emitido:   { label: 'A vencer',  clase: 'chq-emitido' },
  debitado:  { label: 'Debitado',  clase: 'chq-debitado' },
  rechazado: { label: 'Rechazado', clase: 'chq-rechazado' },
  anulado:   { label: 'Anulado',   clase: 'chq-anulado' }
};

function fmtFecha(f) {
  if (!f) return '—';
  const [y, m, d] = String(f).slice(0, 10).split('-');
  return `${d}/${m}/${y}`;
}

function diasPara(f) {
  const hoy = new Date(hoyISO() + 'T00:00:00');
  const v = new Date(String(f).slice(0, 10) + 'T00:00:00');
  return Math.round((v - hoy) / 86400000);
}

function compraLabel(c) {
  const prov = PROVEEDORES.find(p => p.id === c.proveedor_id);
  return `${prov ? prov.nombre : 'Sin proveedor'} · ${c.nro_factura || ('Compra #' + c.compra_id)}`;
}

function renderCheques() {
  const root = document.getElementById('app-screens');
  const emitidos = CHEQUES.filter(c => c.tipo === 'emitido');

  const aVencer   = emitidos.filter(c => c.estado === 'emitido');
  const totalVenc = aVencer.reduce((a, c) => a + Number(c.monto || 0), 0);
  const sinImput  = emitidos.reduce((a, c) => a + Number(c.sin_imputar || 0), 0);
  const vencidos  = aVencer.filter(c => diasPara(c.fecha_pago) < 0);

  // Próximos vencimientos agrupados por fecha: la base del flujo de fondos.
  const porFecha = {};
  for (const c of aVencer) porFecha[c.fecha_pago] = (porFecha[c.fecha_pago] || 0) + Number(c.monto || 0);
  const fechas = Object.keys(porFecha).sort();

  const filas = emitidos.map(c => {
    const est = ESTADO_CHQ[c.estado] || { label: c.estado, clase: '' };
    const dias = diasPara(c.fecha_pago);
    const alerta = c.estado === 'emitido' && dias < 0;
    const imps = CHQ_IMPUT[c.id] || [];
    const detalle = imps.length
      ? imps.map(i => {
          const co = COMPRA_INFO[i.op_id];
          const txt = i.op_tipo === 'compra'
            ? (co && co.nro_factura ? co.nro_factura : 'Compra #' + i.op_id)
            : 'Gasto #' + i.op_id;
          return `<span class="chq-chip">${esc(txt)}</span>`;
        }).join(' ')
      : `<span class="chq-chip chq-chip-warn">sin imputar</span>`;
    return `<tr${alerta ? ' class="chq-alerta"' : ''}>
      <td>${esc(c.numero)}</td>
      <td>${esc(c.contraparte || '—')}</td>
      <td>${fmtFecha(c.fecha_emision)}</td>
      <td>${fmtFecha(c.fecha_pago)}${c.estado === 'emitido' ? `<div class="chq-dias">${dias < 0 ? `venció hace ${-dias} d` : `en ${dias} d`}</div>` : ''}</td>
      <td style="text-align:right">${fmtMonto(Number(c.monto), c.moneda)}</td>
      <td>${detalle}</td>
      <td><span class="chq-badge ${est.clase}">${est.label}</span></td>
      <td><button class="btn-icon chq-del" data-id="${c.id}" title="Eliminar">✕</button></td>
    </tr>`;
  }).join('');

  root.innerHTML = `
    ${tabsHTML()}
    <div class="toolbar">
      <div class="grow"></div>
      <button class="btn btn-primary" id="btn-nuevo-cheque">+ Cheque emitido</button>
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">A vencer</div><div class="kpi-value">${aVencer.length}</div></div>
      <div class="kpi"><div class="kpi-label">Total a pagar</div><div class="kpi-value">${fmtMonto(totalVenc, 'ARS')}</div></div>
      <div class="kpi"><div class="kpi-label">Sin imputar</div><div class="kpi-value">${fmtMonto(sinImput, 'ARS')}</div></div>
      <div class="kpi"><div class="kpi-label">Vencidos sin debitar</div><div class="kpi-value">${vencidos.length}</div></div>
    </div>

    ${fechas.length ? `<div class="chq-flujo">
      <div class="chq-flujo-t">Próximos vencimientos</div>
      ${fechas.map(f => `<div class="chq-flujo-r"><span>${fmtFecha(f)}</span><b>${fmtMonto(porFecha[f], 'ARS')}</b></div>`).join('')}
    </div>` : ''}

    ${emitidos.length === 0
      ? `<div class="empty">Todavía no cargaste cheques. Se cargan los que tengan fecha de pago desde el 1/7/2026.</div>`
      : `<div class="table-wrap"><table class="t">
          <thead><tr>
            <th style="width:90px">N°</th>
            <th>Beneficiario</th>
            <th style="width:96px">Emisión</th>
            <th style="width:120px">Pago</th>
            <th style="width:150px;text-align:right">Monto</th>
            <th style="width:220px">Imputado a</th>
            <th style="width:104px">Estado</th>
            <th style="width:44px"></th>
          </tr></thead>
          <tbody>${filas}</tbody>
        </table></div>`}
  `;

  bindTabs();
  document.getElementById('btn-nuevo-cheque').addEventListener('click', openModalCheque);
  document.querySelectorAll('.chq-del').forEach(b => b.addEventListener('click', () => borrarCheque(b.dataset.id)));
}

function openModalCheque() {
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  const optsProv = '<option value="">Elegí el beneficiario…</option>'
    + PROVEEDORES.map(p => `<option value="${p.id}">${esc(p.nombre)}</option>`).join('');
  const cuentaSup = CUENTAS.find(c => c.codigo === 'supervielle_ars');

  overlay.innerHTML = `
    <div class="modal">
      <div class="card-title">Nuevo cheque emitido</div>

      <div class="chq-2col">
        <div class="field"><label>N° de echeq</label>
          <input class="input" id="chq-num" type="text" placeholder="00000037">
        </div>
        <div class="field"><label>Importe</label>
          <input class="input" id="chq-monto" type="number" step="0.01" placeholder="0.00">
        </div>
      </div>

      <div class="chq-2col">
        <div class="field"><label>Fecha de emisión</label>
          <input class="input" id="chq-fe" type="date" value="${hoyISO()}">
        </div>
        <div class="field"><label>Fecha de pago</label>
          <input class="input" id="chq-fp" type="date">
        </div>
      </div>

      <div class="field"><label>Beneficiario</label>
        <select class="select" id="chq-prov">${optsProv}</select>
      </div>

      <div class="chq-2col">
        <div class="field"><label>CMC7 <span class="chq-hint">opcional</span></label>
          <input class="input" id="chq-cmc7" type="text">
        </div>
        <div class="field"><label>ID Coelsa <span class="chq-hint">opcional</span></label>
          <input class="input" id="chq-coelsa" type="text">
        </div>
      </div>

      <div class="field"><label>Motivo <span class="chq-hint">como figura en el echeq</span></label>
        <input class="input" id="chq-motivo" type="text" placeholder="FA NRO 000500268567">
      </div>

      <div class="chq-imp-box">
        <div class="chq-imp-t">Imputar a facturas pendientes</div>
        <div id="chq-imp-list"><div class="chq-hint">Elegí primero el beneficiario.</div></div>
        <div class="chq-imp-tot" id="chq-imp-tot"></div>
      </div>

      <div class="modal-actions">
        <button class="btn btn-ghost" id="chq-cancel">Cancelar</button>
        <button class="btn btn-primary" id="chq-save">Guardar</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);

  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#chq-cancel').addEventListener('click', close);

  const $ = id => overlay.querySelector('#' + id);

  // Las facturas del proveedor, ordenadas por cercanía al monto del cheque:
  // el caso normal (el cheque paga una factura entera) queda primero.
  function pintarFacturas() {
    const provId = Number($('chq-prov').value) || 0;
    const monto = Number($('chq-monto').value) || 0;
    const lista = $('chq-imp-list');
    if (!provId) { lista.innerHTML = `<div class="chq-hint">Elegí primero el beneficiario.</div>`; totalizar(); return; }
    const facturas = AP_COMPRAS
      .filter(c => c.proveedor_id === provId)
      .sort((a, b) => Math.abs(Number(a.saldo_ap_ars) - monto) - Math.abs(Number(b.saldo_ap_ars) - monto));
    if (!facturas.length) {
      lista.innerHTML = `<div class="chq-hint">Este proveedor no tiene facturas con saldo. El cheque queda sin imputar (anticipo) y lo aplicás cuando cargues la factura.</div>`;
      totalizar(); return;
    }
    lista.innerHTML = facturas.map(c => {
      const saldo = Number(c.saldo_ap_ars);
      const exacto = monto > 0 && Math.abs(saldo - monto) <= 0.02;
      return `<label class="chq-imp-row${exacto ? ' is-sug' : ''}">
        <input type="checkbox" class="chq-imp-ck" data-id="${c.compra_id}" data-saldo="${saldo}" ${exacto ? 'checked' : ''}>
        <span class="chq-imp-lbl">${esc(compraLabel(c))}${exacto ? ' <b class="chq-sug">monto exacto</b>' : ''}</span>
        <span class="chq-imp-monto">${fmtMonto(saldo, 'ARS')}</span>
      </label>`;
    }).join('');
    lista.querySelectorAll('.chq-imp-ck').forEach(ck => ck.addEventListener('change', totalizar));
    totalizar();
  }

  function seleccion() {
    return [...overlay.querySelectorAll('.chq-imp-ck:checked')]
      .map(ck => ({ op_tipo: 'compra', op_id: Number(ck.dataset.id), monto: Number(ck.dataset.saldo) }));
  }

  // Si lo imputado supera el cheque, se recorta el último renglón: nunca se
  // imputa más de lo que el cheque cancela.
  function totalizar() {
    const monto = Number($('chq-monto').value) || 0;
    const sel = seleccion();
    const suma = sel.reduce((a, x) => a + x.monto, 0);
    const dif = Math.round((monto - suma) * 100) / 100;
    const box = $('chq-imp-tot');
    if (!sel.length) { box.innerHTML = monto > 0 ? `<span class="chq-dif-warn">Sin imputar: ${fmtMonto(monto, 'ARS')}</span>` : ''; return; }
    if (Math.abs(dif) <= 0.02) box.innerHTML = `<span class="chq-dif-ok">Cierra exacto ✓</span>`;
    else if (dif > 0) box.innerHTML = `<span class="chq-dif-warn">Queda sin imputar ${fmtMonto(dif, 'ARS')}</span>`;
    else box.innerHTML = `<span class="chq-dif-bad">Lo imputado supera el cheque en ${fmtMonto(-dif, 'ARS')} — se va a recortar</span>`;
  }

  $('chq-prov').addEventListener('change', pintarFacturas);
  $('chq-monto').addEventListener('input', pintarFacturas);

  $('chq-save').addEventListener('click', async () => {
    const numero = $('chq-num').value.trim();
    const monto = Number($('chq-monto').value) || 0;
    const fe = $('chq-fe').value, fp = $('chq-fp').value;
    if (!numero)   return window.toast('Falta el N° de echeq', 'error');
    if (!(monto > 0)) return window.toast('El importe debe ser mayor a 0', 'error');
    if (!fe || !fp) return window.toast('Faltan las fechas', 'error');
    if (fp < fe)   return window.toast('La fecha de pago no puede ser anterior a la de emisión', 'error');

    const prov = PROVEEDORES.find(p => p.id === Number($('chq-prov').value));

    // Recorte: la suma imputada nunca supera el monto del cheque.
    let restante = monto;
    const imps = [];
    for (const s of seleccion()) {
      if (restante <= 0.02) break;
      imps.push({ ...s, monto: Math.min(s.monto, restante) });
      restante = Math.round((restante - Math.min(s.monto, restante)) * 100) / 100;
    }

    try {
      const r = await fetch('/cheques', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          cheque: {
            tipo: 'emitido', numero, monto,
            fecha_emision: fe, fecha_pago: fp,
            moneda: 'ARS',
            cmc7: $('chq-cmc7').value.trim() || null,
            id_coelsa: $('chq-coelsa').value.trim() || null,
            proveedor_id: prov ? prov.id : null,
            contraparte_nombre: prov ? prov.nombre : null,
            contraparte_cuit: prov ? prov.cuit : null,
            cuenta_id: cuentaSup ? cuentaSup.id : null,
            motivo: $('chq-motivo').value.trim() || null
          },
          imputaciones: imps
        })
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
      close();
      window.toast(data.sin_imputar > 0.02
        ? `Cheque guardado · sin imputar ${fmtMonto(data.sin_imputar, 'ARS')}`
        : 'Cheque guardado');
      await loadMovimientos();
    } catch (e) {
      window.toast('Error al guardar: ' + e.message, 'error');
    }
  });
}

async function borrarCheque(id) {
  const c = CHEQUES.find(x => String(x.id) === String(id));
  if (!c) return;
  if (!confirm(`¿Eliminar el cheque N° ${c.numero} por ${fmtMonto(Number(c.monto), c.moneda)}?\n\nSe borran también sus imputaciones y la deuda con el proveedor vuelve a quedar abierta.`)) return;
  try {
    const r = await fetch('/cheques/' + id, { method: 'DELETE' });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
    window.toast('Cheque eliminado');
    await loadMovimientos();
  } catch (e) {
    window.toast('No se pudo eliminar: ' + e.message, 'error');
  }
}

// ── Utilidades ─────────────────────────────────────────────────────────
function esc(s) {
  return String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

// Estilos propios de la pantalla (los componentes generales viven en base.css).
// Colores +/- y badges: si más adelante hay tokens de success/danger, mapearlos.
function inyectarEstilo() {
  if (document.getElementById('mov-style')) return;
  const css = `
    .mov-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .mov-pos{color:#15803D}
    .mov-neg{color:#B91C1C}
    .mov-muted{color:#A8A29E}
    .mov-vinc{font-size:12px;color:#0C447C}
    .mov-op{font-size:12px;color:#57534E;white-space:nowrap}
    .mov-op-num{cursor:pointer;border-bottom:1px dotted #C4BFB8}
    .mov-op-num:hover{color:#6D28D9;border-bottom-color:#6D28D9}
    .mov-op-n{font-size:11px;color:#A8A29E}
    .mov-op-link{font-size:12px;color:#6D28D9;cursor:pointer}
    .mov-op-link:hover{text-decoration:underline}
    .mov-linea-edit{display:inline-block;font-size:12px;padding:2px 8px;border-radius:6px;cursor:pointer;white-space:nowrap}
    .mov-linea-edit.asignada{background:#F3EEFB;color:#6D28D9}
    .mov-linea-edit.asignada:hover{background:#E9DFF8}
    .mov-linea-edit.vacia{color:#C4BFB8;border:1px dashed #E0DBD2}
    .mov-linea-edit.vacia:hover{color:#6D28D9;border-color:#C7B8E0;background:#FAF7FF}
    .mov-linea-sel{font-size:12px;padding:3px 6px;max-width:160px}
    .mov-linea-ro{font-size:12px;color:#B8B2A9;font-style:italic}
    .mov-tag{font-size:12px;color:#57534E;background:#F5F5F4;padding:2px 8px;border-radius:6px}
    .mov-tag-empty{font-size:12px;color:#A8A29E;border:1px dashed #D6D3D1;padding:2px 8px;border-radius:6px}
    .mov-badge{font-size:12px;padding:2px 8px;border-radius:6px}
    .mov-badge-pendiente{background:#FAEEDA;color:#854F0B}
    .mov-badge-auto{background:#F1EFE8;color:#5F5E5A}
    .mov-badge-parcial{background:#E6F1FB;color:#0C447C}
    .mov-badge-conciliado{background:#E1F5EE;color:#0F6E56}
    .mov-toggle{display:inline-flex;border:1px solid #E7E5E4;border-radius:6px;overflow:hidden}
    .mov-tg{padding:7px 16px;background:#fff;border:0;cursor:pointer;font:inherit;color:#78716C}
    .mov-tg.active{background:#D97706;color:#fff}
    .mov-cur{color:#A8A29E;font-weight:400}
    .mov-yahoy{margin:10px 0;padding:10px 12px;background:#FAFAF9;border:1px solid #E7E5E4;border-radius:8px}
    .mov-yahoy-t{font-size:12px;color:#78716C;margin-bottom:6px}
    .mov-yahoy-r{display:flex;justify-content:space-between;gap:10px;font-size:13px;padding:2px 0}
    .mov-del{border:0;background:transparent;color:#A8A29E;cursor:pointer;font-size:15px;padding:2px 7px;border-radius:6px;line-height:1}
    .mov-del:hover{background:#FEE2E2;color:#B91C1C}
    .imp-sub{font-size:13px;color:#78716C;margin:-4px 0 14px}
    .imp-src{border:1px solid #E7E5E4;border-radius:10px;padding:12px 14px;margin-bottom:12px}
    .imp-src-h{font-weight:600;font-size:14px;margin-bottom:4px}
    .imp-tag{font-size:11px;color:#78716C;background:#F5F5F4;padding:1px 7px;border-radius:6px;font-weight:400;margin-left:4px}
    .imp-src-fmt{font-size:12px;color:#57534E;line-height:1.5;margin-bottom:8px}
    .imp-note{font-size:12px;color:#854F0B;background:#FAEEDA;border-radius:6px;padding:6px 8px;margin-bottom:8px}
    .imp-pick{font-size:13px}

    /* ── Pestañas + cheques ── */
    .mov-tabs{display:flex;gap:4px;border-bottom:1px solid #E7E5E4;margin-bottom:14px}
    .mov-tab{background:transparent;border:0;border-bottom:2px solid transparent;padding:9px 16px;cursor:pointer;font:inherit;font-size:14px;color:#78716C;margin-bottom:-1px}
    .mov-tab:hover{color:#44403C}
    .mov-tab.is-on{color:#0F6E56;border-bottom-color:#0F6E56;font-weight:600}
    .mov-tab-n{background:#F1EFE8;color:#57534E;border-radius:10px;padding:1px 7px;font-size:12px;margin-left:2px}
    .mov-tab.is-on .mov-tab-n{background:#E1F5EE;color:#0F6E56}

    .chq-badge{display:inline-block;padding:2px 9px;border-radius:10px;font-size:12px;font-weight:500}
    .chq-emitido{background:#FAEEDA;color:#854F0B}
    .chq-debitado{background:#E1F5EE;color:#0F6E56}
    .chq-rechazado{background:#FEE2E2;color:#B91C1C}
    .chq-anulado{background:#F1EFE8;color:#78716C}
    .chq-dias{font-size:11px;color:#A8A29E}
    .chq-alerta{background:#FEF6F6}
    .chq-alerta .chq-dias{color:#B91C1C;font-weight:600}
    .chq-chip{display:inline-block;background:#F5F5F4;color:#57534E;border-radius:6px;padding:1px 7px;font-size:12px;margin:1px 2px 1px 0}
    .chq-chip-warn{background:#FAEEDA;color:#854F0B}
    .chq-flujo{margin:0 0 14px;padding:10px 12px;background:#FAFAF9;border:1px solid #E7E5E4;border-radius:8px}
    .chq-flujo-t{font-size:12px;color:#78716C;margin-bottom:6px}
    .chq-flujo-r{display:flex;justify-content:space-between;gap:10px;font-size:13px;padding:2px 0}

    .chq-imp-box{margin-top:14px;border:1px solid #E7E5E4;border-radius:10px;padding:12px 14px}
    .chq-imp-t{font-weight:600;font-size:14px;margin-bottom:8px}
    .chq-imp-row{display:flex;align-items:center;gap:9px;padding:6px 8px;border-radius:6px;cursor:pointer;font-size:13px}
    .chq-imp-row:hover{background:#FAFAF9}
    .chq-imp-row.is-sug{background:#E1F5EE}
    .chq-imp-lbl{flex:1}
    .chq-imp-monto{font-variant-numeric:tabular-nums;color:#57534E}
    .chq-sug{color:#0F6E56;font-size:11px;font-weight:600}
    .chq-imp-tot{margin-top:8px;font-size:13px;text-align:right}
    .chq-dif-ok{color:#0F6E56;font-weight:600}
    .chq-dif-warn{color:#854F0B}
    .chq-dif-bad{color:#B91C1C;font-weight:600}
    .chq-hint{font-size:12px;color:#A8A29E;font-weight:400}
    .chq-2col{display:grid;grid-template-columns:1fr 1fr;gap:0 12px}
    .btn-icon{border:0;background:transparent;color:#A8A29E;cursor:pointer;font-size:15px;padding:2px 7px;border-radius:6px;line-height:1}
    .btn-icon:hover{background:#FEE2E2;color:#B91C1C}
  `;
  const style = document.createElement('style');
  style.id = 'mov-style';
  style.textContent = css;
  document.head.appendChild(style);
}
