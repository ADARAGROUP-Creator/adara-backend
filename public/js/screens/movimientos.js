import { sbGet, sbPost, sbPatch } from '../core/sb.js';

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
let EXPANDED = new Set();  // claves de operación actualmente desplegadas
let LINEAS = [];           // catálogo de líneas de negocio
let LINEA_LABEL = {};      // linea_id -> nombre
let VENTA_LINEA = {};      // ventas_ml.id -> linea_negocio_id (línea heredada)
let GASTO_LINEA = {};      // gasto id -> linea_id
let COMPRA_LINEA = {};     // compra_id -> linea_id

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

// Celda "Línea" a nivel OPERACIÓN: editable solo si la operación tiene líneas
// huérfanas (sin contraparte). Si todas tienen contraparte, la línea aparece
// BLOQUEADA (heredada de la operación vinculada).
function celdaLineaOp(g) {
  const orphans = g.lines.filter(m => !tieneContraparte(m));
  if (!orphans.length) {
    const id = lineaHeredada(g.lines[0]);
    const txt = id != null ? (LINEA_LABEL[id] || ('#' + id)) : '(heredada)';
    return `<span class="mov-linea-ro" title="La toma de la operación vinculada (no se edita acá)">${esc(txt)}</span>`;
  }
  const actual = orphans[0].linea_id;
  const nroOp = nroOperacion(orphans[0]);
  const opts = ['<option value="">— Sin asignar —</option>']
    .concat(LINEAS.map(l => `<option value="${l.id}" ${String(actual) === String(l.id) ? 'selected' : ''}>${esc(LINEA_LABEL[l.id])}</option>`))
    .join('');
  return `<select class="select mov-linea-sel" data-mov="${orphans[0].id}" data-op="${esc(nroOp)}" title="Asignar línea (se aplica a toda la operación)">${opts}</select>`;
}

// N° de operación del extracto (REFERENCE_ID de MP / ref de banco). Vive en el
// 2° campo de referencia_externa (`fecha|REF|monto|saldo`). Las cargas manuales
// usan `manual-<uuid>` (sin número) → devuelve ''.
function nroOperacion(m) {
  const ref = String(m.referencia_externa || '');
  if (ref.startsWith('manual-') || ref.startsWith('sin_factura_auto')) return '';
  const f = ref.split('|');
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
    const compras = await sbGet('v_compras_ap', 'select=compra_id,linea_id').catch(() => []);
    COMPRA_LINEA = Object.fromEntries(compras.map(c => [c.compra_id, c.linea_id]));
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudieron cargar los movimientos: ${e.message}</div>`;
    return;
  }

  MESES = [...new Set(DATA.map(m => mesDe(m.fecha)).filter(Boolean))].sort().reverse();
  if (!MES || !MESES.includes(MES)) MES = MESES.length ? MESES[0] : '';

  inyectarEstilo();
  render();
}

function render() {
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
  const grupos = gruposDeOperacion(filtrados);

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
    <div class="toolbar">
      <select class="select" id="f-mes" style="width:auto;text-transform:capitalize">${opcionesMes}</select>
      <select class="select" id="f-cuenta" style="width:auto">${opcionesCuenta}</select>
      <input class="input grow" id="f-q" type="text" placeholder="Buscar descripción…" value="${FILTRO.q.replace(/"/g, '&quot;')}">
      <button class="btn btn-ghost" id="btn-importar">Importar</button>
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
          <tbody>${grupos.map(filaOperacionHTML).join('')}</tbody>
        </table></div>`}
  `;

  // Bindings
  document.getElementById('f-mes').addEventListener('change', e => { MES = e.target.value; FILTRO.estado = ''; render(); });
  document.getElementById('f-cuenta').addEventListener('change', e => { FILTRO.cuenta = e.target.value; render(); });
  document.getElementById('f-q').addEventListener('input', e => { FILTRO.q = e.target.value; render(); });
  document.getElementById('btn-nuevo').addEventListener('click', openModalNuevo);
  document.getElementById('btn-importar').addEventListener('click', openModalImportar);
  document.querySelectorAll('.pill').forEach(p => {
    p.addEventListener('click', () => { FILTRO.estado = p.dataset.estado; render(); });
  });
  document.querySelectorAll('.mov-del').forEach(b => {
    b.addEventListener('click', () => borrarMovimiento(b.dataset.id));
  });
  document.querySelectorAll('.mov-exp').forEach(b => {
    b.addEventListener('click', () => {
      const k = b.dataset.key;
      if (EXPANDED.has(k)) EXPANDED.delete(k); else EXPANDED.add(k);
      render();
    });
  });
  document.querySelectorAll('.mov-linea-sel').forEach(s => {
    s.addEventListener('change', () => asignarLinea(s.dataset.mov, s.dataset.op, s.value));
  });
}

// Agrupa los movimientos por número de operación (los que no tienen número son
// su propia operación de 1 línea). Mantiene el orden de aparición.
function gruposDeOperacion(filtrados) {
  const map = new Map();
  const order = [];
  for (const m of filtrados) {
    const k = nroOperacion(m) || ('m' + m.id);
    if (!map.has(k)) { map.set(k, { key: k, lines: [] }); order.push(k); }
    map.get(k).lines.push(m);
  }
  return order.map(k => map.get(k));
}

// Saca el número de operación del final de la descripción (ya está en su columna).
function descLimpia(m) {
  return (m.descripcion || '').replace(/\s*·\s*\d{6,}\s*$/, '').trim();
}

// Estado agregado de una operación a partir de sus líneas.
function estadoOp(lines) {
  const ests = lines.map(estadoDe);
  if (ests.every(e => e === 'conciliado' || e === 'auto')) return ests.includes('conciliado') ? 'conciliado' : 'auto';
  if (ests.every(e => e === 'pendiente')) return 'pendiente';
  return 'parcial';
}

// Fila de una OPERACIÓN (puede tener varias líneas). Si tiene más de una, se
// puede desplegar para ver el detalle línea por línea.
function filaOperacionHTML(g) {
  const lines = g.lines;
  const multi = lines.length > 1;
  const first = lines[0];
  const moneda = monedaDe(first.cuenta_id);
  const neto = lines.reduce((s, m) => s + (Number(m.monto) || 0), 0);
  const neg = neto < 0;
  const cats = [...new Set(lines.map(m => m.categoria))];
  const catUnica = cats.length === 1;
  const catLabel = catUnica ? ((CATEGORIAS.find(c => c[0] === cats[0])?.[1]) || cats[0]) : 'varias';
  const catSinClasif = catUnica && (!cats[0] || cats[0] === 'sin_clasificar');
  const est = estadoOp(lines);
  let vinc = '';
  for (const m of lines) { const v = vinculadoA(m); if (v) { vinc = v; break; } }
  const nroOp = nroOperacion(first);

  let desc;
  if (multi) {
    const pr = lines.slice().sort((a, b) => Math.abs(Number(b.monto) || 0) - Math.abs(Number(a.monto) || 0))[0];
    desc = `${esc(descLimpia(pr)) || 'Operación'} <span class="mov-muted">· ${lines.length} líneas</span>`;
  } else {
    desc = descLimpia(first) ? esc(descLimpia(first)) : '<span class="mov-muted">—</span>';
  }
  const expanded = EXPANDED.has(g.key);
  const toggle = multi
    ? `<button class="mov-exp" data-key="${esc(g.key)}" title="Ver las líneas de esta operación">${expanded ? '▾' : '▸'}</button> `
    : '';
  const delBtn = (!multi && first.origen === 'manual')
    ? `<button class="mov-del" data-id="${first.id}" title="Borrar movimiento">✕</button>` : '';

  let html = `<tr class="${multi ? 'mov-row-op' : ''}">
    <td>${(first.fecha || '').slice(8, 10)}/${(first.fecha || '').slice(5, 7)}</td>
    <td>${cuentaLabel(first.cuenta_id)}</td>
    <td>${toggle}${desc}</td>
    <td class="mov-mono mov-op">${nroOp ? esc(nroOp) : '<span class="mov-muted">—</span>'}</td>
    <td>${catSinClasif ? '<span class="mov-tag-empty">sin clasificar</span>' : `<span class="mov-tag">${esc(catLabel)}</span>`}</td>
    <td>${celdaLineaOp(g)}</td>
    <td style="text-align:right" class="mov-mono ${neg ? 'mov-neg' : 'mov-pos'}">${fmtMonto(neto, moneda)}</td>
    <td class="mov-mono mov-vinc">${vinc || '<span class="mov-muted">—</span>'}</td>
    <td><span class="mov-badge mov-badge-${est}">${LABEL_ESTADO[est] || est}</span></td>
    <td style="text-align:center">${delBtn}</td>
  </tr>`;

  if (multi && expanded) {
    html += `<tr class="mov-detalle"><td colspan="10">${detalleOperacionHTML(lines)}</td></tr>`;
  }
  return html;
}

// Detalle de las líneas de una operación (cuando está desplegada).
function detalleOperacionHTML(lines) {
  return `<div class="mov-det">${lines.map(m => {
    const moneda = monedaDe(m.cuenta_id);
    const neg = Number(m.monto) < 0;
    const est = estadoDe(m);
    const catLabel = (CATEGORIAS.find(c => c[0] === (m.categoria || ''))?.[1]) || m.categoria || '';
    return `<div class="mov-det-row">
      <span class="mov-det-desc">${descLimpia(m) ? esc(descLimpia(m)) : '—'}</span>
      <span class="mov-det-cat">${esc(catLabel)}</span>
      <span class="mov-mono ${neg ? 'mov-neg' : 'mov-pos'}">${fmtMonto(m.monto, moneda)}</span>
      <span class="mov-badge mov-badge-${est}">${LABEL_ESTADO[est] || est}</span>
    </div>`;
  }).join('')}</div>`;
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
    .mov-op{font-size:12px;color:#57534E;user-select:all;white-space:nowrap}
    .mov-row-op td{box-shadow:inset 3px 0 0 #C7B8E0}
    .mov-exp{border:0;background:transparent;color:#6D28D9;cursor:pointer;font-size:12px;padding:0 4px}
    .mov-exp:hover{color:#4C1D95}
    .mov-detalle > td{background:#FBFAF6;padding:8px 14px 8px 40px!important}
    .mov-det-row{display:grid;grid-template-columns:1fr 140px 130px 90px;gap:12px;align-items:center;padding:3px 0;font-size:13px}
    .mov-det-desc{color:#57534E}
    .mov-det-cat{color:#A8A29E;font-size:12px}
    .mov-linea-sel{font-size:12px;padding:3px 6px;max-width:150px}
    .mov-linea-ro{font-size:12px;color:#A8A29E;font-style:italic}
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
  `;
  const style = document.createElement('style');
  style.id = 'mov-style';
  style.textContent = css;
  document.head.appendChild(style);
}
