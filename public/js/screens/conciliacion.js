import { sbGet } from '../core/sb.js';

// ── Pantalla: Conciliación ─────────────────────────────────────────────
// Modelo de interacción: DOS LADOS con diferencia en vivo.
//   Izquierda  = extracto: la plata que se movió, agrupada por operación
//   Derecha    = contrapartes con saldo (compras, gastos, cheques emitidos)
// El extracto manda: es la fuente de verdad y es como se trabaja hoy en papel.
// Se marca de los dos lados, la barra de abajo muestra la diferencia y NADA se
// escribe hasta apretar "Conciliar". Eso resuelve 1:1, 1:N, N:1 y N:M sin UI
// especial para ninguno (ver ADARA-CONCILIACION-BANCARIA.md).
//
// Reglas que implementa:
// - "Espera motor ML" aplica SOLO a movimientos de Mercado Pago con categoría de
//   venta. Toda entrada de banco es conciliación manual (antes, cualquier monto
//   positivo caía en espera y los cobros B2B por Supervielle quedaban en limbo).
// - La sugerencia se muestra CON SUS MOTIVOS y nunca se aplica sola: resalta,
//   no selecciona. El CUIT puntúa aparte del monto, porque identifica al
//   proveedor aunque el pago sea parcial.
// - Diferencia ≠ 0 nunca bloquea: se imputa min(saldoMov, saldoOp) y lo que
//   sobra queda parcial. Una diferencia es señal de dato faltante, no algo a
//   justificar con un formulario.

let CUENTAS = [], CUENTA_BY_ID = {};
let MOVS = [];              // v_movimientos_estado
let COMPRAS = [], GASTOS = [], CHEQUES = [], PROVEEDORES = [];
let PROV_BY_ID = {};
let VINC_BY_MOV = {};       // movimiento_id -> [vinculos]
let COMPLETITUD = [];       // v_completitud_mensual
let MES = '';
let FILTRO = { cuenta: '', estado: 'por_conciliar', qOps: '', qMovs: '' };
let SEL_OPS = new Set();    // claves "tipo:id"
let SEL_MOVS = new Set();   // ids de movimiento
let EXPANDIDO = new Set();  // grupos desplegados
let FOCO = null;            // input enfocado + cursor, para sobrevivir al re-render

const MES_NOM = ['', 'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];
const mesLabel = k => { if (!k) return ''; const [y, m] = k.split('-'); return `${MES_NOM[Number(m)] || m} ${y}`; };
const round2 = n => Math.round((Number(n) || 0) * 100) / 100;

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}
function fmt(n, moneda) {
  const v = Number(n) || 0;
  const abs = Math.abs(v).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return `${v < 0 ? '−' : ''}${moneda === 'USD' ? 'US$ ' : '$ '}${abs}`;
}
function fmtFecha(f) {
  if (!f) return '—';
  const [y, m, d] = String(f).slice(0, 10).split('-');
  return `${d}/${m}`;
}
const LABEL_CUENTA = { supervielle_ars: 'Supervielle', mercadopago_ars: 'Mercado Pago', caja_ars: 'Caja ARS', caja_usd: 'Caja USD' };
function cuentaLabel(id) {
  const c = CUENTA_BY_ID[id];
  if (!c) return '—';
  return LABEL_CUENTA[c.codigo] || c.nombre || c.codigo || ('Cuenta ' + id);
}

// ── Clasificación de movimientos ───────────────────────────────────────
// El motor de cobros de ML solo resuelve movimientos de MP atados a una venta.
// Una entrada en Supervielle (cobro B2B por transferencia) es trabajo manual.
function esEsperaMotor(m) {
  return m.origen === 'mp_account_statement'
      && (m.categoria === 'cobro_venta' || m.categoria === 'devolucion');
}
function esAccionable(m) {
  return (m.estado === 'pendiente' || m.estado === 'parcial') && !esEsperaMotor(m);
}
function saldoMov(m) {
  const s = Number(m.saldo_pendiente);
  return Math.abs(isNaN(s) ? Number(m.monto) || 0 : s);
}

// ── Operaciones con saldo (lado izquierdo) ─────────────────────────────
// Compras, gastos y cheques emitidos se unifican en una sola lista: para
// conciliar son lo mismo, una obligación con saldo.
function operaciones() {
  const out = [];
  for (const c of COMPRAS) {
    const saldo = round2(c.saldo_ap_ars);
    if (!(saldo > 0.02)) continue;
    const p = PROV_BY_ID[c.proveedor_id];
    out.push({
      key: 'compra:' + c.compra_id, op_tipo: 'compra', op_id: c.compra_id,
      titulo: c.nro_factura || ('Compra #' + c.compra_id),
      sub: p ? p.nombre : 'Sin proveedor',
      cuit: p ? p.cuit : null, fecha: c.fecha, saldo, moneda: 'ARS', clase: 'compra'
    });
  }
  for (const g of GASTOS) {
    const saldo = round2(g.saldo_pendiente_ars);
    if (!(saldo > 0.02)) continue;
    const p = PROV_BY_ID[g.proveedor_id];
    out.push({
      key: 'gasto:' + g.id, op_tipo: 'gasto', op_id: g.id,
      titulo: g.descripcion || ('Gasto #' + g.id),
      sub: p ? p.nombre : (g.nro_comprobante || 'Gasto'),
      cuit: p ? p.cuit : null, fecha: g.fecha, saldo, moneda: 'ARS', clase: 'gasto'
    });
  }
  for (const ch of CHEQUES) {
    if (ch.estado !== 'emitido') continue;
    const saldo = round2(Number(ch.monto) - Number(ch.conciliado_ars || 0));
    if (!(saldo > 0.02)) continue;
    out.push({
      key: 'cheque:' + ch.id, op_tipo: 'cheque', op_id: ch.id,
      titulo: 'Echeq N° ' + ch.numero,
      sub: ch.contraparte || 'Cheque', numero: ch.numero,
      cuit: ch.contraparte_cuit, fecha: ch.fecha_pago, saldo, moneda: ch.moneda || 'ARS', clase: 'cheque'
    });
  }
  return out;
}

// ── Sugerencia: puntaje con motivos visibles ───────────────────────────
// Un puntaje que no explica por qué no se usa dos veces. Cada señal suma y se
// muestra como chip. El CUIT vale aunque el monto no coincida: identifica al
// proveedor en pagos parciales, que es donde el matching por monto no llega.
function puntaje(op, mov) {
  const desc = (mov.descripcion || '').toLowerCase();
  const motivos = [];
  let pts = 0;

  // Número de cheque en la descripción del débito: identificador único, la
  // señal más fuerte que existe en todo el circuito.
  if (op.op_tipo === 'cheque' && op.numero) {
    const n = String(op.numero).replace(/^0+/, '');
    if (n && desc.replace(/\D/g, '').includes(n)) { pts += 60; motivos.push('n° cheque'); }
  }
  const dif = Math.abs(op.saldo - saldoMov(mov));
  if (dif <= 0.02) { pts += 50; motivos.push('monto exacto'); }
  else if (op.saldo > 0 && dif / op.saldo <= 0.01) { pts += 25; motivos.push('≈ monto'); }

  if (op.cuit && desc.replace(/\D/g, '').includes(String(op.cuit))) { pts += 30; motivos.push('CUIT'); }

  if (op.fecha && mov.fecha) {
    const d = Math.abs((new Date(mov.fecha) - new Date(op.fecha)) / 86400000);
    // La fecha sola no es señal: casi todo cae dentro de la ventana y ensuciaba
    // cada fila con un chip inútil. Solo desempata cuando ya hay otra señal.
    if (d <= 7 && pts > 0) { pts += 15; motivos.push('fecha'); }
    else if (d <= 7) pts += 5;
  }
  return { pts, motivos };
}

// Mejor puntaje de cada fila contra lo que hay marcado del otro lado.
function scoreOpContraSel(op, movsSel) {
  let best = { pts: 0, motivos: [] };
  for (const m of movsSel) { const s = puntaje(op, m); if (s.pts > best.pts) best = s; }
  return best;
}
function scoreMovContraSel(mov, opsSel) {
  let best = { pts: 0, motivos: [] };
  for (const o of opsSel) { const s = puntaje(o, mov); if (s.pts > best.pts) best = s; }
  return best;
}

export async function loadConciliacion() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando conciliación…</div>`;
  try {
    CUENTAS = await sbGet('cuentas', 'order=id.asc');
    CUENTA_BY_ID = Object.fromEntries(CUENTAS.map(c => [c.id, c]));
    MOVS = await sbGet('v_movimientos_estado', 'order=fecha.desc,id.desc');
    COMPRAS = await sbGet('v_compras_ap', 'saldo_ap_ars=gt.0.02&order=fecha.desc,compra_id.desc').catch(() => []);
    GASTOS = await sbGet('v_gastos_ap', 'estado_pago=in.(pendiente,parcial)&order=fecha.desc,id.desc').catch(() => []);
    CHEQUES = await sbGet('v_cheques', 'estado=eq.emitido&order=fecha_pago.asc,id.asc').catch(() => []);
    PROVEEDORES = await sbGet('proveedores', 'order=nombre.asc').catch(() => []);
    PROV_BY_ID = Object.fromEntries(PROVEEDORES.map(p => [p.id, p]));
    COMPLETITUD = await sbGet('v_completitud_mensual', 'order=periodo.asc').catch(() => []);
    const vinc = await sbGet('vinculos', 'order=id.desc');
    VINC_BY_MOV = {};
    for (const v of vinc) (VINC_BY_MOV[v.movimiento_id] = VINC_BY_MOV[v.movimiento_id] || []).push(v);
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudo cargar la conciliación: ${esc(e.message)}</div>`;
    return;
  }
  const meses = [...new Set(MOVS.map(m => (m.fecha || '').slice(0, 7)).filter(Boolean))].sort().reverse();
  if (!MES || !meses.includes(MES)) MES = meses[0] || new Date().toISOString().slice(0, 7);
  SEL_OPS.clear(); SEL_MOVS.clear();
  inyectarEstilo();
  render();
}

function movsDelMes() {
  return MOVS.filter(m => (m.fecha || '').slice(0, 7) === MES
    && (!FILTRO.cuenta || String(m.cuenta_id) === FILTRO.cuenta));
}

// ── Agrupación: la fila es la OPERACIÓN, no la línea ───────────────────
// Una transferencia de Supervielle viene con sus impuestos como lineas sueltas
// (mismo timestamp), y un cobro de MP con su bonificación de envío y su
// retención (mismo op_id). `grupo_key` en la base ya resuelve la clave por
// origen. Seleccionar el grupo selecciona todas sus lineas: los impuestos
// viajan con la operación que los generó y heredan su linea de negocio.
function agrupar(movs) {
  const map = new Map();
  for (const m of movs) {
    const k = m.grupo_key || ('mov-' + m.id);
    let g = map.get(k);
    if (!g) {
      g = { key: k, lineas: [], fecha: m.fecha, cuenta_id: m.cuenta_id, neto: 0, saldo: 0, origen: m.origen };
      map.set(k, g);
    }
    g.lineas.push(m);
    g.neto = round2(g.neto + Number(m.monto || 0));
    g.saldo = round2(g.saldo + saldoMov(m));
    if (m.fecha < g.fecha) g.fecha = m.fecha;
  }
  for (const g of map.values()) {
    g.principal = g.lineas.reduce((a, b) => Math.abs(Number(b.monto)) > Math.abs(Number(a.monto)) ? b : a);
    g.motivo = motivoBloqueo(g);
  }
  return [...map.values()];
}

// Por qué un pendiente no se puede cerrar todavía. No bloquea el cierre del mes
// (el P&L se arma con facturas, no con el extracto), pero identifica QUÉ carga
// falta y de quién depende.
function motivoBloqueo(g) {
  const d = (g.principal.descripcion || '').toLowerCase();
  if (/deb\. en cta\. comex|cta\. comex/.test(d)) return 'Falta cargar la importación';
  if (g.origen === 'supervielle' && g.neto > 0 && g.principal.categoria === 'cobro_venta')
    return 'Falta la venta B2B (Tango)';
  return null;
}

function movsVisibles() {
  const base = movsDelMes();
  let l;
  if (FILTRO.estado === 'por_conciliar') l = base.filter(esAccionable);
  else if (FILTRO.estado === 'espera') l = base.filter(m => esEsperaMotor(m) && m.estado !== 'conciliado');
  else if (FILTRO.estado === 'conciliado') l = base.filter(m => m.estado === 'conciliado');
  else if (FILTRO.estado === 'auto') l = base.filter(m => m.estado === 'auto');
  else l = base;
  const q = FILTRO.qMovs.trim().toLowerCase();
  if (q) l = l.filter(m => (m.descripcion || '').toLowerCase().includes(q) || String(Math.abs(m.monto)).includes(q));
  // Los seleccionados no se pierden aunque cambie el filtro ni la búsqueda.
  const vistos = new Set(l.map(m => m.id));
  const extra = base.filter(m => SEL_MOVS.has(m.id) && !vistos.has(m.id));
  return [...extra, ...l];
}

function opsVisibles() {
  let l = operaciones();
  const q = FILTRO.qOps.trim().toLowerCase();
  if (q) l = l.filter(o => (o.titulo + ' ' + o.sub).toLowerCase().includes(q));
  return l;
}

function render() {
  const root = document.getElementById('app-screens');
  const base = movsDelMes();
  const ops = opsVisibles();
  const grupos = agrupar(movsVisibles());
  let listos = grupos.filter(g => !g.motivo);
  const trabados = grupos.filter(g => g.motivo);
  const comp = COMPLETITUD.find(c => c.periodo === MES) || null;

  const nAccion = base.filter(esAccionable).length;
  const nEspera = base.filter(m => esEsperaMotor(m) && m.estado !== 'conciliado').length;
  const nConcil = base.filter(m => m.estado === 'conciliado').length;
  const nAuto = base.filter(m => m.estado === 'auto').length;
  const montoAccion = base.filter(esAccionable).reduce((s, m) => s + saldoMov(m), 0);

  const meses = [...new Set(MOVS.map(m => (m.fecha || '').slice(0, 7)).filter(Boolean))].sort().reverse();
  const optMes = meses.map(k => `<option value="${k}" ${k === MES ? 'selected' : ''}>${mesLabel(k)}</option>`).join('');
  const optCuenta = ['<option value="">Todas las cuentas</option>']
    .concat(CUENTAS.map(c => `<option value="${c.id}" ${FILTRO.cuenta === String(c.id) ? 'selected' : ''}>${cuentaLabel(c.id)}</option>`)).join('');

  const pill = (v, l, n) => `<button class="pill ${FILTRO.estado === v ? 'active' : ''}" data-estado="${v}">${l} <span class="num">${n}</span></button>`;

  // Objetos seleccionados (para puntuar el otro lado y totalizar)
  const opsSel = ops.filter(o => SEL_OPS.has(o.key));
  // Marcar de un lado REORDENA el otro por afinidad y sube el mejor candidato
  // arriba de todo. Sin esto la sugerencia era invisible: podía estar 30 filas
  // más abajo. Resalta y acerca; nunca selecciona.
  let opsOrden = ops, mejorOp = 0, mejorMov = 0;
  const gruposSel = grupos.filter(g => SEL_MOVS.has(g.key));
  const movsSel = gruposSel.flatMap(g => g.lineas);
  const totOps = round2(opsSel.reduce((s, o) => s + o.saldo, 0));
  const totMovs = round2(gruposSel.reduce((s, g) => s + g.saldo, 0));
  const dif = round2(totMovs - totOps);

  if (gruposSel.length) {
    const conPts = ops.map(o => ({ o, p: scoreOpContraSel(o, gruposSel.flatMap(g => g.lineas)).pts }));
    conPts.sort((a, b) => b.p - a.p);
    opsOrden = conPts.map(x => x.o);
    mejorOp = conPts.length ? conPts[0].p : 0;
  }
  if (opsSel.length) {
    const conPts = listos.map(g => ({ g, p: scoreMovContraSel(g.principal, opsSel).pts }));
    conPts.sort((a, b) => b.p - a.p);
    listos = conPts.map(x => x.g);
    mejorMov = conPts.length ? conPts[0].p : 0;
  }

  const filaOp = o => {
    const sel = SEL_OPS.has(o.key);
    const s = movsSel.length ? scoreOpContraSel(o, movsSel) : { pts: 0, motivos: [] };
    const sug = s.pts >= 25;
    return `<label class="cc-row${sel ? ' is-sel' : ''}${sug && !sel ? ' is-sug' : ''}" data-op="${o.key}">
      <input type="checkbox" class="cc-ck cc-ck-op" data-key="${o.key}" ${sel ? 'checked' : ''}>
      <span class="cc-main">
        <span class="cc-t">${esc(o.titulo)} <span class="cc-tag cc-tag-${o.clase}">${o.op_tipo}</span></span>
        <span class="cc-s">${esc(o.sub)} · ${fmtFecha(o.fecha)}${s.motivos.length ? ' · ' + s.motivos.map(x => `<b class="cc-why">${x}</b>`).join(' ') : ''}</span>
      </span>
      <span class="cc-m">${fmt(o.saldo, o.moneda)}</span>
    </label>`;
  };

  const filaGrupo = g => {
    const m = g.principal;
    const sel = SEL_MOVS.has(g.key);
    const s = opsSel.length ? scoreMovContraSel(m, opsSel) : { pts: 0, motivos: [] };
    const sug = s.pts >= 25;
    const espera = g.lineas.every(esEsperaMotor);
    const vs = g.lineas.flatMap(x => VINC_BY_MOV[x.id] || []);
    const chips = vs.map(v => `<span class="cc-chip">${v.op_tipo} #${v.op_id} <b class="cc-x" data-unlink="${v.id}">✕</b></span>`).join(' ');
    const multi = g.lineas.length > 1
      ? `<span class="cc-multi" data-exp="${esc(g.key)}">${g.lineas.length} líneas ▾</span>` : '';
    const detalle = (g.lineas.length > 1 && EXPANDIDO.has(g.key))
      ? `<span class="cc-sub">${g.lineas.map(x => `<span class="cc-subr"><i>${esc((x.descripcion || '').slice(0, 52))}</i><b>${fmt(x.monto, 'ARS')}</b></span>`).join('')}</span>` : '';
    return `<label class="cc-row${sel ? ' is-sel' : ''}${sug && !sel ? ' is-sug' : ''}${espera ? ' is-espera' : ''}${g.motivo ? ' is-trabado' : ''}" data-g="${esc(g.key)}">
      <input type="checkbox" class="cc-ck cc-ck-mov" data-key="${esc(g.key)}" ${sel ? 'checked' : ''} ${espera ? 'disabled' : ''}>
      <span class="cc-main">
        <span class="cc-t">${esc((m.descripcion || '').slice(0, 74))} ${multi}</span>
        <span class="cc-s">${fmtFecha(g.fecha)} · ${esc(cuentaLabel(g.cuenta_id))}${espera ? ' · <b class="cc-esp">espera motor ML</b>' : ''}${g.motivo ? ` · <b class="cc-trab">⚠ ${esc(g.motivo)}</b>` : ''}${s.motivos.length ? ' · ' + s.motivos.map(x => `<b class="cc-why">${x}</b>`).join(' ') : ''} ${chips}</span>
        ${detalle}
      </span>
      <span class="cc-m ${g.neto < 0 ? 'cc-neg' : 'cc-pos'}">${fmt(g.neto, CUENTA_BY_ID[g.cuenta_id]?.moneda)}</span>
    </label>`;
  };

  let barra = '';
  if (opsSel.length || movsSel.length) {
    let estado, clase, acciones;
    if (!opsSel.length || !movsSel.length) {
      estado = 'Marcá al menos uno de cada lado'; clase = 'cc-bar-wait'; acciones = '';
    } else if (Math.abs(dif) <= 0.02) {
      estado = 'Cierra exacto ✓'; clase = 'cc-bar-ok';
      acciones = `<button class="btn btn-primary" id="cc-go">Conciliar</button>`;
    } else if (dif < 0) {
      estado = `Pago parcial · queda ${fmt(-dif, 'ARS')} pendiente en la operación`; clase = 'cc-bar-warn';
      acciones = `<button class="btn btn-primary" id="cc-go">Conciliar parcial</button>`;
    } else {
      estado = `Sobran ${fmt(dif, 'ARS')} sin asignar en el movimiento`; clase = 'cc-bar-warn';
      acciones = `<button class="btn btn-primary" id="cc-go">Conciliar igual</button>`;
    }
    barra = `<div class="cc-bar ${clase}">
      <div class="cc-bar-l">
        <span>Operaciones <b>${fmt(totOps, 'ARS')}</b> <i>(${opsSel.length})</i></span>
        <span>Movimientos <b>${fmt(totMovs, 'ARS')}</b> <i>(${movsSel.length})</i></span>
        <span class="cc-bar-dif">${esc(estado)}</span>
      </div>
      <div class="cc-bar-r">
        <button class="btn btn-ghost" id="cc-clear">Limpiar</button>
        ${acciones}
      </div>
    </div>`;
  }

  root.innerHTML = `
    <div class="toolbar">
      <select class="select" id="c-mes" style="width:auto;text-transform:capitalize">${optMes}</select>
      <select class="select" id="c-cuenta" style="width:auto">${optCuenta}</select>
      <div class="grow"></div>
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">Accionable</div><div class="kpi-value">${nAccion}</div></div>
      <div class="kpi"><div class="kpi-label">Monto a conciliar</div><div class="kpi-value">${fmt(montoAccion, 'ARS')}</div></div>
      <div class="kpi"><div class="kpi-label">Operaciones con saldo</div><div class="kpi-value">${operaciones().length}</div></div>
      <div class="kpi"><div class="kpi-label">Explicado del mes</div><div class="kpi-value">${comp ? comp.pct_explicado + '%' : '—'}</div></div>
    </div>

    <div class="pills">
      ${pill('por_conciliar', 'Por conciliar', nAccion)}
      ${pill('espera', 'Espera ML', nEspera)}
      ${pill('conciliado', 'Conciliados', nConcil)}
      ${pill('auto', 'Auto', nAuto)}
      ${pill('todos', 'Todos', base.length)}
    </div>

    <div class="cc-grid">
      <div class="cc-col">
        <div class="cc-head">
          <span>Extracto <i>${grupos.length} operaciones</i></span>
          <input class="input cc-q" id="c-q-movs" placeholder="Buscar…" value="${esc(FILTRO.qMovs)}">
        </div>
        ${opsSel.length && mejorMov < 25 ? '<div class="cc-nosug">Sin sugerencia: ningún movimiento del filtro coincide por monto ni CUIT.</div>' : ''}
        <div class="cc-list">${grupos.length
          ? listos.map(filaGrupo).join('') + (trabados.length
              ? `<div class="cc-sep">Falta cargar la operación · ${trabados.length} · ${fmt(trabados.reduce((a, g) => a + g.saldo, 0), 'ARS')}</div>` + trabados.map(filaGrupo).join('')
              : '')
          : '<div class="empty">No hay movimientos para este filtro.</div>'}</div>
      </div>
      <div class="cc-col">
        <div class="cc-head">
          <span>Operaciones con saldo <i>${ops.length}</i></span>
          <input class="input cc-q" id="c-q-ops" placeholder="Buscar…" value="${esc(FILTRO.qOps)}">
        </div>
        ${gruposSel.length && mejorOp < 25 ? '<div class="cc-nosug">Sin sugerencia: ninguna operación coincide por monto ni CUIT.</div>' : ''}
        <div class="cc-list">${opsOrden.length ? opsOrden.map(filaOp).join('') : '<div class="empty">No hay operaciones con saldo.</div>'}</div>
      </div>
    </div>

    ${barra}
  `;

  document.getElementById('c-mes').addEventListener('change', e => { MES = e.target.value; SEL_OPS.clear(); SEL_MOVS.clear(); render(); });
  document.getElementById('c-cuenta').addEventListener('change', e => { FILTRO.cuenta = e.target.value; render(); });
  // Re-render completo en cada tecla perdería el foco y el cursor: se restauran.
  const bindQ = (id, campo) => {
    const el = document.getElementById(id);
    el.addEventListener('input', e => {
      FILTRO[campo] = e.target.value;
      FOCO = { id, pos: e.target.selectionStart };
      render();
    });
  };
  bindQ('c-q-ops', 'qOps');
  bindQ('c-q-movs', 'qMovs');
  if (FOCO) {
    const el = document.getElementById(FOCO.id);
    if (el) { el.focus(); try { el.setSelectionRange(FOCO.pos, FOCO.pos); } catch (_) {} }
    FOCO = null;
  }
  document.querySelectorAll('.pill').forEach(p => p.addEventListener('click', () => { FILTRO.estado = p.dataset.estado; render(); }));
  document.querySelectorAll('.cc-ck-op').forEach(ck => ck.addEventListener('change', () => {
    ck.checked ? SEL_OPS.add(ck.dataset.key) : SEL_OPS.delete(ck.dataset.key); render();
  }));
  document.querySelectorAll('.cc-ck-mov').forEach(ck => ck.addEventListener('change', () => {
    const k = ck.dataset.key;
    ck.checked ? SEL_MOVS.add(k) : SEL_MOVS.delete(k); render();
  }));
  document.querySelectorAll('.cc-multi').forEach(b => b.addEventListener('click', e => {
    e.preventDefault(); e.stopPropagation();
    const k = b.dataset.exp;
    EXPANDIDO.has(k) ? EXPANDIDO.delete(k) : EXPANDIDO.add(k);
    render();
  }));
  document.querySelectorAll('.cc-x').forEach(b => b.addEventListener('click', e => {
    e.preventDefault(); e.stopPropagation(); desvincular(b.dataset.unlink);
  }));
  const clear = document.getElementById('cc-clear');
  if (clear) clear.addEventListener('click', () => { SEL_OPS.clear(); SEL_MOVS.clear(); render(); });
  const go = document.getElementById('cc-go');
  if (go) go.addEventListener('click', () => conciliarSeleccion(opsSel, gruposSel, dif));
}

// ── Escritura ──────────────────────────────────────────────────────────
// Asignación greedy: cada movimiento se reparte entre las operaciones marcadas
// hasta agotarse. Se imputa siempre min(saldoMov, saldoOp), así ninguna de las
// dos puntas queda sobre-imputada. Lo que sobra queda parcial, a propósito.
async function conciliarSeleccion(opsSel, gruposSel, dif) {
  if (!opsSel.length || !gruposSel.length) return;

  const detalle = Math.abs(dif) <= 0.02
    ? 'La diferencia cierra en cero.'
    : (dif < 0
        ? `Van a quedar ${fmt(-dif, 'ARS')} pendientes en la operación (pago parcial).`
        : `Van a quedar ${fmt(dif, 'ARS')} sin asignar en el movimiento. Si esperabas que cerrara, puede faltar cargar una factura.`);
  const nLineas = gruposSel.reduce((a, g) => a + g.lineas.length, 0);
  if (!confirm(`Conciliar ${opsSel.length} operación/es con ${gruposSel.length} movimiento/s del extracto (${nLineas} líneas).\n\n${detalle}\n\n¿Confirmás?`)) return;

  // Reparto greedy: cada linea del extracto se distribuye entre las operaciones
  // marcadas imputando min(saldoLinea, saldoOp). Los impuestos de una operación
  // se imputan junto con ella, que es como ocurrieron en la realidad.
  const rest = new Map(opsSel.map(o => [o.key, o.saldo]));
  const plan = [];
  for (const g of gruposSel) {
    for (const m of g.lineas) {
      let disp = saldoMov(m);
      if (!(disp > 0.02)) continue;
      for (const o of opsSel) {
        if (disp <= 0.02) break;
        const r = rest.get(o.key);
        if (!(r > 0.02)) continue;
        const imp = round2(Math.min(disp, r));
        plan.push({ movimiento_id: m.id, op_tipo: o.op_tipo, op_id: o.op_id, monto: imp });
        rest.set(o.key, round2(r - imp));
        disp = round2(disp - imp);
      }
    }
  }
  if (!plan.length) return;

  const errores = [];
  for (const v of plan) {
    try {
      const r = await fetch('/vincular', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(v)
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
      // Estado local: con miles de filas, recargar todo desde la base en cada
      // conciliación son segundos de espera por item. Se actualiza en memoria.
      (VINC_BY_MOV[v.movimiento_id] = VINC_BY_MOV[v.movimiento_id] || [])
        .push({ id: data.id || ('tmp-' + Date.now() + Math.random()), ...v });
      aplicarLocal(v);
    } catch (e) { errores.push(`${v.op_tipo} #${v.op_id}: ${e.message}`); }
  }

  SEL_OPS.clear(); SEL_MOVS.clear();
  render();
  if (errores.length) window.toast(`${plan.length - errores.length} de ${plan.length} OK. Error: ${errores[0]}`, 'error');
  else window.toast(`Conciliado · ${plan.length} vínculo/s`);
}

// Descuenta el vínculo del saldo en memoria, del lado del movimiento y del lado
// de la operación, para no volver a pedirle todo a la base.
function aplicarLocal(v) {
  const m = MOVS.find(x => x.id === v.movimiento_id);
  if (m) {
    m.monto_vinculado = round2(Number(m.monto_vinculado || 0) + v.monto);
    m.saldo_pendiente = round2(Math.abs(Number(m.monto)) - m.monto_vinculado);
    m.estado = Math.abs(m.saldo_pendiente) < 0.02 ? 'conciliado' : 'parcial';
  }
  if (v.op_tipo === 'compra') {
    const c = COMPRAS.find(x => x.compra_id === v.op_id);
    if (c) c.saldo_ap_ars = round2(Number(c.saldo_ap_ars) - v.monto);
  } else if (v.op_tipo === 'gasto') {
    const g = GASTOS.find(x => x.id === v.op_id);
    if (g) g.saldo_pendiente_ars = round2(Number(g.saldo_pendiente_ars) - v.monto);
  } else if (v.op_tipo === 'cheque') {
    const ch = CHEQUES.find(x => x.id === v.op_id);
    if (ch) ch.conciliado_ars = round2(Number(ch.conciliado_ars || 0) + v.monto);
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
    .cc-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:12px;padding-bottom:80px}
    .cc-col{border:1px solid #E7E5E4;border-radius:10px;overflow:hidden;background:#fff}
    .cc-head{display:flex;align-items:center;gap:10px;padding:9px 12px;background:#FAFAF9;border-bottom:1px solid #E7E5E4;font-size:13px;font-weight:600;color:#44403C}
    .cc-head i{font-style:normal;color:#A8A29E;font-weight:400}
    .cc-q{margin-left:auto;max-width:190px;padding:5px 9px;font-size:13px}
    .cc-list{max-height:520px;overflow:auto}
    .cc-row{display:flex;align-items:flex-start;gap:9px;padding:8px 12px;border-bottom:1px solid #F5F5F4;cursor:pointer;font-size:13px}
    .cc-row:hover{background:#FAFAF9}
    .cc-row.is-sel{background:#E1F5EE}
    .cc-row.is-sug{background:#FEFCE8}
    .cc-row.is-espera{opacity:.55;cursor:default}
    .cc-ck{margin-top:3px;flex:none}
    .cc-main{flex:1;min-width:0}
    .cc-t{display:block;color:#1C1917;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .cc-s{display:block;font-size:11.5px;color:#A8A29E;margin-top:2px}
    .cc-why{color:#0F6E56;font-weight:600}
    .cc-esp{color:#854F0B;font-weight:600}
    .cc-m{flex:none;font-variant-numeric:tabular-nums;font-size:13px;color:#44403C;text-align:right}
    .cc-neg{color:#B91C1C}
    .cc-pos{color:#15803D}
    .cc-tag{font-size:10px;text-transform:uppercase;letter-spacing:.03em;padding:1px 6px;border-radius:5px;margin-left:4px;vertical-align:1px}
    .cc-tag-compra{background:#E6F1FB;color:#0C447C}
    .cc-tag-gasto{background:#F1EFE8;color:#57534E}
    .cc-tag-cheque{background:#FAEEDA;color:#854F0B}
    .cc-chip{display:inline-block;background:#F1EFE8;color:#57534E;border-radius:5px;padding:0 6px;font-size:11px;margin-left:3px}
    .cc-x{cursor:pointer;color:#A8A29E;margin-left:2px}
    .cc-x:hover{color:#B91C1C}
    .cc-bar{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);z-index:40;
            display:flex;align-items:center;justify-content:space-between;gap:14px;
            padding:11px 16px;border-radius:10px;border:1px solid #E7E5E4;background:#fff;
            box-shadow:0 4px 20px rgba(0,0,0,.14);width:min(1100px,calc(100% - 48px))}
    .cc-bar-l{display:flex;gap:18px;font-size:13px;color:#57534E;flex-wrap:wrap}
    .cc-bar-l b{color:#1C1917;font-variant-numeric:tabular-nums}
    .cc-bar-l i{font-style:normal;color:#A8A29E}
    .cc-bar-r{display:flex;gap:8px;flex:none}
    .cc-bar-dif{font-weight:600}
    .cc-bar-ok .cc-bar-dif{color:#0F6E56}
    .cc-bar-warn .cc-bar-dif{color:#854F0B}
    .cc-bar-wait .cc-bar-dif{color:#A8A29E}
    .cc-nosug{padding:7px 12px;background:#FEFCE8;color:#854F0B;font-size:12px;border-bottom:1px solid #E7E5E4}
    .cc-multi{font-size:11px;color:#0C447C;background:#E6F1FB;border-radius:5px;padding:0 6px;cursor:pointer}
    .cc-sub{display:block;margin-top:5px;padding-left:8px;border-left:2px solid #E7E5E4}
    .cc-subr{display:flex;justify-content:space-between;gap:10px;font-size:11.5px;color:#78716C;padding:1px 0}
    .cc-subr i{font-style:normal}
    .cc-subr b{font-variant-numeric:tabular-nums;font-weight:500}
    .cc-sep{padding:7px 12px;background:#FAEEDA;color:#854F0B;font-size:12px;font-weight:600;border-bottom:1px solid #E7E5E4}
    .cc-trab{color:#854F0B;font-weight:600}
    .cc-row.is-trabado{background:#FFFDF7}
    @media (max-width:900px){.cc-grid{grid-template-columns:1fr}.cc-bar{width:calc(100% - 24px);flex-wrap:wrap}}
  `;
  const style = document.createElement('style');
  style.id = 'con-style';
  style.textContent = css;
  document.head.appendChild(style);
}
