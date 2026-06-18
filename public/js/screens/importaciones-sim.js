import { sbGet, sbPost, sbPatch } from '../core/sb.js';
import { sessionUser } from '../core/sb.js';

// ── Pantalla: Simulador de Importaciones (Fase 1) ───────────────────────
// Herramienta what-if: costo final por producto PUESTO EN ARGENTINA, neto de
// IVA, congelado en ARS al TC. NO crea nada (ni stock, lotes, compras ni caja).
// Solo CALCULA y GUARDA/RECUPERA el escenario como borrador en importacion_sim
// (escenario completo en datos JSONB). Confirmar→stock es Fase 2.
// Diseño cerrado en ADARA-IMPORTACIONES-SIM.md. Reglas: P12 / CF6 / C11-C13.
//
// Reglas de cálculo (del doc, no reinventar):
//  · CIF_i = fob_decl_total_i + flete_DECLARADO·(pesoT_i/Σpeso) + seguro·(fobDeclT_i/Σfob)
//    → flete por PESO, seguro por FOB.
//  · derechos/estadística = % sobre CIF → CAPITALIZAN.
//  · IVA/IIBB/Ganancias percepción = base(CIF+derechos+estad)·% → CRÉDITO FISCAL (no costo).
//  · Costo s/IVA = fob_REAL_total + flete_REAL·(peso) + seguro + derechos + estad + despachante·CIF + fijos.
//  · Subfacturación: fob_real ≥ fob_decl; la diferencia capitaliza al costo SIN crédito fiscal.
//  · Cierre: Σ pagos (USD) = costo capitalizado + crédito fiscal (comparado en USD).

// ── Estado del módulo ───────────────────────────────────────────────────
let CURRENT = { id: null };
let NOMBRE = '';
let P = blankParams();
let FIJOS = [];            // { concepto, monto_usd }
let FIJOS_MODO = 'pct';    // 'pct' | 'monto'
let FIJOS_CRIT = 'kg';     // 'kg'  | 'fob'
let PRODS = [];            // { nombre, fob_decl_u, fob_real_u, cantidad, peso_u, derechos_pct, estadistica_pct, iva_pct, fijo_asignado, _open }
let PAGOS = [];            // { concepto, monto, moneda }
let _prevBolson = 0;

function blankParams() {
  return { tc: '', flete_declarado_usd: '', flete_real_usd: '', seguro_pct: '', despachante_pct: '', iibb_pct: '', ganancias_pct: '' };
}
function blankProd() {
  return { nombre: '', fob_decl_u: '', fob_real_u: '', cantidad: '', peso_u: '', derechos_pct: '', estadistica_pct: '', iva_pct: 21, fijo_asignado: 0, _open: false };
}

// ── Helpers ─────────────────────────────────────────────────────────────
const num = n => { const v = Number(n); return isFinite(v) ? v : 0; };
const round2 = n => Math.round((num(n) + Number.EPSILON) * 100) / 100;
const safeDiv = (a, b) => (num(b) === 0 ? 0 : num(a) / num(b));
const ars = n => '$ ' + num(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const usd = n => 'US$ ' + num(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const pctTxt = n => num(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' %';
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const bolsonActual = () => FIJOS.reduce((s, f) => s + num(f.monto_usd), 0);

// ── Motor de cálculo ────────────────────────────────────────────────────
function calc() {
  const tc = num(P.tc);
  const fleteDecl = num(P.flete_declarado_usd);
  const fleteReal = num(P.flete_real_usd);
  const segPct = num(P.seguro_pct) / 100;
  const despPct = num(P.despachante_pct) / 100;
  const iibbPct = num(P.iibb_pct) / 100;
  const ganPct = num(P.ganancias_pct) / 100;
  const bolson = bolsonActual();

  const rows = PRODS.map(p => {
    const cant = num(p.cantidad);
    return { p, cant, pesoT: num(p.peso_u) * cant, fobDeclT: num(p.fob_decl_u) * cant, fobRealT: num(p.fob_real_u) * cant };
  });
  const sPeso = rows.reduce((s, r) => s + r.pesoT, 0);
  const sFob = rows.reduce((s, r) => s + r.fobDeclT, 0);
  const seguroTotal = segPct * (sFob + fleteDecl);

  rows.forEach(r => {
    r.fleteDeclShare = fleteDecl * safeDiv(r.pesoT, sPeso);
    r.seguro = seguroTotal * safeDiv(r.fobDeclT, sFob);
    r.cif = r.fobDeclT + r.fleteDeclShare + r.seguro;
  });
  const sCif = rows.reduce((s, r) => s + r.cif, 0);

  rows.forEach(r => {
    const p = r.p;
    r.derechos = r.cif * num(p.derechos_pct) / 100;
    r.estadistica = r.cif * num(p.estadistica_pct) / 100;
    r.baseIva = r.cif + r.derechos + r.estadistica;
    r.iva = r.baseIva * num(p.iva_pct) / 100;
    r.iibb = r.baseIva * iibbPct;
    r.ganancias = r.baseIva * ganPct;
    r.fleteReal = fleteReal * safeDiv(r.pesoT, sPeso);
    r.despachante = despPct * r.cif;                 // (despPct·ΣCIF)·(CIF_i/ΣCIF) = despPct·CIF_i
    r.fijo = FIJOS_MODO === 'pct' ? num(p.fijo_asignado) / 100 * bolson : num(p.fijo_asignado);
    r.noDeclarado = (num(p.fob_real_u) - num(p.fob_decl_u)) * r.cant;
    r.costoUsd = r.fobRealT + r.fleteReal + r.seguro + r.derechos + r.estadistica + r.despachante + r.fijo;
    r.creditoUsd = r.iva + r.iibb + r.ganancias;
    r.costoUnitUsd = safeDiv(r.costoUsd, r.cant);
    r.costoUnitArs = r.costoUnitUsd * tc;
  });

  const costoTotUsd = rows.reduce((s, r) => s + r.costoUsd, 0);
  const credTotUsd = rows.reduce((s, r) => s + r.creditoUsd, 0);
  const pagosUsd = PAGOS.reduce((s, pg) => s + (pg.moneda === 'ARS' ? safeDiv(num(pg.monto), tc) : num(pg.monto)), 0);
  const esperado = costoTotUsd + credTotUsd;

  return {
    tc, bolson, sPeso, sFob, sCif, rows,
    costoTotUsd, credTotUsd, costoTotArs: costoTotUsd * tc, credTotArs: credTotUsd * tc,
    pagosUsd, esperado, diff: pagosUsd - esperado
  };
}

// ── Reparto de fijos ────────────────────────────────────────────────────
function baseCriterio(p) {
  return FIJOS_CRIT === 'kg' ? num(p.peso_u) * num(p.cantidad) : num(p.fob_decl_u) * num(p.cantidad);
}
function precargarFijos() {
  const bolson = bolsonActual();
  const base = PRODS.map(baseCriterio);
  const tot = base.reduce((a, b) => a + b, 0);
  PRODS.forEach((p, i) => {
    const share = tot > 0 ? base[i] / tot : safeDiv(1, PRODS.length);
    p.fijo_asignado = round2(FIJOS_MODO === 'pct' ? share * 100 : share * bolson);
  });
  _prevBolson = bolson;
}
function onBolsonChanged() {
  const nb = bolsonActual();
  if (FIJOS_MODO === 'monto' && _prevBolson > 0 && nb !== _prevBolson) {
    const ratio = nb / _prevBolson;
    PRODS.forEach(p => { p.fijo_asignado = round2(num(p.fijo_asignado) * ratio); });
  }
  _prevBolson = nb;
}
function switchModo(nuevo) {
  if (nuevo === FIJOS_MODO) return;
  const bolson = bolsonActual();
  PRODS.forEach(p => {
    p.fijo_asignado = round2(nuevo === 'monto'
      ? num(p.fijo_asignado) / 100 * bolson           // pct → monto
      : (bolson > 0 ? num(p.fijo_asignado) / bolson * 100 : 0)); // monto → pct
  });
  FIJOS_MODO = nuevo;
  _prevBolson = bolson;
}
function repartirResto() {
  const bolson = bolsonActual();
  const ceros = PRODS.filter(p => num(p.fijo_asignado) === 0);
  if (!ceros.length) { window.toast('No hay productos en cero para repartir', 'error'); return; }
  const asignado = PRODS.reduce((s, p) => s + num(p.fijo_asignado), 0);
  const resto = FIJOS_MODO === 'pct' ? 100 - asignado : bolson - asignado;
  if (resto <= 0) { window.toast('No queda resto para repartir', 'error'); return; }
  const base = ceros.map(baseCriterio);
  const tot = base.reduce((a, b) => a + b, 0);
  ceros.forEach((p, k) => { p.fijo_asignado = round2(resto * (tot > 0 ? base[k] / tot : 1 / ceros.length)); });
  _prevBolson = bolson;
}

// ── Carga inicial ───────────────────────────────────────────────────────
export async function loadImportacionesSim() {
  const root = document.getElementById('app-screens');
  inyectarEstilo();
  if (!PRODS.length && !CURRENT.id) nuevaSim(false);
  render();
}

function nuevaSim(repaint = true) {
  CURRENT = { id: null };
  NOMBRE = '';
  P = blankParams();
  FIJOS = [];
  FIJOS_MODO = 'pct';
  FIJOS_CRIT = 'kg';
  PRODS = [blankProd()];
  PAGOS = [];
  _prevBolson = 0;
  if (repaint) render();
}

// ── Serialización ───────────────────────────────────────────────────────
function buildDatos() {
  return {
    v: 1,
    params: {
      tc: num(P.tc), flete_declarado_usd: num(P.flete_declarado_usd), flete_real_usd: num(P.flete_real_usd),
      seguro_pct: num(P.seguro_pct), despachante_pct: num(P.despachante_pct), iibb_pct: num(P.iibb_pct), ganancias_pct: num(P.ganancias_pct)
    },
    fijos: FIJOS.map(f => ({ concepto: f.concepto || '', monto_usd: num(f.monto_usd) })),
    fijos_modo: FIJOS_MODO,
    fijos_criterio: FIJOS_CRIT,
    productos: PRODS.map(p => ({
      nombre: p.nombre || '', fob_decl_u: num(p.fob_decl_u), fob_real_u: num(p.fob_real_u),
      cantidad: num(p.cantidad), peso_u: num(p.peso_u), derechos_pct: num(p.derechos_pct),
      estadistica_pct: num(p.estadistica_pct), iva_pct: num(p.iva_pct), fijo_asignado: num(p.fijo_asignado)
    })),
    pagos: PAGOS.map(pg => ({ concepto: pg.concepto || '', monto: num(pg.monto), moneda: pg.moneda === 'ARS' ? 'ARS' : 'USD' }))
  };
}
function loadDatos(d) {
  d = d || {};
  const pa = d.params || {};
  P = {
    tc: pa.tc ?? '', flete_declarado_usd: pa.flete_declarado_usd ?? '', flete_real_usd: pa.flete_real_usd ?? '',
    seguro_pct: pa.seguro_pct ?? '', despachante_pct: pa.despachante_pct ?? '', iibb_pct: pa.iibb_pct ?? '', ganancias_pct: pa.ganancias_pct ?? ''
  };
  FIJOS = (d.fijos || []).map(f => ({ concepto: f.concepto || '', monto_usd: f.monto_usd ?? '' }));
  FIJOS_MODO = d.fijos_modo === 'monto' ? 'monto' : 'pct';
  FIJOS_CRIT = d.fijos_criterio === 'fob' ? 'fob' : 'kg';
  PRODS = (d.productos || []).map(p => ({ ...blankProd(), ...p, _open: false }));
  if (!PRODS.length) PRODS = [blankProd()];
  PAGOS = (d.pagos || []).map(pg => ({ concepto: pg.concepto || '', monto: pg.monto ?? '', moneda: pg.moneda === 'ARS' ? 'ARS' : 'USD' }));
  _prevBolson = bolsonActual();
}

async function guardar() {
  if (!NOMBRE.trim()) { window.toast('Poné un nombre a la simulación', 'error'); return; }
  const c = calc();
  const row = {
    nombre: NOMBRE.trim(),
    estado: 'borrador',
    datos: buildDatos(),
    costo_total_ars: round2(c.costoTotArs),
    credito_total_ars: round2(c.credTotArs)
  };
  try {
    if (CURRENT.id) {
      row.actualizado_en = new Date().toISOString();
      await sbPatch('importacion_sim', `id=eq.${CURRENT.id}`, row);
      window.toast('Simulación actualizada');
    } else {
      const u = sessionUser();
      row.creado_por = u ? u.usuario : null;
      const [nuevo] = await sbPost('importacion_sim', row);
      CURRENT.id = nuevo.id;
      window.toast('Simulación guardada');
      const tit = document.getElementById('imp-titulo');
      if (tit) tit.textContent = 'Editando: ' + row.nombre + ' (#' + nuevo.id + ')';
    }
  } catch (e) {
    window.toast('Error al guardar: ' + e.message, 'error');
  }
}

async function abrirListado() {
  let sims = [];
  try {
    sims = await sbGet('importacion_sim', 'select=id,nombre,estado,costo_total_ars,credito_total_ars,actualizado_en&order=actualizado_en.desc,id.desc');
  } catch (e) { window.toast('Error al listar: ' + e.message, 'error'); return; }

  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  const filas = sims.length ? sims.map(s => `
    <tr>
      <td>${esc(s.nombre)}</td>
      <td><span class="imp-badge imp-badge-${s.estado === 'confirmada' ? 'conf' : 'borr'}">${s.estado}</span></td>
      <td class="imp-mono" style="text-align:right">${ars(s.costo_total_ars)}</td>
      <td class="imp-muted" style="font-size:12px">${(s.actualizado_en || '').slice(0, 10)}</td>
      <td><button class="btn btn-ghost imp-mini" data-load="${s.id}">Cargar</button></td>
    </tr>`).join('') : `<tr><td colspan="5" class="imp-muted" style="padding:18px;text-align:center">Todavía no hay simulaciones guardadas.</td></tr>`;
  overlay.innerHTML = `<div class="modal" style="max-width:680px">
    <div class="card-title" style="margin-bottom:12px">Mis simulaciones</div>
    <div class="table-wrap"><table class="t"><thead><tr>
      <th>Nombre</th><th>Estado</th><th style="text-align:right">Costo total</th><th>Actualizada</th><th></th>
    </tr></thead><tbody>${filas}</tbody></table></div>
    <div class="modal-actions"><button class="btn btn-ghost" id="imp-cerrar">Cerrar</button></div>
  </div>`;
  document.body.appendChild(overlay);
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#imp-cerrar').addEventListener('click', close);
  overlay.querySelectorAll('[data-load]').forEach(b => b.addEventListener('click', () => {
    const s = sims.find(x => x.id === +b.dataset.load);
    if (!s) return;
    loadDatos(s.datos || {});       // listado no trae datos → refetch puntual
    cargarPorId(s.id, close);
  }));
}

async function cargarPorId(id, close) {
  try {
    const [full] = await sbGet('importacion_sim', `id=eq.${id}&select=id,nombre,datos`);
    if (!full) { window.toast('No se encontró la simulación', 'error'); return; }
    CURRENT = { id: full.id };
    NOMBRE = full.nombre || '';
    loadDatos(full.datos || {});
    if (close) close();
    render();
    window.toast('Simulación cargada');
  } catch (e) { window.toast('Error al cargar: ' + e.message, 'error'); }
}

// ── Render ──────────────────────────────────────────────────────────────
const inp = (k, i, f, val, extra = '') =>
  `<input class="imp-in" data-k="${k}" data-i="${i}" data-f="${f}" value="${esc(val)}" ${extra}>`;
const inpNum = (k, i, f, val, extra = '') =>
  inp(k, i, f, val, `type="number" step="any" inputmode="decimal" ${extra}`);

function render() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `
    <div class="card" style="margin-bottom:14px">
      <div class="imp-bar">
        <div class="grow">
          <div id="imp-titulo" class="imp-titulo">${CURRENT.id ? 'Editando: ' + esc(NOMBRE) + ' (#' + CURRENT.id + ')' : 'Simulación nueva (sin guardar)'}</div>
          <input class="input imp-nombre" id="imp-nombre" placeholder="Nombre de la simulación (ej. Notebooks marzo 2026)" value="${esc(NOMBRE)}">
        </div>
        <button class="btn btn-ghost" id="imp-nueva">Nueva</button>
        <button class="btn btn-ghost" id="imp-listar">Mis simulaciones</button>
        <button class="btn btn-primary" id="imp-guardar">Guardar</button>
      </div>
    </div>
    <div id="imp-body"></div>`;

  root.querySelector('#imp-nombre').addEventListener('input', e => { NOMBRE = e.target.value; });
  root.querySelector('#imp-nueva').addEventListener('click', () => { if (confirm('¿Empezar una simulación nueva? Se pierde lo no guardado.')) nuevaSim(); });
  root.querySelector('#imp-listar').addEventListener('click', abrirListado);
  root.querySelector('#imp-guardar').addEventListener('click', guardar);

  renderBody();
}

function renderBody() {
  const body = document.getElementById('imp-body');
  body.innerHTML = paramsCard() + fijosCard() + prodsCard() + pagosCard();
  bindDelegation(body);
  paint();
}

function paramsCard() {
  const f = (lbl, fld, hint = '') => `
    <label class="imp-fld"><span>${lbl}</span>${inpNum('param', 0, fld, P[fld])}${hint ? `<small>${hint}</small>` : ''}</label>`;
  return `<div class="card" style="margin-bottom:14px">
    <div class="card-title">Parámetros generales</div>
    <div class="imp-params">
      ${f('TC ($/USD)', 'tc', 'congela el costo en ARS')}
      ${f('Flete declarado (USD)', 'flete_declarado_usd', 'va al CIF · por peso')}
      ${f('Flete real (USD)', 'flete_real_usd', 'costo verdadero · por peso')}
      ${f('Seguro %', 'seguro_pct', 'sobre FOB+flete decl.')}
      ${f('Despachante %', 'despachante_pct', 'sobre el CIF')}
      ${f('IIBB percep. %', 'iibb_pct', 'crédito fiscal')}
      ${f('Ganancias percep. %', 'ganancias_pct', 'crédito fiscal')}
    </div>
  </div>`;
}

function fijosCard() {
  const filas = FIJOS.map((f, i) => `
    <div class="imp-fijo-item">
      ${inp('fijo', i, 'concepto', f.concepto, 'placeholder="Concepto (TCA, SEDI, transporte…)"')}
      ${inpNum('fijo', i, 'monto_usd', f.monto_usd, 'placeholder="USD"')}
      <button class="imp-del" data-act="delfijo" data-i="${i}" title="Eliminar">✕</button>
    </div>`).join('') || `<div class="imp-muted" style="padding:6px 0">Sin conceptos fijos. Sumá uno con “+ Concepto”.</div>`;
  const pill = (val, lbl, act) => `<button class="pill imp-pill" data-act="${act}" data-val="${val}">${lbl}</button>`;
  return `<div class="card" style="margin-bottom:14px">
    <div class="card-title">Costos fijos (bolsón)</div>
    <div class="imp-toolbar">
      <div class="imp-seg"><span class="imp-seg-lbl">Modo</span>${pill('pct', '%', 'modo')}${pill('monto', '$', 'modo')}</div>
      <div class="imp-seg"><span class="imp-seg-lbl">Precarga</span>${pill('kg', 'por kg', 'crit')}${pill('fob', 'por FOB', 'crit')}</div>
      <button class="btn btn-ghost imp-mini" data-act="precargar">Precargar</button>
      <button class="btn btn-ghost imp-mini" data-act="repartir">Repartir resto</button>
      <div class="grow"></div>
      <button class="btn btn-ghost imp-mini" data-act="addfijo">+ Concepto</button>
    </div>
    <div class="imp-fijos">${filas}</div>
    <div class="imp-reparto">
      <span>Bolsón total: <b id="imp-bolson" class="imp-mono"></b></span>
      <span id="imp-reparto-ind" class="imp-mono"></span>
    </div>
  </div>`;
}

function prodHeadFijo() { return FIJOS_MODO === 'pct' ? 'Fijo %' : 'Fijo $'; }

function prodsCard() {
  return `<div class="card" style="margin-bottom:14px">
    <div class="imp-toolbar"><div class="card-title" style="margin:0">Productos</div><div class="grow"></div>
      <button class="btn btn-ghost imp-mini" data-act="addprod">+ Producto</button></div>
    <div class="table-wrap"><table class="t imp-prods"><thead><tr>
      <th></th><th>Producto</th><th>Cant.</th><th>Peso u (kg)</th>
      <th>FOB decl. u</th><th>FOB real u</th><th>Der. %</th><th>Estad. %</th><th>IVA %</th>
      <th id="imp-th-fijo">${prodHeadFijo()}</th>
      <th style="text-align:right">Costo u USD</th><th style="text-align:right">Costo u ARS</th>
      <th style="text-align:right">Costo total USD</th><th></th>
    </tr></thead><tbody id="imp-prods-body">${prodsRows()}</tbody>
    <tfoot><tr class="imp-tfoot">
      <td colspan="12" style="text-align:right">Totales</td>
      <td class="imp-mono" style="text-align:right" id="imp-tot-costo-usd"></td><td></td>
    </tr>
    <tr class="imp-tfoot imp-tfoot-sub">
      <td colspan="10"></td>
      <td colspan="2" style="text-align:right">Costo capitalizado · Crédito fiscal</td>
      <td colspan="2"><div id="imp-tot-resumen" class="imp-mono"></div></td>
    </tr></tfoot></table></div>
  </div>`;
}

function prodsRows() {
  return PRODS.map((p, i) => `
    <tr data-row="${i}">
      <td><button class="imp-exp" data-act="exp" data-i="${i}" title="Detalle">${p._open ? '▾' : '▸'}</button></td>
      <td>${inp('prod', i, 'nombre', p.nombre, 'placeholder="Nombre"')}</td>
      <td>${inpNum('prod', i, 'cantidad', p.cantidad)}</td>
      <td>${inpNum('prod', i, 'peso_u', p.peso_u)}</td>
      <td>${inpNum('prod', i, 'fob_decl_u', p.fob_decl_u)}</td>
      <td>${inpNum('prod', i, 'fob_real_u', p.fob_real_u)}</td>
      <td>${inpNum('prod', i, 'derechos_pct', p.derechos_pct)}</td>
      <td>${inpNum('prod', i, 'estadistica_pct', p.estadistica_pct)}</td>
      <td>${inpNum('prod', i, 'iva_pct', p.iva_pct)}</td>
      <td><input class="imp-in" id="prodfijo-${i}" data-k="prod" data-i="${i}" data-f="fijo_asignado" type="number" step="any" inputmode="decimal" value="${esc(p.fijo_asignado)}"></td>
      <td class="imp-mono" style="text-align:right" id="pu-usd-${i}"></td>
      <td class="imp-mono" style="text-align:right" id="pu-ars-${i}"></td>
      <td class="imp-mono" style="text-align:right" id="pt-usd-${i}"></td>
      <td><button class="imp-del" data-act="delprod" data-i="${i}" title="Eliminar">✕</button></td>
    </tr>
    <tr class="imp-detail" id="imp-det-${i}" style="display:${p._open ? 'table-row' : 'none'}">
      <td colspan="14"><div id="imp-det-c-${i}"></div></td>
    </tr>`).join('');
}

function pagosCard() {
  const filas = PAGOS.map((pg, i) => `
    <div class="imp-pago-item">
      ${inp('pago', i, 'concepto', pg.concepto, 'placeholder="Concepto del pago"')}
      ${inpNum('pago', i, 'monto', pg.monto, 'placeholder="Monto"')}
      <select class="imp-in imp-sel" data-k="pago" data-i="${i}" data-f="moneda">
        <option value="USD" ${pg.moneda === 'USD' ? 'selected' : ''}>USD</option>
        <option value="ARS" ${pg.moneda === 'ARS' ? 'selected' : ''}>ARS</option>
      </select>
      <button class="imp-del" data-act="delpago" data-i="${i}" title="Eliminar">✕</button>
    </div>`).join('') || `<div class="imp-muted" style="padding:6px 0">Sin pagos cargados.</div>`;
  return `<div class="card" style="margin-bottom:14px">
    <div class="imp-toolbar"><div class="card-title" style="margin:0">Pagos y control de cierre</div><div class="grow"></div>
      <button class="btn btn-ghost imp-mini" data-act="addpago">+ Pago</button></div>
    <div class="imp-pagos">${filas}</div>
    <div class="imp-cierre" id="imp-cierre"></div>
  </div>`;
}

// ── Repintado de celdas calculadas (sin re-render de inputs) ─────────────
function paint() {
  const c = calc();
  const set = (id, txt) => { const el = document.getElementById(id); if (el) el.textContent = txt; };

  set('imp-bolson', usd(c.bolson));
  const asignado = PRODS.reduce((s, p) => s + num(p.fijo_asignado), 0);
  const ind = document.getElementById('imp-reparto-ind');
  if (ind) {
    if (FIJOS_MODO === 'pct') {
      const falta = round2(100 - asignado);
      ind.textContent = `Asignado: ${pctTxt(asignado)} · Falta a 100%: ${pctTxt(falta)}`;
      ind.className = 'imp-mono ' + (Math.abs(falta) < 0.01 ? 'imp-ok' : 'imp-warn');
    } else {
      const resto = round2(c.bolson - asignado);
      ind.textContent = `Asignado: ${usd(asignado)} · Resto del bolsón: ${usd(resto)}`;
      ind.className = 'imp-mono ' + (Math.abs(resto) < 0.01 ? 'imp-ok' : 'imp-warn');
    }
  }

  c.rows.forEach((r, i) => {
    set(`pu-usd-${i}`, usd(r.costoUnitUsd));
    set(`pu-ars-${i}`, ars(r.costoUnitArs));
    set(`pt-usd-${i}`, usd(r.costoUsd));
    if (PRODS[i] && PRODS[i]._open) {
      const cont = document.getElementById(`imp-det-c-${i}`);
      if (cont) cont.innerHTML = detalleHTML(r, c.tc);
    }
  });

  set('imp-tot-costo-usd', usd(c.costoTotUsd));
  const res = document.getElementById('imp-tot-resumen');
  if (res) res.innerHTML =
    `Costo: <b>${usd(c.costoTotUsd)}</b> · ${ars(c.costoTotArs)}<br>Crédito: <b>${usd(c.credTotUsd)}</b> · ${ars(c.credTotArs)}`;

  const cierre = document.getElementById('imp-cierre');
  if (cierre) {
    const ok = Math.abs(c.diff) < 0.01;
    cierre.className = 'imp-cierre ' + (ok ? 'imp-cierre-ok' : 'imp-cierre-bad');
    cierre.innerHTML = `
      <div class="imp-cierre-row"><span>Σ Pagos (USD)</span><b class="imp-mono">${usd(c.pagosUsd)}</b></div>
      <div class="imp-cierre-row"><span>Costo capitalizado + Crédito fiscal (USD)</span><b class="imp-mono">${usd(c.esperado)}</b></div>
      <div class="imp-cierre-row imp-cierre-diff"><span>${ok ? '✓ Cierra' : 'Diferencia'}</span><b class="imp-mono">${usd(c.diff)}</b></div>
      ${ok ? '' : `<div class="imp-cierre-hint">${c.diff > 0 ? 'Pagaste de más respecto al costo+crédito: falta cargar un gasto como costo.' : 'Falta anotar un pago (o sobra costo cargado).'}</div>`}`;
  }
}

function detalleHTML(r, tc) {
  const cant = r.cant || 0;
  const u = v => safeDiv(v, cant);
  const line = (lbl, totU, strong) => `
    <tr class="${strong ? 'imp-d-strong' : ''}">
      <td>${lbl}</td>
      <td class="imp-mono">${usd(u(totU))}</td><td class="imp-mono">${usd(totU)}</td>
      <td class="imp-mono">${ars(u(totU) * tc)}</td><td class="imp-mono">${ars(totU * tc)}</td>
    </tr>`;
  const head = `<thead><tr><th></th><th>USD unit</th><th>USD total</th><th>ARS unit</th><th>ARS total</th></tr></thead>`;
  return `<div class="imp-det-grid">
    <div>
      <div class="imp-det-h">Costo s/IVA (capitaliza al lote)</div>
      <table class="imp-dt">${head}<tbody>
        ${line('FOB declarado', r.fobDeclT)}
        ${line('No declarado (subfact.)', r.noDeclarado)}
        ${line('Flete real (por peso)', r.fleteReal)}
        ${line('Seguro', r.seguro)}
        ${line('Derechos', r.derechos)}
        ${line('Estadística', r.estadistica)}
        ${line('Despachante', r.despachante)}
        ${line('Fijos', r.fijo)}
        ${line('Costo s/IVA', r.costoUsd, true)}
      </tbody></table>
    </div>
    <div>
      <div class="imp-det-h">Crédito fiscal (NO es costo → Posición Fiscal)</div>
      <table class="imp-dt">${head}<tbody>
        ${line('IVA', r.iva)}
        ${line('IIBB percepción', r.iibb)}
        ${line('Ganancias percepción', r.ganancias)}
        ${line('Crédito fiscal', r.creditoUsd, true)}
      </tbody></table>
      <div class="imp-det-cif">CIF: <b class="imp-mono">${usd(r.cif)}</b></div>
    </div>
  </div>`;
}

// ── Delegación de eventos ────────────────────────────────────────────────
function syncProdFijoInputs() {
  PRODS.forEach((p, i) => { const el = document.getElementById(`prodfijo-${i}`); if (el) el.value = p.fijo_asignado; });
}
function pintarPills() {
  document.querySelectorAll('.imp-pill').forEach(b => {
    const on = (b.dataset.act === 'modo' && b.dataset.val === FIJOS_MODO) || (b.dataset.act === 'crit' && b.dataset.val === FIJOS_CRIT);
    b.classList.toggle('active', on);
  });
  const th = document.getElementById('imp-th-fijo'); if (th) th.textContent = prodHeadFijo();
}

function bindDelegation(body) {
  const onEdit = e => {
    const t = e.target;
    if (!t.dataset || !t.dataset.k) return;
    const k = t.dataset.k, i = +t.dataset.i, f = t.dataset.f, v = t.value;
    if (k === 'param') P[f] = v;
    else if (k === 'fijo') { FIJOS[i][f] = v; if (f === 'monto_usd') { onBolsonChanged(); syncProdFijoInputs(); } }
    else if (k === 'prod') PRODS[i][f] = v;
    else if (k === 'pago') PAGOS[i][f] = v;
    paint();
  };
  body.addEventListener('input', onEdit);
  body.addEventListener('change', onEdit);

  body.addEventListener('click', e => {
    const btn = e.target.closest('[data-act]');
    if (!btn) return;
    const act = btn.dataset.act, i = +btn.dataset.i;
    switch (act) {
      case 'addfijo': FIJOS.push({ concepto: '', monto_usd: '' }); renderBody(); break;
      case 'delfijo': FIJOS.splice(i, 1); onBolsonChanged(); renderBody(); break;
      case 'addprod': PRODS.push(blankProd()); renderBody(); break;
      case 'delprod': PRODS.splice(i, 1); if (!PRODS.length) PRODS = [blankProd()]; renderBody(); break;
      case 'addpago': PAGOS.push({ concepto: '', monto: '', moneda: 'USD' }); renderBody(); break;
      case 'delpago': PAGOS.splice(i, 1); renderBody(); break;
      case 'exp': {
        PRODS[i]._open = !PRODS[i]._open;
        const tr = document.getElementById(`imp-det-${i}`);
        if (tr) tr.style.display = PRODS[i]._open ? 'table-row' : 'none';
        btn.textContent = PRODS[i]._open ? '▾' : '▸';
        paint();
        break;
      }
      case 'modo': switchModo(btn.dataset.val); syncProdFijoInputs(); pintarPills(); paint(); break;
      case 'crit': FIJOS_CRIT = btn.dataset.val; pintarPills(); break;
      case 'precargar': precargarFijos(); syncProdFijoInputs(); paint(); break;
      case 'repartir': repartirResto(); syncProdFijoInputs(); paint(); break;
    }
  });
  pintarPills();
}

// ── Estilo scopeado (imp-*) ──────────────────────────────────────────────
function inyectarEstilo() {
  if (document.getElementById('imp-style')) return;
  const css = `
    .imp-bar{display:flex;align-items:flex-end;gap:10px}
    .imp-bar .grow{flex:1}
    .imp-titulo{font-size:13px;color:#78716C;margin-bottom:4px}
    .imp-nombre{width:100%;box-sizing:border-box}
    .imp-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .imp-muted{color:#78716C}
    .imp-params{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
    .imp-fld{display:flex;flex-direction:column;gap:4px;font-size:13px;color:#44403C}
    .imp-fld small{color:#A8A29E;font-size:11px}
    .imp-in{width:100%;box-sizing:border-box;padding:8px 9px;border:1px solid #E7E5E4;border-radius:6px;font-size:14px;font-family:inherit;background:#fff}
    .imp-in:focus{outline:none;border-color:#D97706}
    .imp-toolbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px}
    .imp-toolbar .grow{flex:1}
    .imp-seg{display:flex;align-items:center;gap:4px}
    .imp-seg-lbl{font-size:12px;color:#78716C;margin-right:2px}
    .imp-pill{cursor:pointer}
    .imp-mini{padding:6px 12px;font-size:13px}
    .imp-del{border:0;background:transparent;color:#A8A29E;cursor:pointer;font-size:14px;padding:4px 6px}
    .imp-del:hover{color:#B91C1C}
    .imp-fijo-item{display:grid;grid-template-columns:1fr 160px 32px;gap:8px;margin-bottom:8px;align-items:center}
    .imp-pago-item{display:grid;grid-template-columns:1fr 160px 90px 32px;gap:8px;margin-bottom:8px;align-items:center}
    .imp-reparto{display:flex;justify-content:space-between;align-items:center;border-top:1px solid #E7E5E4;margin-top:10px;padding-top:10px;font-size:14px}
    .imp-ok{color:#0F6E56;font-weight:600}
    .imp-warn{color:#854F0B;font-weight:600}
    #imp-body .table-wrap{border:1px solid #E7E5E4;border-radius:12px;overflow:auto;background:#fff}
    #imp-body table.t{width:100%;border-collapse:collapse;font-size:13px}
    #imp-body table.t thead th{background:#FAFAF9;color:#78716C;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.03em;padding:9px 8px;border-bottom:1px solid #E7E5E4;text-align:left;white-space:nowrap}
    #imp-body table.t tbody td{padding:6px 8px;border-bottom:1px solid #F5F5F4;vertical-align:middle;color:#1C1917}
    .imp-prods .imp-in{min-width:64px;padding:6px 7px;font-size:13px}
    .imp-prods td:nth-child(2) .imp-in{min-width:120px}
    .imp-exp{border:1px solid #E7E5E4;background:#fff;border-radius:6px;cursor:pointer;width:26px;height:26px;color:#78716C}
    .imp-tfoot td{background:#FAFAF9;font-weight:600;border-top:1px solid #E7E5E4!important}
    .imp-tfoot-sub td{font-weight:500;color:#57534E}
    .imp-detail td{background:#FCFBF9;padding:14px 16px!important}
    .imp-det-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}
    .imp-det-h{font-size:12px;font-weight:700;color:#44403C;text-transform:uppercase;letter-spacing:.03em;margin-bottom:6px}
    .imp-dt{width:100%;border-collapse:collapse;font-size:12px}
    .imp-dt th{text-align:right;color:#A8A29E;font-weight:600;font-size:10px;padding:3px 6px;border-bottom:1px solid #EEE}
    .imp-dt th:first-child{text-align:left}
    .imp-dt td{padding:3px 6px;text-align:right;color:#44403C}
    .imp-dt td:first-child{text-align:left;color:#1C1917}
    .imp-d-strong td{font-weight:700;border-top:1px solid #E7E5E4;color:#1C1917}
    .imp-det-cif{margin-top:8px;font-size:12px;color:#78716C;text-align:right}
    .imp-cierre{border-radius:10px;padding:14px 16px;margin-top:8px;border:1px solid #E7E5E4}
    .imp-cierre-ok{background:#E1F5EE;border-color:#A7E0CC}
    .imp-cierre-bad{background:#FBF1E6;border-color:#EBCBA0}
    .imp-cierre-row{display:flex;justify-content:space-between;font-size:14px;padding:3px 0}
    .imp-cierre-diff{border-top:1px dashed rgba(0,0,0,.15);margin-top:6px;padding-top:8px;font-size:15px}
    .imp-cierre-hint{font-size:12px;color:#854F0B;margin-top:6px}
    .imp-badge{font-size:12px;padding:2px 8px;border-radius:6px}
    .imp-badge-borr{background:#FAEEDA;color:#854F0B}
    .imp-badge-conf{background:#E1F5EE;color:#0F6E56}
    @media(max-width:760px){.imp-det-grid{grid-template-columns:1fr}}
  `;
  const st = document.createElement('style');
  st.id = 'imp-style';
  st.textContent = css;
  document.head.appendChild(st);
}
