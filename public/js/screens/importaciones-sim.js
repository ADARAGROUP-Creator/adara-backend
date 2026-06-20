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
//  · Cierre: Σ pagos (USD) = costo capitalizado + crédito fiscal. CADA pago lleva su propio TC.

// ── Estado del módulo ───────────────────────────────────────────────────
let CURRENT = { id: null };
let NOMBRE = '';
let P = blankParams();
let FIJOS = [];            // { concepto, monto_usd }
let FIJOS_MODO = 'pct';    // 'pct' | 'monto'
let FIJOS_CRIT = 'kg';     // 'kg'  | 'fob'
let PRODS = [];            // { nombre, fob_decl_u, fob_real_u, cantidad, peso_u, derechos_pct, estadistica_pct, iva_pct, fijo_asignado, _open }
let PAGOS = [];            // { concepto, monto, moneda, tc }
let _prevBolson = 0;

function blankParams() {
  return { tc: '', flete_declarado_usd: '', flete_real_usd: '', seguro_pct: '', seguro_monto: '', despachante_pct: '', despachante_monto: '', iibb_pct: '', ganancias_pct: '', coima_pct: '' };
}
function blankProd() {
  return { nombre: '', fob_real_u: '', fob_decl_u: '', cantidad: '', peso_u: '', derechos_pct: '', derechos_pct_decl: '', estadistica_pct: '', estadistica_pct_decl: '', imp_internos_pct: '', iva_pct: 21, fijo_asignado: 0, declaro_distinto: false, _open: false };
}
function blankPago() {
  return { concepto: '', monto: '', moneda: 'USD', tc: num(P.tc) > 0 ? P.tc : '' };
}

// ── Helpers ─────────────────────────────────────────────────────────────
function num(n) { const v = Number(n); return isFinite(v) ? v : 0; }
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
  const segMonto = num(P.seguro_monto);
  const despPct = num(P.despachante_pct) / 100;
  const despMonto = num(P.despachante_monto);
  const iibbPct = num(P.iibb_pct) / 100;
  const ganPct = num(P.ganancias_pct) / 100;
  const coimaPct = num(P.coima_pct) / 100;
  const bolsonManual = bolsonActual();
  const fleteDiff = fleteReal - fleteDecl;   // (real − declarado) → va al bolsón de fijos
  // Override de declaración: vacío ⇒ usa el real (sin subfacturación / sin coima).
  const eff = (ov, real) => (ov === '' || ov === null || ov === undefined) ? num(real) : num(ov);

  const rows = PRODS.map(p => {
    const cant = num(p.cantidad);
    return { p, cant, pesoT: num(p.peso_u) * cant, fobDeclT: eff(p.fob_decl_u, p.fob_real_u) * cant, fobRealT: num(p.fob_real_u) * cant };
  });
  const sPeso = rows.reduce((s, r) => s + r.pesoT, 0);
  const sFob = rows.reduce((s, r) => s + r.fobDeclT, 0);
  const fobRealTot = rows.reduce((s, r) => s + r.fobRealT, 0);
  // Seguro: si hay monto cargado pisa el %. Se prorratea por FOB.
  const seguroTotal = segMonto > 0 ? segMonto : segPct * (sFob + fleteDecl);

  // CIF usa SIEMPRE el flete DECLARADO (base aduanera).
  rows.forEach(r => {
    r.fleteDeclShare = fleteDecl * safeDiv(r.pesoT, sPeso);
    r.seguro = seguroTotal * safeDiv(r.fobDeclT, sFob);
    r.cif = r.fobDeclT + r.fleteDeclShare + r.seguro;
  });
  const sCif = rows.reduce((s, r) => s + r.cif, 0);
  // Despachante: si hay monto cargado pisa el %. NO entra al CIF → va al bolsón de fijos.
  const despTotal = despMonto > 0 ? despMonto : despPct * sCif;

  // Bolsón EFECTIVO = fijos manuales + diferencia de flete + despachante.
  // Se reparte entre productos por fijo_asignado (como el usuario lo marca).
  const bolson = bolsonManual + fleteDiff + despTotal;

  rows.forEach(r => {
    const p = r.p;
    // Tasas: el campo primario es el REAL; el declarado es override (vacío = real).
    r.derechos = r.cif * eff(p.derechos_pct_decl, p.derechos_pct) / 100;       // lo que PAGÁS (declarado)
    r.estadistica = r.cif * eff(p.estadistica_pct_decl, p.estadistica_pct) / 100;
    r.impInternos = r.cif * num(p.imp_internos_pct) / 100;       // se trata como derechos
    r.baseIva = r.cif + r.derechos + r.estadistica + r.impInternos;
    r.iva = r.baseIva * num(p.iva_pct) / 100;
    r.iibb = r.baseIva * iibbPct;
    r.ganancias = r.baseIva * ganPct;
    // Reclasificación: ahorro = (real − declarado) de derechos+estad; coima = % del ahorro.
    r.derReal = r.cif * num(p.derechos_pct) / 100;
    r.estReal = r.cif * num(p.estadistica_pct) / 100;
    r.ahorro = (r.derReal - r.derechos) + (r.estReal - r.estadistica);
    r.coima = r.ahorro * coimaPct;                                // capitaliza, SIN crédito
    // El COSTO de flete usa el DECLARADO (por peso); la diferencia ya está en el bolsón.
    r.fleteCosto = r.fleteDeclShare;
    r.fijo = FIJOS_MODO === 'pct' ? num(p.fijo_asignado) / 100 * bolson : num(p.fijo_asignado);
    r.noDeclarado = r.fobRealT - r.fobDeclT;
    // Despachante NO es línea propia del costo: vive dentro de r.fijo (bolsón).
    r.costoUsd = r.fobRealT + r.fleteCosto + r.seguro + r.derechos + r.estadistica + r.impInternos + r.coima + r.fijo;
    r.creditoUsd = r.iva + r.iibb + r.ganancias;
    r.costoUnitUsd = safeDiv(r.costoUsd, r.cant);
    r.costoUnitArs = r.costoUnitUsd * tc;
  });

  const derechosTot = rows.reduce((s, r) => s + r.derechos, 0);
  const estadTot = rows.reduce((s, r) => s + r.estadistica, 0);
  const impIntTot = rows.reduce((s, r) => s + r.impInternos, 0);
  const ivaTot = rows.reduce((s, r) => s + r.iva, 0);
  const iibbTot = rows.reduce((s, r) => s + r.iibb, 0);
  const ganTot = rows.reduce((s, r) => s + r.ganancias, 0);
  const ahorroTot = rows.reduce((s, r) => s + r.ahorro, 0);
  const coimaTot = rows.reduce((s, r) => s + r.coima, 0);
  const baseIvaTot = sCif + derechosTot + estadTot + impIntTot;
  const tributosTot = derechosTot + estadTot + impIntTot + ivaTot + iibbTot + ganTot;

  const costoTotUsd = rows.reduce((s, r) => s + r.costoUsd, 0);
  const credTotUsd = rows.reduce((s, r) => s + r.creditoUsd, 0);

  // Cada pago lleva su propio TC: los ARS se convierten con el TC de su fila
  // (fallback al TC general si la fila no tiene), los USD van directo.
  const pagosRows = PAGOS.map(pg => {
    const t = num(pg.tc) > 0 ? num(pg.tc) : tc;
    const u = pg.moneda === 'ARS' ? safeDiv(num(pg.monto), t) : num(pg.monto);
    return { usd: u, t };
  });
  const pagosUsd = pagosRows.reduce((s, r) => s + r.usd, 0);
  const esperado = costoTotUsd + credTotUsd;

  return {
    tc, bolson, bolsonManual, fleteDiff, sPeso, sFob, fobRealTot, sCif, seguroTotal, despTotal, rows, pagosRows,
    derechosTot, estadTot, impIntTot, ivaTot, iibbTot, ganTot, baseIvaTot, tributosTot,
    ahorroTot, coimaTot, netoReclasif: ahorroTot - coimaTot,
    costoTotUsd, credTotUsd, costoTotArs: costoTotUsd * tc, credTotArs: credTotUsd * tc,
    pagosUsd, esperado, diff: pagosUsd - esperado
  };
}

// Bolsón efectivo (manual + diferencia de flete + despachante). Lo usa el reparto.
function bolsonEfectivo() { return calc().bolson; }

// ── Reparto de fijos ────────────────────────────────────────────────────
function baseCriterio(p) {
  return FIJOS_CRIT === 'kg' ? num(p.peso_u) * num(p.cantidad) : num(p.fob_decl_u) * num(p.cantidad);
}
function precargarFijos() {
  const bolson = bolsonEfectivo();
  const base = PRODS.map(baseCriterio);
  const tot = base.reduce((a, b) => a + b, 0);
  PRODS.forEach((p, i) => {
    const share = tot > 0 ? base[i] / tot : safeDiv(1, PRODS.length);
    p.fijo_asignado = round2(FIJOS_MODO === 'pct' ? share * 100 : share * bolson);
  });
  _prevBolson = bolson;
}
function onBolsonChanged() {
  const nb = bolsonEfectivo();
  if (FIJOS_MODO === 'monto' && _prevBolson > 0 && nb !== _prevBolson) {
    const ratio = nb / _prevBolson;
    PRODS.forEach(p => { p.fijo_asignado = round2(num(p.fijo_asignado) * ratio); });
  }
  _prevBolson = nb;
}
function switchModo(nuevo) {
  if (nuevo === FIJOS_MODO) return;
  const bolson = bolsonEfectivo();
  PRODS.forEach(p => {
    p.fijo_asignado = round2(nuevo === 'monto'
      ? num(p.fijo_asignado) / 100 * bolson           // pct → monto
      : (bolson > 0 ? num(p.fijo_asignado) / bolson * 100 : 0)); // monto → pct
  });
  FIJOS_MODO = nuevo;
  _prevBolson = bolson;
}
function repartirResto() {
  const bolson = bolsonEfectivo();
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
    v: 2,
    params: {
      tc: num(P.tc), flete_declarado_usd: num(P.flete_declarado_usd), flete_real_usd: num(P.flete_real_usd),
      seguro_pct: num(P.seguro_pct), seguro_monto: num(P.seguro_monto),
      despachante_pct: num(P.despachante_pct), despachante_monto: num(P.despachante_monto),
      iibb_pct: num(P.iibb_pct), ganancias_pct: num(P.ganancias_pct), coima_pct: num(P.coima_pct)
    },
    fijos: FIJOS.map(f => ({ concepto: f.concepto || '', monto_usd: num(f.monto_usd) })),
    fijos_modo: FIJOS_MODO,
    fijos_criterio: FIJOS_CRIT,
    productos: PRODS.map(p => {
      const ov = x => (x === '' || x == null) ? null : num(x);
      return {
        nombre: p.nombre || '', fob_real_u: num(p.fob_real_u), fob_decl_u: ov(p.fob_decl_u),
        cantidad: num(p.cantidad), peso_u: num(p.peso_u),
        derechos_pct: num(p.derechos_pct), derechos_pct_decl: ov(p.derechos_pct_decl),
        estadistica_pct: num(p.estadistica_pct), estadistica_pct_decl: ov(p.estadistica_pct_decl),
        imp_internos_pct: num(p.imp_internos_pct), iva_pct: num(p.iva_pct),
        fijo_asignado: num(p.fijo_asignado), declaro_distinto: !!p.declaro_distinto
      };
    }),
    pagos: PAGOS.map(pg => ({ concepto: pg.concepto || '', monto: num(pg.monto), moneda: pg.moneda === 'ARS' ? 'ARS' : 'USD', tc: num(pg.tc) }))
  };
}
function loadDatos(d) {
  d = d || {};
  const pa = d.params || {};
  P = {
    tc: pa.tc ?? '', flete_declarado_usd: pa.flete_declarado_usd ?? '', flete_real_usd: pa.flete_real_usd ?? '',
    seguro_pct: pa.seguro_pct ?? '', seguro_monto: pa.seguro_monto ?? '',
    despachante_pct: pa.despachante_pct ?? '', despachante_monto: pa.despachante_monto ?? '',
    iibb_pct: pa.iibb_pct ?? '', ganancias_pct: pa.ganancias_pct ?? '', coima_pct: pa.coima_pct ?? ''
  };
  FIJOS = (d.fijos || []).map(f => ({ concepto: f.concepto || '', monto_usd: f.monto_usd ?? '' }));
  FIJOS_MODO = d.fijos_modo === 'monto' ? 'monto' : 'pct';
  FIJOS_CRIT = d.fijos_criterio === 'fob' ? 'fob' : 'kg';
  PRODS = (d.productos || []).map(p => num(d.v) >= 2 ? loadProdV2(p) : migrarProdV1(p));
  if (!PRODS.length) PRODS = [blankProd()];
  // Compat: pagos viejos sin tc → toman el TC general guardado como fallback.
  PAGOS = (d.pagos || []).map(pg => ({
    concepto: pg.concepto || '', monto: pg.monto ?? '',
    moneda: pg.moneda === 'ARS' ? 'ARS' : 'USD',
    tc: (pg.tc ?? '') === '' || num(pg.tc) === 0 ? (pa.tc ?? '') : pg.tc
  }));
  _prevBolson = bolsonEfectivo();
}

function loadProdV2(p) {
  return {
    ...blankProd(),
    nombre: p.nombre || '', fob_real_u: p.fob_real_u ?? '', fob_decl_u: p.fob_decl_u ?? '',
    cantidad: p.cantidad ?? '', peso_u: p.peso_u ?? '',
    derechos_pct: p.derechos_pct ?? '', derechos_pct_decl: p.derechos_pct_decl ?? '',
    estadistica_pct: p.estadistica_pct ?? '', estadistica_pct_decl: p.estadistica_pct_decl ?? '',
    imp_internos_pct: p.imp_internos_pct ?? '', iva_pct: p.iva_pct ?? 21,
    fijo_asignado: p.fijo_asignado ?? 0, declaro_distinto: !!p.declaro_distinto, _open: false
  };
}
// Migración v1→v2: en v1 el campo primario era el DECLARADO y el real era opcional.
// En v2 el primario es el REAL y el declarado es override. Preserva resultados.
function migrarProdV1(p) {
  const realDer = (p.derechos_pct_real == null || p.derechos_pct_real === '') ? num(p.derechos_pct) : num(p.derechos_pct_real);
  const ovDer = num(p.derechos_pct) !== realDer ? num(p.derechos_pct) : '';
  const realEst = (p.estadistica_pct_real == null || p.estadistica_pct_real === '') ? num(p.estadistica_pct) : num(p.estadistica_pct_real);
  const ovEst = num(p.estadistica_pct) !== realEst ? num(p.estadistica_pct) : '';
  const realFob = (p.fob_real_u == null || p.fob_real_u === '' || num(p.fob_real_u) === 0) ? num(p.fob_decl_u) : num(p.fob_real_u);
  const ovFob = num(p.fob_decl_u) !== realFob ? num(p.fob_decl_u) : '';
  return {
    ...blankProd(),
    nombre: p.nombre || '', fob_real_u: realFob || '', fob_decl_u: ovFob,
    cantidad: p.cantidad ?? '', peso_u: p.peso_u ?? '',
    derechos_pct: realDer || '', derechos_pct_decl: ovDer,
    estadistica_pct: realEst || '', estadistica_pct_decl: ovEst,
    imp_internos_pct: p.imp_internos_pct ?? '', iva_pct: p.iva_pct ?? 21,
    fijo_asignado: p.fijo_asignado ?? 0,
    declaro_distinto: ovDer !== '' || ovEst !== '' || ovFob !== '', _open: false
  };
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
  overlay.querySelectorAll('[data-load]').forEach(b => b.addEventListener('click', () => cargarPorId(+b.dataset.load, close)));
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
const field = (lbl, htmlInput, hint = '') =>
  `<label class="imp-field"><span class="imp-flbl">${lbl}</span>${htmlInput}${hint ? `<small>${hint}</small>` : ''}</label>`;

function render() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="imp-wrap">
    <div class="card imp-topbar">
      <div class="imp-topbar-l">
        <div id="imp-titulo" class="imp-titulo">${CURRENT.id ? 'Editando: ' + esc(NOMBRE) + ' (#' + CURRENT.id + ')' : 'Simulación nueva (sin guardar)'}</div>
        <input class="imp-in imp-nombre" id="imp-nombre" placeholder="Nombre de la simulación (ej. Notebooks marzo 2026)" value="${esc(NOMBRE)}">
      </div>
      <div class="imp-topbar-r">
        <button class="btn btn-ghost" id="imp-nueva">Nueva</button>
        <button class="btn btn-ghost" id="imp-listar">Mis simulaciones</button>
        <button class="btn btn-primary" id="imp-guardar">Guardar</button>
      </div>
    </div>
    <div id="imp-body"></div>
  </div>`;

  root.querySelector('#imp-nombre').addEventListener('input', e => { NOMBRE = e.target.value; });
  root.querySelector('#imp-nueva').addEventListener('click', () => { if (confirm('¿Empezar una simulación nueva? Se pierde lo no guardado.')) nuevaSim(); });
  root.querySelector('#imp-listar').addEventListener('click', abrirListado);
  root.querySelector('#imp-guardar').addEventListener('click', guardar);

  bindDelegation(document.getElementById('imp-body'));   // una sola vez por render
  renderBody();
}

function renderBody() {
  const body = document.getElementById('imp-body');
  body.innerHTML = paramsCard() + fijosCard() + prodsSection() + totalesCard() + reclasifCard() + tablaTotalCard() + pagosCard();
  pintarPills();
  paint();
}

function paramsCard() {
  const f = (lbl, fld, hint = '') => field(lbl, inpNum('param', 0, fld, P[fld]), hint);
  return `<div class="card imp-card">
    <div class="card-title">Detalles generales</div>
    <div class="imp-params">
      ${f('TC ($/USD)', 'tc', 'congela el costo en ARS')}
      ${f('Flete declarado (USD)', 'flete_declarado_usd', 'va al CIF · por peso')}
      ${f('Flete real (USD)', 'flete_real_usd', 'costo real · por peso')}
      ${f('Seguro %', 'seguro_pct', 'sobre FOB+flete decl.')}
      ${f('Seguro $ (USD)', 'seguro_monto', 'si lo cargás, pisa el %')}
      ${f('Despachante %', 'despachante_pct', 'sobre el CIF')}
      ${f('Despachante $ (USD)', 'despachante_monto', 'si lo cargás, pisa el %')}
      ${f('IIBB percep. %', 'iibb_pct', 'crédito fiscal')}
      ${f('Ganancias percep. %', 'ganancias_pct', 'crédito fiscal')}
      ${f('Coima %', 'coima_pct', 'sobre derechos+estad. evitados')}
    </div>
    <div class="imp-bases" id="imp-bases"></div>
  </div>`;
}

function fijosCard() {
  const filasManual = FIJOS.length ? FIJOS.map((f, i) => `
    <div class="imp-fijo-item">
      ${inp('fijo', i, 'concepto', f.concepto, 'placeholder="Concepto (TCA, SEDI, transporte…)"')}
      ${inpNum('fijo', i, 'monto_usd', f.monto_usd, 'placeholder="USD"')}
      <button class="imp-del" data-act="delfijo" data-i="${i}" title="Eliminar">✕</button>
    </div>`).join('') : `<div class="imp-muted imp-empty">Sin conceptos fijos manuales. Sumá uno con “+ Concepto”.</div>`;
  const filasAuto = `
    <div class="imp-fijo-item imp-fijo-auto">
      <span class="imp-fijo-autolbl">Diferencia flete (real − decl.) <span class="imp-auto-badge">auto</span></span>
      <b class="imp-mono imp-fijo-automonto" id="imp-fijo-fletediff">—</b>
      <span></span>
    </div>
    <div class="imp-fijo-item imp-fijo-auto">
      <span class="imp-fijo-autolbl">Despachante <span class="imp-auto-badge">auto</span></span>
      <b class="imp-mono imp-fijo-automonto" id="imp-fijo-despachante">—</b>
      <span></span>
    </div>`;
  const pill = (val, lbl, act) => `<button class="imp-pill" data-act="${act}" data-val="${val}">${lbl}</button>`;
  return `<div class="card imp-card">
    <div class="card-title">Costos fijos (bolsón)</div>
    <div class="imp-toolbar">
      <div class="imp-seg"><span class="imp-seg-lbl">Modo</span><div class="imp-pillgroup">${pill('pct', '%', 'modo')}${pill('monto', '$', 'modo')}</div></div>
      <div class="imp-seg"><span class="imp-seg-lbl">Precarga por</span><div class="imp-pillgroup">${pill('kg', 'kg', 'crit')}${pill('fob', 'FOB', 'crit')}</div></div>
      <button class="btn btn-ghost imp-mini" data-act="precargar">Precargar</button>
      <button class="btn btn-ghost imp-mini" data-act="repartir">Repartir resto</button>
      <div class="grow"></div>
      <button class="btn btn-ghost imp-mini" data-act="addfijo">+ Concepto</button>
    </div>
    <div class="imp-fijos">${filasManual}${filasAuto}</div>
    <div class="imp-fijos-nota">La diferencia de flete y el despachante se cargan solos al bolsón (no entran al CIF) y se reparten por producto como marques abajo. Acordate de Precargar para repartir el 100%.</div>
    <div class="imp-reparto">
      <span>Bolsón total: <b id="imp-bolson" class="imp-mono"></b></span>
      <span id="imp-reparto-ind" class="imp-mono"></span>
    </div>
  </div>`;
}

function prodsSection() {
  return `<div class="imp-prods-head">
      <div class="card-title" style="margin:0">Productos</div>
      <button class="btn btn-ghost imp-mini" data-act="addprod">+ Producto</button>
    </div>
    <div class="imp-prods" id="imp-prods">${PRODS.map(prodCard).join('')}</div>`;
}

function prodCard(p, i) {
  const fnum = (f, lbl, hint = '') => field(lbl, inpNum('prod', i, f, p[f]), hint);
  const ivaVal = num(p.iva_pct);
  const ivaSel = `<select class="imp-in imp-sel" data-k="prod" data-i="${i}" data-f="iva_pct">
      <option value="21" ${ivaVal === 21 ? 'selected' : ''}>21 %</option>
      <option value="10.5" ${ivaVal === 10.5 ? 'selected' : ''}>10,5 %</option>
      ${ivaVal !== 21 && ivaVal !== 10.5 ? `<option value="${esc(p.iva_pct)}" selected>${esc(p.iva_pct)} %</option>` : ''}
    </select>`;
  const chk = `<label class="imp-chk"><input type="checkbox" data-k="prod" data-i="${i}" data-f="declaro_distinto" ${p.declaro_distinto ? 'checked' : ''}> Declaro distinto (subfacturación / reclasificación)</label>`;
  const overrides = `<div class="imp-prod-override" style="display:${p.declaro_distinto ? 'grid' : 'none'}">
      ${fnum('fob_decl_u', 'FOB declarado u', 'lo que declarás (≤ real)')}
      ${fnum('derechos_pct_decl', 'Derechos declarado %', 'lo que pagás')}
      ${fnum('estadistica_pct_decl', 'Estad. declarado %', 'lo que pagás')}
    </div>`;
  return `<div class="card imp-prod" data-row="${i}">
    <div class="imp-prod-head">
      <span class="imp-prod-num">${i + 1}</span>
      <input class="imp-in imp-prod-nombre" data-k="prod" data-i="${i}" data-f="nombre" value="${esc(p.nombre)}" placeholder="Nombre del producto">
      <div class="imp-prod-cost">
        <div class="imp-cost-usd"><span class="imp-cost-lbl">Costo unit.</span><b id="pu-usd-${i}" class="imp-mono"></b></div>
        <div class="imp-cost-ars imp-mono" id="pu-ars-${i}"></div>
      </div>
      <button class="imp-del" data-act="delprod" data-i="${i}" title="Eliminar producto">✕</button>
    </div>
    <div class="imp-prod-grid">
      ${fnum('cantidad', 'Cantidad')}
      ${fnum('peso_u', 'Peso u (kg)')}
      ${fnum('fob_real_u', 'FOB u (USD)')}
      ${fnum('derechos_pct', 'Derechos %')}
      ${fnum('estadistica_pct', 'Estadística %')}
      ${fnum('imp_internos_pct', 'Imp. int. %')}
      ${field('IVA %', ivaSel)}
      ${field(`<span class="imp-fijo-lbl">Fijo ${FIJOS_MODO === 'pct' ? '%' : '$'}</span>`,
        `<input class="imp-in" id="prodfijo-${i}" data-k="prod" data-i="${i}" data-f="fijo_asignado" type="number" step="any" inputmode="decimal" value="${esc(p.fijo_asignado)}">`)}
    </div>
    ${chk}
    ${overrides}
    <div class="imp-prod-foot">
      <button class="imp-exp" data-act="exp" data-i="${i}">${p._open ? '▾ Ocultar detalle' : '▸ Ver detalle'}</button>
      <div class="imp-prod-tot">Costo total: <b id="pt-usd-${i}" class="imp-mono"></b></div>
    </div>
    <div class="imp-detail" id="imp-det-${i}" style="display:${p._open ? 'block' : 'none'}">
      <div id="imp-det-c-${i}"></div>
    </div>
  </div>`;
}

function totalesCard() {
  return `<div class="card imp-card imp-totales">
    <div class="imp-tot-block imp-tot-costo">
      <span class="imp-tot-lbl">Costo capitalizado (CMV s/IVA)</span>
      <div class="imp-tot-val"><b id="imp-tot-costo-usd" class="imp-mono"></b></div>
      <div class="imp-tot-sub imp-mono" id="imp-tot-costo-ars"></div>
    </div>
    <div class="imp-tot-block imp-tot-cred">
      <span class="imp-tot-lbl">Crédito fiscal (→ Posición Fiscal)</span>
      <div class="imp-tot-val"><b id="imp-tot-cred-usd" class="imp-mono"></b></div>
      <div class="imp-tot-sub imp-mono" id="imp-tot-cred-ars"></div>
    </div>
  </div>`;
}

function reclasifCard() {
  return `<div class="card imp-card imp-reclasif" id="imp-reclasif" style="display:none">
    <div class="card-title">Reclasificación de posición (coima)</div>
    <div class="imp-reclasif-grid">
      <div class="imp-reclasif-block"><span class="imp-reclasif-lbl">Ahorro derechos + estadística</span><b id="imp-ahorro" class="imp-mono"></b></div>
      <div class="imp-reclasif-block imp-reclasif-coima"><span class="imp-reclasif-lbl">Coima (capitaliza, sin crédito)</span><b id="imp-coima" class="imp-mono"></b></div>
      <div class="imp-reclasif-block imp-reclasif-neto"><span class="imp-reclasif-lbl">Neto a favor (ahorro − coima)</span><b id="imp-neto" class="imp-mono"></b></div>
    </div>
    <div class="imp-fijos-nota">El ahorro es lo que NO pagás de derechos+estadística al declarar otra posición (no es costo). La coima sí capitaliza al costo y es salida de caja: cargala también como pago en el cierre.</div>
  </div>`;
}

function tablaTotalCard() {
  return `<div class="card imp-card">
    <div class="card-title">Tabla total del despacho</div>
    <div class="imp-tt-legend"><span class="imp-tt-dot imp-tt-cap"></span>capitaliza al costo · <span class="imp-tt-dot imp-tt-cred"></span>crédito fiscal (Posición Fiscal) · resto = base</div>
    <div class="table-wrap"><table class="t imp-tt">
      <thead><tr><th>Concepto</th><th style="text-align:right">USD</th><th style="text-align:right">ARS</th></tr></thead>
      <tbody id="imp-tt-body"></tbody>
    </table></div>
  </div>`;
}

function pagosCard() {
  const filas = PAGOS.map((pg, i) => `
    <div class="imp-pago-item" data-prow="${i}">
      ${inp('pago', i, 'concepto', pg.concepto, 'placeholder="Concepto del pago"')}
      ${inpNum('pago', i, 'monto', pg.monto, 'placeholder="Monto"')}
      <select class="imp-in imp-sel" data-k="pago" data-i="${i}" data-f="moneda">
        <option value="USD" ${pg.moneda === 'USD' ? 'selected' : ''}>USD</option>
        <option value="ARS" ${pg.moneda === 'ARS' ? 'selected' : ''}>ARS</option>
      </select>
      <input class="imp-in imp-pago-tc" id="pagotc-${i}" data-k="pago" data-i="${i}" data-f="tc" type="number" step="any" inputmode="decimal" value="${esc(pg.tc)}" placeholder="TC" ${pg.moneda === 'USD' ? 'disabled' : ''}>
      <span class="imp-pago-usd imp-mono" id="pago-usd-${i}"></span>
      <button class="imp-del" data-act="delpago" data-i="${i}" title="Eliminar pago">✕</button>
    </div>`).join('') || `<div class="imp-muted imp-empty">Sin pagos cargados.</div>`;
  return `<div class="card imp-card">
    <div class="imp-prods-head" style="margin:0 0 12px">
      <div class="card-title" style="margin:0">Pagos y control de cierre</div>
      <button class="btn btn-ghost imp-mini" data-act="addpago">+ Pago</button>
    </div>
    <div class="imp-pago-headrow">
      <span>Concepto</span><span>Monto</span><span>Moneda</span><span>TC del pago</span><span style="text-align:right">= USD</span><span></span>
    </div>
    <div class="imp-pagos">${filas}</div>
    <div class="imp-cierre" id="imp-cierre"></div>
  </div>`;
}

// ── Repintado de celdas calculadas (sin re-render de inputs) ─────────────
function paint() {
  const c = calc();
  const set = (id, txt) => { const el = document.getElementById(id); if (el) el.textContent = txt; };

  // Tira de bases calculadas (detalles generales)
  const bases = document.getElementById('imp-bases');
  if (bases) {
    const chip = (lbl, val) => `<div class="imp-base"><span class="imp-base-lbl">${lbl}</span><b class="imp-mono">${val}</b></div>`;
    bases.innerHTML =
      chip('Σ FOB declarado', usd(c.sFob)) +
      chip('Σ FOB real', usd(c.fobRealTot)) +
      chip('Σ Peso', c.sPeso.toLocaleString('es-AR', { maximumFractionDigits: 3 }) + ' kg') +
      chip('CIF total', usd(c.sCif)) +
      chip('Seguro total', usd(c.seguroTotal)) +
      chip('Despachante total', usd(c.despTotal));
  }

  set('imp-bolson', usd(c.bolson));
  set('imp-fijo-fletediff', usd(c.fleteDiff));
  set('imp-fijo-despachante', usd(c.despTotal));
  const asignado = PRODS.reduce((s, p) => s + num(p.fijo_asignado), 0);
  const ind = document.getElementById('imp-reparto-ind');
  if (ind) {
    if (FIJOS_MODO === 'pct') {
      const falta = round2(100 - asignado);
      ind.textContent = `Asignado ${pctTxt(asignado)} · Falta a 100%: ${pctTxt(falta)}`;
      ind.className = 'imp-mono ' + (Math.abs(falta) < 0.01 ? 'imp-ok' : 'imp-warn');
    } else {
      const resto = round2(c.bolson - asignado);
      ind.textContent = `Asignado ${usd(asignado)} · Resto: ${usd(resto)}`;
      ind.className = 'imp-mono ' + (Math.abs(resto) < 0.01 ? 'imp-ok' : 'imp-warn');
    }
  }

  c.rows.forEach((r, i) => {
    set(`pu-usd-${i}`, usd(r.costoUnitUsd));
    set(`pu-ars-${i}`, ars(r.costoUnitArs));
    set(`pt-usd-${i}`, usd(r.costoUsd) + '  ·  ' + ars(r.costoUsd * c.tc));
    if (PRODS[i] && PRODS[i]._open) {
      const cont = document.getElementById(`imp-det-c-${i}`);
      if (cont) cont.innerHTML = detalleHTML(r, c.tc);
    }
  });

  set('imp-tot-costo-usd', usd(c.costoTotUsd));
  set('imp-tot-costo-ars', ars(c.costoTotArs));
  set('imp-tot-cred-usd', usd(c.credTotUsd));
  set('imp-tot-cred-ars', ars(c.credTotArs));

  // Resumen de reclasificación (coima): se muestra solo si hay algo
  const recl = document.getElementById('imp-reclasif');
  if (recl) {
    const hay = Math.abs(c.ahorroTot) > 0.005 || Math.abs(c.coimaTot) > 0.005;
    recl.style.display = hay ? 'block' : 'none';
    if (hay) {
      set('imp-ahorro', usd(c.ahorroTot) + '  ·  ' + ars(c.ahorroTot * c.tc));
      set('imp-coima', usd(c.coimaTot) + '  ·  ' + ars(c.coimaTot * c.tc));
      set('imp-neto', usd(c.netoReclasif) + '  ·  ' + ars(c.netoReclasif * c.tc));
    }
  }

  // Tabla total del despacho
  const ttBody = document.getElementById('imp-tt-body');
  if (ttBody) {
    const tt = (lbl, v, cls = '') => `<tr class="${cls}"><td>${lbl}</td><td class="imp-mono" style="text-align:right">${usd(v)}</td><td class="imp-mono" style="text-align:right">${ars(v * c.tc)}</td></tr>`;
    ttBody.innerHTML =
      tt('Total CIF', c.sCif, 'imp-tt-baserow') +
      tt('Derechos', c.derechosTot, 'imp-tt-caprow') +
      tt('Estadística', c.estadTot, 'imp-tt-caprow') +
      tt('Imp. internos', c.impIntTot, 'imp-tt-caprow') +
      tt('Base de IVA', c.baseIvaTot, 'imp-tt-baserow') +
      tt('IVA', c.ivaTot, 'imp-tt-credrow') +
      tt('IIBB percepción', c.iibbTot, 'imp-tt-credrow') +
      tt('Ganancias percepción', c.ganTot, 'imp-tt-credrow') +
      tt('Total tributos del despacho', c.tributosTot, 'imp-tt-totalrow');
  }

  c.pagosRows.forEach((pr, i) => {
    const el = document.getElementById(`pago-usd-${i}`);
    if (el) el.textContent = usd(pr.usd);
  });

  const cierre = document.getElementById('imp-cierre');
  if (cierre) {
    const ok = Math.abs(c.diff) < 0.01;
    cierre.className = 'imp-cierre ' + (ok ? 'imp-cierre-ok' : 'imp-cierre-bad');
    cierre.innerHTML = `
      <div class="imp-cierre-row"><span>Σ Pagos (USD)</span><b class="imp-mono">${usd(c.pagosUsd)}</b></div>
      <div class="imp-cierre-row"><span>Costo capitalizado + Crédito fiscal (USD)</span><b class="imp-mono">${usd(c.esperado)}</b></div>
      <div class="imp-cierre-row imp-cierre-diff"><span>${ok ? '✓ Cierra' : 'Diferencia'}</span><b class="imp-mono">${usd(c.diff)}</b></div>
      ${ok ? '' : `<div class="imp-cierre-hint">${c.diff > 0 ? 'Pagaste de más respecto a costo+crédito: falta cargar un gasto como costo.' : 'Falta anotar un pago (o sobra costo cargado).'}</div>`}`;
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
    <div class="imp-det-col">
      <div class="imp-det-h">Costo s/IVA (capitaliza al lote)</div>
      <div class="imp-dt-wrap"><table class="imp-dt">${head}<tbody>
        ${line('FOB declarado', r.fobDeclT)}
        ${line('No declarado (subfact.)', r.noDeclarado)}
        ${line('Flete declarado (por peso)', r.fleteCosto)}
        ${line('Seguro', r.seguro)}
        ${line('Derechos', r.derechos)}
        ${line('Estadística', r.estadistica)}
        ${line('Imp. internos', r.impInternos)}
        ${line('Coima clasificación', r.coima)}
        ${line('Fijos (incl. flete-dif. + despachante)', r.fijo)}
        ${line('Costo s/IVA', r.costoUsd, true)}
      </tbody></table></div>
    </div>
    <div class="imp-det-col">
      <div class="imp-det-h">Crédito fiscal (NO es costo → Posición Fiscal)</div>
      <div class="imp-dt-wrap"><table class="imp-dt">${head}<tbody>
        ${line('IVA', r.iva)}
        ${line('IIBB percepción', r.iibb)}
        ${line('Ganancias percepción', r.ganancias)}
        ${line('Crédito fiscal', r.creditoUsd, true)}
      </tbody></table></div>
      <div class="imp-det-cif">CIF del producto: <b class="imp-mono">${usd(r.cif)}</b></div>
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
  document.querySelectorAll('.imp-fijo-lbl').forEach(el => { el.textContent = 'Fijo ' + (FIJOS_MODO === 'pct' ? '%' : '$'); });
}

function bindDelegation(body) {
  const onEdit = e => {
    const t = e.target;
    if (!t.dataset || !t.dataset.k) return;
    const k = t.dataset.k, i = +t.dataset.i, f = t.dataset.f, v = t.value;
    if (k === 'param') {
      P[f] = v;
      // Estos parámetros mueven el bolsón efectivo (flete-diff + despachante).
      if (['flete_declarado_usd', 'flete_real_usd', 'despachante_pct', 'despachante_monto'].includes(f)) {
        onBolsonChanged(); syncProdFijoInputs();
      }
    }
    else if (k === 'fijo') { FIJOS[i][f] = v; if (f === 'monto_usd') { onBolsonChanged(); syncProdFijoInputs(); } }
    else if (k === 'prod') {
      if (f === 'declaro_distinto') {
        PRODS[i].declaro_distinto = t.checked;
        if (!t.checked) { PRODS[i].fob_decl_u = ''; PRODS[i].derechos_pct_decl = ''; PRODS[i].estadistica_pct_decl = ''; }
        renderBody();
        return;
      }
      PRODS[i][f] = v;
    }
    else if (k === 'pago') {
      PAGOS[i][f] = v;
      if (f === 'moneda') {
        const tcEl = document.getElementById(`pagotc-${i}`);
        if (tcEl) {
          tcEl.disabled = (v === 'USD');
          if (v === 'ARS' && !num(PAGOS[i].tc) && num(P.tc)) { PAGOS[i].tc = P.tc; tcEl.value = P.tc; }
        }
      }
    }
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
      case 'addpago': PAGOS.push(blankPago()); renderBody(); break;
      case 'delpago': PAGOS.splice(i, 1); renderBody(); break;
      case 'exp': {
        PRODS[i]._open = !PRODS[i]._open;
        const box = document.getElementById(`imp-det-${i}`);
        if (box) box.style.display = PRODS[i]._open ? 'block' : 'none';
        btn.textContent = PRODS[i]._open ? '▾ Ocultar detalle' : '▸ Ver detalle';
        paint();
        break;
      }
      case 'modo': switchModo(btn.dataset.val); syncProdFijoInputs(); pintarPills(); paint(); break;
      case 'crit': FIJOS_CRIT = btn.dataset.val; pintarPills(); break;
      case 'precargar': precargarFijos(); syncProdFijoInputs(); paint(); break;
      case 'repartir': repartirResto(); syncProdFijoInputs(); paint(); break;
    }
  });
}

// ── Estilo scopeado (imp-*) ──────────────────────────────────────────────
function inyectarEstilo() {
  if (document.getElementById('imp-style')) return;
  const css = `
    .imp-wrap{max-width:1080px;margin:0 auto}
    .imp-card{margin-bottom:16px;padding:18px 20px}
    .imp-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .imp-muted{color:#78716C}
    .imp-empty{padding:10px 2px;font-size:13px}
    .imp-in{width:100%;box-sizing:border-box;padding:9px 11px;border:1px solid #E7E5E4;border-radius:8px;font-size:14px;font-family:inherit;background:#fff;color:#1C1917}
    .imp-in:focus{outline:none;border-color:#D97706;box-shadow:0 0 0 3px rgba(217,118,6,.12)}
    .imp-in:disabled{background:#F5F5F4;color:#A8A29E}
    .imp-in[type=number]::-webkit-inner-spin-button,.imp-in[type=number]::-webkit-outer-spin-button{-webkit-appearance:none;margin:0}
    .imp-in[type=number]{-moz-appearance:textfield;appearance:textfield}
    .imp-chk{display:flex;align-items:center;gap:8px;font-size:13px;color:#57534E;margin-top:14px;cursor:pointer}
    .imp-chk input{width:16px;height:16px;accent-color:#D97706;cursor:pointer}
    .imp-prod-override{grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:13px;margin-top:12px;padding:14px;background:#FBF4E9;border:1px dashed #EBD9BC;border-radius:10px}
    .imp-prod-num{flex:none;width:26px;height:26px;border-radius:7px;background:#D97706;color:#fff;font-size:13px;font-weight:700;display:flex;align-items:center;justify-content:center}

    /* Topbar */
    .imp-topbar{display:flex;gap:16px;align-items:flex-end;flex-wrap:wrap}
    .imp-topbar-l{flex:1;min-width:240px}
    .imp-titulo{font-size:12px;color:#78716C;margin-bottom:6px}
    .imp-nombre{font-size:15px}
    .imp-topbar-r{display:flex;gap:8px;flex-wrap:wrap}

    /* Parámetros */
    .imp-params{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px}
    .imp-field{display:flex;flex-direction:column;gap:5px;font-size:13px;color:#44403C;min-width:0}
    .imp-flbl,.imp-fijo-lbl{font-weight:500}
    .imp-field small{color:#A8A29E;font-size:11px;line-height:1.3}

    /* Reclasificación (coima) */
    .imp-reclasif-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:4px}
    .imp-reclasif-block{border:1px solid #EFEDEB;border-radius:10px;padding:14px 16px;background:#FAFAF9}
    .imp-reclasif-lbl{font-size:12px;color:#78716C;display:block;margin-bottom:6px}
    .imp-reclasif-block b{font-size:16px;color:#1C1917}
    .imp-reclasif-coima{background:#FBF1E6;border-color:#EBCBA0}
    .imp-reclasif-neto{background:#E1F5EE;border-color:#A7E0CC}
    @media(max-width:720px){.imp-reclasif-grid{grid-template-columns:1fr}}

    /* Bases calculadas (detalles generales) */
    .imp-bases{display:flex;flex-wrap:wrap;gap:10px;margin-top:16px;padding-top:14px;border-top:1px solid #F0EEEC}
    .imp-base{display:flex;flex-direction:column;gap:2px;background:#FAFAF9;border:1px solid #EFEDEB;border-radius:8px;padding:8px 12px;min-width:130px}
    .imp-base-lbl{font-size:11px;color:#A8A29E;text-transform:uppercase;letter-spacing:.03em}
    .imp-base b{font-size:14px;color:#1C1917}

    /* Tabla total del despacho */
    .imp-tt-legend{font-size:12px;color:#78716C;margin-bottom:10px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
    .imp-tt-dot{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:2px}
    .imp-tt-cap{background:#EBD9BC}
    .imp-tt-cred{background:#CFE0F1}
    #imp-body table.imp-tt tbody td{font-size:14px}
    .imp-tt-caprow td:first-child{border-left:3px solid #EBD9BC}
    .imp-tt-credrow td:first-child{border-left:3px solid #CFE0F1}
    .imp-tt-baserow td{background:#FAFAF9;color:#57534E;font-weight:600}
    .imp-tt-totalrow td{background:#F5F4F2;font-weight:700;font-size:15px!important;border-top:2px solid #E7E5E4}

    /* Fijos */
    .imp-toolbar{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:14px}
    .imp-toolbar .grow{flex:1}
    .imp-seg{display:flex;align-items:center;gap:7px}
    .imp-seg-lbl{font-size:12px;color:#78716C}
    .imp-pillgroup{display:inline-flex;border:1px solid #E7E5E4;border-radius:8px;overflow:hidden}
    .imp-pill{cursor:pointer;border:0;background:#fff;color:#57534E;font-size:13px;padding:7px 14px;font-family:inherit;border-right:1px solid #E7E5E4}
    .imp-pill:last-child{border-right:0}
    .imp-pill.active{background:#D97706;color:#fff;font-weight:600}
    .imp-mini{padding:7px 13px;font-size:13px}
    .imp-del{border:1px solid #E7E5E4;background:#fff;color:#A8A29E;cursor:pointer;font-size:13px;width:32px;height:32px;border-radius:8px;flex:none}
    .imp-del:hover{color:#B91C1C;border-color:#F0C9C2}
    .imp-fijo-item{display:grid;grid-template-columns:1fr 150px 32px;gap:10px;margin-bottom:10px;align-items:center}
    .imp-fijo-auto{background:#FAFAF9;border:1px dashed #E0DDDA;border-radius:8px;padding:8px 10px}
    .imp-fijo-autolbl{font-size:13px;color:#57534E;display:flex;align-items:center;gap:8px}
    .imp-auto-badge{font-size:10px;text-transform:uppercase;letter-spacing:.04em;background:#EFEDEB;color:#857F79;padding:1px 6px;border-radius:5px}
    .imp-fijo-automonto{text-align:right;color:#1C1917}
    .imp-fijos-nota{font-size:12px;color:#A8A29E;margin:6px 2px 0;line-height:1.4}
    .imp-reparto{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;border-top:1px solid #E7E5E4;margin-top:12px;padding-top:12px;font-size:14px}
    .imp-ok{color:#0F6E56;font-weight:600}
    .imp-warn{color:#854F0B;font-weight:600}

    /* Productos como tarjetas */
    .imp-prods-head{display:flex;justify-content:space-between;align-items:center;margin:4px 2px 12px}
    .imp-prod{padding:16px 18px;margin-bottom:14px;border-left:4px solid #D97706;background:#FFFDFB}
    .imp-prod-head{display:flex;align-items:center;gap:14px;margin-bottom:14px}
    .imp-prod-nombre{flex:1;font-size:15px;font-weight:500}
    .imp-prod-cost{text-align:right;flex:none;line-height:1.25}
    .imp-cost-lbl{font-size:11px;color:#A8A29E;margin-right:6px;text-transform:uppercase;letter-spacing:.04em}
    .imp-cost-usd b{font-size:17px;color:#1C1917}
    .imp-cost-ars{font-size:13px;color:#78716C}
    .imp-prod-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(135px,1fr));gap:13px}
    .imp-prod-foot{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-top:14px;padding-top:12px;border-top:1px solid #F5F5F4}
    .imp-exp{border:1px solid #E7E5E4;background:#FAFAF9;border-radius:8px;cursor:pointer;font-size:13px;padding:7px 13px;color:#57534E;font-family:inherit}
    .imp-exp:hover{border-color:#D97706;color:#D97706}
    .imp-prod-tot{font-size:14px;color:#44403C}
    .imp-prod-tot b{color:#1C1917}

    /* Detalle expandible */
    .imp-detail{margin-top:14px;background:#FCFBF9;border:1px solid #F0EEEC;border-radius:10px;padding:16px}
    .imp-det-grid{display:grid;grid-template-columns:1fr 1fr;gap:22px}
    .imp-det-h{font-size:11px;font-weight:700;color:#854F0B;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px}
    .imp-det-col:nth-child(2) .imp-det-h{color:#0C447C}
    .imp-dt-wrap{overflow-x:auto}
    .imp-dt{width:100%;border-collapse:collapse;font-size:12.5px}
    .imp-dt th{text-align:right;color:#A8A29E;font-weight:600;font-size:10.5px;padding:4px 8px;border-bottom:1px solid #EAE8E6;white-space:nowrap}
    .imp-dt th:first-child{text-align:left}
    .imp-dt td{padding:5px 8px;text-align:right;color:#44403C;white-space:nowrap}
    .imp-dt td:first-child{text-align:left;color:#1C1917}
    .imp-d-strong td{font-weight:700;border-top:1px solid #E0DDDA;color:#1C1917}
    .imp-det-cif{margin-top:10px;font-size:13px;color:#78716C;text-align:right}

    /* Totales */
    .imp-totales{display:grid;grid-template-columns:1fr 1fr;gap:18px}
    .imp-tot-block{border-radius:10px;padding:16px 18px}
    .imp-tot-costo{background:#FBF4E9;border:1px solid #EBD9BC}
    .imp-tot-cred{background:#EEF4FB;border:1px solid #CFE0F1}
    .imp-tot-lbl{font-size:12px;font-weight:600;color:#57534E;text-transform:uppercase;letter-spacing:.03em}
    .imp-tot-val b{font-size:22px;color:#1C1917;display:block;margin-top:6px}
    .imp-tot-sub{font-size:14px;color:#78716C;margin-top:2px}

    /* Pagos */
    .imp-pago-headrow,.imp-pago-item{display:grid;grid-template-columns:1fr 130px 88px 110px 130px 32px;gap:10px;align-items:center}
    .imp-pago-headrow{font-size:11px;color:#A8A29E;text-transform:uppercase;letter-spacing:.04em;padding:0 2px 8px;font-weight:600}
    .imp-pago-item{margin-bottom:10px}
    .imp-pago-usd{text-align:right;font-size:13px;color:#0F6E56;font-weight:600}
    .imp-cierre{border-radius:10px;padding:16px 18px;margin-top:14px;border:1px solid #E7E5E4}
    .imp-cierre-ok{background:#E1F5EE;border-color:#A7E0CC}
    .imp-cierre-bad{background:#FBF1E6;border-color:#EBCBA0}
    .imp-cierre-row{display:flex;justify-content:space-between;gap:14px;font-size:14px;padding:4px 0}
    .imp-cierre-diff{border-top:1px dashed rgba(0,0,0,.15);margin-top:7px;padding-top:9px;font-size:15px;font-weight:600}
    .imp-cierre-hint{font-size:12.5px;color:#854F0B;margin-top:7px}

    .imp-badge{font-size:12px;padding:2px 8px;border-radius:6px}
    .imp-badge-borr{background:#FAEEDA;color:#854F0B}
    .imp-badge-conf{background:#E1F5EE;color:#0F6E56}

    @media(max-width:720px){
      .imp-det-grid,.imp-totales{grid-template-columns:1fr}
      .imp-prod-head{flex-wrap:wrap}
      .imp-pago-headrow{display:none}
      .imp-pago-item{grid-template-columns:1fr 1fr;gap:8px}
      .imp-pago-item .imp-pago-usd{grid-column:1/-1;text-align:left}
    }
  `;
  const st = document.createElement('style');
  st.id = 'imp-style';
  st.textContent = css;
  document.head.appendChild(st);
}
