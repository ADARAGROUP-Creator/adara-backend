import { sbGet, sbPost, sbPatch } from '../core/sb.js';

// ── Pantalla: Flex ──────────────────────────────────────────────────────
// Control semanal de envíos Flex contra el resumen del proveedor.
// Cada envío se clasifica (vista v_flex_envios) en logística + zona + precio:
//   - Logística: CABA vendido 12:00–16:00 → "CABA tardía"; el resto → MEF.
//   - Zona: vía flex_partido_zona (CABA entra automático; el resto se asigna acá).
//   - Precio: flex_precio (logística × zona), editable abajo.
// La semana se agrupa por fecha_despacho_flex (lunes base; sábado se entrega lunes).
// Replica el formato del resumen MEF (día × zona) para cuadrarlo uno a uno.
// Ver ADARA-FLEX.md.

let SEMANAS = [];   // v_flex_semanas
let SEL = null;     // semana_lunes seleccionada (YYYY-MM-DD)
let ENVIOS = [];    // v_flex_envios de la semana seleccionada
let ZONAS = [];     // flex_zona
let LOGS = [];      // flex_logistica
let PRECIOS = [];   // flex_precio
let MAPA = [];      // flex_partido_zona
let SINZONA = [];   // v_flex_partidos_sin_zona (global, por volumen)
let RESUMEN = {};   // total pegado del resumen por logística (sólo en sesión)
let VERPRECIOS = false;

const DOW = { 1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves', 5: 'Viernes', 6: 'Sábado', 7: 'Domingo' };
// Qué zonas muestra cada logística (MEF todas; CABA tardía sólo CABA).
const ZONAS_LOG = { mef: ['CABA', 'GBA1', 'GBA2', 'GBA3'], caba_tardia: ['CABA'] };

function fmtMonto(valor) {
  const n = Number(valor) || 0;
  const abs = Math.abs(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return `${n < 0 ? '−' : ''}$ ${abs}`;
}
function fmtSemana(iso) {
  if (!iso) return '—';
  const lun = new Date(iso + 'T00:00:00');
  const sab = new Date(lun); sab.setDate(sab.getDate() + 5);
  const d = x => x.toLocaleDateString('es-AR', { day: '2-digit', month: '2-digit' });
  return `${d(lun)} – ${d(sab)}`;
}
function esc(s) { return String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }

// ── Mapas auxiliares ────────────────────────────────────────────────────
function logIdByCodigo(cod) { const l = LOGS.find(x => x.codigo === cod); return l ? l.id : null; }
function logNombre(cod) { const l = LOGS.find(x => x.codigo === cod); return l ? l.nombre : cod; }
function zonaByCodigo(cod) { return ZONAS.find(z => z.codigo === cod) || null; }
function precioDe(logCod, zonaCod) {
  const z = zonaByCodigo(zonaCod); const lid = logIdByCodigo(logCod);
  if (!z || !lid) return null;
  const p = PRECIOS.find(x => x.logistica_id === lid && x.zona_id === z.id);
  return p ? Number(p.precio) : null;
}

// ── Carga ───────────────────────────────────────────────────────────────
export async function loadFlex() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando Flex…</div>`;
  try {
    ZONAS   = await sbGet('flex_zona', 'order=orden.asc,id.asc');
    LOGS    = await sbGet('flex_logistica', 'order=id.asc');
    PRECIOS = await sbGet('flex_precio', 'select=id,logistica_id,zona_id,precio&order=id.asc');
    MAPA    = await sbGet('flex_partido_zona', 'select=id,partido,zona_id&order=id.asc');
    SEMANAS = await sbGet('v_flex_semanas', 'order=semana_lunes.desc');
    SINZONA = await sbGet('v_flex_partidos_sin_zona', 'order=envios.desc');
    SEL = SEMANAS.length ? SEMANAS[0].semana_lunes : null;
    await recargarSemana();
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudo cargar Flex: ${e.message}</div>`;
    return;
  }
  inyectarEstilo();
  render();
}

async function recargarSemana() {
  ENVIOS = SEL ? await sbGet('v_flex_envios', `semana_lunes=eq.${SEL}&order=dow.asc,id.asc`) : [];
}

async function recargarMapeo() {
  MAPA    = await sbGet('flex_partido_zona', 'select=id,partido,zona_id&order=id.asc');
  SINZONA = await sbGet('v_flex_partidos_sin_zona', 'order=envios.desc');
  SEMANAS = await sbGet('v_flex_semanas', 'order=semana_lunes.desc');
  await recargarSemana();
}

// ── Render ──────────────────────────────────────────────────────────────
function render() {
  const root = document.getElementById('app-screens');
  const sem = SEMANAS.find(s => s.semana_lunes === SEL);
  const totalSinZona = SINZONA.reduce((a, s) => a + Number(s.envios), 0);

  const optSemanas = SEMANAS.map(s =>
    `<option value="${s.semana_lunes}" ${s.semana_lunes === SEL ? 'selected' : ''}>${fmtSemana(s.semana_lunes)} · ${s.envios} env</option>`
  ).join('');

  root.innerHTML = `
    <div class="flx-tabs">
      <a class="flx-tab" href="#ventas_ml">Ventas ML</a>
      <a class="flx-tab active" href="#flex">Flex</a>
    </div>

    <div class="flx-bar">
      <div>
        <label class="flx-lbl">Semana</label>
        <select class="select flx-sel" id="flx-semana">${optSemanas || '<option>—</option>'}</select>
      </div>
      <button class="btn btn-ghost" id="flx-precios-toggle">${VERPRECIOS ? 'Ocultar precios' : 'Precios'}</button>
    </div>

    <div class="flx-kpis">
      <div class="kpi"><div class="kpi-label">Envíos de la semana</div><div class="kpi-value">${sem ? sem.envios : 0}</div><div class="kpi-sub">${sem ? sem.envios_mef : 0} MEF · ${sem ? sem.envios_caba_tardia : 0} CABA tardía</div></div>
      <div class="kpi"><div class="kpi-label">Importe estimado</div><div class="kpi-value">${fmtMonto(sem ? sem.importe_estimado : 0)}</div><div class="kpi-sub">cant × precio (zonas mapeadas)</div></div>
      <div class="kpi ${totalSinZona ? 'kpi-warn' : ''}"><div class="kpi-label">Partidos sin zona</div><div class="kpi-value">${SINZONA.length}</div><div class="kpi-sub">${totalSinZona} envíos sin clasificar</div></div>
    </div>

    ${VERPRECIOS ? bloquePrecios() : ''}

    ${SINZONA.length ? bloqueAsignador() : '<div class="flx-ok">✓ Todos los partidos con envíos están mapeados a una zona.</div>'}

    ${bloqueLogistica('mef')}
    ${bloqueLogistica('caba_tardia')}
  `;

  bind();
}

// ── Bloque por logística (grilla día × zona, estilo resumen) ────────────
function bloqueLogistica(logCod) {
  const cols = ZONAS_LOG[logCod] || [];
  const envs = ENVIOS.filter(e => e.logistica_codigo === logCod && e.zona_codigo);
  // matriz[dow][zonaCod] = cantidad
  const mat = {}; for (let d = 1; d <= 6; d++) { mat[d] = {}; cols.forEach(c => mat[d][c] = 0); }
  envs.forEach(e => { if (e.dow >= 1 && e.dow <= 6 && mat[e.dow][e.zona_codigo] != null) mat[e.dow][e.zona_codigo]++; });

  const totZona = {}; cols.forEach(c => totZona[c] = 0);
  let totalCant = 0, totalImp = 0;
  let faltaPrecio = false;

  const filas = [];
  for (let d = 1; d <= 6; d++) {
    let impDia = 0; const celdas = [];
    cols.forEach(c => {
      const cant = mat[d][c]; totZona[c] += cant;
      const pr = precioDe(logCod, c);
      if (cant > 0 && pr == null) faltaPrecio = true;
      impDia += cant * (pr || 0);
      celdas.push(`<td class="flx-num">${cant || '·'}</td>`);
    });
    totalCant += cols.reduce((a, c) => a + mat[d][c], 0);
    totalImp += impDia;
    filas.push(`<tr><td class="flx-dia">${DOW[d]}</td>${celdas.join('')}<td class="flx-num flx-mono">${impDia ? fmtMonto(impDia) : '·'}</td></tr>`);
  }

  const totCeldas = cols.map(c => `<td class="flx-num flx-tot">${totZona[c] || 0}</td>`).join('');
  const subtotales = cols.map(c => {
    const pr = precioDe(logCod, c);
    return `<td class="flx-num flx-mono flx-sub">${pr != null ? fmtMonto(totZona[c] * pr) : '<span class="flx-falta">s/precio</span>'}</td>`;
  }).join('');

  const header = cols.map(c => `<th class="flx-num">${c}</th>`).join('');

  // Comparación con el resumen del proveedor
  const pegado = RESUMEN[logCod];
  const dif = (pegado != null && pegado !== '') ? (totalImp - Number(pegado)) : null;
  const difCls = dif == null ? '' : (Math.abs(dif) < 1 ? 'flx-dif-ok' : 'flx-dif-bad');

  return `
    <div class="flx-block">
      <div class="flx-block-h">${logNombre(logCod)}</div>
      <table class="flx-table">
        <thead><tr><th>Día</th>${header}<th class="flx-num">Importe</th></tr></thead>
        <tbody>${filas.join('')}</tbody>
        <tfoot>
          <tr class="flx-foot"><td>Envíos</td>${totCeldas}<td class="flx-num flx-tot">${totalCant}</td></tr>
          <tr class="flx-foot"><td>Subtotal</td>${subtotales}<td class="flx-num flx-mono flx-tot">${fmtMonto(totalImp)}</td></tr>
        </tfoot>
      </table>
      ${faltaPrecio ? '<div class="flx-falta-msg">⚠ Faltan precios para alguna zona usada esta semana. Cargalos en "Precios".</div>' : ''}
      <div class="flx-cuadre">
        <label>Total del resumen del proveedor</label>
        <input class="input flx-resumen" data-log="${logCod}" inputmode="decimal" placeholder="0,00" value="${pegado != null ? pegado : ''}">
        <span class="flx-dif ${difCls}">${dif == null ? '' : (dif === 0 ? '✓ Coincide' : `Diferencia: ${fmtMonto(dif)} (${dif > 0 ? 'ADARA calcula de más' : 'el proveedor cobra de más'})`)}</span>
      </div>
    </div>`;
}

// ── Asignador de zonas (partidos sin zona, por volumen) ─────────────────
function bloqueAsignador() {
  const top = SINZONA.slice(0, 25);
  const filas = top.map(s => {
    const opts = ZONAS.map(z => `<option value="${z.id}">${z.codigo}</option>`).join('');
    return `<tr>
      <td>${esc(s.partido)}</td>
      <td class="flx-num">${s.envios}</td>
      <td><select class="select flx-assign" data-partido="${esc(s.partido)}"><option value="">— asignar zona —</option>${opts}</select></td>
    </tr>`;
  }).join('');
  return `
    <div class="flx-block flx-asign">
      <div class="flx-block-h">Asignar zonas <span class="flx-muted">· ${SINZONA.length} partidos sin clasificar (mostrando top ${top.length} por volumen)</span></div>
      <table class="flx-table flx-table-asign">
        <thead><tr><th>Partido</th><th class="flx-num">Envíos (total)</th><th>Zona</th></tr></thead>
        <tbody>${filas}</tbody>
      </table>
      <div class="flx-muted">Asignás una vez por partido y aplica a todas las semanas. CABA ya entra automático.</div>
    </div>`;
}

// ── Bloque precios (editable) ───────────────────────────────────────────
function bloquePrecios() {
  const filas = [];
  ['mef', 'caba_tardia'].forEach(logCod => {
    (ZONAS_LOG[logCod] || []).forEach(zc => {
      const pr = precioDe(logCod, zc);
      filas.push(`<tr>
        <td>${logNombre(logCod)}</td>
        <td>${zc}</td>
        <td><input class="input flx-precio" data-log="${logCod}" data-zona="${zc}" inputmode="decimal" value="${pr != null ? pr : ''}" placeholder="0,00"></td>
        <td><button class="btn btn-ghost flx-precio-save" data-log="${logCod}" data-zona="${zc}">Guardar</button></td>
      </tr>`);
    });
  });
  return `
    <div class="flx-block">
      <div class="flx-block-h">Precios por logística × zona</div>
      <table class="flx-table flx-table-precios">
        <thead><tr><th>Logística</th><th>Zona</th><th>Precio unitario</th><th></th></tr></thead>
        <tbody>${filas.join('')}</tbody>
      </table>
    </div>`;
}

// ── Eventos ─────────────────────────────────────────────────────────────
function bind() {
  const $ = sel => document.getElementById(sel);

  const semSel = $('flx-semana');
  if (semSel) semSel.addEventListener('change', async () => {
    SEL = semSel.value; await recargarSemana(); render();
  });

  const tog = $('flx-precios-toggle');
  if (tog) tog.addEventListener('click', () => { VERPRECIOS = !VERPRECIOS; render(); });

  document.querySelectorAll('.flx-assign').forEach(s =>
    s.addEventListener('change', () => asignarZona(s.dataset.partido, s.value)));

  document.querySelectorAll('.flx-precio-save').forEach(b =>
    b.addEventListener('click', () => {
      const inp = document.querySelector(`.flx-precio[data-log="${b.dataset.log}"][data-zona="${b.dataset.zona}"]`);
      guardarPrecio(b.dataset.log, b.dataset.zona, inp ? inp.value : '');
    }));

  // Diferencia en vivo al pegar el total del resumen (sin re-render completo)
  document.querySelectorAll('.flx-resumen').forEach(inp =>
    inp.addEventListener('input', () => { RESUMEN[inp.dataset.log] = inp.value.replace(/\./g, '').replace(',', '.'); render(); restoreFocus(inp); }));
}

// Mantener el foco/caret tras el re-render del input de resumen
function restoreFocus(prev) {
  const next = document.querySelector(`.flx-resumen[data-log="${prev.dataset.log}"]`);
  if (next) { next.focus(); const v = next.value.length; next.setSelectionRange(v, v); }
}

async function asignarZona(partido, zonaIdStr) {
  if (!partido || !zonaIdStr) return;
  const zona_id = Number(zonaIdStr);
  try {
    const ex = MAPA.find(m => m.partido === partido);
    if (ex) await sbPatch('flex_partido_zona', `id=eq.${ex.id}`, { zona_id });
    else await sbPost('flex_partido_zona', { partido, zona_id });
    await recargarMapeo();
    render();
    window.toast(`"${partido}" → ${ZONAS.find(z => z.id === zona_id)?.codigo || ''}`);
  } catch (e) { window.toast('No se pudo asignar: ' + e.message, 'error'); }
}

async function guardarPrecio(logCod, zonaCod, valorStr) {
  const z = zonaByCodigo(zonaCod); const lid = logIdByCodigo(logCod);
  const precio = Number(String(valorStr).replace(/\./g, '').replace(',', '.'));
  if (!z || !lid || !(precio >= 0) || !valorStr) { window.toast('Precio inválido', 'error'); return; }
  try {
    const ex = PRECIOS.find(p => p.logistica_id === lid && p.zona_id === z.id);
    if (ex) await sbPatch('flex_precio', `id=eq.${ex.id}`, { precio });
    else await sbPost('flex_precio', { logistica_id: lid, zona_id: z.id, precio });
    PRECIOS = await sbGet('flex_precio', 'select=id,logistica_id,zona_id,precio&order=id.asc');
    SEMANAS = await sbGet('v_flex_semanas', 'order=semana_lunes.desc');
    render();
    window.toast('Precio guardado');
  } catch (e) { window.toast('No se pudo guardar el precio: ' + e.message, 'error'); }
}

// ── Estilos ─────────────────────────────────────────────────────────────
function inyectarEstilo() {
  if (document.getElementById('flx-style')) return;
  const css = `
    .flx-tabs{display:flex;gap:4px;border-bottom:1px solid #E7E5E4;margin-bottom:16px}
    .flx-tab{padding:8px 16px;font-size:14px;font-weight:600;color:#78716C;text-decoration:none;border-bottom:2px solid transparent;margin-bottom:-1px}
    .flx-tab:hover{color:#1C1917}
    .flx-tab.active{color:#1C1917;border-bottom-color:#1C1917}
    .flx-bar{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;margin-bottom:16px}
    .flx-lbl{display:block;font-size:12px;color:#78716C;margin-bottom:4px}
    .flx-sel{min-width:240px}
    .flx-kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:18px}
    .flx-kpis .kpi-warn .kpi-value{color:#854F0B}
    .flx-block{background:#fff;border:1px solid #E7E5E4;border-radius:12px;padding:16px;margin-bottom:16px}
    .flx-block-h{font-size:15px;font-weight:700;color:#1C1917;margin-bottom:12px}
    .flx-muted{color:#A8A29E;font-size:12px;font-weight:400}
    .flx-table{width:100%;border-collapse:collapse;font-size:14px}
    .flx-table th,.flx-table td{padding:7px 10px;border-bottom:1px solid #F5F5F4;text-align:left}
    .flx-table thead th{font-size:12px;color:#78716C;font-weight:600;text-transform:uppercase;letter-spacing:.03em}
    .flx-num{text-align:right}
    .flx-dia{font-weight:600;color:#57534E}
    .flx-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .flx-foot td{border-top:2px solid #E7E5E4;font-weight:700;color:#1C1917}
    .flx-tot{font-weight:700}
    .flx-sub{color:#57534E}
    .flx-falta{color:#B91C1C;font-size:12px}
    .flx-falta-msg{color:#854F0B;font-size:13px;margin-top:8px}
    .flx-cuadre{display:flex;align-items:center;gap:10px;margin-top:14px;flex-wrap:wrap}
    .flx-cuadre label{font-size:13px;color:#57534E}
    .flx-cuadre .input{max-width:180px}
    .flx-dif{font-size:13px;font-weight:600}
    .flx-dif-ok{color:#0F6E56}
    .flx-dif-bad{color:#B91C1C}
    .flx-asign{border-color:#FBD9A8;background:#FFFBF4}
    .flx-table-asign td,.flx-table-precios td{border-bottom:1px solid #F5F5F4}
    .flx-table-asign .select{min-width:160px}
    .flx-ok{color:#0F6E56;font-size:14px;background:#E1F5EE;border-radius:10px;padding:10px 14px;margin-bottom:16px}
    .flx-table-precios .input{max-width:160px}
  `;
  const style = document.createElement('style');
  style.id = 'flx-style';
  style.textContent = css;
  document.head.appendChild(style);
}
