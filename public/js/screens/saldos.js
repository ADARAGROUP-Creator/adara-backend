import { sbGet, sbPost, sbPatch } from '../core/sb.js';

// ── Pantalla: Saldos — saldo por cuenta + carga del ancla de arranque ──────
//
// Qué resuelve: muestra el saldo en vivo de cada cuenta (banco/MP/caja) y
// permite cargar el "saldo de arranque" (ancla) a una fecha de corte.
//
// Modelo (ver ADARA-DECISIONES.md A2 + ADARA-CONCILIACION-BANCARIA.md):
//   saldo = ancla(al corte) + Σ movimientos POSTERIORES al corte.
// El ancla se guarda en `saldos_iniciales` a nivel CUENTA (linea_id = NULL):
// el saldo total de la cuenta queda exacto desde el corte; el saldo por línea
// se construye hacia adelante con los movimientos (que sí traen línea).
// La vista `v_saldo_cuenta` hace el cálculo; acá solo leemos y cargamos el ancla.
//
// Por qué un día de corte y no "ahora": `movimientos.fecha` es por día (sin
// hora). El corte limpio es el FIN de un día (default 12/6/2026): así los
// movimientos del 13 en adelante suman, y los anteriores (ej. abril) ya quedan
// dentro del ancla y no se vuelven a contar.
//
// MP: cargar SOLO el "dinero disponible", NO el "por cobrar" (ese entra solo
// como movimiento cuando MP lo libera; anclarlo lo contaría dos veces).

const CORTE_DEFAULT = '2026-06-12';

let CUENTAS = [];     // filas de v_saldo_cuenta
let ANCLAS = {};      // cuenta_id -> { id, monto, fecha } (saldos_iniciales sin línea)

// ── Carga ──────────────────────────────────────────────────────────────────
export async function loadSaldos() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando saldos…</div>`;
  try {
    CUENTAS = await sbGet('v_saldo_cuenta',
      'select=cuenta_id,codigo,nombre,tipo,moneda,ancla,fecha_corte,movimientos_post,saldo&order=codigo.asc');
    const anclas = await sbGet('saldos_iniciales', 'select=id,cuenta_id,monto,fecha&linea_id=is.null');
    ANCLAS = {};
    for (const a of anclas) ANCLAS[a.cuenta_id] = { id: a.id, monto: Number(a.monto), fecha: a.fecha };
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudieron cargar los saldos: ${esc(e.message)}</div>`;
    return;
  }
  inyectarEstilo();
  render();
}

// ── Render ───────────────────────────────────────────────────────────────────
function render() {
  const root = document.getElementById('app-screens');

  const totalARS = CUENTAS.filter(c => c.moneda === 'ARS').reduce((s, c) => s + num(c.saldo), 0);
  const totalUSD = CUENTAS.filter(c => c.moneda === 'USD').reduce((s, c) => s + num(c.saldo), 0);
  const sinAncla = CUENTAS.filter(c => !c.fecha_corte).length;

  const aviso = sinAncla
    ? `<div class="sld-aviso">⚠ <strong>${sinAncla} cuenta(s) sin ancla cargada.</strong> Mientras no cargues el saldo de arranque, el saldo es <em>provisorio</em>: suma todos los movimientos cargados (incluido lo viejo). Al cargar el ancla con su fecha de corte, lo anterior queda absorbido y deja de contar.</div>`
    : '';

  const cards = CUENTAS.map(cardHTML).join('');

  root.innerHTML = `
    <div class="sld-intro">
      Cargá el <strong>saldo de arranque</strong> (ancla) de cada cuenta a una fecha de corte.
      El saldo se calcula como <em>ancla + movimientos posteriores al corte</em>.
      Para <strong>MP</strong>, cargá solo el <strong>dinero disponible</strong>, no el "por cobrar".
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">Total ARS</div><div class="kpi-value">${fmtMoneda(totalARS, 'ARS')}</div></div>
      <div class="kpi"><div class="kpi-label">Total USD</div><div class="kpi-value">${fmtMoneda(totalUSD, 'USD')}</div></div>
      <div class="kpi"><div class="kpi-label">Cuentas</div><div class="kpi-value">${CUENTAS.length}</div></div>
    </div>

    ${aviso}

    <div class="sld-cards">${cards}</div>
  `;

  CUENTAS.forEach(c => {
    const btn = document.getElementById('sld-save-' + c.cuenta_id);
    if (btn) btn.addEventListener('click', () => guardarAncla(c.cuenta_id));
    const inp = document.getElementById('sld-monto-' + c.cuenta_id);
    if (inp) inp.addEventListener('keydown', e => { if (e.key === 'Enter') guardarAncla(c.cuenta_id); });
  });
}

function cardHTML(c) {
  const a = ANCLAS[c.cuenta_id];
  const tieneAncla = !!c.fecha_corte;
  const saldoCls = num(c.saldo) < 0 ? 'sld-neg' : 'sld-pos';
  const tipoLbl = { banco: 'Banco', mp: 'Mercado Pago', caja: 'Caja' }[c.tipo] || c.tipo;

  const montoVal = a ? a.monto : '';
  const fechaVal = a ? a.fecha : CORTE_DEFAULT;

  return `
    <div class="sld-card">
      <div class="sld-card-head">
        <div>
          <div class="sld-nombre">${esc(c.nombre)}</div>
          <div class="sld-meta">${tipoLbl} · ${c.moneda}</div>
        </div>
        <div class="sld-saldo ${saldoCls}">
          ${fmtMoneda(c.saldo, c.moneda)}
          <div class="sld-saldo-lbl">${tieneAncla ? 'saldo' : 'provisorio'}</div>
        </div>
      </div>

      <div class="sld-desglose">
        <span>Ancla${tieneAncla ? ' (al ' + fmtFecha(c.fecha_corte) + ')' : ''}: <strong>${fmtMoneda(c.ancla, c.moneda)}</strong></span>
        <span>Movimientos ${tieneAncla ? 'desde el corte' : '(todos)'}: <strong>${fmtMoneda(c.movimientos_post, c.moneda)}</strong></span>
      </div>

      <div class="sld-form">
        <label class="sld-lbl">Saldo de arranque
          <input class="input sld-monto" id="sld-monto-${c.cuenta_id}" type="text" inputmode="decimal"
                 placeholder="0,00" value="${montoVal !== '' ? fmtNum(montoVal, 2) : ''}">
        </label>
        <label class="sld-lbl">Al cierre del
          <input class="input sld-fecha" id="sld-fecha-${c.cuenta_id}" type="date" value="${fechaVal}">
        </label>
        <button class="btn btn-primary" id="sld-save-${c.cuenta_id}">${tieneAncla ? 'Actualizar' : 'Guardar'}</button>
      </div>
    </div>
  `;
}

// ── Guardar ancla ──────────────────────────────────────────────────────────
async function guardarAncla(cuentaId) {
  const inpM = document.getElementById('sld-monto-' + cuentaId);
  const inpF = document.getElementById('sld-fecha-' + cuentaId);
  const btn = document.getElementById('sld-save-' + cuentaId);
  const monto = parseMonto(inpM.value);
  const fecha = inpF.value;

  if (!Number.isFinite(monto)) { window.toast('Monto inválido', 'error'); inpM.focus(); return; }
  if (!fecha) { window.toast('Elegí la fecha de corte', 'error'); inpF.focus(); return; }

  const prev = ANCLAS[cuentaId];
  btn.disabled = true; const txt = btn.textContent; btn.textContent = 'Guardando…';
  try {
    if (prev && prev.id) {
      await sbPatch('saldos_iniciales', 'id=eq.' + prev.id, { monto, fecha });
    } else {
      await sbPost('saldos_iniciales', { cuenta_id: cuentaId, monto, fecha, linea_id: null });
    }
    window.toast('Saldo de arranque guardado', 'ok');
    await loadSaldos();
  } catch (e) {
    btn.disabled = false; btn.textContent = txt;
    window.toast('No se pudo guardar: ' + e.message, 'error');
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function num(v) { return Number(v) || 0; }

function fmtNum(n, dec = 0) {
  return num(n).toLocaleString('es-AR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function fmtMoneda(n, moneda) {
  const pref = moneda === 'USD' ? 'US$ ' : '$ ';
  return pref + fmtNum(n, 2);
}

function fmtFecha(iso) {
  if (!iso) return '—';
  const p = String(iso).split('-');
  return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : iso;
}

// Parser de monto tolerante: acepta "1.234.567,89" (es-AR), "1234567.89", "1,234.56".
function parseMonto(s) {
  if (typeof s === 'number') return s;
  let t = String(s).trim().replace(/[^\d.,-]/g, '');
  if (!t) return NaN;
  const lastComma = t.lastIndexOf(',');
  const lastDot = t.lastIndexOf('.');
  if (lastComma > -1 && lastDot > -1) {
    if (lastComma > lastDot) t = t.replace(/\./g, '').replace(',', '.'); // coma decimal
    else t = t.replace(/,/g, '');                                        // punto decimal
  } else if (lastComma > -1) {
    const after = t.length - lastComma - 1;
    t = (after >= 1 && after <= 2) ? t.replace(',', '.') : t.replace(/,/g, '');
  }
  return Number(t);
}

function esc(s) {
  return String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

function inyectarEstilo() {
  if (document.getElementById('sld-style')) return;
  const css = `
    .sld-intro{font-size:13px;line-height:1.5;color:#57534E;background:#F5F5F4;border:1px solid #E7E5E4;border-radius:10px;padding:11px 14px}
    .sld-intro em{font-style:normal;font-weight:600}
    .sld-aviso{margin:4px 0 14px;padding:9px 12px;font-size:13px;line-height:1.45;color:#854F0B;background:#FAEEDA;border:1px solid #F0DCB8;border-radius:8px}
    .sld-aviso em{font-style:normal;font-weight:600}
    .sld-cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px}
    .sld-card{border:1px solid #E7E5E4;border-radius:12px;padding:16px;background:#fff}
    .sld-card-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
    .sld-nombre{font-size:15px;font-weight:700;color:#1C1917}
    .sld-meta{font-size:12px;color:#A8A29E;margin-top:2px}
    .sld-saldo{text-align:right;font-size:20px;font-weight:700;font-variant-numeric:tabular-nums;line-height:1.1}
    .sld-saldo-lbl{font-size:11px;font-weight:500;color:#A8A29E;margin-top:2px}
    .sld-pos{color:#15803D}
    .sld-neg{color:#B91C1C}
    .sld-desglose{display:flex;flex-direction:column;gap:3px;margin:12px 0;font-size:12px;color:#78716C;border-top:1px dashed #E7E5E4;padding-top:10px}
    .sld-desglose strong{color:#44403C;font-variant-numeric:tabular-nums}
    .sld-form{display:flex;flex-wrap:wrap;align-items:flex-end;gap:8px;margin-top:6px}
    .sld-lbl{display:flex;flex-direction:column;gap:3px;font-size:11px;color:#78716C}
    .sld-monto{width:140px;text-align:right;font-variant-numeric:tabular-nums}
    .sld-fecha{width:auto}
  `;
  const style = document.createElement('style');
  style.id = 'sld-style';
  style.textContent = css;
  document.head.appendChild(style);
}
