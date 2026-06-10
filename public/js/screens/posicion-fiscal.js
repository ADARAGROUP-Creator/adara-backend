import { sbGet } from '../core/sb.js';

// ── Pantalla: Posición Fiscal — IVA, IIBB y pagos a cuenta por período ───────
// Lee v_posicion_fiscal (mensual). Mide cuánto le debe la empresa al fisco y
// cómo pega en la caja (P12). Es un tablero DISTINTO del Resultado: el IVA NO
// es rentabilidad, es flujo financiero; vive solo acá.
//
// El IVA sale de v_control_mensual (misma fuente que el panel de Resultado, para
// que no diverjan). Las capas de IIBB se suman encima:
//   - iibb_retenido     ← ventas_ml.impuestos (monto canónico, venta por venta,
//                         completo desde enero). Es PAGO A CUENTA, no costo.
//   - iibb_percepciones ← compra_componentes (pago a cuenta).
//   - impuesto_cheque   ← retenciones (impuesto a débitos/créditos bancarios).
//
// Límites actuales (ver ADARA-IMPUESTOS.md / P12):
//  - IVA crédito INCOMPLETO: faltan compras/gastos con factura A → el "IVA a
//    pagar" sale alto. El débito sí es exacto. Se llena con Tango/B2B + gastos.
//  - IIBB DETERMINADO (alícuota × base por jurisdicción) PENDIENTE de las
//    alícuotas del contador. Hoy solo se ven los pagos a cuenta; el saldo real
//    (determinado − pagos a cuenta) no se puede cerrar todavía.
//  - GANANCIAS pendiente de la tasa del contador.
//  - Apertura por jurisdicción y el impuesto al cheque arrancan en abril (gap
//    histórico ene/feb del settlement de MP, irrecuperable). El MONTO de IIBB
//    retenido sí está completo desde enero. Ver ADARA-RETENCIONES-IIBB.md.

function num(v) { return Number(v) || 0; }
function fmtNum(n, dec = 0) {
  return num(n).toLocaleString('es-AR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}
function fmtMoney(n) { return '$ ' + fmtNum(n, 2); }
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
const MESES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];
function fmtPeriodo(p) {
  const [y, m] = String(p || '').split('-');
  const i = parseInt(m, 10) - 1;
  return (MESES[i] || m) + ' ' + y;
}

let DATA = [];   // filas de v_posicion_fiscal

export async function loadPosicionFiscal() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando posición fiscal…</div>`;
  try {
    DATA = await sbGet('v_posicion_fiscal', 'select=*&order=periodo.asc');
    render();
  } catch (e) {
    root.innerHTML = `<div class="error"><strong>Error al cargar Posición Fiscal.</strong><br>${esc(e.message)}</div>`;
  }
}

const STYLE = `
<style>
.pf .pf-aviso{display:flex;gap:9px;align-items:flex-start;background:var(--acc-bg);border:1px solid #F3D6A3;color:var(--acc-dark);border-radius:var(--r);padding:12px 14px;font-size:13.5px;line-height:1.5;margin:0 0 16px}
.pf .pf-aviso .ic{flex-shrink:0;font-size:15px}
.pf .pf-aviso b{font-weight:700}

.pf .pf-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:0 0 22px}
.pf .pf-kpi{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:16px 18px;min-width:0}
.pf .pf-kpi .l{font-size:11.5px;color:var(--text-muted);font-weight:600;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px}
.pf .pf-kpi .v{font-weight:700;font-size:22px;line-height:1.15;color:var(--text);letter-spacing:-0.02em;font-variant-numeric:tabular-nums;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.pf .pf-kpi .s{font-size:11px;color:var(--text-muted);margin-top:5px;line-height:1.35}
.pf .pf-kpi.acc .v{color:var(--acc)}

.pf .pf-sec{margin:0 0 26px}
.pf .pf-sec-h{display:flex;align-items:baseline;gap:10px;margin:0 0 10px}
.pf .pf-sec-h h3{font-size:16px;font-weight:700;color:var(--text);margin:0}
.pf .pf-sec-h .tag{font-size:11px;font-weight:600;color:var(--text-muted)}

.pf .tbl{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);overflow-x:auto}
.pf table{width:100%;border-collapse:collapse;font-size:14px;min-width:640px}
.pf thead th{text-align:right;padding:11px 14px;font-weight:600;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em;background:var(--surface-alt);border-bottom:1px solid var(--border);white-space:nowrap}
.pf thead th:first-child{text-align:left}
.pf tbody td{padding:11px 14px;border-top:1px solid var(--border);text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap;color:var(--text)}
.pf tbody td:first-child{text-align:left;font-weight:500}
.pf tbody tr:hover{background:var(--surface-alt)}
.pf .apagar{font-weight:600}
.pf .favor{color:#15803D;font-weight:600}
.pf .pend{color:var(--text-soft)}
.pf .acct{color:var(--text-muted)}
.pf tr.tot td{border-top:2px solid var(--border-strong);font-weight:700;background:var(--surface-alt)}
.pf tr.tot:hover td{background:var(--surface-alt)}
.pf .empty{padding:34px;text-align:center;color:var(--text-muted)}
.pf .note{margin-top:10px;font-size:12.5px;line-height:1.55;color:var(--text-muted)}
.pf .note b{color:var(--text);font-weight:600}
</style>`;

// IVA a pagar: positivo = a pagar; negativo = saldo a favor (carryforward)
function ivaCell(n) {
  const v = num(n);
  if (v < 0) return `<span class="favor">${fmtMoney(-v)} a favor</span>`;
  return `<span class="apagar">${fmtMoney(v)}</span>`;
}

function render() {
  const root = document.getElementById('app-screens');
  const filas = [...DATA].sort((a, b) => String(a.periodo).localeCompare(String(b.periodo)));

  const T = filas.reduce((a, f) => ({
    iva_debito: a.iva_debito + num(f.iva_debito),
    iva_credito: a.iva_credito + num(f.iva_credito),
    iva_a_pagar: a.iva_a_pagar + num(f.iva_a_pagar),
    iibb_retenido: a.iibb_retenido + num(f.iibb_retenido),
    iibb_percepciones: a.iibb_percepciones + num(f.iibb_percepciones),
    impuesto_cheque: a.impuesto_cheque + num(f.impuesto_cheque),
  }), { iva_debito:0, iva_credito:0, iva_a_pagar:0, iibb_retenido:0, iibb_percepciones:0, impuesto_cheque:0 });

  const pagoCuentaIibb = T.iibb_retenido + T.iibb_percepciones;

  if (!filas.length) {
    root.innerHTML = `${STYLE}<div class="pf"><div class="empty">Todavía no hay datos fiscales cargados.</div></div>`;
    return;
  }

  // ── Tabla IVA ──────────────────────────────────────────────────────────
  const ivaBody = filas.map(f => `<tr>
      <td>${esc(fmtPeriodo(f.periodo))}</td>
      <td>${fmtMoney(f.iva_debito)}</td>
      <td>${num(f.iva_credito) ? fmtMoney(f.iva_credito) : '<span class="pend">0,00</span>'}</td>
      <td>${ivaCell(f.iva_a_pagar)}</td>
    </tr>`).join('');
  const ivaTot = `<tr class="tot">
      <td>Total</td>
      <td>${fmtMoney(T.iva_debito)}</td>
      <td>${fmtMoney(T.iva_credito)}</td>
      <td>${ivaCell(T.iva_a_pagar)}</td>
    </tr>`;

  // ── Tabla IIBB ─────────────────────────────────────────────────────────
  const iibbBody = filas.map(f => `<tr>
      <td>${esc(fmtPeriodo(f.periodo))}</td>
      <td class="acct">${fmtMoney(f.iibb_retenido)}</td>
      <td class="acct">${num(f.iibb_percepciones) ? fmtMoney(f.iibb_percepciones) : '<span class="pend">0,00</span>'}</td>
      <td><span class="pend">pendiente</span></td>
      <td><span class="pend">pendiente</span></td>
    </tr>`).join('');
  const iibbTot = `<tr class="tot">
      <td>Total</td>
      <td class="acct">${fmtMoney(T.iibb_retenido)}</td>
      <td class="acct">${fmtMoney(T.iibb_percepciones)}</td>
      <td><span class="pend">—</span></td>
      <td><span class="pend">—</span></td>
    </tr>`;

  // ── Tabla Otros (impuesto al cheque) ───────────────────────────────────
  const chBody = filas.map(f => `<tr>
      <td>${esc(fmtPeriodo(f.periodo))}</td>
      <td>${num(f.impuesto_cheque) ? fmtMoney(f.impuesto_cheque) : '<span class="pend">0,00</span>'}</td>
    </tr>`).join('');
  const chTot = `<tr class="tot"><td>Total</td><td>${fmtMoney(T.impuesto_cheque)}</td></tr>`;

  root.innerHTML = `${STYLE}
  <div class="pf">
    <div class="toolbar"><span class="grow"></span>
      <button class="btn btn-ghost" id="pf-reload">Actualizar</button>
    </div>

    <div class="pf-aviso"><span class="ic">⚠</span><div>
      La <b>Posición Fiscal</b> mide cuánto se le debe al fisco y cómo pega en la <b>caja</b> — es distinto del
      Resultado (el IVA no es rentabilidad). Hoy el <b>IVA a pagar sale alto porque el crédito está incompleto</b>
      (faltan compras y gastos con factura A; se llena con Tango/B2B y gastos). El <b>IIBB determinado</b> (alícuota ×
      base por jurisdicción) y <b>Ganancias</b> quedan pendientes de las alícuotas/tasa del contador. Las
      <b>retenciones y percepciones son pagos a cuenta</b> (plata adelantada), no costo: descuentan lo que queda por
      pagar, no se suman al impuesto.
    </div></div>

    <div class="pf-kpis">
      <div class="pf-kpi acc"><div class="l">IVA a pagar (acumulado)</div><div class="v">${fmtMoney(T.iva_a_pagar)}</div><div class="s">débito − crédito · crédito incompleto</div></div>
      <div class="pf-kpi"><div class="l">IIBB pagos a cuenta</div><div class="v">${fmtMoney(pagoCuentaIibb)}</div><div class="s">retenciones + percepciones</div></div>
      <div class="pf-kpi"><div class="l">Impuesto al cheque</div><div class="v">${fmtMoney(T.impuesto_cheque)}</div><div class="s">débitos/créditos bancarios</div></div>
      <div class="pf-kpi"><div class="l">IIBB determinado</div><div class="v pend" style="font-size:15px;font-weight:600">pendiente contador</div><div class="s">alícuotas por jurisdicción</div></div>
    </div>

    <div class="pf-sec">
      <div class="pf-sec-h"><h3>IVA</h3><span class="tag">débito − crédito = a pagar · mensual, devengado</span></div>
      <div class="tbl"><table>
        <thead><tr><th>Período</th><th>Débito (ventas)</th><th>Crédito (compras/gastos)</th><th>A pagar</th></tr></thead>
        <tbody>${ivaBody}${ivaTot}</tbody>
      </table></div>
      <div class="note"><b>Crédito incompleto:</b> solo están cargadas unas pocas compras con IVA. A medida que entren
      las compras (Tango/B2B) y los gastos con factura A, el crédito sube y el «a pagar» baja al valor real.</div>
    </div>

    <div class="pf-sec">
      <div class="pf-sec-h"><h3>Ingresos Brutos</h3><span class="tag">Convenio Multilateral · retenido y percibido = pago a cuenta</span></div>
      <div class="tbl"><table>
        <thead><tr><th>Período</th><th>Retenido (a cuenta)</th><th>Percepciones (a cuenta)</th><th>Determinado</th><th>Saldo</th></tr></thead>
        <tbody>${iibbBody}${iibbTot}</tbody>
      </table></div>
      <div class="note"><b>Determinado y saldo pendientes:</b> el IIBB determinado (alícuota × base por jurisdicción)
      necesita las alícuotas del contador. Recién con eso se calcula el saldo (determinado − pagos a cuenta) y se
      suma el determinado al Resultado. El <b>monto retenido</b> sale de la columna IMPUESTOS de Ventas ML (completo
      desde enero, venta por venta); la apertura por provincia arranca en abril.</div>
    </div>

    <div class="pf-sec">
      <div class="pf-sec-h"><h3>Otros</h3><span class="tag">impuesto al cheque · Ganancias</span></div>
      <div class="tbl"><table>
        <thead><tr><th>Período</th><th>Impuesto al cheque</th></tr></thead>
        <tbody>${chBody}${chTot}</tbody>
      </table></div>
      <div class="note"><b>Impuesto al cheque</b> (débitos/créditos bancarios): a definir con el contador si va como
      costo financiero al Resultado y qué porción es computable como pago a cuenta de Ganancias. <b>Ganancias:</b>
      pendiente de la tasa del contador; se sumará como renglón final del Resultado, no acá.</div>
    </div>
  </div>`;

  document.getElementById('pf-reload').addEventListener('click', loadPosicionFiscal);
}
