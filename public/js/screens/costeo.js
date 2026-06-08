import { sbGet } from '../core/sb.js';

// ── Pantalla: Inventario — Valorización de stock (hash #costeo) ─────────────
// Foto del stock valuado a costo + productos sin costo cargado. Lee:
//  - v_valorizacion_stock : inventario a costo por familia × depósito.
//  - v_skus_sin_costo     : SKUs con stock y costo $0 (subestiman valorización/CMV).
// El CMV mensual y el margen viven ahora en la pantalla Resultado.
//
// La pantalla inyecta su propio <style> scopeado con prefijo `.cost`
// (las clases `.num`/`.psi-aviso` que usaba la versión vieja no existen en base.css).

function num(v) { return Number(v) || 0; }
function fmtNum(n, dec = 0) {
  return num(n).toLocaleString('es-AR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}
function fmtMoney(n) { return '$ ' + fmtNum(n, 2); }
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// Tipo de cambio USD→ARS. HARDCODE temporal — a futuro se trae del BCRA.
// Cambiar este único valor hasta tener la integración con el Banco Central.
const TC_USD = 1465;
function fmtUsd(ars) { return 'US$ ' + fmtNum(num(ars) / TC_USD, 2); }

// ── Nombres legibles ───────────────────────────────────────────────────────
const FAM_NOMBRE = {
  electronica: 'Electrónica',
  mochila_individual: 'Mochila individual',
  mochila_sindical: 'Mochila sindical',
  vaso_termico: 'Vaso térmico',
  luminaria: 'Luminaria',
  repelente: 'Repelente',
};
function famNombre(f) {
  if (!f) return '—';
  return FAM_NOMBRE[f] || (f.charAt(0).toUpperCase() + f.slice(1).replace(/_/g, ' '));
}

const DEPO_NOMBRE = {
  DEP:  'Depósito',
  DJ:   'Dep José Cramer',
  MENV: 'Mercado Envíos - Colecta',
  MFUL: 'Mercado Libre Full',
};
const depoNombre = c => DEPO_NOMBRE[c] || c || '—';
function depositosNombre(str) {
  if (!str) return '—';
  return String(str).split(',').map(c => depoNombre(c.trim())).join(', ');
}

const dash = '<span class="dash">—</span>';
const udsCell = n => num(n) > 0 ? fmtNum(n) : dash;

// ── Carga ──────────────────────────────────────────────────────────────────
export async function loadCosteo() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando inventario…</div>`;
  try {
    const [val, sinCosto] = await Promise.all([
      sbGet('v_valorizacion_stock', 'select=*&order=valorizado.desc.nullslast'),
      sbGet('v_skus_sin_costo', 'select=*&order=unidades.desc')
    ]);
    render(val, sinCosto);
  } catch (e) {
    root.innerHTML = `<div class="error"><strong>Error al cargar Inventario.</strong><br>${esc(e.message)}</div>`;
  }
}

// ── Estilos scopeados (.cost) ───────────────────────────────────────────────
const STYLE = `
<style>
.cost .cost-h{margin:28px 0 10px;font-size:15px;font-weight:600;color:var(--text)}

.cost .cost-kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px;margin:14px 0 6px}
.cost .cost-kpi{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:16px 18px;min-width:0}
.cost .cost-kpi .l{font-size:11.5px;color:var(--text-muted);font-weight:600;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px}
.cost .cost-kpi .v{font-weight:700;font-size:22px;line-height:1.15;color:var(--text);letter-spacing:-0.02em;font-variant-numeric:tabular-nums;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cost .cost-kpi.acc .v{color:var(--acc)}
.cost .cost-kpi .s{font-size:12px;color:var(--text-muted);font-weight:500;margin-top:7px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

.cost .cost-aviso{display:flex;gap:9px;align-items:flex-start;background:var(--acc-bg);border:1px solid #F3D6A3;color:var(--acc-dark);border-radius:var(--r);padding:12px 14px;font-size:13.5px;line-height:1.5;margin:0 0 4px}
.cost .cost-aviso .ic{flex-shrink:0;font-size:15px;line-height:1.3}
.cost .cost-aviso b{font-weight:700}

.cost .tbl{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);overflow:hidden}
.cost table{width:100%;border-collapse:collapse;font-size:14.5px}
.cost thead th{text-align:left;padding:11px 16px;font-weight:600;font-size:11.5px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.05em;background:var(--surface-alt);border-bottom:1px solid var(--border);white-space:nowrap}
.cost tbody td{padding:11px 16px;border-top:1px solid var(--border);vertical-align:middle;color:var(--text)}
.cost th.r,.cost td.r{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.cost tbody tr:hover{background:var(--surface-alt)}
.cost .muted{color:var(--text-muted)}
.cost .dash{color:var(--text-soft)}
.cost .code{font-family:'JetBrains Mono','SF Mono',Menlo,monospace;font-size:13px;font-weight:500}

.cost tr.grp{cursor:pointer}
.cost tr.grp td{font-weight:600}
.cost tr.grp .chev{display:inline-block;width:14px;color:var(--text-muted);font-size:10px;transition:transform .15s;transform:translateY(-1px)}
.cost tr.grp.open .chev{transform:rotate(90deg)}
.cost tr.sub td{background:#FCFCFB;border-top:1px solid var(--border)}
.cost tr.sub:hover td{background:var(--surface-alt)}
.cost tr.sub .dep{padding-left:36px;color:var(--text-muted)}

.cost tr.tot td{border-top:2px solid var(--border-strong);font-weight:700;background:var(--surface-alt)}
.cost tr.tot:hover td{background:var(--surface-alt)}
.cost tr.tot .lbl{text-transform:uppercase;letter-spacing:.04em;font-size:12.5px}

.cost .cost-note{margin-top:16px;background:var(--surface-alt);border:1px solid var(--border);border-radius:var(--r);padding:12px 14px;font-size:13px;line-height:1.55;color:var(--text-muted)}
.cost .cost-note b{color:var(--text);font-weight:600}
.cost .empty{padding:34px;text-align:center;color:var(--text-muted);font-size:14px}
</style>`;

// ── Render ───────────────────────────────────────────────────────────────
function render(val, sinCosto) {
  const root = document.getElementById('app-screens');

  const valorizadoTotal = val.reduce((s, r) => s + num(r.valorizado), 0);
  const udsSinCosto = val.reduce((s, r) => s + num(r.unidades_sin_costo), 0);

  const aviso = udsSinCosto > 0
    ? `<div class="cost-aviso"><span class="ic">⚠</span><div><b>${fmtNum(udsSinCosto)} unidades sin costo cargado.</b>
        La valorización queda subestimada hasta cargar esos costos en los SKUs.</div></div>`
    : '';

  // ── Valorización (familia agrupada, electrónica desplegable, + total) ─────
  const famMap = {};
  for (const r of val) (famMap[r.familia || '—'] ||= []).push(r);
  const famOrden = Object.entries(famMap).map(([familia, rows]) => ({
    familia, rows,
    uds:  rows.reduce((s, r) => s + num(r.unidades), 0),
    valz: rows.reduce((s, r) => s + num(r.valorizado), 0),
    sc:   rows.reduce((s, r) => s + num(r.unidades_sin_costo), 0),
  })).sort((a, b) => b.valz - a.valz);

  const totUds = famOrden.reduce((s, g) => s + g.uds, 0);
  const totValz = famOrden.reduce((s, g) => s + g.valz, 0);
  const totSc = famOrden.reduce((s, g) => s + g.sc, 0);

  let valBody = '';
  if (!famOrden.length) {
    valBody = `<tr><td colspan="6" class="empty">Sin stock.</td></tr>`;
  } else {
    for (const g of famOrden) {
      if (g.rows.length > 1) {
        const fam = esc(g.familia);
        valBody += `<tr class="grp" data-fam="${fam}">
          <td><span class="chev">▸</span>${esc(famNombre(g.familia))}</td>
          <td class="muted">${g.rows.length} depósitos</td>
          <td class="r">${fmtNum(g.uds)}</td>
          <td class="r">${fmtMoney(g.valz)}</td>
          <td class="r">${fmtUsd(g.valz)}</td>
          <td class="r">${udsCell(g.sc)}</td>
        </tr>`;
        for (const r of g.rows.slice().sort((a, b) => num(b.valorizado) - num(a.valorizado))) {
          valBody += `<tr class="sub" data-fam="${fam}" style="display:none">
            <td></td>
            <td class="dep">${esc(depoNombre(r.deposito))}</td>
            <td class="r">${fmtNum(r.unidades)}</td>
            <td class="r">${fmtMoney(r.valorizado)}</td>
            <td class="r">${fmtUsd(r.valorizado)}</td>
            <td class="r">${udsCell(r.unidades_sin_costo)}</td>
          </tr>`;
        }
      } else {
        const r = g.rows[0];
        valBody += `<tr>
          <td>${esc(famNombre(g.familia))}</td>
          <td>${esc(depoNombre(r.deposito))}</td>
          <td class="r">${fmtNum(r.unidades)}</td>
          <td class="r">${fmtMoney(r.valorizado)}</td>
          <td class="r">${fmtUsd(r.valorizado)}</td>
          <td class="r">${udsCell(r.unidades_sin_costo)}</td>
        </tr>`;
      }
    }
    valBody += `<tr class="tot">
      <td class="lbl" colspan="2">Total</td>
      <td class="r">${fmtNum(totUds)}</td>
      <td class="r">${fmtMoney(totValz)}</td>
      <td class="r">${fmtUsd(totValz)}</td>
      <td class="r">${udsCell(totSc)}</td>
    </tr>`;
  }

  // ── SKUs sin costo ────────────────────────────────────────────────────────
  let scBody;
  if (sinCosto && sinCosto.length) {
    const totSkUds = sinCosto.reduce((s, r) => s + num(r.unidades), 0);
    scBody = sinCosto.map(r => `<tr>
        <td class="code">${esc(r.codigo)}</td>
        <td>${esc(r.descripcion)}</td>
        <td>${esc(famNombre(r.familia))}</td>
        <td class="muted">${esc(depositosNombre(r.depositos))}</td>
        <td class="r">${fmtNum(r.unidades)}</td>
      </tr>`).join('')
      + `<tr class="tot"><td class="lbl" colspan="4">Total</td><td class="r">${fmtNum(totSkUds)}</td></tr>`;
  } else {
    scBody = `<tr><td colspan="5" class="empty">Todos los SKUs con stock tienen costo cargado ✓</td></tr>`;
  }

  // ── Ensamblado ────────────────────────────────────────────────────────────
  root.innerHTML = `${STYLE}
  <div class="cost">
    <div class="toolbar"><span class="grow"></span>
      <button class="btn btn-ghost" id="cost-reload">Actualizar</button>
    </div>

    ${aviso}

    <div class="cost-kpis">
      <div class="cost-kpi acc"><div class="l">Valorización stock</div><div class="v">${fmtMoney(valorizadoTotal)}</div><div class="s">${fmtUsd(valorizadoTotal)} · TC $${fmtNum(TC_USD)}</div></div>
      <div class="cost-kpi"><div class="l">Uds sin costo</div><div class="v">${fmtNum(udsSinCosto)}</div></div>
    </div>

    <h3 class="cost-h">Valorización de stock</h3>
    <div class="tbl"><table>
      <thead><tr>
        <th>Familia</th><th>Depósito</th>
        <th class="r">Unidades</th><th class="r">Valorizado</th><th class="r">Valorizado USD</th><th class="r">Uds s/costo</th>
      </tr></thead>
      <tbody>${valBody}</tbody>
    </table></div>

    <h3 class="cost-h">SKUs sin costo cargado${(sinCosto && sinCosto.length) ? ` (${sinCosto.length})` : ''}</h3>
    <div class="tbl"><table>
      <thead><tr>
        <th>Código</th><th>Producto</th><th>Familia</th><th>Depósito(s)</th><th class="r">Unidades</th>
      </tr></thead>
      <tbody>${scBody}</tbody>
    </table></div>

    <div class="cost-note">
      <b>Valorización a costo.</b> Es cuánto vale tu stock a hoy, valuado al costo de los lotes (no al precio de venta).
      Los SKUs sin costo cargado figuran en $0 y por eso subestiman el total. El CMV mensual y el margen se
      muestran en la pantalla <b>Resultado</b>.
    </div>
  </div>`;

  document.getElementById('cost-reload').addEventListener('click', loadCosteo);

  root.querySelectorAll('.cost tr.grp').forEach(tr => {
    tr.addEventListener('click', () => {
      const fam = tr.getAttribute('data-fam');
      const abrir = tr.classList.toggle('open');
      root.querySelectorAll(`.cost tr.sub[data-fam="${fam}"]`)
        .forEach(c => { c.style.display = abrir ? '' : 'none'; });
    });
  });
}
