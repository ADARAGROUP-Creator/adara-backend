import { sbGet } from '../core/sb.js';

// ── Pantalla: Compras (capa 2) ──────────────────────────────────────────
// Facturas de compra de MERCADERÍA (local). A diferencia de Gastos, una compra
// se vuelve STOCK (lotes) y recién impacta el P&L como CMV al vender (FIFO).
// Dos pestañas: Facturas (lista + alta) y Cuenta corriente por proveedor
// (suma compras + gastos del proveedor = lo que le debés / te debe).
// Importaciones (USD, prorrateo de nacionalización) son otro flujo, pendiente.

let LINEAS = [], LINEA_LABEL = {};
let PROVEEDORES = [], PROV_BY_ID = {};
let SKUS = [], SKU_BY_ID = {};
let COMPRAS = [];     // v_compras_ap
let GASTOS_AP = [];   // v_gastos_ap (para cuenta corriente)
let TAB = 'facturas';
let FILTRO = { periodo: '', q: '' };

const hoyISO = () => new Date().toISOString().slice(0, 10);
const lineaLabel = l => l.nombre || l.descripcion || l.codigo || ('Línea ' + l.id);
const provLabel = p => p && (p.nombre || p.cuit || ('Proveedor #' + p.id)) || '— Sin proveedor —';
const skuLabel = s => `${s.codigo}${s.descripcion ? ' — ' + s.descripcion : ''}`;
const num = n => Number(n) || 0;
const money = n => '$ ' + num(n).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ddmm = f => `${(f || '').slice(8, 10)}/${(f || '').slice(5, 7)}`;
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

export async function loadCompras() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando compras…</div>`;
  try {
    LINEAS = await sbGet('lineas_negocio', 'order=id.asc');
    LINEA_LABEL = Object.fromEntries(LINEAS.map(l => [l.id, lineaLabel(l)]));
    SKUS = await sbGet('skus', 'activo=eq.true&order=codigo.asc&select=id,codigo,descripcion,alicuota_iva');
    SKU_BY_ID = Object.fromEntries(SKUS.map(s => [s.id, s]));
    await recargar();
  } catch (e) {
    root.innerHTML = `<div class="error">No se pudo cargar Compras: ${e.message}</div>`;
    return;
  }
  inyectarEstilo();
  render();
}

async function recargar() {
  PROVEEDORES = await sbGet('proveedores', 'order=nombre.asc').catch(() => []);
  PROV_BY_ID = Object.fromEntries(PROVEEDORES.map(p => [p.id, p]));
  COMPRAS = await sbGet('v_compras_ap', 'order=fecha.desc,compra_id.desc');
  GASTOS_AP = await sbGet('v_gastos_ap', 'select=proveedor_id,a_pagar_ars,vinculado_ars,saldo_pendiente_ars').catch(() => []);
}

function estadoPago(c) {
  const saldo = num(c.saldo_ap_ars);
  if (saldo < 0.02) return 'pagado';
  if (num(c.pagado_ars) > 0) return 'parcial';
  return 'pendiente';
}

function render() {
  const root = document.getElementById('app-screens');
  const tab = (val, label) => `<button class="pill ${TAB === val ? 'active' : ''}" data-tab="${val}">${label}</button>`;
  root.innerHTML = `
    <div class="pills" style="margin-bottom:14px">
      ${tab('facturas', 'Facturas')}
      ${tab('cc', 'Cuenta corriente')}
    </div>
    <div id="com-body"></div>`;
  root.querySelectorAll('.pill[data-tab]').forEach(p =>
    p.addEventListener('click', () => { TAB = p.dataset.tab; render(); }));
  if (TAB === 'facturas') renderFacturas();
  else renderCC();
}

// ── Pestaña Facturas ────────────────────────────────────────────────────
function renderFacturas() {
  const body = document.getElementById('com-body');
  const activas = COMPRAS.filter(c => c.estado_compra !== 'anulada');

  const comprado = activas.reduce((s, c) => s + num(c.total_facturado_ars), 0);
  const pendiente = activas.reduce((s, c) => s + num(c.saldo_ap_ars), 0);

  const periodos = [...new Set(activas.map(c => c.periodo).filter(Boolean))].sort().reverse();
  const optPeriodo = '<option value="">Todos los períodos</option>' +
    periodos.map(p => `<option value="${p}" ${FILTRO.periodo === p ? 'selected' : ''}>${p}</option>`).join('');

  const filtradas = activas.filter(c => {
    if (FILTRO.periodo && c.periodo !== FILTRO.periodo) return false;
    if (FILTRO.q) {
      const txt = `${provLabel(PROV_BY_ID[c.proveedor_id])} ${LINEA_LABEL[c.linea_id] || ''}`.toLowerCase();
      if (!txt.includes(FILTRO.q.toLowerCase())) return false;
    }
    return true;
  });

  body.innerHTML = `
    <div class="toolbar" style="justify-content:space-between">
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <select class="select" id="cf-periodo" style="width:auto">${optPeriodo}</select>
        <input class="input" id="cf-q" placeholder="Buscar proveedor / línea…" value="${esc(FILTRO.q)}" style="width:220px">
      </div>
      <button class="btn btn-primary" id="cf-nueva">+ Nueva compra</button>
    </div>

    <div class="kpi-grid" style="margin:14px 0">
      <div class="kpi"><div class="kpi-label">Compras</div><div class="kpi-value">${activas.length}</div></div>
      <div class="kpi"><div class="kpi-label">Comprado</div><div class="kpi-value">${money(comprado)}</div></div>
      <div class="kpi"><div class="kpi-label">Pendiente de pago</div><div class="kpi-value">${money(pendiente)}</div></div>
    </div>

    ${filtradas.length === 0
      ? `<div class="empty">No hay compras cargadas todavía.</div>`
      : `<div class="table-wrap"><table class="t">
          <thead><tr>
            <th style="width:62px">Fecha</th><th>Proveedor</th><th style="width:150px">Línea</th>
            <th style="width:140px;text-align:right">Total</th>
            <th style="width:140px;text-align:right">Pagado</th>
            <th style="width:140px;text-align:right">Saldo</th>
            <th style="width:96px">Pago</th>
          </tr></thead>
          <tbody>${filtradas.map(filaCompra).join('')}</tbody>
        </table></div>`}
  `;

  document.getElementById('cf-periodo').addEventListener('change', e => { FILTRO.periodo = e.target.value; renderFacturas(); });
  document.getElementById('cf-q').addEventListener('input', e => { FILTRO.q = e.target.value; renderFacturas(); });
  document.getElementById('cf-nueva').addEventListener('click', openAlta);
}

function filaCompra(c) {
  const est = estadoPago(c);
  return `<tr>
    <td>${ddmm(c.fecha)}</td>
    <td>${esc(provLabel(PROV_BY_ID[c.proveedor_id]))}</td>
    <td class="com-muted">${esc(LINEA_LABEL[c.linea_id] || '—')}</td>
    <td style="text-align:right" class="com-mono">${money(c.total_facturado_ars)}</td>
    <td style="text-align:right" class="com-mono com-muted">${money(c.pagado_ars)}</td>
    <td style="text-align:right" class="com-mono">${money(c.saldo_ap_ars)}</td>
    <td><span class="com-badge com-badge-${est}">${est}</span></td>
  </tr>`;
}

// ── Pestaña Cuenta corriente ──────────────────────────────────────────────
function renderCC() {
  const body = document.getElementById('com-body');
  const acc = {}; // proveedor_id -> {fact, pag, saldo}
  const add = (pid, fact, pag, saldo) => {
    if (pid == null) return;
    const a = acc[pid] || (acc[pid] = { fact: 0, pag: 0, saldo: 0 });
    a.fact += fact; a.pag += pag; a.saldo += saldo;
  };
  for (const c of COMPRAS) {
    if (c.estado_compra === 'anulada') continue;
    add(c.proveedor_id, num(c.total_facturado_ars), num(c.pagado_ars), num(c.saldo_ap_ars));
  }
  for (const g of GASTOS_AP) {
    add(g.proveedor_id, num(g.a_pagar_ars), num(g.vinculado_ars), num(g.saldo_pendiente_ars));
  }

  const filas = Object.entries(acc)
    .map(([pid, a]) => ({ pid, ...a }))
    .sort((x, y) => Math.abs(y.saldo) - Math.abs(x.saldo));

  const totalDeuda = filas.reduce((s, f) => s + Math.max(0, f.saldo), 0);

  body.innerHTML = `
    <div class="kpi-grid" style="margin:0 0 14px">
      <div class="kpi"><div class="kpi-label">Proveedores con saldo</div><div class="kpi-value">${filas.filter(f => Math.abs(f.saldo) >= 0.02).length}</div></div>
      <div class="kpi"><div class="kpi-label">Total que debés</div><div class="kpi-value">${money(totalDeuda)}</div></div>
    </div>
    <p class="com-sub">Suma compras + gastos por proveedor. Saldo positivo = le debés; negativo = te debe.</p>
    ${filas.length === 0
      ? `<div class="empty">Todavía no hay operaciones con proveedores.</div>`
      : `<div class="table-wrap"><table class="t">
          <thead><tr>
            <th>Proveedor</th>
            <th style="width:150px;text-align:right">Facturado</th>
            <th style="width:150px;text-align:right">Pagado</th>
            <th style="width:200px;text-align:right">Saldo</th>
          </tr></thead>
          <tbody>${filas.map(filaCC).join('')}</tbody>
        </table></div>`}
  `;
}

function filaCC(f) {
  const saldo = f.saldo;
  let etiqueta = '<span class="com-muted">saldada</span>';
  if (saldo >= 0.02) etiqueta = `<span class="com-debe">le debés ${money(saldo)}</span>`;
  else if (saldo <= -0.02) etiqueta = `<span class="com-favor">te debe ${money(-saldo)}</span>`;
  return `<tr>
    <td>${esc(provLabel(PROV_BY_ID[f.pid]))}</td>
    <td style="text-align:right" class="com-mono com-muted">${money(f.fact)}</td>
    <td style="text-align:right" class="com-mono com-muted">${money(f.pag)}</td>
    <td style="text-align:right" class="com-mono">${etiqueta}</td>
  </tr>`;
}

// ── Alta de compra local ──────────────────────────────────────────────────
let ITEMS = [];

function openAlta() {
  ITEMS = [{ sku_id: '', cantidad: '', costo: '', iva: 0.21 }];
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal" style="max-width:680px">
      <div class="card-title">Nueva compra local</div>

      <div class="field">
        <label>Proveedor</label>
        <div style="display:flex;gap:8px;align-items:center">
          <select class="select" id="c-prov" style="flex:1">${optProv()}</select>
          <button class="btn btn-ghost com-mini" id="c-prov-add" type="button">+ Nuevo</button>
        </div>
        <div id="c-prov-new" style="display:none;flex-direction:column;gap:8px;margin-top:8px">
          <div style="display:flex;gap:8px;align-items:center">
            <input class="input" id="c-prov-cuit" placeholder="CUIT (sin guiones)" style="width:200px">
            <span id="c-prov-status" style="font-size:12px;color:#78716C"></span>
          </div>
          <div style="display:flex;gap:8px;align-items:center">
            <input class="input" id="c-prov-nombre" placeholder="Nombre (se completa con el CUIT)" style="flex:1">
            <button class="btn btn-primary com-mini" id="c-prov-crear" type="button">Crear</button>
          </div>
        </div>
      </div>

      <div class="com-row3">
        <div class="field"><label>Fecha</label><input class="input" type="date" id="c-fecha" value="${hoyISO()}"></div>
        <div class="field"><label>Nº factura (opcional)</label><input class="input" id="c-factura" placeholder="A 0001-00001234"></div>
        <div class="field"><label>Línea de negocio</label><select class="select" id="c-linea"><option value="">Elegí línea…</option>${LINEAS.map(l => `<option value="${l.id}">${esc(LINEA_LABEL[l.id])}</option>`).join('')}</select></div>
      </div>

      <div class="com-block">
        <div class="com-block-h"><span>Productos (mercadería)</span><button class="btn btn-ghost com-mini" id="c-add-item" type="button">+ Agregar</button></div>
        <div id="c-items"></div>
      </div>

      <div class="com-block">
        <div class="com-block-h"><span>Impuestos de la factura (crédito fiscal — no son costo)</span></div>
        <div class="com-row3">
          <div class="field"><label>IVA (automático)</label><input class="input" id="c-iva-disp" readonly value="$ 0,00"></div>
          <div class="field"><label>Percepción IIBB</label><input class="input com-fisc" id="c-iibb" inputmode="decimal" placeholder="0,00"></div>
          <div class="field"><label>Percepción Ganancias</label><input class="input com-fisc" id="c-gan" inputmode="decimal" placeholder="0,00"></div>
        </div>
      </div>

      <div class="com-resumen" id="c-resumen"></div>

      <div class="modal-actions">
        <button class="btn btn-ghost" id="c-cancel" type="button">Cancelar</button>
        <button class="btn btn-primary" id="c-guardar" type="button">Guardar compra</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  const $ = s => overlay.querySelector(s);
  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  $('#c-cancel').addEventListener('click', close);

  // Alta rápida de proveedor
  $('#c-prov-add').addEventListener('click', () => {
    const box = $('#c-prov-new');
    box.style.display = box.style.display === 'none' ? 'flex' : 'none';
  });
  async function buscarPadronProv() {
    const cuit = $('#c-prov-cuit').value.replace(/\D/g, '');
    const st = $('#c-prov-status');
    if (cuit.length !== 11) { st.textContent = cuit.length ? '⚠ El CUIT debe tener 11 dígitos' : ''; return null; }
    st.textContent = 'Buscando en ARCA…'; st.style.color = '#78716C';
    try {
      const r = await fetch('/padron/' + cuit);
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || 'no encontrado');
      if (d.nombre) $('#c-prov-nombre').value = d.nombre;
      st.textContent = '✓ ' + d.nombre + (d.estado ? ' · ' + d.estado : ''); st.style.color = '#0F6E56';
      return d.nombre;
    } catch (e) {
      st.textContent = '⚠ ' + e.message; st.style.color = '#B91C1C';
      return null;
    }
  }
  $('#c-prov-cuit').addEventListener('blur', buscarPadronProv);

  $('#c-prov-crear').addEventListener('click', async () => {
    const cuit = $('#c-prov-cuit').value.replace(/\D/g, '');
    if (cuit.length !== 11) { window.toast('El CUIT es obligatorio (11 dígitos)', 'error'); return; }
    if (!$('#c-prov-nombre').value.trim()) await buscarPadronProv();
    const nombre = $('#c-prov-nombre').value.trim();
    if (!nombre) { window.toast('No se pudo traer el nombre desde ARCA; revisá el CUIT o escribilo a mano', 'error'); return; }
    try {
      const r = await fetch('/proveedores', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nombre, cuit })
      });
      const p = await r.json();
      if (!r.ok) throw new Error(p.error || r.statusText);
      PROVEEDORES.push(p); PROV_BY_ID[p.id] = p;
      PROVEEDORES.sort((a, b) => provLabel(a).localeCompare(provLabel(b)));
      $('#c-prov').innerHTML = optProv(p.id);
      $('#c-prov-new').style.display = 'none';
      $('#c-prov-nombre').value = ''; $('#c-prov-cuit').value = ''; $('#c-prov-status').textContent = '';
      window.toast('Proveedor creado');
    } catch (e) { window.toast('Error: ' + e.message, 'error'); }
  });

  // Ítems
  const itemsBox = $('#c-items');
  const pintarItems = () => {
    itemsBox.innerHTML = ITEMS.map((it, i) => `
      <div class="com-item" data-i="${i}">
        <select class="select com-it-sku"><option value="">SKU…</option>${SKUS.map(s => `<option value="${s.id}" ${String(it.sku_id) === String(s.id) ? 'selected' : ''}>${esc(skuLabel(s))}</option>`).join('')}</select>
        <input class="input com-it-cant" inputmode="decimal" placeholder="Cant." value="${esc(it.cantidad)}">
        <input class="input com-it-costo" inputmode="decimal" placeholder="Costo unit." value="${esc(it.costo)}">
        <select class="select com-it-iva">
          <option value="0.21" ${num(it.iva) === 0.21 ? 'selected' : ''}>IVA 21%</option>
          <option value="0.105" ${num(it.iva) === 0.105 ? 'selected' : ''}>IVA 10,5%</option>
          <option value="0.27" ${num(it.iva) === 0.27 ? 'selected' : ''}>IVA 27%</option>
          <option value="0" ${num(it.iva) === 0 ? 'selected' : ''}>Exento</option>
        </select>
        <span class="com-it-sub com-mono">${money(num(it.cantidad) * num(it.costo))}</span>
        <button class="com-it-del" type="button" title="Quitar" ${ITEMS.length === 1 ? 'style="visibility:hidden"' : ''}>✕</button>
      </div>`).join('');
    pintarResumen();
  };
  const leerItems = () => {
    itemsBox.querySelectorAll('.com-item').forEach(row => {
      const i = +row.dataset.i;
      ITEMS[i].sku_id = row.querySelector('.com-it-sku').value;
      ITEMS[i].cantidad = row.querySelector('.com-it-cant').value;
      ITEMS[i].costo = row.querySelector('.com-it-costo').value;
      ITEMS[i].iva = Number(row.querySelector('.com-it-iva').value);
    });
  };
  itemsBox.addEventListener('input', e => {
    if (e.target.matches('.com-it-cant, .com-it-costo')) {
      const row = e.target.closest('.com-item'); const i = +row.dataset.i;
      ITEMS[i].cantidad = row.querySelector('.com-it-cant').value;
      ITEMS[i].costo = row.querySelector('.com-it-costo').value;
      row.querySelector('.com-it-sub').textContent = money(num(ITEMS[i].cantidad) * num(ITEMS[i].costo));
      pintarResumen();
    }
  });
  itemsBox.addEventListener('change', e => {
    const row = e.target.closest('.com-item'); if (!row) return;
    const i = +row.dataset.i;
    if (e.target.matches('.com-it-sku')) {
      leerItems();
      ITEMS[i].sku_id = e.target.value;
      const s = SKU_BY_ID[e.target.value];
      if (s && s.alicuota_iva != null) ITEMS[i].iva = Number(s.alicuota_iva);
      pintarItems(); // refresca el % de IVA de la fila según el SKU
    } else if (e.target.matches('.com-it-iva')) {
      ITEMS[i].iva = Number(e.target.value);
      pintarResumen();
    }
  });
  itemsBox.addEventListener('click', e => {
    if (e.target.matches('.com-it-del')) { leerItems(); ITEMS.splice(+e.target.closest('.com-item').dataset.i, 1); pintarItems(); }
  });
  $('#c-add-item').addEventListener('click', () => { leerItems(); ITEMS.push({ sku_id: '', cantidad: '', costo: '', iva: 0.21 }); pintarItems(); });

  const ivaTotal = () => ITEMS.reduce((s, it) => s + num(it.cantidad) * num(it.costo) * num(it.iva), 0);
  const pintarResumen = () => {
    const neto = ITEMS.reduce((s, it) => s + num(it.cantidad) * num(it.costo), 0);
    const iva = ivaTotal();
    const iibb = num($('#c-iibb').value), gan = num($('#c-gan').value);
    const total = neto + iva + iibb + gan;
    const disp = $('#c-iva-disp'); if (disp) disp.value = money(iva);
    $('#c-resumen').innerHTML = `
      <div class="com-res-r"><span>Productos (neto, va al costo)</span><span class="com-mono">${money(neto)}</span></div>
      <div class="com-res-r com-muted"><span>+ IVA (auto) · IIBB · Ganancias (crédito)</span><span class="com-mono">${money(iva + iibb + gan)}</span></div>
      <div class="com-res-r com-res-strong"><span>Total factura (lo que le debés)</span><span class="com-mono">${money(total)}</span></div>`;
  };
  overlay.querySelectorAll('.com-fisc').forEach(el => el.addEventListener('input', pintarResumen));
  pintarItems();

  // Guardar
  $('#c-guardar').addEventListener('click', async () => {
    leerItems();
    const fecha = $('#c-fecha').value;
    const linea_id = $('#c-linea').value;
    if (!fecha) { window.toast('Falta la fecha', 'error'); return; }
    if (!linea_id) { window.toast('Elegí la línea de negocio', 'error'); return; }
    const items = ITEMS
      .filter(it => it.sku_id && num(it.cantidad) > 0 && num(it.costo) >= 0)
      .map(it => ({ sku_id: +it.sku_id, cantidad: num(it.cantidad), costo_unitario: num(it.costo) }));
    if (!items.length) { window.toast('Cargá al menos un producto con SKU, cantidad y costo', 'error'); return; }

    const payload = {
      compra: {
        proveedor_id: $('#c-prov').value ? +$('#c-prov').value : null,
        linea_id: +linea_id,
        fecha,
        nro_factura: $('#c-factura').value.trim() || null
      },
      items,
      fiscales: { iva: ivaTotal(), iibb: num($('#c-iibb').value), ganancias: num($('#c-gan').value) }
    };

    const btn = $('#c-guardar'); btn.disabled = true;
    try {
      const r = await fetch('/compras', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || (r.status + ' ' + r.statusText));
      window.toast('Compra cargada');
      close();
      await recargar();
      render();
    } catch (e) {
      window.toast('Error al guardar: ' + e.message, 'error');
      btn.disabled = false;
    }
  });
}

function optProv(selId) {
  return '<option value="">— Sin proveedor —</option>' +
    PROVEEDORES.map(p => `<option value="${p.id}" ${String(selId) === String(p.id) ? 'selected' : ''}>${esc(provLabel(p))}</option>`).join('');
}

function inyectarEstilo() {
  if (document.getElementById('com-style')) return;
  const css = `
    .com-mono{font-family:'JetBrains Mono',ui-monospace,monospace;font-variant-numeric:tabular-nums}
    .com-muted{color:#78716C}
    .com-sub{font-size:13px;color:#78716C;margin:-4px 0 12px}
    .com-badge{font-size:12px;padding:2px 8px;border-radius:6px}
    .com-badge-pendiente{background:#FAEEDA;color:#854F0B}
    .com-badge-parcial{background:#E6F1FB;color:#0C447C}
    .com-badge-pagado{background:#E1F5EE;color:#0F6E56}
    .com-debe{color:#B91C1C;font-weight:600}
    .com-favor{color:#0F6E56;font-weight:600}
    .com-mini{padding:6px 12px;font-size:13px}
    .com-row3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}
    .com-block{border:1px solid #E7E5E4;border-radius:10px;padding:12px;margin:12px 0}
    .com-block-h{display:flex;justify-content:space-between;align-items:center;font-size:13px;font-weight:600;color:#44403C;margin-bottom:8px}
    .com-item{display:grid;grid-template-columns:1fr 70px 108px 108px 96px 26px;gap:8px;align-items:center;margin-bottom:8px}
    .com-it-sub{text-align:right;font-size:13px;color:#57534E}
    .com-it-del{border:0;background:transparent;color:#A8A29E;cursor:pointer;font-size:14px}
    .com-it-del:hover{color:#B91C1C}
    .com-resumen{border-top:1px solid #E7E5E4;margin-top:12px;padding-top:12px}
    .com-res-r{display:flex;justify-content:space-between;font-size:14px;padding:3px 0}
    .com-res-strong{font-weight:700;font-size:15px;border-top:1px dashed #E7E5E4;margin-top:4px;padding-top:8px}
  `;
  const style = document.createElement('style');
  style.id = 'com-style';
  style.textContent = css;
  document.head.appendChild(style);
}
