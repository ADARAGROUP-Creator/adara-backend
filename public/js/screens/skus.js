import { sbGet, sbPatch, sbPost } from '../core/sb.js';

const FAMILIAS = [
  { val: '',                     label: 'Sin clasificar' },
  { val: 'electronica',          label: 'Electrónica' },
  { val: 'luminaria',            label: 'Luminaria' },
  { val: 'mochila_sindical',     label: 'Mochila sindical' },
  { val: 'mochila_individual',   label: 'Mochila individual' }
];

// Alícuotas de IVA vigentes en Argentina que aplican a productos. Se guardan como fracción
// (0.105), no como porcentaje (10.5). Era texto libre y permitía errores silenciosos y caros:
// un SKU cargado al 10% en vez de 10,5% calcula mal el crédito fiscal de cada compra futura.
const ALICUOTAS = [
  { val: 0,    label: 'Exento (0%)' },
  { val: 10.5, label: '10,5% — informática, bienes de capital' },
  { val: 21,   label: '21% — general' }
];

let DATA = [];
let FILTRO_FAM = 'all';   // 'all' | '' (sin clasificar) | nombre familia
let BUSQ = '';
let COSTOS = {};          // sku_id -> costo unitario actual (neto, ARS) desde v_costo_sku_actual

export async function loadSkus() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando catálogo…</div>`;

  try {
    const [skus, costos] = await Promise.all([
      sbGet('skus', 'order=codigo.asc'),
      sbGet('v_costo_sku_actual', 'select=sku_id,costo_unit')
    ]);
    DATA = skus;
    COSTOS = {};
    for (const c of costos) COSTOS[c.sku_id] = c.costo_unit;
    render();
  } catch (e) {
    console.error('Load SKUs falló:', e);
    root.innerHTML = `<div class="error"><strong>Error cargando SKUs:</strong><br>${e.message}</div>`;
  }
}

function render() {
  const root = document.getElementById('app-screens');
  const conteoPorFamilia = countByFamilia();
  const visibles = filtered();

  root.innerHTML = `
    <div class="toolbar">
      <div class="grow">
        <input class="input" id="busq" placeholder="Buscar por código o descripción…" value="${escapeAttr(BUSQ)}">
      </div>
      <button class="btn btn-primary" id="btn-nuevo">+ Nuevo SKU</button>
    </div>

    <div class="pills" id="pills">
      ${pill('all', 'Todos', DATA.length)}
      ${pill('', 'Sin clasificar', conteoPorFamilia[''] || 0)}
      ${pill('electronica', 'Electrónica', conteoPorFamilia['electronica'] || 0)}
      ${pill('luminaria', 'Luminaria', conteoPorFamilia['luminaria'] || 0)}
      ${pill('mochila_sindical', 'Mochila sindical', conteoPorFamilia['mochila_sindical'] || 0)}
      ${pill('mochila_individual', 'Mochila individual', conteoPorFamilia['mochila_individual'] || 0)}
    </div>

    <div class="table-wrap">
      ${visibles.length === 0 ? '<div class="empty">No hay SKUs que coincidan con el filtro</div>' : `
      <table class="t">
        <thead>
          <tr>
            <th class="col-code">Código</th>
            <th class="col-desc">Descripción</th>
            <th class="col-fam">Familia</th>
            <th class="col-iva">IVA</th>
            <th class="col-costo">Costo actual</th>
            <th class="col-act">Activo</th>
          </tr>
        </thead>
        <tbody>
          ${visibles.map(rowHTML).join('')}
        </tbody>
      </table>
      `}
    </div>

    <div style="font-size:13px;color:var(--text-muted);margin-top:10px;text-align:right">
      Mostrando ${visibles.length} de ${DATA.length}
    </div>
  `;

  // Listeners
  document.getElementById('busq').addEventListener('input', e => {
    BUSQ = e.target.value;
    rerenderTabla();
  });
  document.querySelectorAll('#pills .pill').forEach(p => {
    p.addEventListener('click', () => {
      FILTRO_FAM = p.dataset.fam;
      render();
    });
  });
  document.getElementById('btn-nuevo').addEventListener('click', openModalNuevo);
  bindRowEvents();
}

function rerenderTabla() {
  // Solo re-renderiza la tabla y el contador (más rápido que render() completo)
  const visibles = filtered();
  const wrap = document.querySelector('.table-wrap');
  wrap.innerHTML = visibles.length === 0 ? '<div class="empty">No hay SKUs que coincidan con el filtro</div>' : `
    <table class="t">
      <thead><tr>
        <th class="col-code">Código</th><th class="col-desc">Descripción</th>
        <th class="col-fam">Familia</th><th class="col-iva">IVA</th><th class="col-costo">Costo actual</th><th class="col-act">Activo</th>
      </tr></thead>
      <tbody>${visibles.map(rowHTML).join('')}</tbody>
    </table>
  `;
  document.querySelector('.table-wrap + div').textContent = `Mostrando ${visibles.length} de ${DATA.length}`;
  bindRowEvents();
}

function bindRowEvents() {
  document.querySelectorAll('.sel-fam').forEach(s => {
    s.addEventListener('change', async e => {
      const id = +e.target.dataset.id;
      const val = e.target.value || null;
      await updateField(id, { familia: val }, e.target);
      // Reflejar estilo "sin clasificar"
      e.target.classList.toggle('empty', !val);
    });
  });
  document.querySelectorAll('.in-iva').forEach(sel => {
    sel.addEventListener('change', async e => {
      const id = +e.target.dataset.id;
      const newVal = +(parseFloat(e.target.value) / 100).toFixed(4);
      await updateField(id, { alicuota_iva: newVal }, e.target);
    });
  });
  document.querySelectorAll('.sw-activo').forEach(sw => {
    sw.addEventListener('change', async e => {
      const id = +e.target.dataset.id;
      const val = e.target.checked;
      await updateField(id, { activo: val }, e.target);
      // Aplicar estilo a la fila
      e.target.closest('tr').classList.toggle('inactive', !val);
    });
  });
}

async function updateField(id, patch, el) {
  try {
    const updated = await sbPatch('skus', `id=eq.${id}`, patch);
    // Actualizar DATA local
    const idx = DATA.findIndex(r => r.id === id);
    if (idx >= 0 && updated && updated[0]) DATA[idx] = updated[0];
    window.toast('Guardado');
    // Refrescar contadores de pills si cambió familia
    if ('familia' in patch) refreshPills();
  } catch (e) {
    console.error('Update falló:', e);
    window.toast('Error guardando: ' + e.message, 'error');
  }
}

function refreshPills() {
  const conteo = countByFamilia();
  const map = {'all': DATA.length, '': conteo[''] || 0, 'electronica': conteo['electronica']||0,
               'luminaria': conteo['luminaria']||0, 'mochila_sindical': conteo['mochila_sindical']||0,
               'mochila_individual': conteo['mochila_individual']||0};
  document.querySelectorAll('#pills .pill').forEach(p => {
    const n = map[p.dataset.fam] || 0;
    p.querySelector('.num').textContent = n;
  });
}

function rowHTML(r) {
  return `
    <tr class="${r.activo ? '' : 'inactive'}">
      <td class="col-code">${escapeHTML(r.codigo)}</td>
      <td class="col-desc">${escapeHTML(r.descripcion || '')}</td>
      <td class="col-fam">
        <select class="inline-select sel-fam ${r.familia ? '' : 'empty'}" data-id="${r.id}">
          ${FAMILIAS.map(f => `<option value="${f.val}" ${(r.familia||'')===f.val?'selected':''}>${f.label}</option>`).join('')}
        </select>
      </td>
      <td class="col-iva">
        <select class="inline-select in-iva" data-id="${r.id}">${opcionesIva(r.alicuota_iva)}</select>
      </td>
      <td class="col-costo">${fmtCosto(r.id)}</td>
      <td class="col-act">
        <label class="sw"><input type="checkbox" class="sw-activo" data-id="${r.id}" ${r.activo?'checked':''}><span class="slider"></span></label>
      </td>
    </tr>
  `;
}

function fmtCosto(id) {
  const c = COSTOS[id];
  if (c == null) return '—';
  return '$ ' + Number(c).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function pill(val, label, num) {
  const active = FILTRO_FAM === val;
  return `<div class="pill ${active?'active':''}" data-fam="${val}">${label}<span class="num">${num}</span></div>`;
}

function filtered() {
  const q = BUSQ.trim().toLowerCase();
  return DATA.filter(r => {
    if (FILTRO_FAM !== 'all') {
      const f = r.familia || '';
      if (f !== FILTRO_FAM) return false;
    }
    if (q) {
      const codigo = (r.codigo||'').toLowerCase();
      const desc = (r.descripcion||'').toLowerCase();
      if (codigo.indexOf(q) < 0 && desc.indexOf(q) < 0) return false;
    }
    return true;
  });
}

function countByFamilia() {
  const c = {};
  DATA.forEach(r => { const f = r.familia || ''; c[f] = (c[f]||0)+1; });
  return c;
}

function formatIva(v) {
  if (v === null || v === undefined) return '21,00%';
  const pct = (parseFloat(v) * 100);
  return pct.toFixed(2).replace('.', ',') + '%';
}

// Opciones del select de IVA para un SKU. Si el valor guardado no es ninguna de las alícuotas
// estándar (dato viejo o cargado a mano antes del dropdown), se agrega como opción propia para
// no pisarlo en silencio al re-renderizar la fila.
function opcionesIva(actual) {
  const pct = actual === null || actual === undefined ? 21 : +(parseFloat(actual) * 100).toFixed(2);
  const lista = ALICUOTAS.some(a => Math.abs(a.val - pct) < 0.001)
    ? ALICUOTAS
    : [...ALICUOTAS, { val: pct, label: formatIva(pct / 100) + ' (fuera de lista)' }];
  return lista
    .map(a => `<option value="${a.val}" ${Math.abs(a.val - pct) < 0.001 ? 'selected' : ''}>${escapeHTML(a.label)}</option>`)
    .join('');
}

function escapeHTML(s) {
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function escapeAttr(s) { return escapeHTML(s); }

// === Modal Nuevo SKU ===
function openModalNuevo() {
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal">
      <div class="modal-title">Nuevo SKU</div>
      <div class="modal-sub">Da de alta un producto en el catálogo</div>
      <div class="field">
        <label>Código <span style="color:var(--red)">*</span></label>
        <input class="input" id="nf-codigo" placeholder="ej: AU007B" autofocus>
        <div class="hint">Único. Es el código que usás en ML, Tango, etc.</div>
      </div>
      <div class="field">
        <label>Descripción <span style="color:var(--red)">*</span></label>
        <input class="input" id="nf-desc" placeholder="ej: Auricular Buds 9 Blanco">
      </div>
      <div class="field">
        <label>Familia</label>
        <select class="select" id="nf-fam">
          ${FAMILIAS.map(f => `<option value="${f.val}" ${f.val==='electronica'?'selected':''}>${f.label}</option>`).join('')}
        </select>
      </div>
      <div class="field">
        <label>Alícuota IVA</label>
        <select class="select" id="nf-iva">
          ${ALICUOTAS.map(a => `<option value="${a.val}" ${a.val === 21 ? 'selected' : ''}>${a.label}</option>`).join('')}
        </select>
        <div class="hint">Notebooks, tablets y bienes de informática van al 10,5%</div>
      </div>
      <div class="modal-actions">
        <button class="btn btn-ghost" id="nf-cancel">Cancelar</button>
        <button class="btn btn-primary" id="nf-save">Crear SKU</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);

  const close = () => overlay.remove();
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
  overlay.querySelector('#nf-cancel').addEventListener('click', close);
  overlay.querySelector('#nf-codigo').focus();

  overlay.querySelector('#nf-save').addEventListener('click', async () => {
    const codigo = overlay.querySelector('#nf-codigo').value.trim();
    const desc = overlay.querySelector('#nf-desc').value.trim();
    const fam = overlay.querySelector('#nf-fam').value || null;
    const ivaNum = parseFloat(overlay.querySelector('#nf-iva').value);   // el select ya entrega un número limpio

    if (!codigo) { window.toast('Falta el código', 'error'); return; }
    if (!desc) { window.toast('Falta la descripción', 'error'); return; }
    if (isNaN(ivaNum) || ivaNum < 0 || ivaNum > 100) { window.toast('IVA inválido', 'error'); return; }
    if (DATA.some(r => r.codigo === codigo)) { window.toast('Ya existe un SKU con ese código', 'error'); return; }

    try {
      const created = await sbPost('skus', {
        codigo, descripcion: desc, familia: fam,
        alicuota_iva: +(ivaNum / 100).toFixed(4), activo: true
      });
      if (created && created[0]) DATA.push(created[0]);
      DATA.sort((a,b) => (a.codigo||'').localeCompare(b.codigo||''));
      window.toast('SKU creado: ' + codigo);
      close();
      render();
    } catch (e) {
      console.error('Crear SKU falló:', e);
      window.toast('Error: ' + e.message, 'error');
    }
  });
}
