import { sbPost, sbPatch } from '../core/sb.js';

// ── Precios: bloque de configuración ────────────────────────────────────
// Tasas globales (pricing_tasas), canales (pricing_canal) y comisión de ML por
// categoría (pricing_comision_categoria). Son parámetros de SIMULACIÓN (PRC1):
// el IIBB de acá no es el del CM03 ni lo reemplaza (PRC2).
// `u` = helpers de formato de precios.js ({ fmt$, fmtPct, num, esc }).

const REDONDEOS = [1, 10, 100, 1000];

export function configHTML({ TASAS, CANALES, CATS }, u) {
  const tasa = (campo, label, hint) => `
    <div class="field"><label>${label}</label>
      <input class="inline-input" data-tasa="${campo}" inputmode="decimal" value="${Number(TASAS[campo] || 0)}"> %
      ${hint ? `<div class="hint">${hint}</div>` : ''}</div>`;

  const canales = CANALES.map(c => `
    <tr data-canal="${u.esc(c.codigo)}">
      <td class="prc-code">${u.esc(c.codigo)}</td>
      <td class="prc-wrap">${u.esc(c.nombre)}</td>
      <td><input class="inline-input" data-canal-campo="costo_financiacion_pct" inputmode="decimal" value="${Number(c.costo_financiacion_pct)}"> %</td>
      <td><select class="inline-select" data-canal-campo="redondeo_a">${REDONDEOS.map(r => `<option value="${r}" ${Number(c.redondeo_a) === r ? 'selected' : ''}>$ ${r.toLocaleString('es-AR')}</option>`).join('')}</select></td>
      <td style="text-align:center"><input type="checkbox" data-canal-campo="activo" ${c.activo ? 'checked' : ''}></td>
    </tr>`).join('');

  const cats = CATS.map(c => `
    <tr data-cat="${c.id}">
      <td>${u.esc(c.categoria)}</td>
      <td><input class="inline-input" data-cat-campo="comision_pct" inputmode="decimal" value="${Number(c.comision_pct)}"> %</td>
      <td style="text-align:center"><input type="checkbox" data-cat-campo="activo" ${c.activo ? 'checked' : ''}></td>
    </tr>`).join('');

  return `
    <div class="prc-cfg">
      <div class="prc-cfg-grid">
        <div>
          <h4>Tasas de simulación</h4>
          ${tasa('iibb_pct', 'IIBB', 'Sobre venta neta. Es de simulación: no es la alícuota del CM03.')}
          ${tasa('idc_pct', 'Impuesto al cheque (IDC)', 'Sobre venta neta.')}
          ${tasa('iigg_pct', 'Ganancias', 'Sólo sobre ganancia bruta positiva.')}
          <button class="btn btn-primary" data-cfg="guardar-tasas">Guardar tasas</button>
        </div>
        <div>
          <h4>Canales</h4>
          <table class="t">
            <thead><tr><th>Código</th><th>Nombre</th><th>Costo cuotas</th><th>Redondeo</th><th>Activo</th></tr></thead>
            <tbody>${canales}</tbody>
          </table>
          <div class="hint">El costo de cuotas se suma a la comisión de ML. Se guarda al cambiar el valor.</div>
        </div>
        <div>
          <h4>Comisión ML por categoría</h4>
          ${CATS.length ? `<table class="t">
            <thead><tr><th>Categoría</th><th>Comisión</th><th>Activa</th></tr></thead>
            <tbody>${cats}</tbody>
          </table>` : '<div class="empty" style="padding:12px">Sin categorías cargadas: los canales de ML calculan sin comisión.</div>'}
          <div style="display:flex;gap:8px;margin-top:10px">
            <input class="input" id="cfg-cat-nombre" placeholder="Categoría (ej. Smartwatch)">
            <input class="input" id="cfg-cat-pct" placeholder="%" inputmode="decimal" style="width:80px">
            <button class="btn btn-ghost" data-cfg="agregar-cat">Agregar</button>
          </div>
        </div>
      </div>
    </div>`;
}

// host se recrea en cada render de precios.js, así que bindear acá no acumula listeners.
export function bindConfig(host, { TASAS }, alGuardar, u) {
  const pct = (valor, max = 100) => {
    const n = u.num(valor);
    return n === null || Number.isNaN(n) || n < 0 || n >= max ? NaN : n;
  };

  host.addEventListener('click', async e => {
    const b = e.target.closest('[data-cfg]');
    if (!b) return;
    try {
      if (b.dataset.cfg === 'guardar-tasas') {
        const cambios = {};
        for (const inp of host.querySelectorAll('[data-tasa]')) {
          const n = pct(inp.value);
          if (Number.isNaN(n)) { window.toast('Tasa inválida: ' + inp.dataset.tasa, 'error'); return; }
          cambios[inp.dataset.tasa] = n;
        }
        if (TASAS.id) await sbPatch('pricing_tasas', 'id=eq.1', cambios);
        else await sbPost('pricing_tasas', { id: 1, ...cambios });
        window.toast('Tasas guardadas');
        await alGuardar();
      }
      if (b.dataset.cfg === 'agregar-cat') {
        const nombre = host.querySelector('#cfg-cat-nombre').value.trim();
        const n = pct(host.querySelector('#cfg-cat-pct').value || '0');
        if (!nombre || Number.isNaN(n)) { window.toast('Completá categoría y comisión', 'error'); return; }
        await sbPost('pricing_comision_categoria', { categoria: nombre, comision_pct: n });
        window.toast(`Categoría "${nombre}" agregada`);
        await alGuardar();
      }
    } catch (err) { window.toast('No se pudo guardar: ' + err.message, 'error'); }
  });

  host.addEventListener('change', async e => {
    const el = e.target;
    try {
      const trCanal = el.closest('tr[data-canal]');
      if (trCanal && el.dataset.canalCampo) {
        const campo = el.dataset.canalCampo;
        let valor = campo === 'activo' ? el.checked : Number(el.value);
        if (campo === 'costo_financiacion_pct') {
          valor = pct(el.value);
          if (Number.isNaN(valor)) { window.toast('Costo de cuotas inválido', 'error'); return; }
        }
        await sbPatch('pricing_canal', `codigo=eq.${encodeURIComponent(trCanal.dataset.canal)}`, { [campo]: valor });
        window.toast('Canal actualizado');
        await alGuardar();
        return;
      }
      const trCat = el.closest('tr[data-cat]');
      if (trCat && el.dataset.catCampo) {
        const campo = el.dataset.catCampo;
        const valor = campo === 'activo' ? el.checked : pct(el.value);
        if (Number.isNaN(valor)) { window.toast('Comisión inválida', 'error'); return; }
        await sbPatch('pricing_comision_categoria', `id=eq.${trCat.dataset.cat}`, { [campo]: valor });
        window.toast('Comisión actualizada');
        await alGuardar();
      }
    } catch (err) { window.toast('No se pudo guardar: ' + err.message, 'error'); }
  });
}
