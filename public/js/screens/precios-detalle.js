import { sbPost, sbPatch } from '../core/sb.js';

// ── Precios: detalle de un SKU ──────────────────────────────────────────
// Desglose por canal + edición de margen, utilidad neta objetivo y PVP manual
// (pricing_margen), y del envío / cargo fijo de ML del producto (pricing_producto).
// Prioridad, igual que pricing: PVP manual > utilidad neta > margen %.
// Recalcula en vivo sin guardar; "Guardar" persiste sólo lo que cambió.

const MARGEN_DEFAULT = 5;

export function abrirDetallePrecio({ sku, prod, canales, margenes, calcular, alGuardar, util: u }) {
  if (!prod || !(Number(prod.costo_sin_iva) > 0)) {
    window.toast('Cargá primero el costo de simulación del SKU', 'error');
    return;
  }
  const actual = c => margenes[`${sku.id}|${c.codigo}`] || null;
  const val = v => (v === null || v === undefined ? '' : Number(v));

  const filas = canales.map(c => {
    const m = actual(c) || {};
    return `<tr data-canal="${u.esc(c.codigo)}">
      <td><strong>${u.esc(c.codigo)}</strong><div class="prc-muted">${u.esc(c.nombre)}</div></td>
      <td><input class="inline-input" data-m="margen_pct" inputmode="decimal" value="${m.margen_pct != null ? Number(m.margen_pct) : MARGEN_DEFAULT}"></td>
      <td><input class="inline-input" data-m="utilidad_neta" inputmode="decimal" value="${val(m.utilidad_neta)}" placeholder="—"></td>
      <td><input class="inline-input" data-m="pvp_manual" inputmode="decimal" value="${val(m.pvp_manual)}" placeholder="—"></td>
      <td class="prc-num prc-precio" data-out="precio"></td>
      <td class="prc-num" data-out="neto"></td>
      <td class="prc-num" data-out="comision"></td>
      <td class="prc-num" data-out="impuestos"></td>
      <td class="prc-num" data-out="fijos"></td>
      <td class="prc-num" data-out="ganancia"></td>
      <td class="prc-num" data-out="margen"></td>
    </tr>`;
  }).join('');

  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal" style="max-width:1180px;width:96vw">
      <div class="modal-title">${u.esc(sku.codigo)} · ${u.esc(sku.descripcion)}</div>
      <div class="modal-sub">Costo de simulación ${u.fmt$(prod.costo_sin_iva)} + IVA ${u.fmtPct(prod.iva_pct)} · categoría ${u.esc(prod.categoria || 'sin categoría (sin comisión ML)')}</div>
      <div style="display:flex;gap:16px;flex-wrap:wrap">
        <div class="field"><label>Envío ML (bruto, con IVA)</label>
          <input class="input" id="det-envio" inputmode="decimal" value="${Number(prod.envio_ml_monto || 0)}" style="width:140px">
          <div class="hint">Lo que cobra ML por el envío gratis. Sólo canales ML.</div></div>
        <div class="field"><label>Cargo fijo ML (bruto)</label>
          <input class="input" id="det-fijo" inputmode="decimal" value="${Number(prod.cargo_fijo_ml_monto || 0)}" style="width:140px">
          <div class="hint">Cargo por unidad de bajo precio.</div></div>
      </div>
      <div class="table-wrap">
        <table class="t prc-det">
          <thead><tr>
            <th>Canal</th><th>Margen %</th><th>Utilidad neta $</th><th>PVP manual $</th>
            <th class="prc-num">Precio c/IVA</th><th class="prc-num">Neto s/IVA</th><th class="prc-num">Comisión + cuotas</th>
            <th class="prc-num">IIBB + IDC</th><th class="prc-num">Envío + fijo + estr.</th><th class="prc-num">Ganancia neta</th><th class="prc-num">Margen</th>
          </tr></thead>
          <tbody>${filas}</tbody>
        </table>
      </div>
      <div class="hint">Prioridad: PVP manual &gt; utilidad neta &gt; margen %. Dejá vacío lo que no uses. Margen y ganancia son netos de Ganancias, sobre venta sin IVA.</div>
      <div class="modal-actions">
        <button class="btn btn-ghost" id="det-cancel">Cancelar</button>
        <button class="btn btn-primary" id="det-save">Guardar</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);

  // Lee el estado actual del formulario: producto + margen por canal.
  function leer() {
    const envio = u.num(overlay.querySelector('#det-envio').value) ?? 0;
    const fijo = u.num(overlay.querySelector('#det-fijo').value) ?? 0;
    const p = { ...prod, envio_ml_monto: envio, cargo_fijo_ml_monto: fijo };
    const porCanal = canales.map(c => {
      const tr = overlay.querySelector(`tr[data-canal="${CSS.escape(c.codigo)}"]`);
      const campo = k => u.num(tr.querySelector(`[data-m="${k}"]`).value);
      return { canal: c, tr, margen_pct: campo('margen_pct'), utilidad_neta: campo('utilidad_neta'), pvp_manual: campo('pvp_manual') };
    });
    return { p, porCanal };
  }

  function invalido(n, { min = -Infinity, max = Infinity } = {}) {
    return n !== null && (Number.isNaN(n) || n < min || n > max);
  }

  function pintar() {
    const { p, porCanal } = leer();
    for (const f of porCanal) {
      const out = k => f.tr.querySelector(`[data-out="${k}"]`);
      const m = { ...(actual(f.canal) || {}), margen_pct: f.margen_pct ?? MARGEN_DEFAULT, utilidad_neta: f.utilidad_neta, pvp_manual: f.pvp_manual };
      const malo = invalido(f.margen_pct, { max: 99 }) || invalido(f.utilidad_neta) || invalido(f.pvp_manual, { min: 0.01 })
        || invalido(p.envio_ml_monto, { min: 0 }) || invalido(p.cargo_fijo_ml_monto, { min: 0 });
      const r = malo ? null : calcular(sku.id, f.canal, { prod: p, margen: m });
      if (!r || !r.valid) {
        out('precio').innerHTML = `<span class="prc-err">${malo ? 'valor inválido' : u.esc(r?.error || 'error')}</span>`;
        ['neto', 'comision', 'impuestos', 'fijos', 'ganancia', 'margen'].forEach(k => { out(k).textContent = '—'; });
        continue;
      }
      out('precio').textContent = u.fmt$(r.roundedPrice);
      out('neto').textContent = u.fmt$(r.netSalePrice);
      out('comision').textContent = u.fmt$(r.marketplaceFeeAmount + r.salesCommissionAmount);
      out('impuestos').textContent = u.fmt$(r.iibbAmount + r.idcAmount);
      out('fijos').textContent = u.fmt$(r.fixedCosts);
      out('ganancia').innerHTML = `<span class="${r.netProfit < 0 ? 'prc-err' : ''}">${u.fmt$(r.netProfit)}</span>`;
      out('margen').innerHTML = `<span class="prc-mg ${r.netProfit < 0 ? 'neg' : ''}">${u.fmtPct(r.marginOnNetSale)}</span>`;
    }
  }

  async function guardar() {
    const { p, porCanal } = leer();
    if (invalido(p.envio_ml_monto, { min: 0 }) || invalido(p.cargo_fijo_ml_monto, { min: 0 })) {
      window.toast('Envío o cargo fijo inválido', 'error'); return;
    }
    for (const f of porCanal) {
      if (invalido(f.margen_pct, { max: 99 }) || invalido(f.utilidad_neta) || invalido(f.pvp_manual, { min: 0.01 })) {
        window.toast(`Valor inválido en ${f.canal.codigo}`, 'error'); return;
      }
    }
    try {
      if (Number(p.envio_ml_monto) !== Number(prod.envio_ml_monto || 0) || Number(p.cargo_fijo_ml_monto) !== Number(prod.cargo_fijo_ml_monto || 0)) {
        await sbPatch('pricing_producto', `sku_id=eq.${sku.id}`, { envio_ml_monto: p.envio_ml_monto, cargo_fijo_ml_monto: p.cargo_fijo_ml_monto });
      }
      for (const f of porCanal) {
        const ex = actual(f.canal);
        const nuevo = { margen_pct: f.margen_pct ?? MARGEN_DEFAULT, utilidad_neta: f.utilidad_neta, pvp_manual: f.pvp_manual };
        const antes = { margen_pct: ex?.margen_pct != null ? Number(ex.margen_pct) : MARGEN_DEFAULT, utilidad_neta: ex?.utilidad_neta != null ? Number(ex.utilidad_neta) : null, pvp_manual: ex?.pvp_manual != null ? Number(ex.pvp_manual) : null };
        if (JSON.stringify(nuevo) === JSON.stringify(antes)) continue;
        if (ex) await sbPatch('pricing_margen', `id=eq.${ex.id}`, nuevo);
        else await sbPost('pricing_margen', { sku_id: sku.id, canal_codigo: f.canal.codigo, ...nuevo });
      }
      overlay.remove();
      window.toast('Precios guardados');
      await alGuardar();
    } catch (e) { window.toast('No se pudo guardar: ' + e.message, 'error'); }
  }

  overlay.addEventListener('input', pintar);
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  overlay.querySelector('#det-cancel').addEventListener('click', () => overlay.remove());
  overlay.querySelector('#det-save').addEventListener('click', guardar);
  pintar();
}
