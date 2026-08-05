import { sbCount } from '../core/sb.js';

// ── Banner de estado del token de ML ─────────────────────────────────
// Con el token caído no corre el sync: no entran ventas nuevas y no se
// actualizan ni el débito fiscal ni el stock. Antes esto era invisible en la
// app (/health existía pero nadie lo miraba) y se detectaba días después, al
// notar que faltaban ventas. Ahora se ve al abrir.
async function bannerML() {
  let h;
  try {
    const r = await fetch('/health');   // ruta pública, no necesita token
    h = await r.json();
  } catch { return ''; }

  const ml = h && h.ml;
  if (!ml) return '';                    // backend viejo, sin el bloque `ml`
  if (ml.conectado && !ml.requiere_reconexion) return '';

  const box = (color, fondo, titulo, cuerpo) => `
    <div class="card" style="border-left:4px solid ${color};background:${fondo};margin-bottom:16px">
      <div class="card-title" style="color:${color}">${titulo}</div>
      <div style="font-size:13px;line-height:1.6">${cuerpo}</div>
    </div>`;

  const reconectar = `<a href="/ml/auth" target="_blank"><strong>Reconectar Mercado Libre →</strong></a>`;

  if (!ml.tiene_refresh) {
    return box('#c62828', '#fff5f5',
      '🔴 Mercado Libre sin renovación automática',
      `No hay <code>refresh_token</code> guardado: el token se cae solo cada ~6 h y hay que
       reconectar a mano cada vez. Para arreglarlo de raíz hay que habilitar
       <strong>offline_access</strong> en el panel de desarrolladores de ML y volver a autorizar.<br>
       ${ml.vencido ? 'El token <strong>está vencido ahora</strong>: el sync no está corriendo.' : `Vence en ${ml.expira_en_min} min.`}<br>
       ${reconectar}`);
  }
  if (ml.requiere_reconexion) {
    return box('#c62828', '#fff5f5',
      '🔴 Mercado Libre desconectado',
      `El <code>refresh_token</code> fue rechazado por ML (revocado o rotado). El sync está detenido:
       no entran ventas nuevas ni se actualizan débito fiscal y stock.<br>${reconectar}`);
  }
  return box('#ef6c00', '#fffaf0',
    '⚠️ Token de Mercado Libre vencido',
    `El token venció y todavía no se renovó. Si en unos minutos sigue así, revisá los logs
     del backend o reconectá.<br>${reconectar}`);
}

export async function loadHome() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando KPIs…</div>`;

  try {
    const [skusTotal, skusSinFamilia, ventasTotal, comprasTotal, gastosTotal, movsTotal, avisoML] = await Promise.all([
      sbCount('skus'),
      sbCount('skus', 'familia=is.null'),
      sbCount('ventas'),
      sbCount('compras'),
      sbCount('gastos'),
      sbCount('movimientos'),
      bannerML()
    ]);

    const skusListos = skusTotal - skusSinFamilia;
    const pctListos = skusTotal > 0 ? Math.round((skusListos / skusTotal) * 100) : 0;
    const skusEstado = skusSinFamilia === 0
      ? { txt: `${pctListos}% clasificados`, cls: 'ok' }
      : { txt: `${skusSinFamilia} sin clasificar`, cls: '' };

    root.innerHTML = `
      ${avisoML}
      <div class="kpi-grid">

        <div class="kpi">
          <div class="kpi-label">SKUs cargados</div>
          <div class="kpi-value">${skusTotal}</div>
          <div class="kpi-sub ${skusEstado.cls}">${skusEstado.txt}</div>
        </div>

        <div class="kpi">
          <div class="kpi-label">Ventas</div>
          <div class="kpi-value">${ventasTotal}</div>
          <div class="kpi-sub">registradas en base</div>
        </div>

        <div class="kpi">
          <div class="kpi-label">Compras</div>
          <div class="kpi-value">${comprasTotal}</div>
          <div class="kpi-sub">registradas en base</div>
        </div>

        <div class="kpi">
          <div class="kpi-label">Gastos</div>
          <div class="kpi-value">${gastosTotal}</div>
          <div class="kpi-sub">registrados en base</div>
        </div>

        <div class="kpi">
          <div class="kpi-label">Movimientos</div>
          <div class="kpi-value">${movsTotal}</div>
          <div class="kpi-sub">extracto · MP · caja</div>
        </div>

      </div>

      <div class="card">
        <div class="card-title">Próximos pasos</div>
        <div class="steps">
          <div class="step"><span class="step-dot"></span>Cargar saldos iniciales al 31/12/2025</div>
          <div class="step"><span class="step-dot dim"></span>Registrar primeras ventas y compras</div>
          <div class="step"><span class="step-dot dim"></span>Conciliar movimientos del extracto bancario</div>
        </div>
      </div>
    `;
  } catch (e) {
    console.error('Home falló:', e);
    root.innerHTML = `<div class="error"><strong>Error cargando KPIs:</strong><br>${e.message}<br><br>Abrí F12 → Consola para más detalle.</div>`;
  }
}
