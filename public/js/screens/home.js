import { sbCount } from '../core/sb.js';

export async function loadHome() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando KPIs…</div>`;

  try {
    const [skusTotal, skusSinFamilia, ventasTotal, comprasTotal, gastosTotal, movsTotal] = await Promise.all([
      sbCount('skus'),
      sbCount('skus', 'familia=is.null'),
      sbCount('ventas'),
      sbCount('compras'),
      sbCount('gastos'),
      sbCount('movimientos')
    ]);

    const skusListos = skusTotal - skusSinFamilia;
    const pctListos = skusTotal > 0 ? Math.round((skusListos / skusTotal) * 100) : 0;
    const skusEstado = skusSinFamilia === 0
      ? { txt: `${pctListos}% clasificados`, cls: 'ok' }
      : { txt: `${skusSinFamilia} sin clasificar`, cls: '' };

    root.innerHTML = `
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
