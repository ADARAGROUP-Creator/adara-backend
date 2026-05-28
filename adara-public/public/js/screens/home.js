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

    root.innerHTML = `
      <div class="kpi-grid">
        <div class="kpi">
          <div class="kpi-label">SKUs cargados</div>
          <div class="kpi-value">${skusTotal}</div>
          <div class="kpi-sub">${skusSinFamilia} sin clasificar · ${pctListos}% listos</div>
        </div>
        <div class="kpi" style="--kpi-color:var(--green)">
          <div class="kpi-label">Ventas</div>
          <div class="kpi-value">${ventasTotal}</div>
          <div class="kpi-sub">registradas en DB</div>
        </div>
        <div class="kpi" style="--kpi-color:var(--blue)">
          <div class="kpi-label">Compras</div>
          <div class="kpi-value">${comprasTotal}</div>
          <div class="kpi-sub">registradas en DB</div>
        </div>
        <div class="kpi" style="--kpi-color:var(--red)">
          <div class="kpi-label">Gastos</div>
          <div class="kpi-value">${gastosTotal}</div>
          <div class="kpi-sub">registrados en DB</div>
        </div>
        <div class="kpi" style="--kpi-color:var(--muted)">
          <div class="kpi-label">Movimientos</div>
          <div class="kpi-value">${movsTotal}</div>
          <div class="kpi-sub">extracto / MP / caja</div>
        </div>
      </div>

      <div class="card">
        <div class="card-title">Próximos pasos</div>
        <div style="font-size:.7rem;color:var(--muted);line-height:1.8">
          • Clasificar los ${skusSinFamilia} SKUs sin familia (próxima pantalla: Maestros — SKUs)<br>
          • Cargar saldos iniciales al 31/12/2025 (próxima pantalla: Maestros — Saldos)<br>
          • Recién después: conciliación, ventas, gastos
        </div>
      </div>
    `;
  } catch (e) {
    console.error('Home falló:', e);
    root.innerHTML = `<div class="error"><strong>Error cargando KPIs:</strong><br>${e.message}<br><br>Abrí F12 → Consola para más detalle.</div>`;
  }
}
