import { initSB } from './core/sb.js';
import { loadHome } from './screens/home.js';

(async () => {
  try {
    await initSB();
    await loadHome();
  } catch (e) {
    console.error('Bootstrap falló:', e);
    document.getElementById('app-screens').innerHTML =
      `<div class="error"><strong>No se pudo inicializar la app.</strong><br>Abrí la consola del navegador (F12 → Consola) y mandame lo que aparece.<br><br>Detalle: ${e.message}</div>`;
  }
})();
