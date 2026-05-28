import { initSB } from './core/sb.js';
import { loadHome } from './screens/home.js';
import { loadSkus } from './screens/skus.js';

const screens = {
  home: { fn: loadHome, title: 'Home', sub: 'Resumen del estado actual' },
  skus: { fn: loadSkus, title: 'SKUs', sub: 'Catálogo de productos' }
};

async function route() {
  const hash = (location.hash || '#home').slice(1);
  const screen = screens[hash] || screens.home;
  // Actualizar nav activo
  document.querySelectorAll('.nav-item').forEach(a => {
    a.classList.toggle('active', a.getAttribute('href') === '#' + hash);
  });
  // Actualizar topbar
  document.getElementById('topbar-title').textContent = screen.title;
  document.getElementById('topbar-sub').textContent = screen.sub;
  // Cargar pantalla
  try {
    await screen.fn();
  } catch (e) {
    console.error('Pantalla falló:', e);
    document.getElementById('app-screens').innerHTML =
      `<div class="error"><strong>Error al cargar la pantalla.</strong><br>${e.message}</div>`;
  }
}

// Toast global
window.toast = function(msg, type='ok') {
  const host = document.getElementById('toast-host');
  const el = document.createElement('div');
  el.className = 'toast ' + type;
  el.textContent = msg;
  host.appendChild(el);
  setTimeout(() => { el.style.opacity='0'; el.style.transition='opacity 0.2s'; setTimeout(()=>el.remove(), 200); }, 2200);
};

(async () => {
  try {
    await initSB();
    window.addEventListener('hashchange', route);
    await route();
  } catch (e) {
    console.error('Bootstrap falló:', e);
    document.getElementById('app-screens').innerHTML =
      `<div class="error"><strong>No se pudo inicializar la app.</strong><br>Abrí F12 → Consola.<br><br>Detalle: ${e.message}</div>`;
  }
})();
