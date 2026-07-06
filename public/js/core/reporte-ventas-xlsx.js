// core/reporte-ventas-xlsx.js — Genera reporte XLSX de ventas por SKU
// Uso: import { exportarVentasXLSX } from '../core/reporte-ventas-xlsx.js';
//      exportarVentasXLSX('2026-06-01', '2026-06-07');

import { sbGet } from './sb.js';

// ── Cargar SheetJS bajo demanda ────────────────────────────────────────
let _xlsxReady = false;
async function ensureXLSX() {
  if (_xlsxReady || typeof XLSX !== 'undefined') { _xlsxReady = true; return; }
  await new Promise((res, rej) => {
    const s = document.createElement('script');
    s.src = 'https://cdn.sheetjs.com/xlsx-0.20.3/package/dist/xlsx.full.min.js';
    s.onload = () => { _xlsxReady = true; res(); };
    s.onerror = () => rej(new Error('No se pudo cargar SheetJS'));
    document.head.appendChild(s);
  });
}

// ── Helpers ────────────────────────────────────────────────────────────
const REASON_MAP = {
  'PDD9939': 'Arrepentido',   'PDD9942': 'Otro producto',
  'PDD9946': 'Roto/dañado',   'PDD9949': 'No funciona',
  'PDD9953': 'Embalaje/faltante',
};

const n  = v => v || 0;
const ff = f => f ? f.substring(0, 10) : null;
const r2 = v => Math.round((v || 0) * 100) / 100;

function getOp(v) {
  if (v.ml_status === 'cancelled') return v.fecha_entrega ? 'DEVOLUCION' : 'CANCELADA';
  if (v.devuelta || v.claim_id) return 'DEVOLUCION';
  return 'VENTA';
}

function getMotivo(v) {
  if (v.motivo_devolucion) return REASON_MAP[v.motivo_devolucion] || v.motivo_devolucion;
  return v.motivo_cancelacion || '';
}

function esDudoso(v) {
  const op = getOp(v);
  if (op !== 'CANCELADA' && op !== 'DEVOLUCION') return false;
  const ee = v.estado_envio || '';
  const cs = v.claim_status || '';
  return (ee === 'despachado' || ee === 'entregado') && cs !== 'reingresado' && cs !== 'perdida';
}

// ── Función principal ──────────────────────────────────────────────────
export async function exportarVentasXLSX(desde, hasta) {
  if (!desde || !hasta) throw new Error('Faltan fechas desde/hasta');

  await ensureXLSX();

  // Fetch ventas
  const ventas = await sbGet('ventas_ml',
    `fecha=gte.${desde}&fecha=lte.${hasta}&order=fecha.asc,hora_venta.asc&select=*`
  );

  if (!ventas.length) {
    window.toast('No hay ventas en ese rango', 'error');
    return null;
  }

  // Agrupar por SKU
  const bySku = {};
  ventas.forEach(v => {
    const sku = v.sku || 'SIN_SKU';
    (bySku[sku] = bySku[sku] || []).push(v);
  });
  const skus = Object.keys(bySku).sort();

  const wb = XLSX.utils.book_new();

  // ── Hoja RESUMEN ─────────────────────────────────────────────────
  const resH = ['SKU', 'Ventas', 'Canceladas', 'Devoluciones', 'Cant Neta',
                'Bruto', 'Comisión', 'Envío', 'Impuestos', 'A cobrar'];
  const resD = [resH];
  const tot = { v:0, c:0, d:0, cn:0, br:0, com:0, env:0, imp:0, ac:0 };

  skus.forEach(sku => {
    const rows = bySku[sku];
    let nV=0, nC=0, nD=0, cN=0, br=0, com=0, env=0, imp=0, ac=0;
    rows.forEach(v => {
      const op = getOp(v), cant = v.cantidad || 1;
      if (op === 'VENTA')      { nV += cant; cN += cant; }
      else if (op === 'CANCELADA')  { nC += cant; }
      else if (op === 'DEVOLUCION') { nD += cant; }
      if (op !== 'CANCELADA') {
        br += n(v.importe_bruto); com += n(v.cargo_venta);
        env += n(v.cargo_envio); imp += n(v.impuestos); ac += n(v.por_cobrar);
      }
    });
    resD.push([sku, nV, nC, nD, cN, r2(br), r2(com), r2(env), r2(imp), r2(ac)]);
    tot.v+=nV; tot.c+=nC; tot.d+=nD; tot.cn+=cN;
    tot.br+=br; tot.com+=com; tot.env+=env; tot.imp+=imp; tot.ac+=ac;
  });
  resD.push(['TOTAL', tot.v, tot.c, tot.d, tot.cn,
    r2(tot.br), r2(tot.com), r2(tot.env), r2(tot.imp), r2(tot.ac)]);

  const wsR = XLSX.utils.aoa_to_sheet(resD);
  wsR['!cols'] = [{wch:10},{wch:8},{wch:10},{wch:12},{wch:10},
                  {wch:14},{wch:14},{wch:14},{wch:12},{wch:14}];
  XLSX.utils.book_append_sheet(wb, wsR, 'RESUMEN');

  // ── Hojas por SKU ────────────────────────────────────────────────
  const HEADERS = [
    '#', 'FECHA VENTA', 'N VENTA', 'CANT', 'F ENTREGA',
    'CANT F DEVOLUCION', 'MOTIVO', 'STOCK DUDOSO', 'CANT SALDO',
    'Factura', 'Importe', 'Cargo venta', 'x vender cuotas',
    'Cargo envio', 'Impuestos', 'A cobrar', 'Cobrado',
    'Reintegros', 'Saldo a cobrar', 'operacion', 'F.Cobro'
  ];

  skus.forEach(sku => {
    const rows = bySku[sku];
    const data = [HEADERS];
    let saldoAcum = 0, totCant = 0, nRows = 0, nCanc = 0;
    let sI=0, sC=0, sF=0, sE=0, sIm=0, sAc=0;

    rows.forEach((v, i) => {
      const op = getOp(v), cant = v.cantidad || 1;
      const esCanc = op === 'CANCELADA';
      const esDev  = op === 'DEVOLUCION';

      if (!esCanc) { saldoAcum += cant; totCant += cant; }
      nRows++;
      if (esCanc) nCanc++;

      const pc = n(v.por_cobrar);
      if (!esCanc) {
        sI += n(v.importe_bruto); sC += n(v.cargo_venta);
        sF += n(v.costo_financiero); sE += n(v.cargo_envio);
        sIm += n(v.impuestos); sAc += pc;
      }

      data.push([
        i + 1,
        ff(v.fecha),
        String(v.ml_order_id || ''),
        cant,
        ff(v.fecha_entrega),
        esDev ? cant : null,
        getMotivo(v) || null,
        esDudoso(v) ? 'SI' : null,
        saldoAcum,
        v.factura_tango || null,
        n(v.importe_bruto),
        n(v.cargo_venta),
        n(v.costo_financiero),
        n(v.cargo_envio),
        n(v.impuestos),
        pc,
        v.conciliado ? 'Sí' : 'No',
        v.monto_reembolso ? -Math.abs(v.monto_reembolso) : null,
        esCanc ? null : pc,
        op,
        ff(v.fecha_cobro),
      ]);
    });

    data.push([
      null, 'TOTALES', null, totCant,
      null, null, null, null, null, null,
      r2(sI), r2(sC), r2(sF), r2(sE), r2(sIm), r2(sAc),
      null, null, null,
      `${nRows} ventas / ${nCanc} canc`,
      null,
    ]);

    const name = (sku || 'SIN_SKU').substring(0, 31).replace(/[\/\\*?\[\]]/g, '-');
    const ws = XLSX.utils.aoa_to_sheet(data);
    ws['!cols'] = [
      {wch:5}, {wch:12}, {wch:18}, {wch:6}, {wch:12},
      {wch:16}, {wch:16}, {wch:13}, {wch:11}, {wch:14},
      {wch:14}, {wch:13}, {wch:15}, {wch:13}, {wch:12},
      {wch:14}, {wch:10}, {wch:13}, {wch:15}, {wch:18}, {wch:12},
    ];
    XLSX.utils.book_append_sheet(wb, ws, name);
  });

  // ── Descargar ────────────────────────────────────────────────────
  const fileName = `ADARA_Ventas_SKU_${desde}_a_${hasta}.xlsx`;
  XLSX.writeFile(wb, fileName);
  window.toast(`Descargado: ${skus.length} SKUs, ${ventas.length} ventas`);
  return fileName;
}
