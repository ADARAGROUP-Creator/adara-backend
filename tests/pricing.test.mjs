// Tests del motor de precios (public/js/core/pricing.js). Correr: npm test
// Los casos B2B son el port de scripts/test-b2b-fixed-fee.cjs de pricing-adara-online.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  calculatePriceSummary as retail,
  calculateB2bPriceSummary as b2b,
  mercadoLibreClassicOption,
  defaultTaxSettings,
  normalizeOption,
  roundPrice,
  priceForMercadoLibreUpload,
} from '../public/js/core/pricing.js';

const close = (actual, expected, eps = 0.01) =>
  assert.ok(Math.abs(actual - expected) < eps, `${actual} ≠ ${expected}`);

// Ejemplo documentado en ADARA-PRICING.md: SKU 101, ML Clásica, 24/9/2026.
test('SKU 101 en MC con utilidad objetivo da $500.800', () => {
  const r = retail(
    { cost_without_vat: 342000, vat_rate: 10.5 },
    mercadoLibreClassicOption(),
    { marketplace_fee_rate: 12.5 },
    { iibb_rate: 5, idc_rate: 0, iigg_rate: 0 },
    { fixed_fee_amount: 0, shipping_cost_amount: 7290 },
    { desiredNetProfit: 30756 },
  );
  assert.equal(r.valid, true);
  close(r.shippingCostAmount, 6024.79);
  close(r.price, 500752.80);
  assert.equal(r.roundedPrice, 500800);
});

test('PVP manual no se recalcula y expone el margen real', () => {
  const r = retail({ cost_without_vat: 1000, vat_rate: 21 }, mercadoLibreClassicOption(),
    { marketplace_fee_rate: 10 }, defaultTaxSettings(), null, { salePrice: 2000 });
  assert.equal(r.roundedPrice, 2000);
  close(r.netSalePrice, 2000 / 1.21);
  close(r.marginRate, r.marginOnNetSale);
});

test('Ganancias sólo sobre ganancia bruta positiva', () => {
  const r = retail({ cost_without_vat: 5000, vat_rate: 21 }, mercadoLibreClassicOption(),
    { marketplace_fee_rate: 0 }, { iibb_rate: 0, idc_rate: 0, iigg_rate: 35 }, null, { salePrice: 1000 });
  assert.ok(r.grossProfit < 0);
  assert.equal(r.incomeTaxAmount, 0);
});

test('canal directo: sin comisión ML, envío ni IVA salvo que se prendan', () => {
  const o = normalizeOption({ code: 'EF' });
  assert.equal(o.channel_type, 'directo');
  assert.equal(o.applies_marketplace_fee, false);
  assert.equal(o.applies_vat, false);
  const ml = normalizeOption({ code: 'MP6', financing_fee_rate: '9.5' });
  assert.equal(ml.channel_type, 'mercadolibre');
  assert.equal(ml.applies_marketplace_fee, true);
  assert.equal(ml.financing_fee_rate, 9.5);
});

test('margen + costos ≥ 100 % es inválido', () => {
  const r = retail({ cost_without_vat: 1000, vat_rate: 21 }, mercadoLibreClassicOption(),
    { marketplace_fee_rate: 60 }, { iibb_rate: 5 }, null, { desiredMarginRate: 50 });
  assert.equal(r.valid, false);
  assert.match(r.error, /100%/);
});

test('redondeo y precio de lista para promo', () => {
  assert.equal(roundPrice(1249, 100, 'nearest'), 1200);
  assert.equal(roundPrice(1201, 100, 'up'), 1300);
  assert.equal(roundPrice(1299, 100, 'down'), 1200);
  assert.equal(priceForMercadoLibreUpload(73210.37, 0), 73210.37);
  assert.equal(priceForMercadoLibreUpload(73210.37, 5), Math.round((73210.37 / 0.95) * 100) / 100);
});

// ── B2B: exención del cargo fijo por debajo de $33.000 por unidad ──────────
const args = [
  { sku: 'CHEAP', cost_without_vat: 12000, vat_rate: 21 },
  mercadoLibreClassicOption(),
  { marketplace_fee_rate: 15 },
  defaultTaxSettings(),
  { fixed_fee_amount: 3000, shipping_cost_amount: 1000 },
];

test('B2B: bajo $33.000 no paga cargo fijo', () => {
  for (const price of [20000, 32999.99]) {
    const result = b2b(...args, { salePrice: price });
    const normal = retail(...args, { salePrice: price });
    assert.equal(result.valid, true);
    assert.equal(result.fixedFeeAmount, 0);
    assert.ok(normal.fixedFeeAmount > 0);
    assert.ok(result.netProfit > normal.netProfit);
    assert.equal(result.shippingCostAmount, normal.shippingCostAmount);
  }
});

test('B2B: desde $33.000 paga cargo fijo; el aporte de ML baja el precio del comprador', () => {
  for (const price of [33000, 40000]) {
    assert.equal(b2b(...args, { salePrice: price }).fixedFeeAmount, retail(...args, { salePrice: price }).fixedFeeAmount);
  }
  assert.equal(b2b(...args, { salePrice: 34000, meliContributionAmount: 2000 }).fixedFeeAmount, 0);
});

test('B2B: precio sugerido sin fijo y consistente', () => {
  const target = { desiredMarginRate: 10, roundTo: 1 };
  const suggested = b2b(...args, target);
  assert.equal(suggested.valid, true);
  assert.equal(suggested.fixedFeeAmount, 0);
  assert.ok(suggested.roundedPrice < retail(...args, target).roundedPrice);
  assert.equal(b2b(...args, { salePrice: suggested.roundedPrice }).fixedFeeAmount, 0);
});
