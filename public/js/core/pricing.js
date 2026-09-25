// core/pricing.js — Motor de cálculo de precios por canal.
// Port 1:1 de `lib/pricing.ts` de pricing-adara-online (commit 8b56bcd, 24/9/2026),
// primer paso de la unificación de pricing en ADARA APP. Ver ADARA-PRICING.md.
//
// Módulo PURO: no lee ni escribe base, no llama a ML. Recibe los datos y devuelve números.
// Mantiene los nombres de campo de pricing (cost_without_vat, vat_rate, iibb_rate…) para
// que la migración de datos mapee directo. Todas las tasas van en PORCENTAJE (21 = 21 %).
//
// Es SIMULACIÓN (PRC1): usa el costo editable y las tasas propias de pricing, que no tienen
// que coincidir con FIFO (CF6) ni con el IIBB del CM03 (IIBB3). Sus números nunca
// alimentan la administración (PRC2).

export function money(value) {
  if (value === undefined || value === null || Number.isNaN(value)) return '-';
  return new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 }).format(value);
}

export function moneyWithCents(value) {
  if (value === undefined || value === null || Number.isNaN(value)) return '-';
  return new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value);
}

export function percent(value) {
  if (value === undefined || value === null || Number.isNaN(value)) return '-';
  return `${Number(value).toLocaleString('es-AR', { maximumFractionDigits: 2 })}%`;
}

// Precio de lista que, con el descuento de la promo, deja el precio final pedido.
export function promoListPrice(finalPrice, discountRate) {
  const price = Number(finalPrice || 0);
  const discount = Number(discountRate || 0);
  if (!price || !Number.isFinite(price) || discount <= 0) return null;
  if (discount >= 100) return null;
  return price / (1 - discount / 100);
}

// Sólo un precio de lista CALCULADO se normaliza a centavos antes de mandarlo a ML.
// Un PVP explícito ya tiene la precisión que cargó el usuario.
export function priceForMercadoLibreUpload(finalPrice, discountRate) {
  if (discountRate <= 0) return finalPrice;
  const listPrice = promoListPrice(finalPrice, discountRate);
  return listPrice === null ? null : Math.round(listPrice * 100) / 100;
}

export function toNumber(value) {
  if (value.trim() === '') return null;
  const parsed = Number(value.replace(',', '.'));
  return Number.isFinite(parsed) ? parsed : null;
}

export function rateToDecimal(rate) {
  return Number(rate || 0) / 100;
}

// mode: 'nearest' | 'up' | 'down'
export function roundPrice(value, roundTo, mode) {
  const step = Math.max(1, Number(roundTo || 1));
  if (mode === 'up') return Math.ceil(value / step) * step;
  if (mode === 'down') return Math.floor(value / step) * step;
  return Math.round(value / step) * step;
}

export function defaultTaxSettings() {
  return { key: 'default', iibb_rate: 0, idc_rate: 0, iigg_rate: 0, structure_rate: 0, notes: '' };
}

export function mercadoLibreClassicOption() {
  return {
    code: 'MC',
    name: 'MercadoLibre Clásica',
    channel_type: 'mercadolibre',
    installment_count: null,
    financing_fee_rate: 0,
    applies_marketplace_fee: true,
    applies_shipping: true,
    applies_iibb: true,
    applies_idc: true,
    applies_iigg: true,
    applies_structure: true,
    applies_vat: true,
    active: true,
  };
}

function defaultTrue(value) { return value !== false; }
function defaultFalse(value) { return value === true; }

// Canal de ML (MC, MPx): todo aplica salvo que se apague explícito.
// Canal directo (EF, TR, TN…): nada aplica salvo que se prenda explícito.
export function normalizeOption(option) {
  const channelType = option.channel_type ||
    (option.code?.startsWith('MP') || option.code === 'MC' ? 'mercadolibre' : 'directo');
  const isMl = channelType === 'mercadolibre' || option.code === 'MC' || option.code?.startsWith('MP');
  const flag = v => (isMl ? defaultTrue(v) : defaultFalse(v));

  return {
    ...option,
    channel_type: channelType,
    financing_fee_rate: Number(option.financing_fee_rate || 0),
    applies_marketplace_fee: flag(option.applies_marketplace_fee),
    applies_shipping: flag(option.applies_shipping),
    applies_iibb: flag(option.applies_iibb),
    applies_idc: flag(option.applies_idc),
    applies_iigg: flag(option.applies_iigg),
    applies_structure: flag(option.applies_structure),
    applies_vat: flag(option.applies_vat),
  };
}

export function calculateMercadoLibrePrice(product, option, categoryFee, taxes = defaultTaxSettings(),
  shippingCost, desiredProfitRate = 5, roundTo = 100, roundingMode = 'nearest') {
  return calculatePriceSummary(product, option, categoryFee, taxes, shippingCost, {
    desiredMarginRate: desiredProfitRate,
    desiredNetProfit: null,
    roundTo,
    roundingMode,
  });
}

// Regla de negocio de la cuenta: exención del cargo fijo por debajo de $33.000 por unidad.
// El aporte de ML no integra el precio del comprador.
export function calculateB2bPriceSummary(product, option, categoryFee, taxes = defaultTaxSettings(),
  shippingCost, target = {}) {
  const withoutFixed = calculatePriceSummary(product, option, categoryFee, taxes,
    { ...shippingCost, fixed_fee_amount: 0 }, target);
  const customerPrice = Number(target.salePrice ?? (withoutFixed.valid ? withoutFixed.roundedPrice : 0))
    - Number(target.meliContributionAmount || 0);
  if (withoutFixed.valid && customerPrice > 0 && customerPrice < 33000) return withoutFixed;
  return calculatePriceSummary(product, option, categoryFee, taxes, shippingCost, target);
}

// Calcula el precio de un producto en un canal. Tres modos, por prioridad:
//   1. target.salePrice (PVP manual): no recalcula, expone el margen real.
//   2. target.desiredNetProfit (utilidad neta en $): despeja el precio.
//   3. target.desiredMarginRate (margen % sobre venta neta, default 5): despeja el precio.
//
// product:     { cost_without_vat, vat_rate }
// rawOption:   canal (ver mercadoLibreClassicOption / normalizeOption)
// categoryFee: { marketplace_fee_rate }                      — comisión ML de la categoría
// taxes:       { iibb_rate, idc_rate, iigg_rate }
// shippingCost:{ fixed_fee_amount, shipping_cost_amount }   — importes BRUTOS (con IVA)
// target:      { desiredMarginRate, desiredNetProfit, salePrice, structureAmount,
//                manualShippingAmount, salesCommissionRate, saleAppliesVat, costVatRate,
//                roundTo, roundingMode }
export function calculatePriceSummary(product, rawOption, categoryFee, taxes = defaultTaxSettings(),
  shippingCost, target = {}) {
  const option = normalizeOption(rawOption);
  const costWithoutVat = Number(product.cost_without_vat || 0);
  const productVatRate = Number(product.vat_rate || 21);
  const appliesVat = target.saleAppliesVat ?? option.applies_vat;
  const saleVatRate = appliesVat ? productVatRate : 0;
  // IVA del costo que NO se recupera como crédito (p. ej. compra sin factura A): suma al costo.
  const rawCostVatRate = Number(target.costVatRate ?? 0);
  const costVatRate = Math.max(0, Math.min(productVatRate, rawCostVatRate));
  const costVatAmount = costWithoutVat * rateToDecimal(costVatRate);
  const costForProfit = costWithoutVat + costVatAmount;
  const marketplaceFeeRate = option.applies_marketplace_fee ? Number(categoryFee?.marketplace_fee_rate || 0) : 0;
  const financingFeeRate = Number(option.financing_fee_rate || 0);
  const marginRate = Number(target.desiredMarginRate ?? 5);
  const desiredNetProfit = target.desiredNetProfit ?? null;

  const roundTo = target.roundTo ?? 100;
  const roundingMode = target.roundingMode ?? 'nearest';

  const iibbRate = option.applies_iibb ? Number(taxes.iibb_rate || 0) : 0;
  const idcRate = option.applies_idc ? Number(taxes.idc_rate || 0) : 0;
  const iiggRate = option.applies_iigg ? Number(taxes.iigg_rate || 0) : 0;
  const structureAmount = option.applies_structure ? Number(target.structureAmount || 0) : 0;
  const salesCommissionRate = option.applies_marketplace_fee ? 0 : Number(target.salesCommissionRate || 0);

  // Cargo fijo y envío de ML vienen con IVA 21 %: para rentabilidad se usa el neto.
  const fixedFeeAmountGross = Number(shippingCost?.fixed_fee_amount || 0);
  const fixedFeeAmount = option.applies_shipping ? fixedFeeAmountGross / 1.21 : fixedFeeAmountGross;
  const shippingCostAmountGross = option.applies_shipping ? Number(shippingCost?.shipping_cost_amount || 0) : 0;
  const shippingCostAmount = option.applies_shipping
    ? shippingCostAmountGross / 1.21
    : Number(target.manualShippingAmount || 0);
  const manualShippingAmount = option.applies_shipping ? 0 : shippingCostAmount;
  const fixedCosts = fixedFeeAmount + shippingCostAmount + structureAmount;
  const salesTaxRate = iibbRate + idcRate;
  const iiggDecimal = rateToDecimal(iiggRate);
  const channelFeeRate = marketplaceFeeRate + financingFeeRate;

  // Comisiones/costos de canal cargados como % sobre precio de venta con IVA.
  // Si están facturados con IVA 21 %, para rentabilidad se usa el neto: precio bruto × % / 1,21.
  // Expresado sobre precio sin IVA de la venta, el factor cambia según si la venta lleva IVA o no.
  const channelFeeRateOnNetSale = ((1 + saleVatRate / 100) / 1.21) * channelFeeRate;
  const salesCommissionRateOnNetSale = (1 + saleVatRate / 100) * salesCommissionRate;
  const saleCostRate = channelFeeRateOnNetSale + salesTaxRate + salesCommissionRateOnNetSale;

  let netSalePrice;
  let price;
  let roundedPrice;
  let effectiveMarginRate = marginRate;
  const explicitSalePrice = target.salePrice ?? null;
  const invalidValues = extra => ({
    marketplaceFeeRate, financingFeeRate, marginRate, salesTaxRate, iiggRate,
    fixedFeeAmount, shippingCostAmount, shippingCostAmountGross, fixedCosts,
    variableRate: saleCostRate, ...extra,
  });

  if (iiggDecimal >= 1) {
    return invalidResult('Impuesto a las ganancias no puede ser 100% o mayor.', invalidValues());
  }

  if (explicitSalePrice !== null && Number.isFinite(Number(explicitSalePrice)) && Number(explicitSalePrice) > 0) {
    price = Number(explicitSalePrice);
    roundedPrice = price;
    netSalePrice = price / (1 + saleVatRate / 100);
  } else if (desiredNetProfit !== null && Number.isFinite(Number(desiredNetProfit))) {
    const denominator = 1 - rateToDecimal(saleCostRate);
    if (denominator <= 0) {
      return invalidResult('La suma de comisión, cuotas e impuestos de venta llega o supera el 100%.', invalidValues());
    }
    const requiredGrossProfit = Number(desiredNetProfit) / (1 - iiggDecimal);
    netSalePrice = (costForProfit + fixedCosts + requiredGrossProfit) / denominator;
    effectiveMarginRate = netSalePrice > 0 ? (Number(desiredNetProfit) / netSalePrice) * 100 : 0;
    price = netSalePrice * (1 + saleVatRate / 100);
    roundedPrice = roundPrice(price, roundTo, roundingMode);
  } else {
    const desiredNetMarginDecimal = rateToDecimal(marginRate);
    const denominator = 1 - rateToDecimal(saleCostRate) - desiredNetMarginDecimal / (1 - iiggDecimal);
    if (denominator <= 0) {
      return invalidResult('La suma de margen, comisión, cuotas e impuestos de venta llega o supera el 100%.',
        invalidValues({ variableRate: saleCostRate + marginRate }));
    }
    netSalePrice = (costForProfit + fixedCosts) / denominator;
    price = netSalePrice * (1 + saleVatRate / 100);
    roundedPrice = roundPrice(price, roundTo, roundingMode);
  }

  const roundedNetSalePrice = roundedPrice / (1 + saleVatRate / 100);

  const vatAmount = roundedPrice - roundedNetSalePrice;
  const marketplaceFeeAmount = (roundedPrice * rateToDecimal(channelFeeRate)) / 1.21;
  const salesCommissionAmount = roundedPrice * rateToDecimal(salesCommissionRate);
  const iibbAmount = roundedNetSalePrice * rateToDecimal(iibbRate);
  const idcAmount = roundedNetSalePrice * rateToDecimal(idcRate);
  const grossProfit = roundedNetSalePrice - costForProfit - fixedCosts - marketplaceFeeAmount
    - salesCommissionAmount - iibbAmount - idcAmount;
  const incomeTaxAmount = Math.max(grossProfit, 0) * rateToDecimal(iiggRate);
  const netProfit = grossProfit - incomeTaxAmount;
  const marginOnCost = costForProfit > 0 ? (netProfit / costForProfit) * 100 : 0;
  const marginOnNetSale = roundedNetSalePrice > 0 ? (netProfit / roundedNetSalePrice) * 100 : 0;
  if (explicitSalePrice !== null && Number.isFinite(Number(explicitSalePrice)) && Number(explicitSalePrice) > 0) {
    effectiveMarginRate = marginOnNetSale;
  }
  const variableRate = saleCostRate + effectiveMarginRate;

  return {
    valid: true,
    price,
    roundedPrice,
    netSalePrice: roundedNetSalePrice,
    vatAmount,
    marketplaceFeeAmount,
    iibbAmount,
    idcAmount,
    grossProfit,
    incomeTaxAmount,
    netProfit,
    marginOnCost,
    marginOnNetSale,
    marketplaceFeeRate,
    financingFeeRate,
    marginRate: effectiveMarginRate,
    desiredNetProfit: desiredNetProfit ?? netProfit,
    taxesRate: salesTaxRate + iiggRate,
    salesTaxRate,
    iibbRate,
    idcRate,
    iiggRate,
    structureRate: 0,
    structureAmount,
    costVatRate,
    costVatAmount,
    costForProfit,
    salesCommissionRate,
    salesCommissionAmount,
    fixedFeeAmount,
    shippingCostAmount,
    shippingCostAmountGross,
    manualShippingAmount,
    fixedCosts,
    saleVatRate,
    appliesVat: Boolean(appliesVat),
    appliesMarketplaceFee: Boolean(option.applies_marketplace_fee),
    appliesShipping: Boolean(option.applies_shipping),
    variableRate,
    error: null,
  };
}

function invalidResult(message, values) {
  return {
    valid: false,
    price: null,
    roundedPrice: null,
    netSalePrice: null,
    vatAmount: null,
    marketplaceFeeAmount: null,
    salesCommissionAmount: null,
    iibbAmount: null,
    idcAmount: null,
    structureAmount: null,
    costVatRate: null,
    costVatAmount: null,
    costForProfit: null,
    grossProfit: null,
    incomeTaxAmount: null,
    netProfit: null,
    marginOnCost: null,
    marginOnNetSale: null,
    desiredNetProfit: null,
    ...values,
    error: message,
  };
}
