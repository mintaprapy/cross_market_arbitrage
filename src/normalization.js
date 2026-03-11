const TROY_OUNCE_IN_GRAMS = 31.1034768;
const TROY_OUNCE_IN_KG = TROY_OUNCE_IN_GRAMS / 1000;

export function isFiniteNumber(value) {
  return Number.isFinite(value);
}

export function normalizeToDomesticUnit(unit, price, usdCny) {
  if (!isFiniteNumber(price)) return null;

  switch (unit) {
    case 'CNY_PER_GRAM':
    case 'CNY_PER_KG':
    case 'CNY_PER_BARREL':
      return price;
    case 'USD_PER_TROY_OUNCE':
      if (!isFiniteNumber(usdCny)) return null;
      return price * usdCny;
    case 'USD_PER_BARREL':
      if (!isFiniteNumber(usdCny)) return null;
      return price * usdCny;
    default:
      return price;
  }
}

export function convertExternalToComparableUnit(assetKey, externalPrice, externalUnit, usdCny) {
  if (!isFiniteNumber(externalPrice)) return null;
  const cnyValue = normalizeToDomesticUnit(externalUnit, externalPrice, usdCny);

  switch (assetKey) {
    case 'gold':
      // USD/oz -> CNY/g
      return cnyValue / TROY_OUNCE_IN_GRAMS;
    case 'silver':
      // USD/oz -> CNY/kg
      return cnyValue / TROY_OUNCE_IN_KG;
    case 'oil':
      // USD/barrel -> CNY/barrel
      return cnyValue;
    default:
      return cnyValue;
  }
}

export function computeSpread(domestic, externalComparable) {
  if (!isFiniteNumber(domestic) || !isFiniteNumber(externalComparable)) {
    return { absolute: null, percent: null };
  }

  const absolute = domestic - externalComparable;
  const percent = externalComparable === 0 ? null : domestic / externalComparable - 1;
  return { absolute, percent };
}

export function mean(values) {
  const cleanValues = values.filter(isFiniteNumber);
  if (!cleanValues.length) return null;
  return cleanValues.reduce((sum, value) => sum + value, 0) / cleanValues.length;
}

export function stdDev(values) {
  const cleanValues = values.filter(isFiniteNumber);
  if (cleanValues.length < 2) return null;
  const avg = mean(cleanValues);
  const variance =
    cleanValues.reduce((sum, value) => sum + (value - avg) ** 2, 0) / cleanValues.length;
  return Math.sqrt(variance);
}

export function zScore(current, values) {
  if (!isFiniteNumber(current) || !values.length) return null;
  const avg = mean(values);
  const sd = stdDev(values);
  if (avg == null || sd == null || sd === 0) return null;
  return (current - avg) / sd;
}

export function percentile(current, values) {
  if (!isFiniteNumber(current) || !values.length) return null;
  const sorted = values.filter(isFiniteNumber).sort((a, b) => a - b);
  if (!sorted.length) return null;
  const below = sorted.filter((value) => value <= current).length;
  return below / sorted.length;
}

export function takeLastNumeric(values, limit) {
  return values.filter(isFiniteNumber).slice(-limit);
}
