import unittest

from cross_market_monitor.domain.formulas import POUNDS_PER_METRIC_TON, TROY_OUNCE_IN_GRAMS, compute_spread, normalize_domestic_price
from cross_market_monitor.domain.models import PairConfig


def build_pair(formula: str, tax_mode: str, domestic_unit: str, target_unit: str) -> PairConfig:
    return PairConfig(
        group_name="TEST",
        domestic_source="domestic",
        domestic_symbol="domestic",
        domestic_label="domestic",
        overseas_source="overseas",
        overseas_symbol="overseas",
        overseas_label="overseas",
        formula=formula,
        domestic_unit=domestic_unit,
        target_unit=target_unit,
        tax_mode=tax_mode,
    )


class FormulaTests(unittest.TestCase):
    def test_gold_formula_matches_document(self) -> None:
        pair = build_pair("gold", "gross", "CNY_PER_GRAM", "USD_PER_OUNCE")
        price = normalize_domestic_price(1142.0, pair, 6.869)
        self.assertIsNotNone(price)
        expected = 1142.0 * TROY_OUNCE_IN_GRAMS / 6.869
        self.assertEqual(round(price or 0, 6), round(expected, 6))

    def test_silver_net_is_lower_than_gross(self) -> None:
        gross_pair = build_pair("silver", "gross", "CNY_PER_KG", "USD_PER_OUNCE")
        net_pair = build_pair("silver", "net", "CNY_PER_KG", "USD_PER_OUNCE")
        gross = normalize_domestic_price(22000.0, gross_pair, 6.9)
        net = normalize_domestic_price(22000.0, net_pair, 6.9)
        self.assertIsNotNone(gross)
        self.assertIsNotNone(net)
        self.assertLess(net or 0, gross or 0)

    def test_copper_net_is_lower_than_gross(self) -> None:
        gross_pair = build_pair("copper", "gross", "CNY_PER_TON", "USD_PER_POUND")
        net_pair = build_pair("copper", "net", "CNY_PER_TON", "USD_PER_POUND")
        gross = normalize_domestic_price(76000.0, gross_pair, 6.9)
        net = normalize_domestic_price(76000.0, net_pair, 6.9)
        self.assertIsNotNone(gross)
        self.assertIsNotNone(net)
        self.assertLess(net or 0, gross or 0)

    def test_spread_uses_domestic_minus_overseas_and_symmetric_pct(self) -> None:
        spread, spread_pct = compute_spread(100.0, 90.0)
        self.assertEqual(spread, 10.0)
        self.assertAlmostEqual(spread_pct or 0, 20.0 / 190.0)

    def test_cotton_formula_converts_cny_per_ton_to_usd_per_pound(self) -> None:
        pair = build_pair("cotton", "gross", "CNY_PER_TON", "USD_PER_POUND")
        price = normalize_domestic_price(14_800.0, pair, 7.2)
        expected = 14_800.0 / 7.2 / POUNDS_PER_METRIC_TON
        self.assertEqual(round(price or 0, 8), round(expected, 8))

    def test_sugar_formula_converts_cny_per_ton_to_usd_per_pound(self) -> None:
        pair = build_pair("sugar", "gross", "CNY_PER_TON", "USD_PER_POUND")
        price = normalize_domestic_price(6_300.0, pair, 7.2)
        expected = 6_300.0 / 7.2 / POUNDS_PER_METRIC_TON
        self.assertEqual(round(price or 0, 8), round(expected, 8))


if __name__ == "__main__":
    unittest.main()
