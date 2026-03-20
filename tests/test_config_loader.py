from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from cross_market_monitor.infrastructure.config_loader import load_config


class ConfigLoaderTests(unittest.TestCase):
    def test_load_config_merges_imported_fragments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            config_dir = root / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            (config_dir / "app.yaml").write_text(
                textwrap.dedent(
                    """
                    app:
                      name: split
                      fx_source: fx
                      sqlite_path: data/monitor.db
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )
            (config_dir / "sources.yaml").write_text(
                textwrap.dedent(
                    """
                    sources:
                      domestic:
                        kind: mock_quote
                        base_url: http://local
                      overseas:
                        kind: mock_quote
                        base_url: http://local
                      fx:
                        kind: mock_fx
                        base_url: http://local
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )
            (config_dir / "pairs.yaml").write_text(
                textwrap.dedent(
                    """
                    pairs:
                      - group_name: AU_XAU_TEST
                        domestic_source: domestic
                        domestic_symbol: nf_AU0
                        domestic_label: AU Main
                        overseas_source: overseas
                        overseas_symbol: XAU
                        overseas_label: XAU
                        formula: gold
                        domestic_unit: CNY_PER_GRAM
                        target_unit: USD_PER_OUNCE
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )
            (config_dir / "notifiers.yaml").write_text(
                textwrap.dedent(
                    """
                    notifiers:
                      - name: console_alerts
                        kind: console
                        enabled: true
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )
            (config_dir / "monitor.yaml").write_text(
                textwrap.dedent(
                    """
                    imports:
                      - app.yaml
                      - sources.yaml
                      - pairs.yaml
                      - notifiers.yaml
                    app:
                      export_dir: exports
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )

            config = load_config(config_dir / "monitor.yaml")

            self.assertEqual(config.app.name, "split")
            self.assertEqual(config.app.fx_source, "fx")
            self.assertEqual(config.app.export_dir, str((root / "exports").resolve()))
            self.assertEqual(len(config.pairs), 1)
            self.assertEqual(config.notifiers[0].name, "console_alerts")

    def test_load_config_ignores_missing_optional_imports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            config_dir = root / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            (config_dir / "app.yaml").write_text(
                textwrap.dedent(
                    """
                    app:
                      name: split
                      fx_source: fx
                      sqlite_path: data/monitor.db
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )
            (config_dir / "sources.yaml").write_text(
                textwrap.dedent(
                    """
                    sources:
                      domestic:
                        kind: mock_quote
                        base_url: http://local
                      overseas:
                        kind: mock_quote
                        base_url: http://local
                      fx:
                        kind: mock_fx
                        base_url: http://local
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )
            (config_dir / "pairs.yaml").write_text(
                textwrap.dedent(
                    """
                    pairs:
                      - group_name: AU_XAU_TEST
                        domestic_source: domestic
                        domestic_symbol: nf_AU0
                        domestic_label: AU Main
                        overseas_source: overseas
                        overseas_symbol: XAU
                        overseas_label: XAU
                        formula: gold
                        domestic_unit: CNY_PER_GRAM
                        target_unit: USD_PER_OUNCE
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )
            (config_dir / "monitor.yaml").write_text(
                textwrap.dedent(
                    """
                    imports:
                      - app.yaml
                      - sources.yaml
                      - pairs.yaml
                    optional_imports:
                      - monitor.secrets.local.yaml
                      - monitor.notifiers.local.yaml
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )

            config = load_config(config_dir / "monitor.yaml")

            self.assertEqual(config.app.name, "split")
            self.assertEqual(len(config.pairs), 1)
            self.assertEqual(config.notifiers, [])

    def test_load_config_merges_relative_trading_calendar_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            config_dir = root / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            (config_dir / "calendar.yaml").write_text(
                textwrap.dedent(
                    """
                    domestic:
                      weekends_closed: true
                      non_trading_dates_local:
                        - 2026-01-01
                        - 2026-01-02
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )
            (config_dir / "monitor.yaml").write_text(
                textwrap.dedent(
                    """
                    app:
                      name: test
                      domestic_trading_calendar_path: calendar.yaml
                      domestic_non_trading_dates_local:
                        - 2026-02-01
                      fx_source: fx
                      sqlite_path: data/monitor.db
                    sources:
                      domestic:
                        kind: mock_quote
                        base_url: http://local
                      overseas:
                        kind: mock_quote
                        base_url: http://local
                      fx:
                        kind: mock_fx
                        base_url: http://local
                    pairs:
                      - group_name: AU_XAU_TEST
                        domestic_source: domestic
                        domestic_symbol: nf_AU0
                        domestic_label: AU Main
                        overseas_source: overseas
                        overseas_symbol: XAU
                        overseas_label: XAU
                        formula: gold
                        domestic_unit: CNY_PER_GRAM
                        target_unit: USD_PER_OUNCE
                    """
                ).strip() + "\n",
                encoding="utf-8",
            )

            config = load_config(config_dir / "monitor.yaml")

            self.assertTrue(config.app.domestic_weekends_closed)
            self.assertEqual(
                [item.isoformat() for item in config.app.domestic_non_trading_dates_local],
                ["2026-02-01", "2026-01-01", "2026-01-02"],
            )
            self.assertEqual(
                config.app.domestic_trading_calendar_path,
                str((config_dir / "calendar.yaml").resolve()),
            )
            self.assertEqual(
                config.app.sqlite_path,
                str((root / "data" / "monitor.db").resolve()),
            )
