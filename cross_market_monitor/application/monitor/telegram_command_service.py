from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from cross_market_monitor.application.common import display_group_name, display_source_name, format_local_display_timestamp, variant_group_base
from cross_market_monitor.infrastructure.http_client import HttpClient

LOGGER = logging.getLogger("cross_market_monitor")
TELEGRAM_API_BASE = "https://api.telegram.org"
MENU_HELP = "查看帮助"
MENU_QUERY = "跨市场交易查询"


@dataclass(slots=True)
class TelegramChannel:
    notifier_name: str
    bot_token: str
    chat_id: str
    timeout_sec: int
    timezone_name: str
    update_offset: int | None = None
    commands_registered: bool = False


@dataclass(slots=True)
class TelegramResponse:
    text: str
    reply_markup: dict | None = None


class TelegramCommandService:
    def __init__(self, context, query, *, poll_interval_sec: int = 5) -> None:
        self.context = context
        self.query = query
        self.poll_interval_sec = poll_interval_sec
        self.timezone = ZoneInfo(context.config.app.timezone)
        self.channels = self._build_channels()
        self.alias_map = self._build_alias_map()

    @property
    def enabled(self) -> bool:
        return bool(self.channels)

    async def run_forever(self) -> None:
        if not self.channels:
            return
        while not self.context.stop_event.is_set():
            for channel in self.channels:
                try:
                    await asyncio.to_thread(self._poll_channel_once, channel)
                except Exception:  # pragma: no cover - background task guard
                    LOGGER.exception("Telegram command poll failed for %s", channel.notifier_name)
            try:
                await asyncio.wait_for(
                    self.context.stop_event.wait(),
                    timeout=self.poll_interval_sec,
                )
            except TimeoutError:
                continue

    def _build_channels(self) -> list[TelegramChannel]:
        channels: list[TelegramChannel] = []
        seen: set[tuple[str, str]] = set()
        for notifier in self.context.config.notifiers:
            if not notifier.enabled or notifier.kind != "telegram":
                continue
            if not notifier.bot_token or not notifier.chat_id:
                continue
            key = (notifier.bot_token, notifier.chat_id)
            if key in seen:
                continue
            seen.add(key)
            channels.append(
                TelegramChannel(
                    notifier_name=notifier.name,
                    bot_token=notifier.bot_token,
                    chat_id=str(notifier.chat_id),
                    timeout_sec=notifier.timeout_sec,
                    timezone_name=self.context.config.app.timezone,
                )
            )
        return channels

    def _build_alias_map(self) -> dict[str, str]:
        alias_map: dict[str, str] = {}
        grouped: dict[str, list[str]] = {}
        enabled_group_names = {pair.group_name for pair in self.context.enabled_pairs}
        for pair in self.context.enabled_pairs:
            grouped.setdefault(variant_group_base(pair.group_name), []).append(pair.group_name)

        for group_name in sorted(self.context.pair_map):
            if group_name not in enabled_group_names:
                continue
            self._register_alias(alias_map, group_name, group_name)
            self._register_alias(alias_map, display_group_name(group_name), group_name)

        for base_name, group_names in grouped.items():
            preferred = next(
                (name for name in group_names if name.endswith("_GROSS")),
                group_names[0],
            )
            self._register_alias(alias_map, base_name, preferred)

        return alias_map

    def _register_alias(self, alias_map: dict[str, str], alias: str, group_name: str) -> None:
        normalized = self._normalize_alias(alias)
        if normalized and normalized not in alias_map:
            alias_map[normalized] = group_name

    @staticmethod
    def _normalize_alias(value: str) -> str:
        return "".join(
            ch for ch in value.strip().upper()
            if ch.isalnum() or ("\u4e00" <= ch <= "\u9fff")
        )

    def _poll_channel_once(self, channel: TelegramChannel) -> None:
        http = HttpClient(timeout_sec=channel.timeout_sec)
        self._ensure_commands_registered(http, channel)
        params: dict[str, str] = {}
        if channel.update_offset is not None:
            params["offset"] = str(channel.update_offset)
        response = http.get_json(
            f"{TELEGRAM_API_BASE}/bot{channel.bot_token}/getUpdates",
            params=params,
        )
        for item in response.get("result", []):
            update_id = item.get("update_id")
            if isinstance(update_id, int):
                channel.update_offset = update_id + 1

            callback_query = item.get("callback_query")
            if isinstance(callback_query, dict):
                chat_id = str(callback_query.get("message", {}).get("chat", {}).get("id", ""))
                if chat_id != channel.chat_id:
                    continue
                callback_id = callback_query.get("id")
                data = (callback_query.get("data") or "").strip()
                reply = self._handle_callback_data(data)
                if callback_id:
                    self._answer_callback(http, channel, str(callback_id))
                if reply:
                    self._send_message(http, channel, reply)
                continue

            message = item.get("message") or item.get("edited_message")
            if not isinstance(message, dict):
                continue
            chat_id = str(message.get("chat", {}).get("id", ""))
            if chat_id != channel.chat_id:
                continue
            text = (message.get("text") or "").strip()
            if not text:
                continue
            reply = self._handle_text(text)
            if reply:
                self._send_message(http, channel, reply)

    def _ensure_commands_registered(self, http: HttpClient, channel: TelegramChannel) -> None:
        if channel.commands_registered:
            return
        commands = [
            {"command": "help", "description": "查看帮助"},
            {"command": "query", "description": "跨市场交易查询"},
        ]
        http.post_json(
            f"{TELEGRAM_API_BASE}/bot{channel.bot_token}/setMyCommands",
            {"commands": commands},
        )
        channel.commands_registered = True

    def _send_message(self, http: HttpClient, channel: TelegramChannel, response: TelegramResponse) -> None:
        payload = {
            "chat_id": channel.chat_id,
            "text": response.text,
            "disable_web_page_preview": True,
        }
        if response.reply_markup is not None:
            payload["reply_markup"] = response.reply_markup
        http.post_json(
            f"{TELEGRAM_API_BASE}/bot{channel.bot_token}/sendMessage",
            payload,
        )

    def _answer_callback(self, http: HttpClient, channel: TelegramChannel, callback_query_id: str) -> None:
        http.post_json(
            f"{TELEGRAM_API_BASE}/bot{channel.bot_token}/answerCallbackQuery",
            {"callback_query_id": callback_query_id},
        )

    def _handle_text(self, raw_text: str) -> TelegramResponse | None:
        parts = raw_text.split(maxsplit=1)
        command = parts[0].split("@", 1)[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        if raw_text == MENU_HELP or command in {"/start", "/help"}:
            return TelegramResponse(self._help_text(), reply_markup=self._main_menu_markup())
        if raw_text == MENU_QUERY or command in {"/query", "/pairs"}:
            return self._query_menu_text()
        if command in {"/quote", "/pair", "/status"}:
            if not arg:
                return TelegramResponse(
                    "用法: /quote <交易对>\n也可点击“跨市场交易查询”选择交易对。",
                    reply_markup=self._main_menu_markup(),
                )
            return TelegramResponse(
                self._pair_snapshot_text(arg),
                reply_markup=self._main_menu_markup(),
            )
        normalized = self._normalize_alias(raw_text)
        if normalized in self.alias_map:
            return TelegramResponse(
                self._pair_snapshot_text(raw_text),
                reply_markup=self._main_menu_markup(),
            )
        return TelegramResponse("未识别操作。请使用下方菜单。", reply_markup=self._main_menu_markup())

    def _handle_callback_data(self, data: str) -> TelegramResponse | None:
        if not data:
            return None
        if data.startswith("pair:"):
            group_name = data.split(":", 1)[1].strip()
            if group_name not in self.context.pair_map:
                return TelegramResponse("未识别交易对。", reply_markup=self._main_menu_markup())
            return TelegramResponse(
                self._pair_snapshot_text(group_name),
                reply_markup=self._main_menu_markup(),
            )
        return TelegramResponse("未识别菜单操作。", reply_markup=self._main_menu_markup())

    def _main_menu_markup(self) -> dict:
        return {
            "keyboard": [[{"text": MENU_HELP}, {"text": MENU_QUERY}]],
            "resize_keyboard": True,
            "is_persistent": True,
        }

    def _query_menu_text(self) -> TelegramResponse:
        items: list[dict[str, str]] = []
        seen: set[str] = set()
        for pair in self.context.enabled_pairs:
            label = display_group_name(pair.group_name)
            if label in seen:
                continue
            seen.add(label)
            items.append({"text": label, "callback_data": f"pair:{pair.group_name}"})
        if not items:
            return TelegramResponse("当前没有可查询交易对。", reply_markup=self._main_menu_markup())
        buttons = [items[i : i + 3] for i in range(0, len(items), 3)]
        return TelegramResponse("请选择交易对：", reply_markup={"inline_keyboard": buttons})

    def _help_text(self) -> str:
        return (
            "可用菜单：\n"
            "1. 查看帮助\n"
            "2. 跨市场交易查询\n"
            "\n"
            "命令兼容：\n"
            "/help - 查看帮助\n"
            "/query - 打开交易对菜单\n"
            "/quote AU_XAU\n"
            "/quote CU_COPPER\n"
            "/quote CU_COPPER除税"
        )

    def _pair_snapshot_text(self, alias: str) -> str:
        group_name = self.alias_map.get(self._normalize_alias(alias))
        if not group_name:
            return f"未识别交易对: {alias}\n请点击“跨市场交易查询”选择交易对。"

        item = self.query.get_snapshot_row(group_name)
        if item is None:
            return f"{display_group_name(group_name)} 暂无可用快照。"

        commodity = item.get("commodity_spec") or {}
        unit = commodity.get("normalized_unit_label") or item.get("target_unit", "")
        local_ts = self._format_local_ts(item.get("ts_local") or item.get("ts"))
        domestic_label = self._leg_price_label("国内", item.get("domestic_source"))
        overseas_label = self._leg_price_label("海外", item.get("overseas_source"))
        return "\n".join(
            [
                f"{display_group_name(group_name)}",
                f"状态: {item.get('status') or '--'}",
                f"价差百分比: {self._fmt_pct(item.get('spread_pct'))}",
                f"理论价差: {self._fmt_num(item.get('spread'))} {unit}".rstrip(),
                f"Z-Score: {self._fmt_num(item.get('zscore'))}",
                f"{domestic_label}: {self._fmt_num(item.get('domestic_last_raw'))}",
                f"国内换算价: {self._fmt_num(item.get('normalized_last'))} {unit}".rstrip(),
                f"{overseas_label}: {self._fmt_num(item.get('overseas_last'))} {unit}".rstrip(),
                f"汇率: {self._fmt_num(item.get('fx_rate'))}",
                f"时效: 国内 {self._fmt_age(item.get('domestic_age_sec'))} / 海外 {self._fmt_age(item.get('overseas_age_sec'))} / 汇率 {self._fmt_age(item.get('fx_age_sec'))}",
                f"时间: {local_ts}",
            ]
        )

    def _format_local_ts(self, value: str | None) -> str:
        if not value:
            return "--"
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
        return format_local_display_timestamp(parsed, self.timezone)

    @staticmethod
    def _leg_price_label(prefix: str, source_name: str | None) -> str:
        source = display_source_name(source_name)
        if source == "--":
            return f"{prefix}价格" if prefix == "国内" else f"{prefix}最新价"
        suffix = "价格"
        return f"{prefix}{source}{suffix}"

    @staticmethod
    def _fmt_num(value: float | None) -> str:
        if value is None:
            return "--"
        return f"{value:.4f}"

    @staticmethod
    def _fmt_pct(value: float | None) -> str:
        if value is None:
            return "--"
        return f"{value * 100:.2f}%"

    @staticmethod
    def _fmt_age(value: float | None) -> str:
        if value is None:
            return "--"
        return f"{value:.1f}s"
