# Cross Market Commodity Monitor

基于文档落地的一版可运行实现，目标是把国内期货价格换算成国际统一口径，实时监控跨市场商品价差、统计偏离和基础告警。

## 已实现能力

- Sina 国内主力连续合约采集：`AU / AG / CU / SC`
- OKX 海外映射标的采集：`XAU / XAG / XCU / CL`
- Frankfurter `USD/CNY` 汇率采集，支持失败时降级到配置里的固定汇率
- 国内价格标准化公式：
  - 黄金 `CNY/g -> USD/oz`
  - 白银 `CNY/kg -> USD/oz`
  - 铜 `CNY/ton -> USD/lb`
  - 原油 `CNY/bbl -> USD/bbl`
- 同时支持理论价差和双向可成交价差
- 滚动均值、标准差、z-score、delta_spread
- SQLite 落库：原始行情、汇率、快照、告警
- 通知投递：控制台 notifier，支持扩展 webhook 推送
- CLI 终端输出
- FastAPI Dashboard 与 JSON API
- CSV 导出
- 历史回放分析

## 项目结构

```text
cross_market_monitor/
├── application/     # 编排与告警
├── domain/          # 领域模型、公式、统计
├── infrastructure/  # 外部源与 SQLite
├── interfaces/      # Dashboard
└── main.py          # CLI / Serve 入口
```

## 配置文件

默认配置在 [config/monitor.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/monitor.yaml)。

已预置的监控组：
- `AU_XAU`
- `AG_XAG_GROSS`
- `AG_XAG_NET`
- `CU_XCU_GROSS`
- `CU_XCU_NET`
- `SC_CL`

## 运行方式

单次轮询并打印终端结果：

```bash
python3 -m cross_market_monitor.main run-once
```

连续跑几轮控制台监控：

```bash
python3 -m cross_market_monitor.main console --cycles 5
```

启动 Web Dashboard：

```bash
python3 -m cross_market_monitor.main serve
```

启动后打开：

- [http://127.0.0.1:8000](http://127.0.0.1:8000)
- [http://127.0.0.1:8000/api/snapshot](http://127.0.0.1:8000/api/snapshot)

导出历史快照为 CSV：

```bash
python3 -m cross_market_monitor.main export-csv --dataset snapshots --group-name AU_XAU
```

做一份回放分析：

```bash
python3 -m cross_market_monitor.main replay --group-name AU_XAU --limit 500
```

输出 JSON 分析报告：

```bash
python3 -m cross_market_monitor.main replay --group-name AU_XAU --limit 500 --format json
```

## 数据落库

SQLite 文件默认写入：

- [data/monitor.db](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/data/monitor.db)

核心表：
- `raw_quotes`
- `fx_rates`
- `spread_snapshots`
- `alert_events`
- `notification_deliveries`

## 通知推送

默认启用控制台告警输出。  
如需接入外部消息系统，可在 [monitor.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/monitor.yaml) 的 `notifiers` 中启用 `webhook`，程序会以 JSON `POST` 方式推送告警。

API 也新增了：

- `GET /api/replay/summary?group_name=AU_XAU`
- `GET /api/notification-deliveries`

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 注意事项

- `AG` 和 `CU` 已按文档要求同时保留含税和去税两套监控。
- `SC_CL` 被视为扩散/收敛监控对象，不强行假设零均值收敛。
- 汇率源当前采用公开汇率接口，若后续需要更实时口径，建议切换到交易型 FX 或自有源。
