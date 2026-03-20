# Cross Market Commodity Monitor

将国内商品期货价格统一换算成国际口径，实时监控跨市场价差、FX 风险暂停和历史研究指标。

## 运行诊断导出

项目内置运行诊断导出脚本 [export_runtime_diagnostics.py](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/scripts/export_runtime_diagnostics.py)，用于一次性打包当前配置、数据库快照、API 健康信息、服务日志和环境信息，输出到项目目录下的 [exports](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/exports)。

常用命令：

```bash
python3 scripts/export_runtime_diagnostics.py --hours 12
```

脚本执行完成后会生成两份产物：

- `exports/runtime_diagnostics_YYYYMMDD_HHMMSS/`
- `exports/runtime_diagnostics_YYYYMMDD_HHMMSS.tar.gz`

如果在 Ubuntu + systemd 环境执行，会额外采集：

- `cross-market-monitor` 的 `systemctl` 输出
- 对应 `journalctl` 日志
- `nginx` 与 `systemd-resolved` 的状态和最近日志

## 正式上线执行命令

以下命令按 Ubuntu 服务器首次上线顺序执行，默认项目目录为 `/srv/cross_market_arbitrage`。要求 `Python 3.10+`，推荐 `Ubuntu 22.04+`：

1. 安装系统依赖

```bash
sudo apt-get update
sudo apt-get install -y git curl python3 python3-venv python3-pip nginx
```

2. 准备代码和本地配置

```bash
cd /srv
git clone https://github.com/mintaprapy/cross_market_arbitrage.git
cd /srv/cross_market_arbitrage
cp config/monitor.example.yaml config/monitor.yaml
```

3. 创建虚拟环境并安装依赖

```bash
cd /srv/cross_market_arbitrage
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[tqsdk,parquet]"
```

如果不需要 `Parquet` 导出，可以改成：

```bash
python -m pip install -e ".[tqsdk]"
```

`install-ubuntu.sh` 会复用这套虚拟环境；如果 `.venv` 不存在，它也会自动创建。

4. 检查本地配置

```bash
editor config/monitor.yaml
```

至少确认这些字段：

- `app.sqlite_path`
- `app.export_dir`
- `app.domestic_trading_calendar_path`
- `notifiers`
- `sources.tqsdk_domestic.params.auth_user`
- `sources.tqsdk_domestic.params.auth_password`
- `sources.tqsdk_domestic.params.md_url`（如需要）

5. 执行安装脚本

```bash
sudo ./deploy/bin/install-ubuntu.sh
```

默认会使用当前执行 `sudo` 的用户作为 `APP_USER` / `APP_GROUP`。如果你要改成其他账号，可以这样执行：

```bash
sudo APP_USER=ubuntu APP_GROUP=ubuntu ./deploy/bin/install-ubuntu.sh
```

6. 执行上线后自检

```bash
sudo ./deploy/bin/post-deploy-check.sh
```

7. 查看服务状态和日志

```bash
sudo systemctl status cross-market-monitor --no-pager
sudo journalctl -u cross-market-monitor -n 100 --no-pager
```

8. 验证页面和接口

```bash
curl -fsS http://127.0.0.1:6080/api/health | python3 -m json.tool
curl -fsS http://127.0.0.1:6080/api/snapshot | python3 -m json.tool | head
```

如果服务器装了 `nginx`，`install-ubuntu.sh` 会一并渲染并加载站点配置；如需正式域名，部署前把 `SERVER_NAME` 环境变量传给脚本，或修改 [cross-market-monitor.conf](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/deploy/nginx/cross-market-monitor.conf)。

## 当前覆盖

- 黄金：`AU_XAU`
- 白银：`AG_XAG_GROSS`、`AG_XAG_NET`
- 铜：`CU_COPPER_GROSS`、`CU_COPPER_NET`、`BC_COPPER`
- 原油：`SC_CL`

默认数据源与角色分工：

- 国内主链路：Sina Futures 主连
- 国内影子/历史：TqSdk 主连
- 海外：Binance Futures、OKX Swap
- 备用源：Hyperliquid、CME NYMEX 参考源
- 汇率：Sina FX `fx_susdcny`，Frankfurter 备用

## 已实现能力

- 统一换算公式：`USD/oz`、`USD/lb`、`USD/bbl`
- 理论价差
- `spread`、`spread_pct`、`rolling_mean`、`rolling_std`、`zscore`、`delta_spread`
- FX 跳变检测与策略信号暂停
- 数据质量告警：断流、时间戳过旧、时间偏斜、非正价格
- 每个交易对可单独配置价差上限通知和价差下限通知
- 多候选海外路由回退和源健康统计
- TqSdk 启动回补和影子主连采集
- Binance / OKX 同源海外历史回补
- SQLite 落库：原始行情、汇率、快照、告警、通知投递
- CSV / Parquet 导出
- 回放分析：对冲回归 beta、波动率目标缩放、手续费/滑点/资金费率成本模型
- CLI、FastAPI API、Web Dashboard
- 通知推送：`console`、`webhook`、`feishu`、`wecom`、`telegram`

## 项目结构

```text
cross_market_monitor/
├── application/
│   ├── monitor/     # 轮询、FX、快照、告警、运行时
│   ├── history/     # 历史查询、回补、shadow
│   ├── control/     # 路由偏好
│   └── query/       # 查询与回放
├── domain/          # 领域模型、换算公式、滚动统计
├── infrastructure/
│   ├── marketdata/  # 数据源适配器
│   └── storage/     # SQLite writer/query/state
├── interfaces/
│   ├── api/         # FastAPI 路由
│   └── dashboard/   # HTML / CSS / JS
├── scripts/         # 运维辅助脚本
└── main.py          # CLI / worker / api 入口
```

## 配置

仓库跟踪的是公开示例配置：

- [config/monitor.example.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/monitor.example.yaml)
- [config/domestic_trading_calendar.cn_futures.2026.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/domestic_trading_calendar.cn_futures.2026.yaml)

本地实际运行使用：

- `config/monitor.yaml`

首次拉代码后先复制一份本地配置：

```bash
cp config/monitor.example.yaml config/monitor.yaml
```

`config/monitor.yaml` 已加入 `.gitignore`，用于保留本地凭证、通知地址和运行参数，不会上传到 GitHub。

配置里已经包含：

- 主国内链路配置和海外候选 `overseas_candidates`
- 国内周末/节假日休市日历，通过 `domestic_trading_calendar_path` 引用
- `domestic_product_code`，用于映射 TqSdk 主连代码
- FX 跳变阈值与暂停开关
- 每个交易对单独的通知阈值：
  - `spread_alert_above`
  - `spread_alert_below`
- 每组成本模型参数
- TqSdk 影子采集与启动回补参数
- 通知器模板与过滤器：
  - `categories`
  - `group_names`

## 运行

单次轮询：

```bash
python3 -m cross_market_monitor.main run-once
```

连续控制台输出：

```bash
python3 -m cross_market_monitor.main console --cycles 5
```

启动 Dashboard：

```bash
python3 -m cross_market_monitor.main serve
```

只启动采集 worker：

```bash
python3 -m cross_market_monitor.main run-worker
```

只启动 API / Dashboard，不跑轮询 worker：

```bash
python3 -m cross_market_monitor.main run-api
```

如果要启用 `TqSdk` 启动回补和历史回补，直接在 `config/monitor.yaml` 里填写：

```yaml
sources:
  tqsdk_domestic:
    params:
      auth_user: your_account
      auth_password: your_password
      md_url: wss://free-api.shinnytech.com/t/nfmd/front/mobile
```

不填写认证时，系统会自动跳过需要 `TqSdk` 的链路，不影响主监控。

## 告警阈值与通知

每个交易对都可以单独配置这两个可选阈值：

- `spread_alert_above`
  - 当 `理论价差 >= 该值` 时触发
- `spread_alert_below`
  - 当 `理论价差 <= 该值` 时触发

如果字段不填写，或值为 `null`，表示该条件不生效。

配置示例：

```yaml
pairs:
  - group_name: AU_XAU
    ...
    thresholds:
      spread_pct_abs: 0.02
      zscore_abs: 2.5
      spread_alert_above: 25
      spread_alert_below: -25
      stale_seconds: 180
      max_skew_seconds: 180
      alert_cooldown_seconds: 300
```

飞书和 Telegram 现在支持按告警类别和交易对过滤：

```yaml
notifiers:
  - name: feishu_alerts
    kind: feishu
    enabled: true
    min_severity: warning
    categories: [spread_level]
    group_names: [AU_XAU, SC_CL]
    url: https://open.feishu.cn/open-apis/bot/v2/hook/replace-me

  - name: telegram_alerts
    kind: telegram
    enabled: true
    min_severity: warning
    categories: [spread_level]
    group_names: [AU_XAU, SC_CL]
    bot_token: replace-me
    chat_id: replace-me
```

说明：

- `categories` 不填时，表示不过滤类别
- `group_names` 不填时，表示不过滤交易对
- 你当前这个需求，推荐只给飞书 / Telegram 配 `categories: [spread_level]`

默认地址：

- [http://127.0.0.1:6080](http://127.0.0.1:6080)
- [http://127.0.0.1:6080/api/snapshot](http://127.0.0.1:6080/api/snapshot)
- [http://127.0.0.1:6080/api/health](http://127.0.0.1:6080/api/health)
- [http://127.0.0.1:6080/api/overseas-routes?group_name=AU_XAU](http://127.0.0.1:6080/api/overseas-routes?group_name=AU_XAU)

## Ubuntu systemd

仓库里已经提供了一套 funding 风格的单服务部署模板：

- [systemd/cross-market-monitor.service](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/systemd/cross-market-monitor.service)
- [deploy/systemd/cross-market-monitor.service](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/deploy/systemd/cross-market-monitor.service)
- [deploy/nginx/cross-market-monitor.conf](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/deploy/nginx/cross-market-monitor.conf)
- [deploy/bin/install-ubuntu.sh](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/deploy/bin/install-ubuntu.sh)
- [deploy/bin/post-deploy-check.sh](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/deploy/bin/post-deploy-check.sh)

默认约定：

- 项目目录：`/srv/cross_market_arbitrage`
- 虚拟环境：`/srv/cross_market_arbitrage/.venv`
- 真实配置：`/srv/cross_market_arbitrage/config/monitor.yaml`
- 监听：`127.0.0.1:6080`

推荐安装流程：

```bash
cd /srv/cross_market_arbitrage
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[tqsdk,parquet]"
sudo ./deploy/bin/install-ubuntu.sh
sudo ./deploy/bin/post-deploy-check.sh
```

如果不需要 `TqSdk` 影子链路和 `Parquet` 导出，也可以把安装命令改成：

```bash
python -m pip install -e .
```

`install-ubuntu.sh` 会自动按本地 [config/monitor.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/monitor.yaml) 渲染 systemd unit 里的配置路径、监听地址和 SQLite / export 目录。

查看日志：

```bash
sudo journalctl -u cross-market-monitor -f
```

## Ubuntu 上线前检查清单

按 `Ubuntu + systemd + nginx` 部署时，建议在切流前逐项确认：

1. 目录与权限
```bash
sudo mkdir -p /srv/cross_market_arbitrage
sudo chown -R ubuntu:ubuntu /srv/cross_market_arbitrage
```

2. Python 环境与依赖
```bash
cd /srv/cross_market_arbitrage
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[tqsdk,parquet]"
python -m unittest discover -s tests -v
```

如果你不需要 `TqSdk` / `Parquet`，这里也可以改成：

```bash
python -m pip install -e .
```

3. 配置文件
- 先执行：
```bash
cp config/monitor.example.yaml config/monitor.yaml
```
- 再检查本地 [config/monitor.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/monitor.yaml) 中的：
  - `sqlite_path`
  - `export_dir`
  - `domestic_trading_calendar_path`
  - `notifiers`
  - 每个交易对的 `spread_alert_above / spread_alert_below`
- 如果线上启用 `TqSdk`，确认 `config/monitor.yaml` 已填：
  - `sources.tqsdk_domestic.params.auth_user`
  - `sources.tqsdk_domestic.params.auth_password`
  - `sources.tqsdk_domestic.params.md_url`

4. systemd
```bash
sudo ./deploy/bin/install-ubuntu.sh
sudo systemctl status cross-market-monitor --no-pager
```

5. 本机健康检查
```bash
curl -sf http://127.0.0.1:6080/api/health
curl -sf http://127.0.0.1:6080/api/snapshot
curl -sf "http://127.0.0.1:6080/api/card?group_name=AU_XAU&range_key=24h"
```

6. Nginx
```bash
sudo cp deploy/nginx/cross-market-monitor.conf /etc/nginx/sites-available/cross-market-monitor
sudo ln -sf /etc/nginx/sites-available/cross-market-monitor /etc/nginx/sites-enabled/cross-market-monitor
sudo nginx -t
sudo systemctl reload nginx
curl -I http://your.domain.com/
```

7. 日志与告警
```bash
sudo journalctl -u cross-market-monitor -n 100 --no-pager
```
- 确认没有持续报错、没有反复重启
- 如已启用飞书 / Telegram，建议先手动把某个交易对阈值设得接近当前价差，验证一次通知链路

8. 防火墙与暴露面
- `cross-market-monitor` 只监听 `127.0.0.1:6080`
- 对外只开放 `80/443`
- 不建议直接暴露 `6080`

9. 上线后首日观察
- 观察开盘前后：
  - 国内主链路是否稳定
  - 海外源是否有频繁回退
  - 汇率是否正常更新
  - 告警是否出现误触发
- 常用命令：
```bash
sudo journalctl -u cross-market-monitor -f
curl -sf http://127.0.0.1:6080/api/health
```

## Nginx 反向代理

如果 `systemd` 服务保持监听本机：

```text
127.0.0.1:6080
```

可以在 Nginx 里这样代理：

```nginx
server {
    listen 80;
    server_name your.domain.com;

    location / {
        proxy_pass http://127.0.0.1:6080;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_connect_timeout 10s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
}
```

常见安装步骤：

```bash
sudo cp deploy/nginx/cross-market-monitor.conf /etc/nginx/sites-available/cross-market-monitor
sudo ln -sf /etc/nginx/sites-available/cross-market-monitor /etc/nginx/sites-enabled/cross-market-monitor
sudo nginx -t
sudo systemctl reload nginx
```

如果你已经有 HTTPS 证书，只需要在现有 `443` server block 里把 `location /` 指向：

```text
http://127.0.0.1:6080
```

即可。

## /srv 和 /opt 的区别

这两个目录都能用，区别主要是约定，不是功能。

- `/srv`
  - 更偏向“这台机器对外提供的服务数据”
  - 常见于网站、应用服务、业务代码和站点目录
  - 如果你另一套项目已经放在 `/srv`，继续统一放 `/srv` 也完全合理

- `/opt`
  - 更偏向“额外安装的独立软件包或第三方应用”
  - 适合独立部署、不和系统默认目录混用的应用
  - 我前面给这个项目用 `/opt`，是按“独立应用”这个思路给的默认模板

实际选择建议：

- 如果你的服务器已经形成统一习惯，比如所有业务项目都放 `/srv`，那这个项目也放 `/srv` 更好
- 如果你习惯把独立应用都放 `/opt`，继续放 `/opt` 也没问题

对这个项目来说，关键不是必须用哪个目录，而是保持一致，并同步修改：

- `systemd` 里的 `WorkingDirectory`
- `systemd` 里的 `ExecStart`
- 如果用了脚本或备份路径，也一起改

例如，如果你决定放在：

```text
/srv/cross_market_arbitrage
```

那么 `systemd` 里对应改成：

```ini
WorkingDirectory=/srv/cross_market_arbitrage
ExecStart=/srv/cross_market_arbitrage/.venv/bin/cross-market-monitor --config /srv/cross_market_arbitrage/config/monitor.yaml serve --host 127.0.0.1 --port 6080
```

## 导出与回放

导出 CSV：

```bash
python3 -m cross_market_monitor.main export-csv --dataset snapshots --group-name AU_XAU
```

导出 Parquet：

```bash
python3 -m cross_market_monitor.main export-parquet --dataset snapshots --group-name AU_XAU
```

导出运行诊断：

```bash
python3 scripts/export_runtime_diagnostics.py --hours 12
```

回放摘要：

```bash
python3 -m cross_market_monitor.main replay --group-name AU_XAU --limit 500
```

JSON 报告：

```bash
python3 -m cross_market_monitor.main replay --group-name AU_XAU --limit 500 --format json
```

海外历史回补：

```bash
python3 -m cross_market_monitor.main backfill-overseas --group-name AU_XAU --interval 60m --range-key 30d
```

## 存储

默认 SQLite 路径：

- [data/monitor.db](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/data/monitor.db)

核心表：

- `raw_quotes`
- `fx_rates`
- `spread_snapshots`
- `alert_events`
- `notification_deliveries`

所有表均同时保存 `ts`、`ts_utc`、`ts_local`。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 说明

- `AG`、`CU` 同时保留含税和去税口径。
- `BC_COPPER` 单独成组，便于和 `CU` 对比。
- `SC_CL` 默认按扩散/收敛监控处理，不假设零均值强收敛。
- 国内主链路固定为主连，页面不再切换到具体合约。
- 启动时会尝试用 TqSdk 回补一段国内主连影子历史，并在后台持续采集影子主连数据；这部分数据不参与主价差。
- 页面上可以直接切换海外比较源，例如 `Binance / OKX / CME参考`；待命路由在手动锁定后会被优先尝试。
- 图表历史读取时，如果当前选中的 `Binance / OKX` 海外数据在本地库里不够，会自动按当前海外源做一次同源历史回补并缓存到本地。
- 国内市场休市或午间停盘时，快照可能被判为 `stale`，这是时间对齐保护而不是程序错误。
