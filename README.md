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

以下命令按 Ubuntu 服务器首次上线顺序执行，默认项目目录为 `/srv/cross_market_arbitrage`。要求 `Python 3.10+`，推荐 `Ubuntu 22.04+`。

1. 安装系统依赖

```bash
sudo apt-get update
sudo apt-get install -y git curl python3 python3-venv python3-pip nginx
```

2. 拉代码并准备本地配置

```bash
cd /srv
git clone https://github.com/mintaprapy/cross_market_arbitrage.git
cd /srv/cross_market_arbitrage
cp config/local.example.yaml config/local.yaml
```

3. 创建虚拟环境并安装依赖

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[tqsdk,parquet]"
```

如果不需要 `Parquet` 导出，可以改成：

```bash
python -m pip install -e ".[tqsdk]"
```

4. 检查配置

至少确认这些字段：

- `config/app.yaml` 里的 `sqlite_path / export_dir / domestic_trading_calendar_path`
- `config/local.yaml` 里的 `sources.tqsdk_domestic.params.auth_user`
- `config/local.yaml` 里的 `sources.tqsdk_domestic.params.auth_password`
- `config/local.yaml` 里的 `sources.tqsdk_domestic.params.md_url`
- `config/local.yaml` 里的通知渠道配置

5. 安装 systemd 服务

```bash
sudo cp systemd/cross-market-monitor.service /etc/systemd/system/cross-market-monitor.service
sudo systemctl daemon-reload
sudo systemctl enable --now cross-market-monitor
```

如果服务器运行用户不是 `ubuntu`，先修改 [cross-market-monitor.service](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/systemd/cross-market-monitor.service) 里的：

- `User`
- `Group`
- `WorkingDirectory`
- `ExecStart`
- `ReadWritePaths`

6. 验证服务

```bash
sudo systemctl status cross-market-monitor --no-pager
sudo journalctl -u cross-market-monitor -n 100 --no-pager
curl -fsS http://localhost:6080/api/health | python3 -m json.tool
curl -fsS http://localhost:6080/api/snapshot | python3 -m json.tool | head
```

7. 如需接 Nginx，再手动写站点配置并 reload

```bash
sudo editor /etc/nginx/sites-available/cross-market-monitor
sudo ln -sf /etc/nginx/sites-available/cross-market-monitor /etc/nginx/sites-enabled/cross-market-monitor
sudo nginx -t
sudo systemctl reload nginx
```

## 当前覆盖

- 黄金：`AU_XAU`
- 白银：`AG_XAG_GROSS`、`AG_XAG_NET`
- 铜：`CU_COPPER_GROSS`、`CU_COPPER_NET`、`BC_COPPER`
- 原油：`SC_CL`

默认数据源与角色分工：

- 服务器默认国内主链路：TqSdk 主连
- 本地 `start_macmini.sh` / `start_macmini_local.sh`：Sina Futures 主连
- 国内历史：TqSdk 主连
- 海外：Binance Futures、OKX Swap
- 备用源：Hyperliquid、CME NYMEX 参考源
- 服务器默认汇率：Frankfurter
- 本地 `macmini` 脚本默认汇率：Sina FX `fx_susdcny`，Frankfurter 备用

## 已实现能力

- 统一换算公式：`USD/oz`、`USD/lb`、`USD/bbl`
- 理论价差
- `spread`、`spread_pct`、`rolling_mean`、`rolling_std`、`zscore`、`delta_spread`
- FX 跳变检测与策略信号暂停
- 数据质量告警：断流、时间戳过旧、时间偏斜、非正价格
- 每个交易对可单独配置价差上限通知和价差下限通知
- 多候选海外路由回退和源健康统计
- TqSdk 国内历史回补
- Binance / OKX 同源海外历史回补
- SQLite 落库：原始行情、汇率、快照、告警、通知投递
- CSV / Parquet 导出
- 回放分析：价差百分比均值、标准差、中位数、当前分位数、日波动率
- CLI、FastAPI API、Web Dashboard
- 通知推送：`console`、`webhook`、`feishu`、`wecom`、`telegram`
- Dashboard 内置 `Z-Score` 右侧概率展示和常见分布参考弹层

## 项目结构

```text
cross_market_monitor/
├── application/
│   ├── monitor/     # 轮询、FX、快照、告警、运行时
│   ├── history/     # 历史查询、回补、可选 shadow
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

仓库跟踪的是这些真实配置文件：

- [config/monitor.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/monitor.yaml)
- [config/app.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/app.yaml)
- [config/sources.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/sources.yaml)
- [config/pairs.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/pairs.yaml)
- [config/alert_thresholds.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/alert_thresholds.yaml)
- [config/notifiers.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/notifiers.yaml)
- [config/domestic_trading_calendar.cn_futures.2026.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/domestic_trading_calendar.cn_futures.2026.yaml)

本地敏感覆盖保留 example：

- [config/local.example.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/local.example.yaml)

本地实际运行使用：

- `config/monitor.yaml`
- `config/app.yaml`
- `config/sources.yaml`
- `config/pairs.yaml`
- `config/pair_enabled.yaml`
- `config/alert_thresholds.yaml`
- `config/notifiers.yaml`
- `config/local.yaml`

首次拉代码后只需要按需复制本地覆盖：

```bash
cp config/local.example.yaml config/local.yaml
```

`config/local.yaml` 已加入 `.gitignore`，用于保留本地凭证、通知地址和运行参数，不会上传到 GitHub。

配置现在拆成 7 份：

- `config/monitor.yaml`
  只作为入口文件，列出 `imports`
- `config/app.yaml`
  负责应用级参数、SQLite、导出目录、轮询周期、交易日历
- `config/sources.yaml`
  负责各数据源和非敏感连接参数
- `config/pairs.yaml`
  负责交易对、路由、成本模型和运行阈值
- `config/pair_enabled.yaml`
  只负责每个交易对是否在网页/API 中展示，不影响后台采集
- `config/alert_thresholds.yaml`
  只负责告警阈值，空值表示关闭对应通知
- `config/notifiers.yaml`
  负责非敏感默认通知配置
- `config/local.yaml`
  负责本地敏感凭证和通知渠道覆盖，比如 `TqSdk` 账号密码、飞书/Telegram/Webhook 连接信息

控制网页/API 是否展示某个交易对，直接改这一份：

```yaml
pair_enabled:
  AU_XAU: true
  AL_ALUMINIUM: false
  B_SOYBEAN: false
```

改完后重启对应服务即可生效。后台采集仍然会继续保留这些交易对的数据。

## 运行

单次轮询：

```bash
python3 -m cross_market_monitor.main run-once
```

如需在单次轮询前同时执行启动历史回补：

```bash
python3 -m cross_market_monitor.main run-once --with-startup-history
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

本地 `macmini` 脚本：

```bash
./scripts/start_macmini.sh
./scripts/start_macmini_local.sh
./scripts/stop_macmini.sh
```

说明：

- `start_macmini.sh`
  - 先 `git pull` 并更新依赖，再用 `config/monitor.macmini.yaml` 启动本地网页
- `start_macmini_local.sh`
  - 不做 `git pull`，直接用当前本地代码启动
- `stop_macmini.sh`
  - 停止本地网页进程并释放 `6080` 端口
- 这三个脚本默认都走本地 `macmini` 配置，也就是 `Sina` 国内实时和 `Sina FX`

如果要启用 `TqSdk` 启动回补和历史回补，直接在 `config/local.yaml` 里填写：

```yaml
sources:
  tqsdk_domestic:
    params:
      auth_user: your_account
      auth_password: your_password
      md_url: wss://free-api.shinnytech.com/t/nfmd/front/mobile
```

不填写认证时，系统会自动跳过需要 `TqSdk` 的链路，不影响主监控。

如果要在服务器上单独测试 `TqSdk` 夜盘实时连通性，可以直接运行：

```bash
python3 -m cross_market_monitor.tools.tqsdk_connectivity_check \
  --config config/monitor.yaml \
  --duration-sec 300 \
  --interval-sec 5
```

这个脚本会：

- 自动读取 `config/local.yaml` 里的 `TqSdk` 认证信息
- 对 `AU / AG / CU / BC / SC` 主连做连接与实时取数测试
- 输出 JSON 报告到 `data/tqsdk_connectivity/`

常用参数：

- `--products au ag sc`
  - 只测指定品种
- `--connect-attempts 5`
  - 增加重连次数
- `--connect-timeout-sec 30`
  - 放宽单次连接超时

如果要在服务器上持续运行 `TqSdk` 实时连通性测试，可以直接后台启动：

```bash
cd /srv/cross_market_arbitrage && mkdir -p /tmp/cross_market_monitor && log_file="/tmp/cross_market_monitor/tqsdk_connectivity_loop_$(TZ=Asia/Hong_Kong date +%Y%m%d_%H%M%S).log" && nohup bash -lc 'cd /srv/cross_market_arbitrage || exit 1; source .venv/bin/activate || exit 1; while true; do echo "=== $(TZ=Asia/Hong_Kong date "+%F %T %Z") start ==="; python -m cross_market_monitor.tools.tqsdk_connectivity_check --config config/monitor.yaml --duration-sec 3600 --interval-sec 5 --connect-attempts 5 --connect-timeout-sec 30; rc=$?; echo "=== $(TZ=Asia/Hong_Kong date "+%F %T %Z") end rc=$rc ==="; sleep 10; done' > "$log_file" 2>&1 < /dev/null & echo "LOG=$log_file"
```

常用查看命令：

```bash
pgrep -af tqsdk_connectivity_check
tail -f /tmp/cross_market_monitor/tqsdk_connectivity_loop_*.log
latest=$(ls -t data/tqsdk_connectivity/tqsdk_connectivity_*.json | head -1) && echo "$latest" && cat "$latest"
```

如果要导出线上 `TqSdk` 最近一周是否运行稳定，可以直接运行：

```bash
python3 -m cross_market_monitor.tools.tqsdk_weekly_report --days 7
```

这个脚本会：

- 读取 `data/tqsdk_connectivity/` 下最近 7 天的 `tqsdk_connectivity_*.json`
- 生成一个带 `summary.json`、`REPORT.md` 和原始 JSON 副本的导出目录
- 同时生成一个 `.tar.gz` 归档，方便从服务器下载
- 在控制台直接打印 `is_stable`、`connect_success_rate` 和导出文件路径
- 默认排除 `Asia/Hong_Kong` 时区下 `19:00-19:30` 的官方维护窗口
- 默认不把 `refresh_latency_median_ms` 当成稳定性失败条件；如果需要启用，可显式传：

```bash
python3 -m cross_market_monitor.tools.tqsdk_weekly_report \
  --days 7 \
  --max-refresh-latency-median-ms 1000
```

如果要持续监测 SQLite 数据库体积和增长速度，可以直接运行：

```bash
cd /srv/cross_market_arbitrage
bash scripts/watch_db_growth.sh start
```

说明：

- 在 Ubuntu 服务器上建议显式用 `bash` 启动
  - 脚本默认 shebang 是 `zsh`，部分服务器没有 `/bin/zsh`
- 默认每 `900` 秒采样一次，也就是 `15` 分钟
- 默认配置文件使用 `config/monitor.yaml`

如需调整采样间隔，例如改成每 `300` 秒一次：

```bash
cd /srv/cross_market_arbitrage
DB_GROWTH_INTERVAL_SEC=300 bash scripts/watch_db_growth.sh start
```

常用命令：

```bash
bash scripts/watch_db_growth.sh status
bash scripts/watch_db_growth.sh stop
tail -f logs/db_growth/watch.log
```

输出文件默认在：

- `logs/db_growth/watch.log`
- `logs/db_growth/samples.csv`
- `logs/db_growth/size_latest.txt`
- `logs/db_growth/watch.pid`

## 告警阈值与通知

告警阈值统一放在 `config/alert_thresholds.yaml`，每个交易组都可以单独配置：

- `spread_alert_above`
  - 当 `理论价差 >= 该值` 时触发
- `spread_alert_below`
  - 当 `理论价差 <= 该值` 时触发
- `spread_pct_above`
  - 当 `价差百分比 >= 该值` 时触发
- `spread_pct_below`
  - 当 `价差百分比 <= 该值` 时触发
- `zscore_above`
  - 当 `zscore >= 该值` 时触发
- `zscore_below`
  - 当 `zscore <= 该值` 时触发

如果字段不填写，或值为 `null`，表示该条件不生效。百分比推荐直接写成 `2%` 或 `0.02`，也兼容 `2 %` 这种带空格写法。

配置示例：

```yaml
alert_thresholds:
  AU_XAU:
    spread_alert_above: 25
    spread_alert_below: -25
    spread_pct_above: 2%
    spread_pct_below: -1.5%
    zscore_above: 2.5
    zscore_below:
```

通知器现在只按 `min_severity` 和 `group_names` 过滤。只要某个阈值触发了对应告警，就会发送，不需要再在 `notifiers` 里单独配置 `categories`。

```yaml
notifiers:
  - name: feishu_alerts
    kind: feishu
    enabled: true
    min_severity: warning
    group_names: [AU_XAU, SC_CL]
    url: https://open.feishu.cn/open-apis/bot/v2/hook/replace-me

  - name: telegram_alerts
    kind: telegram
    enabled: true
    min_severity: warning
    group_names: [AU_XAU, SC_CL]
    bot_token: replace-me
    chat_id: replace-me
```

说明：

- `group_names` 不填时，表示不过滤交易对
- 如果一个组在 `config/alert_thresholds.yaml` 里填了阈值，命中后就会推送到所有满足 `min_severity / group_names` 的通知器

### Telegram 查询命令

如果启用了 `telegram` 通知器，系统会同时开启 Telegram 轮询命令，只接受该通知器配置里的 `chat_id`。

可用命令：

- `/help`
- `/pairs`
- `/quote <交易对>`
- `/status <交易对>`

示例：

```text
/pairs
/quote AU_XAU
/quote CU_COPPER
/quote CU_COPPER除税
```

`/quote` 返回的核心字段包括：

- 当前状态
- 实时价差百分比
- 理论价差
- `Z-Score`
- 国内价格
- 国内换算价
- 海外最新价
- 汇率
- 国内 / 海外 / 汇率时效
- 当前香港/北京时间

默认地址：

- `http://<server-ip>:6080/`
- `http://<server-ip>:6080/api/snapshot`
- `http://<server-ip>:6080/api/health`
- `http://<server-ip>:6080/api/overseas-routes?group_name=AU_XAU`

## Ubuntu systemd

仓库里保留的是 funding 风格的顶层单服务模板：

- [systemd/cross-market-monitor.service](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/systemd/cross-market-monitor.service)

默认约定：

- 项目目录：`/srv/cross_market_arbitrage`
- 虚拟环境：`/srv/cross_market_arbitrage/.venv`
- 真实配置：`/srv/cross_market_arbitrage/config/monitor.yaml`
- 监听：`0.0.0.0:6080`

推荐手动安装流程：

```bash
cd /srv/cross_market_arbitrage
cp config/local.example.yaml config/local.yaml
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[tqsdk,parquet]"
sudo cp systemd/cross-market-monitor.service /etc/systemd/system/cross-market-monitor.service
sudo systemctl daemon-reload
sudo systemctl enable --now cross-market-monitor
```

如果不需要 `TqSdk` 历史回补和 `Parquet` 导出，也可以把安装命令改成：

```bash
python -m pip install -e .
```

部署前需要按实际服务器路径手动检查 [cross-market-monitor.service](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/systemd/cross-market-monitor.service) 里的：

- `User`
- `Group`
- `WorkingDirectory`
- `ExecStart`
- `ReadWritePaths`

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
cp config/local.example.yaml config/local.yaml
```
- 再检查本地配置：
  - [config/app.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/app.yaml)
  - [config/sources.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/sources.yaml)
  - [config/pairs.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/pairs.yaml)
  - [config/alert_thresholds.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/alert_thresholds.yaml)
  - [config/notifiers.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/notifiers.yaml)
  - [config/local.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/local.yaml)

4. systemd
```bash
sudo cp systemd/cross-market-monitor.service /etc/systemd/system/cross-market-monitor.service
sudo systemctl daemon-reload
sudo systemctl enable --now cross-market-monitor
sudo systemctl status cross-market-monitor --no-pager
```

5. 本机健康检查
```bash
curl -sf http://localhost:6080/api/health
curl -sf http://localhost:6080/api/snapshot
curl -sf "http://localhost:6080/api/card?group_name=AU_XAU&range_key=24h"
```

6. Nginx
```bash
sudo editor /etc/nginx/sites-available/cross-market-monitor
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
- `cross-market-monitor` 默认监听 `0.0.0.0:6080`
- 如果前面放 `Nginx`，建议公网只开放 `80/443`
- `6080` 至少要通过防火墙或安全组限制来源

9. 上线后首日观察
- 观察开盘前后：
  - 国内主链路是否稳定
  - 海外源是否有频繁回退
  - 汇率是否正常更新
  - 告警是否出现误触发
- 常用命令：
```bash
sudo journalctl -u cross-market-monitor -f
curl -sf http://localhost:6080/api/health
```

## Nginx 反向代理

如果 `systemd` 服务默认监听：

```text
0.0.0.0:6080
```

Nginx 在同机回源时建议显式使用 IPv4 回环地址，避免 `localhost` 被解析到 `::1`：

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
sudo editor /etc/nginx/sites-available/cross-market-monitor
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
ExecStart=/srv/cross_market_arbitrage/.venv/bin/cross-market-monitor --config /srv/cross_market_arbitrage/config/monitor.yaml serve --host 0.0.0.0 --port 6080
```

## 导出与回放

导出 CSV：

```bash
python3 -m cross_market_monitor.main export-csv --dataset snapshots --group-name AU_XAU
```

可导出的 `dataset`：

- `snapshots`
- `alerts`
- `raw_quotes`
- `fx_rates`
- `notification_deliveries`

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

国内历史回补：

```bash
python3 -m cross_market_monitor.main backfill-domestic --group-name AU_XAU --interval 60m --range-key 30d
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
- 默认会用 TqSdk 做国内历史回补；如需影子链路，需要显式开启对应配置。
- 页面上可以直接切换海外比较源，例如 `Binance / OKX / CME参考`；待命路由在手动锁定后会被优先尝试。
- 图表历史读取时，如果当前选中的 `Binance / OKX` 海外数据在本地库里不够，会自动按当前海外源做一次同源历史回补并缓存到本地。
- 国内市场休市或午间停盘时，快照可能被判为 `stale`，这是时间对齐保护而不是程序错误。
