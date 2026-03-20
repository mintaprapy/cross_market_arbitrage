# Ubuntu systemd 部署

当前默认推荐 funding 风格的单进程部署：

- `cross-market-monitor`
  负责轮询、计算、告警、写库、FastAPI 和 Dashboard

仓库内高级场景仍保留 worker/api 模板，但不再是默认推荐方式。

默认约定：

- 项目目录：`/srv/cross_market_arbitrage`
- 虚拟环境：`/srv/cross_market_arbitrage/.venv`
- 真实配置文件：`/srv/cross_market_arbitrage/config/monitor.yaml`
- 监听地址：`127.0.0.1:6080`

## 1. 同步代码

```bash
sudo mkdir -p /srv/cross_market_arbitrage
sudo chown -R ubuntu:ubuntu /srv/cross_market_arbitrage
```

把项目放到：

```bash
/srv/cross_market_arbitrage
```

## 2. 安装 Python 环境

```bash
cd /srv/cross_market_arbitrage
cp config/monitor.example.yaml config/monitor.yaml
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[tqsdk,parquet]"
```

如果你不需要 `TqSdk` 和 `Parquet` 导出，也可以只安装基础依赖：

```bash
python -m pip install -e .
```

`config/monitor.yaml` 默认会引用仓库内的 [domestic_trading_calendar.cn_futures.2026.yaml](/Users/m2/Desktop/Codex2026/cross_market_arbitrage/config/domestic_trading_calendar.cn_futures.2026.yaml)；如果跨年部署，记得同步更新这份交易日历文件。

## 3. 配置 TqSdk 认证

如果线上需要 `TqSdk`，直接编辑仓库内 `config/monitor.yaml`：

- `sources.tqsdk_domestic.params.auth_user`
- `sources.tqsdk_domestic.params.auth_password`
- `sources.tqsdk_domestic.params.md_url`

## 4. 安装 systemd 服务

推荐安装单个 unit：

```bash
sudo cp systemd/cross-market-monitor.service /etc/systemd/system/cross-market-monitor.service
sudo systemctl daemon-reload
sudo systemctl enable --now cross-market-monitor
```

如果你希望直接按仓库模板安装，可以执行：

```bash
sudo ./deploy/bin/install-ubuntu.sh
```

这个脚本会自动根据本地 `config/monitor.yaml` 渲染：

- `--config` 路径
- API 的 `--host / --port`
- `ReadWritePaths` 对应的 SQLite / export 目录
- `User / Group / WorkingDirectory`

## 5. 常用命令

查看状态：

```bash
sudo systemctl status cross-market-monitor
```

重启：

```bash
sudo systemctl restart cross-market-monitor
```

停止：

```bash
sudo systemctl stop cross-market-monitor
```

查看日志：

```bash
sudo journalctl -u cross-market-monitor -f
```

部署完成后，可以直接跑：

```bash
sudo ./deploy/bin/post-deploy-check.sh
```

## 6. 修改路径时需要同步改的项

如果你不用安装脚本，而是手动复制 unit，那么当这些项发生变化时需要一起改：

- `/etc/systemd/system/cross-market-monitor.service` 里的 `WorkingDirectory`
- `/etc/systemd/system/cross-market-monitor.service` 里的 `ExecStart`
- `/etc/systemd/system/cross-market-monitor.service` 里的 `ReadWritePaths`
- 如果用了反向代理，也同步改代理目标地址

## 7. 暴露方式

当前 service 默认只监听本机：

```text
127.0.0.1:6080
```

适合放在 Nginx/Caddy 后面。

仓库里已经提供了可直接使用的 Nginx 模板：

```text
deploy/nginx/cross-market-monitor.conf
```

如果你要直接对外监听，可以把 `ExecStart` 里的：

```text
--host 127.0.0.1
```

改成：

```text
--host 0.0.0.0
```
