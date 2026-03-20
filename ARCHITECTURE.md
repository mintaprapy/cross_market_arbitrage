# Architecture Overview

## 总体架构

系统采用“模块化单体 + Ports and Adapters”：

- `domain`
  负责领域模型、标准化公式、滚动统计和回放输出模型
- `application`
  负责采集编排、FX 风险暂停、告警评估、回放研究
- `infrastructure`
  负责交易所适配器、FX 源、通知器、SQLite 和导出
- `interfaces`
  负责 CLI、FastAPI API 和 Web Dashboard

当前默认运行形态为“模块化单体 + 双进程”：

- `monitor-worker`
  负责采集、计算、告警、历史回补和写库
- `api-dashboard`
  负责 FastAPI、Dashboard 和只读查询

两者共享同一个 SQLite 库，保持部署简单，同时把查询流量和采集写入解耦。

## 核心数据流

1. 拉取 `USD/CNY`
2. 并发请求每个监控组的国内/海外候选路由
3. 记录全部尝试结果和选中的源
4. 将国内价格换算成统一口径
5. 计算：
   - 理论价差
   - `rolling_mean / rolling_std / zscore / delta_spread`
6. 执行数据质量校验：
   - 断流
   - 非正价格
   - 时间戳过旧
   - 国内/海外/FX 时间偏斜
7. 检测 FX 跳变，必要时把信号状态切到 `paused`
8. 持久化原始行情、汇率、快照、告警、通知投递
9. 启动时执行 TqSdk 主连影子历史回补，并启动后台影子采集
10. 显式触发国内/海外历史回补任务
11. 通过 CLI / API / Dashboard 对外展示

## 分层边界

### `marketdata`

- 国内主链路：Sina Futures 主连
- 国内影子/历史：TqSdk 主连
- 海外：Binance Futures、OKX Swap
- 备用：Hyperliquid、CME NYMEX 参考源
- FX：Frankfurter

每个适配器只负责把外部数据转换成统一的 `MarketQuote / FXQuote`。

### `normalizer`

统一处理：

- 单位换算
- 币种换算
- 税口径
- 公式版本

当前支持：

- 黄金 `CNY/g -> USD/oz`
- 白银 `CNY/kg -> USD/oz`
- 铜 `CNY/ton -> USD/lb`
- 原油 `CNY/bbl -> USD/bbl`

### `monitor`

实时链路输出：

- `normalized_last / bid / ask`
- `spread / spread_pct`
- `zscore / delta_spread`
- 可成交方向价差
- `status`
- `signal_state`
- `pause_reason`

### `storage`

持久化设计遵循“原始数据不可丢，派生结果可重算”：

- `raw_quotes`
- `fx_rates`
- `spread_snapshots`
- `latest_snapshots`
- `alert_events`
- `notification_deliveries`
- `route_preferences`

所有表保存：

- `ts`
- `ts_utc`
- `ts_local`

实现上已拆为：

- `sqlite_writer`
- `sqlite_query_repo`
- `sqlite_state_repo`

导出能力：

- SQLite
- CSV
- Parquet

### `replay`

只读取历史快照，不参与实时主链路。当前研究项：

- 阈值突破统计
- 收敛 / 扩散比例
- OLS 对冲回归 beta
- 实现波动率估计
- 目标日波动缩放建议
- 手续费 / 滑点 / 资金费率成本估计
- 成本后净边际统计

## 关键设计决策

- `监控层与执行层分离`
  理论价差看 `last`，执行方向价差看 `bid/ask`
- `配置驱动`
  标的、候选路由、税口径、阈值、成本模型全部来自 YAML
- `主链路收敛`
  国内主监控和主价差固定使用主连，避免“显示一个价格、计算另一个价格”
- `影子采集分层`
  TqSdk 只负责启动回补和后台影子入库，不进入主价差与主告警
- `多候选海外路由`
  海外侧保留主备源与手动切换
- `路由偏好持久化`
  国内/海外手动切换落到 `route_preferences`，重启后恢复
- `海外历史同源化`
  Binance / OKX 历史回补跟随当前选中的海外路由，避免图表展示与实时比较口径不一致
- `查询无副作用`
  `/api/history` 和 `/api/card` 只读本地存储，不再在读请求里隐式触发远端回补
- `FX 跳变 gating`
  FX 短时异常时暂停信号，避免伪信号
- `源健康内建`
  记录 success/failure、延迟、最近错误和最近路由
- `可回放`
  主回放依赖快照库；TqSdk 影子历史额外落入原始行情表，便于后续校验主连口径

## 当前已知行为

- 国内市场午间停盘或休市时，快照可能进入 `stale`。这是时间对齐保护逻辑的结果。
- Hyperliquid 和 CME 参考源已作为适配器与候选路由实现，默认配置中保持备用/可选状态。

## 当前目录落点

- `application/monitor`
  运行时、轮询、FX、路由、快照、告警
- `application/history`
  历史查询、回补、TqSdk shadow
- `application/control`
  路由偏好
- `application/query`
  快照、回放、运维查询
- `infrastructure/storage`
  SQLite writer/query/state
- `interfaces/api`
  FastAPI 路由与 app 装配
- `interfaces/dashboard`
  静态 HTML / CSS / JS

## 后续演进

- 增加交易时段感知，减少午间停盘场景下的误报
- 扩展更多 FX 交易型来源
- 如果原始行情保留期继续拉长，再增加 Parquet 归档和清理任务
- 如果单机 SQLite 写入成为瓶颈，再迁移到 Postgres / Timescale 类存储
