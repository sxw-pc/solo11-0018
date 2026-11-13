# pagewatch（页变哨兵）

> **合规声明（必读）**
>
> - 本工具仅允许监测**自己拥有或已获书面授权**的站点。
> - 抓取层内置目标白名单，默认且仅允许 `127.0.0.1`、`localhost`、`::1`；白名单之外的地址在建立连接前一律拒绝，并写入审计台账。
> - 不实现、不暗示任何绕过反爬、伪造身份、暴力请求或规避检测的能力。请求间隔有下界，超时、响应体大小和并发数均有上限。
> - 仓库内不包含任何真实公司名、真实目标域名、真实密钥或凭据。示例页面与数据均为虚构内容。
> - 告警只写入本机 SQLite 和 JSONL outbox，**不会外发**。

## 项目定位

pagewatch 是面向“**同一批 URL 随时间变化**”的本地时序变更监测工具：周期快照 → 归一化去噪 → 差异计算 → 变更分级 → 摘要输出。它由 YAML 配置驱动，可完全离线运行，结果可复现、可审计。

**是：** 快照版本链、条件请求、内容寻址留档、噪声掩码、行级/块级 diff、三级分级、本地告警台账、静态 HTML 摘要、保留与压缩。

**不是：** 结构化数据抽取管道；表单填报/写入型 RPA；通用爬虫框架；搜索引擎；报表看板产品；浏览器渲染器；反爬工具。

```
            ┌──────────┐   ┌───────────┐   ┌──────────┐   ┌───────────┐
 watchlist ─▶ fetch ────▶ normalize ───▶ diff ────────▶ classify ──┐
            │ 条件请求  │   │ 去噪/裁剪 │   │ 行级+块级 │   │ 三级分级   │
            └────┬─────┘   └─────┬─────┘   └────┬─────┘   └─────┬─────┘
                 ▼               ▼              ▼               ▼
            原始 gzip 留档   规范化块+哈希   变更指标     事件/告警台账
                 └───────────────┴──────────────┴───────────────┘
                                      ▼
                         静态 digest.html / events.json
```

## 快速上手

要求 Python ≥ 3.10，无需浏览器二进制、无需联网。

```bash
pip install -e .                 # 安装项目与运行依赖，注册 pagewatch 命令
pip install -r requirements.txt  # 运行 + 测试依赖（含 pytest）
```

若未执行可编辑安装，下文所有 `pagewatch <cmd>` 均可写成 `python -m pagewatch <cmd>`。

一条命令跑通离线演示：

```bash
python -m pagewatch.demo
```

它会：删除并重建 `data/demo/`、启动本地 Flask 模拟站点、按 step0→step4 推进确定性剧本、执行监测、生成摘要。关键输出：

```
data/out/digest.html       # 静态变更摘要（主要交付物）
data/out/events.json       # 事件 JSON 导出
data/demo/pagewatch.sqlite # 演示数据库
data/demo/outbox/alerts.jsonl
```

打开 `data/out/digest.html` 即可看到按监视项分组的变更时间线、分级徽标、着色 diff 和折叠噪声区。

运行测试：

```bash
pytest -q
pytest --cov=pagewatch --cov-report=term-missing
```

## 目录结构

```
configs/                 watchlist 与 rules 示例
src/pagewatch/           cli / config / models / runner / clock
  storage/               SQLite schema、快照链、事件与告警台账
  fetch/                 白名单、条件请求客户端、gzip 留档
  normalize/             HTML 清理、选择器裁剪、噪声掩码
  diff/                  行级 diff、块对齐、指标、分级
  alert/                 告警资格规则、JSONL outbox
  schedule/              错峰、退避、并发上限
  report/                digest 渲染与只读状态页
  retention/             保留策略与 compact
mocksite/                确定性 Flask 模拟站点与剧本
tests/                   单元、集成、离线守卫与 fixtures
```

## 配置参考

### watchlist.yaml

```yaml
version: 1
watches:
  - name: announcements
    url: http://127.0.0.1:8765/announcements
    interval_seconds: 300
    tags: [notice, priority]
    include_selectors: ["#notice-area"]
    exclude_selectors: [".analytics"]
    noise_patterns: ['更新时间[：:]\s*[0-9:\- ]+']
    keywords: ["重要", "计费"]
    enabled: true
    rule_overrides:
      alert_cooldown_seconds: 600
```

| 字段 | 必填 | 默认 | 说明 |
|---|---:|---|---|
| `name` | 是 | — | 唯一监视项名称；用于数据库与摘要分组 |
| `url` | 是 | — | 绝对 http(s) URL，不可含凭据，主机必须在本地白名单 |
| `interval_seconds` | 是 | — | 周期，整数，下界 `10` 秒 |
| `tags` | 否 | `[]` | 用于 `--tag` 过滤 |
| `include_selectors` | 否 | `[]` | 只提取匹配 CSS 选择器的区域；空表示全文 |
| `exclude_selectors` | 否 | `[]` | 比较前从 DOM 移除的区域 |
| `noise_patterns` | 否 | `[]` | 正则列表；匹配到动态串后替换为稳定 `[NOISE]` |
| `keywords` | 否 | `[]` | 变更后文本命中任一关键词即判 major |
| `enabled` | 否 | `true` | 关闭后不参与调度 |
| `rule_overrides` | 否 | `{}` | 逐监视项覆盖全局 rules 的扁平字段 |

### rules.yaml

| 分区/字段 | 默认 | 说明 |
|---|---:|---|
| `classification.noise_max_change_rate` | `0.02` | 变更率低于此值判 noise（0–1） |
| `classification.major_change_rate` | `0.35` | 变更率达到此值判 major（>0–1） |
| `classification.focus_selectors` | `[]` | 变更触及这些区域即 major（最高优先级） |
| `alerts.severities` | `["major"]` | 产生本地告警的级别子集 |
| `alerts.cooldown_seconds` | `3600` | 同一监视项、同一级别的告警冷却期 |
| `diff.max_chars` | `12000` | 随事件存储的 unified diff 字符上限 |
| `retention.keep_last` | `5` | 每个监视项至少保留的最新快照数 |
| `retention.major_keep_forever` | `true` | major 事件目标快照永久保留 |
| `retention.archive_max_age_days` | `30` | 未被引用旧 gzip 的最短保留天数 |
| `schedule.max_workers` | `4` | 单轮全局并发上限 |
| `schedule.jitter_seconds` | `10` | 按监视项名称确定性错峰的上限 |
| `fetch.timeout_seconds` | `5` | 连接/读取超时 |
| `fetch.max_bytes` | `2097152` | 响应体硬上限（2 MiB） |

变更率定义为 `(新增行 + 删除行) / max(旧行数, 新行数, 1)`。分级优先级：
`focus/keyword/high-rate → major`；否则 `noise-only 或低于噪声下界 → noise`；其余 `minor`。
`check-config` 会校验缺字段、未知字段、URL/凭据/白名单、CSS/正则语法、间隔下界、重复名称和规则范围，错误信息带文件与条目定位。

## 命令参考

```bash
# 校验配置
pagewatch check-config --config configs/watchlist.example.yaml \
                       --rules configs/rules.example.yaml

# 单轮监测：全量或按 name/tag 过滤；--due-only 只跑到期项
pagewatch run-once --data-dir data --tag priority
pagewatch run-once --data-dir data --name docs --json

# 生成摘要；默认导出全部历史，时间参数支持 ISO-8601 或 epoch 秒
pagewatch digest --data-dir data --output data/out/digest.html \
                 --json-out data/out/events.json \
                 --since 2026-01-01T00:00:00Z --until 2026-01-02T00:00:00Z

# 清理：dry-run 只打印对象与释放空间；执行后更新统计台账，可重复执行
pagewatch compact --data-dir data --dry-run
pagewatch compact --data-dir data

# 只读状态页，只允许绑定 127.0.0.1
pagewatch serve-status --data-dir data --port 8766

# 增量同步：把快照/事件/outbox/ gzip 推到对端本地目录（可中断续传）
pagewatch push --data-dir data --target-dir /srv/peer/data
# 对端也可只拉取已暂存在共享 spool 中的批次
pagewatch pull --data-dir /srv/peer/data --spool-dir /srv/peer/data/.sync-spool

# 演示；也可 python -m pagewatch.demo
pagewatch demo
```

`digest` 的 HTML/JSON 排序固定、无 JavaScript、无外部资源；`--generated-at` 可显式固定页眉时间，因此相同数据和参数生成逐字节一致的文件。

## 数据布局与运行时机制

```
data/
  pagewatch.sqlite          # 元数据、快照链、事件、告警、调度、审计
  archive/<sha256>.gz       # 按原始响应内容寻址；gzip mtime=0，自动去重
  outbox/alerts.jsonl       # 追加式告警落盘；只写本机
  out/digest.html           # digest 默认输出
```

SQLite 表概览：

| 表 | 作用 |
|---|---|
| `watches` | 监视项、最近 ETag/Last-Modified、最近抓取状态 |
| `snapshots` | 每监视项版本链：规范化全文、块 JSON、内容哈希、原始哈希、是否 major |
| `probes` | 每次探测记录；304 也写入且 `not_modified=1` |
| `events` | 变更事件：分级、变更率、增删行、区域、关键词、截断 diff |
| `alerts` | 告警台账，`(watch, severity, created_at)` 用于冷却去重 |
| `schedule_state` | 持久化 `next_due_at`、连续失败数、最近结果 |
| `audit_log` | 白名单拒绝、抓取错误、compact 等审计事件 |
| `meta` / `stats` | schema 版本与累计统计 |

- **条件请求**：携带 `If-None-Match` / `If-Modified-Since`；304 不创建快照，只记录 probe 并推进调度。
- **哈希短路**：规范化内容哈希与上一版相同则无事件（例如仅计数器变化被掩码后）。
- **归一化器版本**：快照记录 `normalizer-v1`，便于未来重算比对。
- **调度**：每项独立周期 + 基于名称哈希的确定性错峰；失败按 `interval × 2^failures` 指数退避且封顶 1 小时；恢复成功立即归零。下次时间从“当前时刻”计算，错过超过一轮不补跑。
- **告警**：命中 `alert.severities` 后先写 SQLite，再追加 JSONL；冷却期内同监视项同级重复告警被去重。
- **保留**：`compact` 按监视项保留最新 N 版，major 目标版本豁免；未被任何保留快照引用的旧 gzip 超期后删除。执行幂等并累计统计。

outbox 每行示例（虚构）：

```json
{"created_at":1767225900.0,"dedup_key":"announcements:major","event_id":2,"id":1,"message":"watch=announcements severity=major event=2 change_rate=0.2860","outbox_version":1,"severity":"major","watch_name":"announcements"}
```

## 增量同步链路（push / pull）

同步在既有快照版本链、事件台账、gzip 留档与 outbox 之上，新增一条**面向对端本地目录**的复制链路，用于机器迁移或双机热备；它不发起任何网络连接，只通过一个共享的本地 spool 目录交接批次（可放在挂载盘、U盘或由 rsync/带外方式搬运）。

### 传输单位与完整性取舍：内容寻址批次清单

三类传输单元——原始 gzip、规范化快照行、事件/outbox 行——带有硬跨引用（事件引用两个快照 id、outbox 行引用事件 id）。设计上把一个传输窗口封装成**一个批次（batch）**，并以内容寻址哈希作为批次清单：

- 每个负载按 `blobs/<sha256>` 内容寻址存一份；相同字节（含重试）天然去重、互不覆盖。
- 清单逐个列出 item 及其独立哈希，入库前逐项校验，损坏可定位到具体条目；gzip 还要解压后比对其内容寻址摘要。
- `batch_id = sha256(规范化清单)`，是批次内容的纯函数。**`batch_seq` 仅作排序提示，不参与哈希**——这样中断后重新暂存同一批数据得到相同 id，批次台账本身就是幂等键，重试无需额外协商、也不会产生重复事件行。

选择内容寻址而非"序号 + 整包校验"的理由：序号是易失的协商状态，中断重连、双机角色互换都会让序号漂移；而内容哈希自证身份，对端不需要相信源端的任何计数器，仅凭字节即可验证"这是不是同一批、有没有被截断或篡改"。两级哈希（清单整体 + 每 item）则兼顾防张冠李戴与可定位损坏。

**批次是原子落地单位**：对端先在暂存区校验全部 item 哈希、快照内容哈希、事件→快照与 outbox→事件的引用完整性、以及版本/事件/outbox 的连续性，再在**单个 SQLite 事务 + 一次原子 outbox 重写**内入库。任一校验或写入失败，整批回退并标记 `failed`，从机制上杜绝"对端有事件却缺对应快照"的悬空引用。

### 水位、续传与失败语义

- 三条流各自维护**连续水位**：快照按监视项记最大连续 `version`；事件记全局连续 `events.id`；outbox 以完整 JSONL 行数为水位。水位只在批次持久化成功后推进。
- 源端按对端当前水位只补齐缺失片段；对端存在洞时，只接受从最近连续水位的无缺口前缀，拒绝跨洞插入并给出明确 `gap` 失败，而不是整库重拷。
- 中断（进程被杀、磁盘写满、网络/挂载断开）后，已暂存批次保留在 spool；重试从最近连续水位继续。写入失败有 `failed` 标记与 `.error` 侧车，重新 `push` 会用相同 batch_id 干净重暂存，`pull`/`push` 重复应用同一批次为空操作（显式主键 + 存在性检查）。
- 落盘前做可用空间预检，磁盘不足直接报可重试错误而非半截写入。
- spool 上有一把 `flock` 互斥锁，并发的第二个同步进程快速失败并提示，避免交错写。
- push 的源端读取在 WAL 单一读事务快照上完成；同步进行中本机又跑完一轮监测时，新提交的快照/事件归入**下一批次**，水位推进既不丢也不重。

### 兼容性

同步只**新增**两张表 `sync_watermarks`、`sync_batches`（及 `meta` 中的 `sync_schema_version`），不修改任何既有表结构。旧库没有这两张表时，首次 `push`/`pull` 幂等补建；快照水位从现有最大版本号、事件水位从现有最大 id、outbox 从现有行数起算，因此不会把历史全量重发。所有旧命令（`run-once`、`digest`、`compact`、`serve-status` 等）行为不变。

spool 目录布局：

```
<spool>/
  locks/replication.lock      # 跨进程互斥
  batches/<batch_id>/         # 暂存中：manifest.json + blobs/<sha256>
  applied/<batch_id>.json     # 成功落地后的清单留痕
  failed/<batch_id>.json+.error  # 失败批次与原因，供重试与排查
```

## 本地模拟站点与剧本

站点仅监听 `127.0.0.1`，支持 ETag/Last-Modified 和只读控制端点。页面：公告、文档、价格表、带计数器/时间戳噪声的状态页。剧本由固定种子驱动，不使用墙钟。

| step | 变化 | 预期 |
|---:|---|---|
| 0 | 基线 | 4 个首次快照，无事件 |
| 1 | 文档正文段落变化 | `docs` → minor |
| 2 | 仅状态页计数器变化（被掩码） | 规范化哈希相同，无 major/minor 事件 |
| 3 | `#main-notice` 变化且含关键词“重要/计费” | major + 1 条本地告警 |
| 4 | 无变化 | 全部 304，验证条件请求路径 |

## 测试与验收

- `pytest -q`：80 个测试，全绿；不依赖网络和真实时间。
- `tests/conftest.py` 中的 socket 守卫 monkeypatch `connect` / `connect_ex`，会话结束断言无任何非本机连接尝试。
- 时钟通过 `FrozenClock` 注入；数据目录均使用 `tmp_path` 隔离。
- 核心模块（normalize / diff / classify / retention / sync）单文件行覆盖率均 ≥85%；全项目约 85%。
- 示例：`python -m pagewatch.demo` 在断网环境产出 `data/out/digest.html`；`compact --dry-run` 与正式执行使用同一计划函数。
- 同步验收：`python3 scripts/acceptance_sync.py` 全程离线走查多轮快照/事件 → 中断续传 → 两端逐项比对（版本链、事件条数与 id、outbox 行、gzip 哈希）→ 传输窗口内插入新监测轮验证水位与去重 → 缺水位表老库自动补建且旧命令可用。`tests/test_sync.py` 另含批次篡改、blob 损坏、悬空引用、版本缺口、磁盘写满回退与并发锁等用例。

## 已知限制与明确不实现

- 浏览器渲染与 JavaScript 执行；登录态、验证码、Cookie 会话管理。
- 真实外发通知（邮件/IM/Webhook 发送）；当前仅落地 outbox。
- 图片、PDF、二进制附件变更比对。
- 可视化配置编辑界面；多机分布式调度；代理池与任何反爬对抗手段。
- 对公网站点的开箱监测能力（白名单固定为本机，文档不引导放开）。
- 依赖站点提供稳定 HTML；不解析 SPA 动态内容。
- 只读取页面，不提交任何数据；不做表单填报。
