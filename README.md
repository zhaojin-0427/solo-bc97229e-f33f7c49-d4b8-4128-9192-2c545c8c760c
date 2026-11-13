# 鸟类监测数据湖（Flask + PyArrow + SQLite）

把各监测站逐年新增鸟种的调查 CSV 入湖：每批一个独立 Parquet 文件，目录库（SQLite）
按列名合并出统一的逻辑结构，研究员可按站点预览 / 下载统一结构的 CSV。

调查误交时可**按批次 ID 撤回整批观测**：新预览与下载立即排除该批，撤回记录永久保留；
已固定快照的取数仍完整返回原结果；文件在无读者后自动回收。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

可选：灌入示例数据（含一次「uint64 与 text 冲突 → 提升为 text」的演示），
并演示撤回（`--retract`：发布后撤回最后一批，可在 `/batches` 看到撤回记录与回收状态）：

```bash
docker compose exec web python scripts/seed_demo.py --retract
```

运行端到端自检（解析校验 / 结构合并 / 类型提升 / 并发 / 重启持久化 /
撤回 / 固定快照 / 租约回收 / 多进程 / Web 流程）：

```bash
docker compose exec web python scripts/selftest.py
```

本地开发（不用 Docker）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATA_DIR=./data
flask --app app.web run        # 或 gunicorn -b 0.0.0.0:8000 app.web:app
```

## 数据格式与入湖流程

1. **上传**：每批填写站点 + 选 CSV。CSV 首列为观测日（`YYYY-MM-DD`），其余每列一种鸟；
   列名去空白后非空且不得重复；空单元格一律按 NULL 处理。
2. **逐列声明类型**：每个鸟种列声明为 `uint64`（非负 64 位整数，0 ~ 2^64-1）或 `text`。
   首列观测日归一化为固定列名 `obs_date`（date 类型），鸟种列不得使用该保留名。
3. **发布**：
   - 解析校验（坏整数、坏日期、字段数不齐 → **整批不发布**）；
   - 写 `parquet/<批次UUID>.parquet`：先写临时文件 + fsync，再原子 rename，**既有文件永不重写**；
   - 最后才开启**短暂的**目录库写事务：合并逻辑结构 + 登记批次。
   解析和写文件期间**不持有**目录库写事务；登记失败会删掉未发布的文件，**目录永不引用残件**。

## 逻辑结构合并规则

- 按列名合并；批次缺列在读出时补 NULL；新鸟种自动加列；
- 同鸟种类型冲突（一边 `uint64` 一边 `text`）→ 提升为 `text`；
- 提升后**不得退回**整数（`text` 是吸收态）；旧整数读出时按**十进制**转成文本；
- 并发上传新增不同鸟种、或对同列并发提升：发布事务经 `BEGIN IMMEDIATE` 串行化，
  结构变化与各批记录都完整保留（自检第 3 项覆盖）。撤回只改批次的可取用性，
  从不回退列序或类型提升。

## 批次撤回与文件回收

网页在 `/batches` 提供按**批次 ID**（完整 UUID 或唯一前缀）撤回；批次列表与总览页
也提供「撤回该批」。语义如下：

1. **撤回是目录库里一次不可逆的短事务事实**：`batches.retracted_at` 置位，回收状态置
   `pending`。站点、源文件名、行数、撤回首个时间与原因全部保留，重复撤回复用原记录
   （不覆盖时间 / 原因 / 回收进度）。
2. **新取数立即排除**：此后所有预览 / 下载快照都只含未撤回批次，无需等待文件回收。
3. **已固定的快照仍完整返回**：取数开始时在同一个短事务里登记「逻辑结构 + 文件清单」
   并写入**租约**（`leases` 表），事务立即提交、库锁释放；随后打开 Parquet 的慢过程
   不持有任何库事务。因此：
   - 慢取数**不阻塞**撤回、其他上传或其他取数；
   - 撤回时只要有读者租约，文件保持**待回收**，拿快照者仍能读到完整原结果，
     即使拿快照时尚未打开 Parquet；
   - 多进程共用 `DATA_DIR` 时同一规则成立（租约按批次记录在共享 SQLite 里）。
4. **租约到期/释放后回收**：读者 `release()` 即删除租约并触发回收；读者进程异常退出
   时心跳停止，租约按 `LEASE_TTL_SEC`（默认 30 秒，可环境变量调整）自然过期，文件
   不会被永久占用。
5. **状态与重试**：网页显示 `待回收 / 已回收 / 删除错误`（含错误原因），可单个重试或
   一键重试全部；后台守护线程也会周期性重试。
6. **崩溃接续**：回收永远**先删文件、后写 `done`**。删除前中断 → 重启后仍是 `pending`
   并继续删除；删除后中断 → 重启发现文件已缺失，补记 `done`。
   回收只触碰 `reclaim_*` 列，绝不恢复 `retracted_at` 的可见性，且只删除该批自己登记的
   Parquet 文件名（`parquet_dir` 下直接子文件名），不会误删其他批次。

## 一致性读取

预览 / 下载先在同一**短写事务**内取「逻辑结构 + 文件清单」并登记租约（WAL 模式），
再按该快照读取不可变的 Parquet 文件——一次取数始终使用同一状态的结构与清单。
页面上的 `schema v<N>` 每次结构变化都会递增。

## 持久化与重启

`DATA_DIR`（Docker 卷 `lake_data`）内含 `catalog.db`（WAL）、租约表和全部 Parquet 文件。
重启后直接可用；启动时清理暂存区、接续未完成的回收，并清理崩溃遗留的临时文件。
未被目录引用的孤儿 Parquet 过了宽限期才删除（不影响在途发布，也不会抢在租约回收前删除）。

## 目录结构

```
app/
  catalog.py    # SQLite 目录库：逻辑结构 + 批次清单 + 撤回/回收状态 + 租约，读写事务
  csvparse.py   # CSV 解析与校验（纯标准库）
  store.py      # PyArrow：批次表构建、原子写 Parquet、统一结构读取、CSV 导出
  service.py    # 入湖编排：暂存 -> 校验 -> 写文件 -> 短事务登记；撤回入口；启动清理
  retention.py  # 撤回 / 固定快照租约 / 文件回收（多进程） / 崩溃接续
  web.py        # Flask 路由：上传/声明/发布/预览/下载/撤回/回收
  templates/    # 页面
scripts/
  selftest.py   # 端到端自检
  seed_demo.py  # 示例数据灌入
samples/        # 示例 CSV
```

## 主要 HTTP 入口

| 路由 | 说明 |
| --- | --- |
| `GET /` | 总览：逻辑结构、站点、批次（在用的批次可直接撤回） |
| `GET/POST /upload` | 上传 CSV（第一步） |
| `POST /publish` | 声明类型后发布（第二步） |
| `GET /preview?station=&limit=` | 按站点预览统一结构（固定快照，排除已撤回批次） |
| `GET /download.csv?station=` | 下载统一结构 CSV（固定快照，排除已撤回批次） |
| `GET /batches` | 批次列表：撤回记录、回收状态、按批次 ID 撤回 |
| `POST /retract` | 按批次 ID 撤回（`batch_id` 完整 UUID 或唯一前缀，`note` 可选） |
| `POST /reclaim` | 重试回收单个批次（`batch_id`） |
| `POST /reclaim-pending` | 重试全部待回收 / 删除错误 |
| `GET /healthz` | 健康检查 |

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `./data` | 目录库与 Parquet 所在目录（多进程共享） |
| `SECRET_KEY` | 开发默认值 | Flask session 密钥 |
| `LEASE_TTL_SEC` | `30` | 读者租约有效期（心跳为 1/3 TTL） |
| `RECLAIM_INTERVAL_SEC` | `2` | 后台回收线程扫描间隔 |

