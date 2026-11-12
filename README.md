# 销售地区归属 · 追溯更正平台

按**交易发生日**确定客户地区的销售数仓演示系统。地区归属在入仓/回填切换时**入库存储**，
查询不做任何临时重算；支持按客户整套替换地区有效期、乐观修订并发控制、无长写锁的后台回填，
以及两个 Web 进程并发下的原子切换。

技术栈：FastAPI + SQLite（WAL）+ 原生 HTML/JS 前端 + Nginx 负载均衡 + Docker Compose。

另含 **ERP 日结对账**能力：核对 ERP 日结快照与仓内销售，输出漏单、多单与同 ID 字段差异；
配套一个仅用标准库的本地 ERP 分页源，支持中页断流、延迟与永久坏页故障注入。

## 一键启动

```bash
docker compose up --build
```

浏览器入口：

| 入口 | 说明 |
| --- | --- |
| http://localhost:8080 | Nginx 统一入口，请求在两个 Web 进程间轮询 |
| http://localhost:8081 | 直连 Web 进程 1（容器 sales-web1） |
| http://localhost:8082 | 直连 Web 进程 2（容器 sales-web2） |
| http://localhost:9090 | 直连本地 ERP 分页源（注入/清除断流故障、查看快照） |

SQLite 数据文件位于命名卷 `sales_data`（容器内 `/data/sales.db`），**容器重启/重建后数据保留**。
ERP 源地址在两个 Web 进程中由环境变量 `ERP_SOURCE_URL=http://erp-source:9000` 固定配置，不由前端请求指定。

打开页面后在「④ 演示脚本与自检」页点**一键准备演示数据**，或：

```bash
curl -X POST http://localhost:8080/api/demo/seed
```

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 上传 ERP 销售 CSV（唯一销售ID、客户ID、交易日期、整数分金额） | `POST /api/sales/import`（multipart）/ `POST /api/sales/import-json`（程序化） |
| 按交易日确定地区，归属入库存储，查询不重算 | `sales.region / region_version / unattributed_reason` 落库，查询接口只读取 |
| 按客户替换整套有效期，左闭右开，末段可无结束日 | `validate_entries`：`[from, to)`，末段 `to=NULL`，背靠背合法、重叠/反向 400 拒绝 |
| 空档标为未归属 | 原因码 `gap`；客户从未建时间线为 `no_timeline` |
| 更正携带所见修订，过期拒绝 | 请求必须带 `expected_version`，与当前版本不符返回 `409 stale_version` |
| 同客户并发更正只允许一份成功 | `correction_mutex` 客户级互斥表 + `BEGIN IMMEDIATE`，跨进程生效，败者 `409 concurrent_correction` |
| 后台回填期间仍可导入、查旧结果，回填不持写锁 | 守护线程做只读计算，sleep 延迟期间零锁；仅批量写 staging 与最终切换拿短锁 |
| 切换纳入期间新增销售 | 切换事务内对 staging 缺失的销售再次查库补算，然后整体重写该客户全部销售 |
| 时间线、修订、全部归属一起生效 | 单个 `BEGIN IMMEDIATE` 事务：旧 timeline 置 dead → 新置 active → 销售重写 → 写 revision，原子提交 |
| 失败保留旧版 | 模拟失败/异常时任务标 failed，删除 backfill 行与 staging，释放互斥，active 旧版原样可用 |
| 每次查询返回同版时间线和销售归属 | 原子提交 + WAL 快照读；`/api/sales/{id}` 附带同版时间线快照，`/api/consistency` 可自检 |
| 两个 Web 进程并发不留下旧归属 | 两容器共享同一 WAL 库；切换是单事务，busy_timeout 30s 串行化写者 |
| 重启保留 | 命名卷持久化；启动时把残留 pending/running 任务标记 failed 并清理（原子提交保证不会出现半切换） |
| 可调回填延迟 | 每次更正请求带 `backfill_delay_ms`（0~300000），延迟均匀分摊到每条销售的计算间隔 |
| 跨生效日样例 | C001 样例销售跨 2026-04-01；追溯模板把生效日提前到 2026-03-01，S1002 回填后改挂华南 |

## ERP 日结对账

在「④ ERP 日结对账」页输入**交易日**与 **ERP 快照 ID** 即可发起。源地址固定配置，
ERP 快照内容不变，HTTP 分页返回唯一销售 ID、客户、交易日、整数分金额。

### 可靠性与并发语义

| 需求 | 实现 |
| --- | --- |
| 受理时冻结该日仓内字段副本 | 受理事务内把该日 `sales` 四个源字段复制到 `recon_warehouse_snapshot`；之后导入、地区回填只改 `sales`，对账始终基于冻结副本 |
| 每页与下一游标一起持久提交，不留半页 | 取页在事务外（网络等待不持写锁）；整页行 + 游标推进在同一个 `BEGIN IMMEDIATE` 事务提交，提交点即页边界 |
| 断流/坏页/进程中断可从原进度续跑 | 失败保留已提交页与游标，任务转 `failed`（可续跑）；`init_db` 把残留 running/pending 转 failed 并清锁，续跑从已提交游标继续 |
| 双 Web 进程下同任务并发续跑只执行一份 | `recon_mutex` 以 job_id 为主键的跨进程插入互斥 + 运行租约 `lease_expires_at`，败者立即返回、不重复取数 |
| 网络等待不持数据库写锁 | HTTP 请求在任何事务之外发起；仅整页落库与最终发布拿短写锁 |
| 取完全部源数据才发布结论 | 仅当末页 `next_cursor=null` 后，单事务对比并写入 `recon_result`，任务置 `succeeded`；结论与终态同事务可见 |
| 取数失败不发布不完整结论、不修改销售 | failed 时 `recon_result` 恒为空，结果接口返回 `409 not_published`；全程不写 `sales` |
| 三类差异附双方原值 | 漏单 `missing_in_warehouse`、多单 `extra_in_warehouse` 附整条 JSON 原值；字段差异 `field_diff` 逐字段（客户/交易日/金额）附双方标量原值 |
| 同（交易日，快照）重复受理幂等 | 命中已有任务直接返回，不重复冻结、不重复建任务 |

### 本地 ERP 分页源与故障注入

`erp-source` 服务（仅 Python 标准库）提供游标分页，内置两个快照：

- `snap-demo-20260820`：11 行演示快照，每页 5 行、默认每页延迟 300ms（`ERP_DELAY_MS`）。
- `snapshot-bad`：第 2 页**永久**返回 500（带 resume 也不恢复），用于验证不完整绝不发布。

故障控制（浏览器 :9090 或容器网络）：

```bash
# 注入一次性中页断流：前 1 页正常，第 2 页返回 500；续跑请求带 resume=1 即恢复
curl -X POST "http://localhost:9090/control/fail?snapshot_id=snap-demo-20260820&after_pages=1"
# 清除一次性故障
curl -X POST http://localhost:9090/control/reset
# 查看快照列表
curl http://localhost:9090/snapshots
```

### 对账浏览器演示流程

1. 对账页点「一键准备对账演示数据」，幂等灌入仓内该日 9 行销售。
2. 直接点「受理并开始取数」，观察任务逐页推进（页/行/游标），成功后查看结论：
   漏单 `E9001/E9002`、多单 `S1009`、`S1002` 金额、`S1005` 客户、`S1007` 交易日差异。
3. 对 ERP 源注入一次性断流后重新发起：任务在第 2 页转 `failed` 并保留第 1 页，
   点该行「续跑」即从游标 5 恢复并最终发布；失败期间结果接口不可见。
4. 用快照 `snapshot-bad` 发起：无论续跑多少次都停在 failed、结论表始终为空。
5. 重启验证：`docker compose restart web1 web2` 后，中断任务转 failed、进度保留，可继续续跑。

## 浏览器页面功能

1. **销售导入 / 明细**：上传 CSV、下载样例、按客户/未归属筛选、查看销售详情（含同版时间线快照与未归属原因）。
2. **客户地区有效期**：载入当前时间线（显示所见版本）、可视化增删区间、可调回填延迟、可勾选模拟失败、查看逐版修订 diff。
3. **更正与回填任务**：任务状态（pending/running/succeeded/failed）、暂存进度、每 2 秒自动刷新。
4. **ERP 日结对账**：受理对账并查看逐页进度、断点续跑、三类差异结论（附双方原值）、故障注入与一键准备演示数据。
5. **演示脚本与自检**：一键 seed、一致性自检、推荐演示流程。

## 命令行演示

```bash
# 仅依赖标准库；先 seed 再自动演示：并发互斥 → 回填中导入/查询 → 过期版本拒绝
python3 demo/concurrency_demo.py http://localhost:8080
```

## 主要 API

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/sales/import` | CSV 上传（表头 `sale_id,customer_id,txn_date,amount_cents`） |
| `POST /api/sales/import-json` | JSON 批量导入 `{"rows":[...]}` |
| `GET /api/sales` / `GET /api/sales/{sale_id}` | 销售列表 / 详情（含同版时间线快照） |
| `GET /api/timeline/{customer_id}` | 当前生效整套时间线 + 所见版本号 |
| `POST /api/timeline/{customer_id}` | 替换有效期（首次建线 expected_version=0） |
| `POST /api/corrections` | 追溯更正（携带 expected_version、delay、fail_simulation） |
| `GET /api/corrections[/{id}]` | 任务列表/详情（含 staging 预览） |
| `GET /api/revisions/{customer_id}` | 修订历史与逐条 diff |
| `GET /api/consistency` | 销售归属与版本时间线一致性自检 |
| `POST /api/demo/seed` | 幂等灌入演示数据 |
| `POST /api/recon/jobs` | 受理对账（交易日 + ERP 快照 ID），冻结仓内副本并自动开始取数 |
| `POST /api/recon/jobs/{id}/resume` | 断点续跑（跨进程单实例，并发只执行一份） |
| `GET /api/recon/jobs` / `GET /api/recon/jobs/{id}` | 对账任务列表 / 详情（进度、游标、租约、错误、差异计数） |
| `GET /api/recon/jobs/{id}/results?kind=` | 对账结论（仅 succeeded 可查，否则 409），可按差异类型过滤 |
| `POST /api/demo/recon-seed` | 幂等灌入对账演示所需仓内销售 |

## 数据模型要点

- `region_timeline`：多版本时间线行，`status ∈ active/backfill/dead`，左闭右开区间。
- `correction_mutex`：`customer_id` 主键，进行中的更正一行锁，切换/失败即释放。
- `correction_job`：更正任务与回填状态（进度、延迟、错误、关联修订）。
- `region_staging`：回填计算结果，仅在任务存活期间存在，切换后清理。
- `region_revisions`：每次成功切换一条，含新版本 payload 与新旧逐条 diff。
- `sales`：归属三要素 `region_version / region / unattributed_reason` 持久化。
- `recon_job`：对账任务状态机（pending/running/succeeded/failed）、游标、已取页数/行数、租约、续跑标记与错误。
- `recon_mutex`：`job_id` 主键的跨进程执行锁，同任务并发续跑只允许一份。
- `recon_warehouse_snapshot`：受理时冻结的该日仓内源字段副本，后续导入/回填不影响本次对账。
- `recon_source_row`：ERP 分页源数据，按 `(job_id, sale_id)` 幂等，整页与游标同事务提交。
- `recon_result`：取完才发布的三类结论（漏单/多单/字段差异），附双方原值。

## 本地测试

```bash
pip install -r requirements.txt
pytest -q
```

覆盖：左闭右开边界、重叠拒绝、空档/no_timeline 未归属、追溯回填、乐观版本过期、
同客户并发互斥、回填期间新增销售纳入切换、失败保留旧版、重启持久化与中断恢复、坏 CSV 整批拒绝；
以及对账的冻结副本、三类差异与双方原值、中页断流续跑、永久坏页不发布、同任务并发单实例、
重启恢复、全程不修改 sales（`tests/test_recon.py`）。

无第三方依赖时，可用标准库脚本直接验证对账核心逻辑与真实 HTTP 链路（无需 FastAPI）：

```bash
python3 demo/verify_recon.py        # 34 项：SQLite + 假取页器，覆盖全部对账语义
python3 demo/verify_recon_http.py   # 12 项：启动真实 ERP 源子进程，走真实 urllib 网络
```
