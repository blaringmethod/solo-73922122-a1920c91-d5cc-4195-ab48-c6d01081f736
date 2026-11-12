"""SQLite 连接管理、表结构与启动恢复。

设计要点：
- WAL 模式：多进程（两个 Web 容器）共享同一个数据库文件，读不阻塞写、写不阻塞读。
- 显式事务管理（isolation_level=None），需要写锁时使用 BEGIN IMMEDIATE，
  配合 busy_timeout 让并发写等待而不是立刻报错。
- 地区归属直接落库（sales.region / region_version / unattributed_reason），
  查询时不做任何临时重算。
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS region_timeline (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id    TEXT NOT NULL,
    region         TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to   TEXT,
    version        INTEGER NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('active', 'backfill', 'dead')),
    job_id         INTEGER
);
CREATE INDEX IF NOT EXISTS idx_timeline_lookup
    ON region_timeline (customer_id, status, version);

-- 同一客户同时只允许一份进行中的更正：客户级互斥（一行锁，切走/失败即释放）
CREATE TABLE IF NOT EXISTS correction_mutex (
    customer_id TEXT PRIMARY KEY,
    job_id      INTEGER NOT NULL,
    locked_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sales (
    sale_id             TEXT PRIMARY KEY,
    customer_id         TEXT NOT NULL,
    txn_date            TEXT NOT NULL,
    amount_cents        INTEGER NOT NULL,
    region_version      INTEGER NOT NULL,
    region              TEXT,
    unattributed_reason TEXT,
    imported_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales (customer_id, txn_date);
CREATE INDEX IF NOT EXISTS idx_sales_version  ON sales (region_version);

CREATE TABLE IF NOT EXISTS region_revisions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,
    supersedes_version  INTEGER NOT NULL,
    submitted_at        TEXT NOT NULL,
    activated_at        TEXT NOT NULL,
    note                TEXT,
    payload_json        TEXT NOT NULL,
    diff_json           TEXT,
    UNIQUE (customer_id, version)
);

CREATE TABLE IF NOT EXISTS correction_job (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id       TEXT NOT NULL,
    expected_version  INTEGER NOT NULL,
    new_version       INTEGER NOT NULL,
    status            TEXT NOT NULL
                        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    fail_simulation   INTEGER NOT NULL DEFAULT 0,
    backfill_delay_ms INTEGER NOT NULL DEFAULT 0,
    total_sales       INTEGER NOT NULL DEFAULT 0,
    staged_sales      INTEGER NOT NULL DEFAULT 0,
    note              TEXT,
    submitted_at      TEXT NOT NULL,
    started_at        TEXT,
    finished_at       TEXT,
    error             TEXT,
    revision_id       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_job_customer ON correction_job (customer_id, id);

CREATE TABLE IF NOT EXISTS region_staging (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL,
    customer_id  TEXT NOT NULL,
    sale_id      TEXT NOT NULL,
    txn_date     TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    region       TEXT,
    reason       TEXT,
    UNIQUE (job_id, sale_id)
);
CREATE INDEX IF NOT EXISTS idx_staging_job ON region_staging (job_id);

-- ===========================================================================
-- ERP 日结对账
-- ===========================================================================

-- 对账任务：状态机 pending（受理未启动）→ running（取数中）
--   → succeeded（全部取完并发布结论）/ failed（断流/坏页，可续跑）。
-- 结论只在 succeeded 后发布；failed 永远不产生结论、不修改 sales。
CREATE TABLE IF NOT EXISTS recon_job (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_date         TEXT NOT NULL,            -- 交易日 YYYY-MM-DD
    erp_snapshot_id  TEXT NOT NULL,            -- ERP 快照 ID（快照内容不变）
    source_base_url  TEXT NOT NULL,            -- 受理时固定的源地址
    status           TEXT NOT NULL
                        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    cursor           INTEGER NOT NULL DEFAULT 0,  -- 已持久提交的下一页游标
    pages_fetched    INTEGER NOT NULL DEFAULT 0,
    source_rows      INTEGER NOT NULL DEFAULT 0,  -- 已取到的源行数
    error            TEXT,
    submitted_at     TEXT NOT NULL,
    started_at       TEXT,
    finished_at      TEXT,
    worker_host      TEXT,                     -- 当前/最后执行进程，便于观测双进程
    lease_expires_at TEXT,                     -- 运行租约：仅持有者能继续推进
    resume_requested INTEGER NOT NULL DEFAULT 0  -- 1=本次启动为失败后续跑，取页带 resume 标记
);
CREATE INDEX IF NOT EXISTS idx_recon_status ON recon_job (status);

-- 任务级互斥（跨两个 Web 进程）：同一任务并发续跑只允许一份执行。
CREATE TABLE IF NOT EXISTS recon_mutex (
    job_id    INTEGER PRIMARY KEY,
    host      TEXT NOT NULL,
    locked_at TEXT NOT NULL
);

-- 受理时保存的「该日仓内源字段副本」：后续导入和地区回填不改变本次对账。
CREATE TABLE IF NOT EXISTS recon_warehouse_snapshot (
    job_id       INTEGER NOT NULL,
    sale_id      TEXT NOT NULL,
    customer_id  TEXT NOT NULL,
    txn_date     TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    PRIMARY KEY (job_id, sale_id)
);

-- ERP 源分页数据：每页与下一游标在同一事务持久提交，断流不留半页。
-- INSERT OR REPLACE + UNIQUE 保证同一页重试结果幂等（快照内容不变）。
CREATE TABLE IF NOT EXISTS recon_source_row (
    job_id       INTEGER NOT NULL,
    sale_id      TEXT NOT NULL,
    customer_id  TEXT NOT NULL,
    txn_date     TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    page_no      INTEGER NOT NULL,
    PRIMARY KEY (job_id, sale_id)
);
CREATE INDEX IF NOT EXISTS idx_source_job ON recon_source_row (job_id);

-- 对账结论（仅在全部源数据取完后一次性写入并随任务 succeeded 同事务发布）：
-- kind: missing_in_warehouse（仓内缺失/ERP 多出）
--       extra_in_warehouse（仓内多出/ERP 没有）
--       field_diff（同 ID 字段差异，附双方原值）
CREATE TABLE IF NOT EXISTS recon_result (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          INTEGER NOT NULL,
    kind            TEXT NOT NULL
                        CHECK (kind IN ('missing_in_warehouse',
                                        'extra_in_warehouse',
                                        'field_diff')),
    sale_id         TEXT NOT NULL,
    field_name      TEXT,                      -- field_diff 时的差异字段
    erp_value       TEXT,                      -- ERP 原值（统一存文本）
    warehouse_value TEXT,                      -- 仓内原值
    UNIQUE (job_id, kind, sale_id, field_name)
);
CREATE INDEX IF NOT EXISTS idx_result_job ON recon_result (job_id, kind);
"""


def db_path() -> str:
    return os.environ.get("SALES_DB", "/data/sales.db")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_conn() -> sqlite3.Connection:
    """每个调用方（请求线程 / 后台回填线程）使用自己的连接。"""
    conn = sqlite3.connect(db_path(), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # 自动提交模式，事务全部由代码显式 BEGIN/COMMIT，避免隐式事务与 IMMEDIATE 冲突。
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """建表，并对上次进程异常退出时残留的回填任务做恢复。

    SQLite 的 COMMIT 是原子的：若切换事务没提交，旧时间线就仍然完好，
    残留的 backfill 行与 staging 行直接清掉、任务标 failed 即可。
    """
    conn = get_conn()
    try:
        conn.executescript(SCHEMA)
        stale = conn.execute(
            "SELECT id FROM correction_job WHERE status IN ('pending', 'running')"
        ).fetchall()
        for row in stale:
            job_id = row["id"]
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE correction_job SET status='failed', finished_at=?, "
                    "error=? WHERE id=?",
                    (utcnow(), "进程重启，任务被中断，旧版本继续生效", job_id),
                )
                conn.execute("DELETE FROM region_staging WHERE job_id=?", (job_id,))
                conn.execute(
                    "DELETE FROM region_timeline WHERE status='backfill' AND job_id=?",
                    (job_id,),
                )
                conn.execute(
                    "DELETE FROM correction_mutex WHERE job_id=?", (job_id,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        # 对账任务恢复：每页与游标都是同事务提交，不存在半页；
        # 中断时已取数据与游标完整保留，任务转 failed（可从原进度续跑），
        # 释放跨进程互斥与租约。未发布结论前 failed 不会产生任何结果。
        stale_recon = conn.execute(
            "SELECT id FROM recon_job WHERE status IN ('pending', 'running')"
        ).fetchall()
        for row in stale_recon:
            rid = row["id"]
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE recon_job SET status='failed', finished_at=?, "
                    "worker_host=NULL, lease_expires_at=NULL, error=? WHERE id=?",
                    (utcnow(), "进程重启，取数被中断；已取页面与游标保留，可续跑", rid),
                )
                conn.execute("DELETE FROM recon_mutex WHERE job_id=?", (rid,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
    finally:
        conn.close()
