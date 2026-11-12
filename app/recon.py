"""ERP 日结对账：分页取数、断点续跑、跨进程单实例与结论发布。

并发与可靠性模型（SQLite WAL + 两个 uvicorn 进程）
--------------------------------------------------
1. 受理（短写事务）：校验交易日，写入 recon_job，并把该日仓内 sales 的
   源字段（sale_id/customer_id/txn_date/amount_cents）复制到
   recon_warehouse_snapshot。副本一旦写入不再变化——之后的导入、地区
   回填都只动 sales，对账始终基于这份冻结副本。
2. 跨进程互斥：recon_mutex 以 job_id 为主键，BEGIN IMMEDIATE 下抢锁，
   两个 Web 进程对同一任务并发续跑，只有一个能插入成功；锁内同时写
   带过期时间的租约 lease_expires_at。
3. 分页取数：网络请求在事务外进行，等待期间不持有任何数据库写锁。
   每页成功后，在同一个 BEGIN IMMEDIATE 事务里写入整页行并推进
   cursor/pages_fetched——提交点就是页边界，断流、坏页或进程中断都
   不会留下半页，重启后从已提交游标原样续跑。
4. 失败不发布：任一页失败（HTTP 非 2xx、网络异常、结构非法）任务转
   failed 并保留进度，recon_result 不写入任何内容；sales 全程不被修改。
5. 发布：取完全部页（next_cursor 为 null）后，在单个写事务内对比
   冻结仓内副本与完整源数据，写入三类结论（仓内缺失/仓内多出/同 ID
   字段差异，均附双方原值），任务置 succeeded。结论与终态同事务可见。
"""
from __future__ import annotations

import json
import socket
import sqlite3
import threading
import urllib.error
import urllib.request
from datetime import date
from typing import Any, Callable

from fastapi import HTTPException

from .db import get_conn, utcnow

# 源地址固定配置：环境变量优先，默认指向 compose 内的本地 ERP 源。
DEFAULT_SOURCE_URL = "http://erp-source:9000"
# 单次 HTTP 取页超时（秒）。超时按坏页处理：进度保留，可续跑。
HTTP_TIMEOUT = float(15)
# 运行租约时长（秒）：持有者崩溃后，租约过期才允许其他进程接管续跑。
LEASE_SECONDS = 120

# 结论类型
KIND_MISSING = "missing_in_warehouse"   # ERP 有、仓内没有 → 漏单，建议补采
KIND_EXTRA = "extra_in_warehouse"       # 仓内有、ERP 没有 → 多单，人工核查
KIND_DIFF = "field_diff"                # 同 ID 字段差异，附双方原值

# 测试可替换的取页函数：(base_url, snapshot_id, cursor, resume) -> dict
Fetcher = Callable[[str, str, int, bool], dict]


def _safe_rollback(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def source_base_url() -> str:
    import os
    return os.environ.get("ERP_SOURCE_URL", DEFAULT_SOURCE_URL).rstrip("/")


# ---------------------------------------------------------------------------
# HTTP 取页（标准库，网络等待期间不接触数据库）
# ---------------------------------------------------------------------------

def http_fetch_page(base_url: str, snapshot_id: str, cursor: int, resume: bool) -> dict:
    url = (f"{base_url}/snapshots/{snapshot_id}/sales"
           f"?cursor={cursor}&resume={'1' if resume else '0'}")
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as exc:
        # 5xx/4xx 统一作为坏页：快照 ID 不存在给出明确信息，其余保留可续跑
        detail = exc.read().decode("utf-8", "replace")[:300]
        raise PageFetchError(f"ERP 返回 HTTP {exc.code}：{detail}", retryable=exc.code >= 500)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise PageFetchError(f"网络异常：{exc!r}", retryable=True)
    if status != 200:
        raise PageFetchError(f"ERP 返回非预期状态 {status}", retryable=True)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise PageFetchError(f"响应不是合法 JSON：{exc!r}", retryable=True)
    _validate_page(payload, cursor)
    return payload


def _validate_page(payload: Any, expected_cursor: int) -> None:
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise PageFetchError("响应缺少 items 数组", retryable=True)
    if payload.get("cursor") != expected_cursor:
        raise PageFetchError(
            f"响应游标 {payload.get('cursor')} 与请求游标 {expected_cursor} 不一致",
            retryable=True)
    seen: set[str] = set()
    for i, row in enumerate(payload["items"]):
        if not isinstance(row, dict):
            raise PageFetchError(f"第 {i} 行不是对象", retryable=True)
        sid = row.get("sale_id")
        cid = row.get("customer_id")
        txn = row.get("txn_date")
        amt = row.get("amount_cents")
        if not isinstance(sid, str) or not sid or not isinstance(cid, str) or not cid:
            raise PageFetchError(f"第 {i} 行 sale_id/customer_id 非法", retryable=True)
        if sid in seen:
            raise PageFetchError(f"页内销售ID重复：{sid}", retryable=True)
        seen.add(sid)
        try:
            date.fromisoformat(str(txn))
        except (TypeError, ValueError):
            raise PageFetchError(f"销售 {sid} 交易日非法：{txn!r}", retryable=True)
        if not isinstance(amt, int) or isinstance(amt, bool) or amt < 0:
            raise PageFetchError(f"销售 {sid} 金额需为非负整数分：{amt!r}", retryable=True)
    nc = payload.get("next_cursor")
    if nc is not None and (not isinstance(nc, int) or nc <= expected_cursor):
        raise PageFetchError(f"next_cursor 非法：{nc!r}", retryable=True)


class PageFetchError(Exception):
    def __init__(self, message: str, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


# ---------------------------------------------------------------------------
# 受理
# ---------------------------------------------------------------------------

def accept_job(txn_date: str, erp_snapshot_id: str) -> dict:
    # 交易日格式校验
    try:
        d = date.fromisoformat(txn_date)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400,
                            detail=f"交易日格式非法（要求 YYYY-MM-DD）：{txn_date!r}")
    txn_date = d.isoformat()
    if not erp_snapshot_id or not erp_snapshot_id.strip():
        raise HTTPException(status_code=400, detail="ERP 快照 ID 不能为空")
    erp_snapshot_id = erp_snapshot_id.strip()
    base_url = source_base_url()

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        # 同（交易日，快照）已有任务：幂等返回，不重复受理
        existing = conn.execute(
            "SELECT id, status FROM recon_job WHERE txn_date=? AND erp_snapshot_id=?",
            (txn_date, erp_snapshot_id),
        ).fetchone()
        if existing:
            conn.execute("COMMIT")
            return {"job_id": existing["id"], "status": existing["status"],
                    "deduped": True}

        cur = conn.execute(
            "INSERT INTO recon_job (txn_date, erp_snapshot_id, source_base_url, "
            "status, submitted_at) VALUES (?,?,?, 'pending', ?)",
            (txn_date, erp_snapshot_id, base_url, utcnow()),
        )
        job_id = cur.lastrowid

        # 受理即冻结该日仓内源字段副本（只取对账需要的四个源字段）
        wh_rows = conn.execute(
            "SELECT sale_id, customer_id, txn_date, amount_cents FROM sales "
            "WHERE txn_date=? ORDER BY sale_id",
            (txn_date,),
        ).fetchall()
        conn.executemany(
            "INSERT INTO recon_warehouse_snapshot "
            "(job_id, sale_id, customer_id, txn_date, amount_cents) "
            "VALUES (?,?,?,?,?)",
            [(job_id, r["sale_id"], r["customer_id"], r["txn_date"], r["amount_cents"])
             for r in wh_rows],
        )
        conn.execute("COMMIT")
        return {"job_id": job_id, "status": "pending", "deduped": False,
                "warehouse_rows_frozen": len(wh_rows),
                "source_base_url": base_url}
    except Exception:
        _safe_rollback(conn)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 抢锁 / 续跑入口
# ---------------------------------------------------------------------------

def try_resume(job_id: int, fetcher: Fetcher = http_fetch_page) -> dict:
    """浏览器/重启触发的续跑入口：抢跨进程锁，抢到则后台线程执行。

    返回 {accepted: bool, status: ...}；任务已在运行或已完成时 accepted=False。
    """
    conn = get_conn()
    try:
        job = conn.execute("SELECT * FROM recon_job WHERE id=?", (job_id,)).fetchone()
        if job is None:
            raise HTTPException(status_code=404, detail="对账任务不存在")
        if job["status"] == "succeeded":
            return {"accepted": False, "status": "succeeded",
                    "message": "任务已完成，结论已发布"}
        if job["status"] == "running":
            return {"accepted": False, "status": "running",
                    "message": "任务正在其他进程/线程执行"}
        # pending / failed 才允许启动；failed 启动属于续跑，取页携带 resume 标记
        is_resume = job["status"] == "failed"
        host = socket.gethostname()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO recon_mutex (job_id, host, locked_at) VALUES (?,?,?)",
                (job_id, host, utcnow()),
            )
            conn.execute(
                "UPDATE recon_job SET status='running', started_at=COALESCE(started_at, ?), "
                "worker_host=?, lease_expires_at=datetime('now', ?), error=NULL, "
                "resume_requested=? WHERE id=?",
                (utcnow(), host, f"+{LEASE_SECONDS} seconds",
                 1 if is_resume else 0, job_id),
            )
            conn.execute("COMMIT")
        except sqlite3.IntegrityError:
            _safe_rollback(conn)
            return {"accepted": False, "status": "running",
                    "message": "另一进程已持有该任务的执行锁"}
    finally:
        conn.close()

    threading.Thread(
        target=_run_guarded, args=(job_id, fetcher), daemon=True,
        name=f"recon-{job_id}",
    ).start()
    return {"accepted": True, "status": "running", "worker_host": host}


def _run_guarded(job_id: int, fetcher: Fetcher) -> None:
    """后台线程兜底：任何异常都要落到任务状态并释放锁。"""
    try:
        run_reconciliation(job_id, fetcher)
    except Exception as exc:  # noqa: BLE001 - 后台任务必须可观测
        conn = get_conn()
        try:
            _mark_failed(conn, job_id, f"对账异常：{exc!r}")
        finally:
            conn.close()


def _mark_failed(conn: sqlite3.Connection, job_id: int, message: str) -> None:
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE recon_job SET status='failed', finished_at=?, error=?, "
            "worker_host=NULL, lease_expires_at=NULL WHERE id=?",
            (utcnow(), message, job_id),
        )
        conn.execute("DELETE FROM recon_mutex WHERE job_id=?", (job_id,))
        conn.execute("COMMIT")
    except Exception:
        _safe_rollback(conn)
        raise


# ---------------------------------------------------------------------------
# 取数主循环
# ---------------------------------------------------------------------------

def run_reconciliation(job_id: int, fetcher: Fetcher = http_fetch_page) -> None:
    conn = get_conn()
    host = socket.gethostname()
    try:
        job = conn.execute("SELECT * FROM recon_job WHERE id=?", (job_id,)).fetchone()
        if job is None or job["status"] != "running":
            return
        base_url = job["source_base_url"]
        snapshot_id = job["erp_snapshot_id"]
        # 失败后续跑：本进程整个续跑过程都携带 resume 标记，
        # 源端据此放行已确认恢复的一次性断流；首次启动时为 False。
        resume_mode = bool(job["resume_requested"])

        while True:
            # 每轮重新读游标（游标只由本持有者在页事务里推进）
            job = conn.execute(
                "SELECT * FROM recon_job WHERE id=?", (job_id,)).fetchone()
            cursor = int(job["cursor"])

            # —— 网络等待在事务外：不持有任何数据库写锁 ——
            try:
                page = fetcher(base_url, snapshot_id, cursor, resume=resume_mode)
            except PageFetchError as exc:
                _mark_failed(
                    conn, job_id,
                    f"取数失败于第 {int(job['pages_fetched']) + 1} 页（游标 {cursor}）："
                    f"{exc}；已取 {job['pages_fetched']} 页/{job['source_rows']} 行已保留，可续跑")
                return
            except Exception as exc:  # noqa: BLE001 - 取页器任何异常都转为可续跑失败
                _mark_failed(
                    conn, job_id,
                    f"取数异常于第 {int(job['pages_fetched']) + 1} 页（游标 {cursor}）："
                    f"{exc!r}；已取 {job['pages_fetched']} 页/{job['source_rows']} 行已保留，可续跑")
                return

            # 无论取页器如何实现，主循环统一校验页面结构，坏页不入库、可续跑
            try:
                _validate_page(page, cursor)
            except PageFetchError as exc:
                _mark_failed(
                    conn, job_id,
                    f"坏页于第 {int(job['pages_fetched']) + 1} 页（游标 {cursor}）："
                    f"{exc}；已取 {job['pages_fetched']} 页/{job['source_rows']} 行已保留，可续跑")
                return

            items = page["items"]
            next_cursor = page["next_cursor"]
            page_no = int(job["pages_fetched"]) + 1

            # 空页但未声明结束：源端异常，按坏页处理不推进
            if not items and next_cursor is not None:
                _mark_failed(conn, job_id,
                             f"第 {page_no} 页为空但 next_cursor 非空，源端结构异常，可续跑")
                return

            # —— 整页行 + 下一游标同一事务持久提交（页边界即提交点）——
            try:
                conn.execute("BEGIN IMMEDIATE")
                # 锁仍属于本进程才允许写入（防御并发）
                lock = conn.execute(
                    "SELECT host FROM recon_mutex WHERE job_id=?", (job_id,)).fetchone()
                if lock is None or lock["host"] != host:
                    raise RuntimeError("执行锁已丢失，放弃本次写入")
                # 跨页重复 ID 检测：快照内销售 ID 必须唯一，冲突按坏页处理不推进
                incoming = [r["sale_id"] for r in items]
                placeholders = ",".join("?" for _ in incoming)
                dup = conn.execute(
                    f"SELECT sale_id FROM recon_source_row WHERE job_id=? "
                    f"AND sale_id IN ({placeholders})",
                    [job_id, *incoming],
                ).fetchall()
                if dup:
                    conn.execute("ROLLBACK")
                    _mark_failed(
                        conn, job_id,
                        f"第 {page_no} 页出现与已取数据重复的销售 ID："
                        f"{[r['sale_id'] for r in dup]}，快照 ID 应唯一，可续跑")
                    return
                conn.executemany(
                    "INSERT INTO recon_source_row "
                    "(job_id, sale_id, customer_id, txn_date, amount_cents, page_no) "
                    "VALUES (?,?,?,?,?,?)",
                    [(job_id, r["sale_id"], r["customer_id"], r["txn_date"],
                      r["amount_cents"], page_no) for r in items],
                )
                source_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM recon_source_row WHERE job_id=?",
                    (job_id,)).fetchone()["c"]
                if next_cursor is None:
                    new_cursor = cursor + len(items)
                else:
                    new_cursor = int(next_cursor)
                conn.execute(
                    "UPDATE recon_job SET cursor=?, pages_fetched=?, source_rows=?, "
                    "lease_expires_at=datetime('now', ?) WHERE id=?",
                    (new_cursor, page_no, source_count,
                     f"+{LEASE_SECONDS} seconds", job_id),
                )
                conn.execute("COMMIT")
            except Exception:
                _safe_rollback(conn)
                raise

            if next_cursor is None:
                break  # 全部源数据取完

        _publish_results(conn, job_id)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 结论发布（仅在全部源数据取完后执行，单事务，不修改 sales）
# ---------------------------------------------------------------------------

def _publish_results(conn: sqlite3.Connection, job_id: int) -> None:
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = conn.execute("SELECT * FROM recon_job WHERE id=?", (job_id,)).fetchone()
        if job is None or job["status"] != "running":
            raise RuntimeError("任务状态已变化，拒绝发布")

        erp_rows = {
            r["sale_id"]: r for r in conn.execute(
                "SELECT sale_id, customer_id, txn_date, amount_cents "
                "FROM recon_source_row WHERE job_id=?", (job_id,)).fetchall()
        }
        wh_rows = {
            r["sale_id"]: r for r in conn.execute(
                "SELECT sale_id, customer_id, txn_date, amount_cents "
                "FROM recon_warehouse_snapshot WHERE job_id=?", (job_id,)).fetchall()
        }

        results: list[tuple[Any, ...]] = []

        # 1) 仓内缺失（ERP 有、仓内没有）：漏单 → 建议补采
        for sid in sorted(set(erp_rows) - set(wh_rows)):
            e = erp_rows[sid]
            results.append((job_id, KIND_MISSING, sid, None,
                            _pack(e), None))

        # 2) 仓内多出（仓内有、ERP 没有）：多单 → 人工核查
        for sid in sorted(set(wh_rows) - set(erp_rows)):
            w = wh_rows[sid]
            results.append((job_id, KIND_EXTRA, sid, None,
                            None, _pack(w)))

        # 3) 同 ID 字段差异：客户、交易日、金额逐字段对比，附双方原值
        for sid in sorted(set(erp_rows) & set(wh_rows)):
            e, w = erp_rows[sid], wh_rows[sid]
            for field in ("customer_id", "txn_date", "amount_cents"):
                ev, wv = e[field], w[field]
                # 金额两边都是整数；其余字符串比较
                if str(ev) != str(wv):
                    results.append((job_id, KIND_DIFF, sid, field,
                                    str(ev), str(wv)))

        conn.executemany(
            "INSERT INTO recon_result (job_id, kind, sale_id, field_name, "
            "erp_value, warehouse_value) VALUES (?,?,?,?,?,?)",
            results,
        )
        counts = {KIND_MISSING: 0, KIND_EXTRA: 0, KIND_DIFF: 0}
        for row in results:
            counts[row[1]] += 1
        conn.execute(
            "UPDATE recon_job SET status='succeeded', finished_at=?, "
            "worker_host=NULL, lease_expires_at=NULL, error=NULL, "
            "resume_requested=0 WHERE id=?",
            (utcnow(), job_id),
        )
        conn.execute("DELETE FROM recon_mutex WHERE job_id=?", (job_id,))
        conn.execute("COMMIT")
    except Exception:
        _safe_rollback(conn)
        raise


def _pack(row: sqlite3.Row) -> str:
    """把一条源字段原值打包为 JSON 文本，随结论返回双方原值。"""
    return json.dumps(
        {"sale_id": row["sale_id"], "customer_id": row["customer_id"],
         "txn_date": row["txn_date"], "amount_cents": row["amount_cents"]},
        ensure_ascii=False)


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------

def get_job_detail(job_id: int) -> dict:
    conn = get_conn()
    try:
        job = conn.execute("SELECT * FROM recon_job WHERE id=?", (job_id,)).fetchone()
        if job is None:
            raise HTTPException(status_code=404, detail="对账任务不存在")
        d = dict(job)
        d["warehouse_rows"] = conn.execute(
            "SELECT COUNT(*) AS c FROM recon_warehouse_snapshot WHERE job_id=?",
            (job_id,)).fetchone()["c"]
        d["counts"] = {
            "missing_in_warehouse": conn.execute(
                "SELECT COUNT(*) AS c FROM recon_result WHERE job_id=? AND kind=?",
                (job_id, KIND_MISSING)).fetchone()["c"],
            "extra_in_warehouse": conn.execute(
                "SELECT COUNT(*) AS c FROM recon_result WHERE job_id=? AND kind=?",
                (job_id, KIND_EXTRA)).fetchone()["c"],
            "field_diff": conn.execute(
                "SELECT COUNT(*) AS c FROM recon_result WHERE job_id=? AND kind=?",
                (job_id, KIND_DIFF)).fetchone()["c"],
        }
        d["resumable"] = d["status"] == "failed"
        d["published"] = d["status"] == "succeeded"
        return d
    finally:
        conn.close()


def list_jobs() -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM recon_job ORDER BY id DESC LIMIT 100").fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["resumable"] = d["status"] == "failed"
            out.append(d)
        return out
    finally:
        conn.close()


def get_results(job_id: int, kind: str | None = None) -> dict:
    conn = get_conn()
    try:
        job = conn.execute("SELECT status FROM recon_job WHERE id=?",
                           (job_id,)).fetchone()
        if job is None:
            raise HTTPException(status_code=404, detail="对账任务不存在")
        if job["status"] != "succeeded":
            raise HTTPException(
                status_code=409,
                detail={"error": "not_published",
                        "message": "源数据尚未全部取完，结论未发布；不完整结果不可查看",
                        "status": job["status"]})
        sql = ("SELECT id, kind, sale_id, field_name, erp_value, warehouse_value "
               "FROM recon_result WHERE job_id=?")
        params: list[Any] = [job_id]
        if kind:
            if kind not in (KIND_MISSING, KIND_EXTRA, KIND_DIFF):
                raise HTTPException(status_code=400, detail=f"未知差异类型：{kind}")
            sql += " AND kind=?"
            params.append(kind)
        sql += " ORDER BY kind, sale_id, field_name"
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        for r in rows:
            # 漏单/多单的双方原值是打包 JSON；字段差异的 erp_value/warehouse_value
            # 是标量字符串（如 "8950"），保持原样、仅在是 JSON 打包时解析。
            ev = r.pop("erp_value")
            wv = r.pop("warehouse_value")
            r["erp_value"] = ev
            r["warehouse_value"] = wv
            r["erp_raw"] = json.loads(ev) if ev and ev.startswith("{") else None
            r["warehouse_raw"] = json.loads(wv) if wv and wv.startswith("{") else None
        return {"job_id": job_id, "items": rows}
    finally:
        conn.close()
