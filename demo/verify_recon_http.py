#!/usr/bin/env python3
"""真实 HTTP 端到端：启动本地 ERP 源子进程，app.recon 用真实 urllib 取页。

验证网络等待不持锁、真实断流(500)→resume 续跑、真实永久坏页不发布。
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import types
import urllib.error
import urllib.request

# 桩 fastapi
fastapi = types.ModuleType("fastapi")


class HTTPException(Exception):
    def __init__(self, status_code, detail):
        self.status_code = status_code
        self.detail = detail


fastapi.HTTPException = HTTPException
sys.modules["fastapi"] = fastapi

PORT = 9120
_BASE = f"http://127.0.0.1:{PORT}"
_TMP = tempfile.mkdtemp(prefix="recon_http_")
os.environ["SALES_DB"] = os.path.join(_TMP, "http.db")
os.environ["ERP_SOURCE_URL"] = _BASE
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import recon  # noqa: E402
from app.db import get_conn, init_db, utcnow  # noqa: E402

PASS = FAIL = 0


def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} {extra}")


def http(method, path):
    req = urllib.request.Request(_BASE + path, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def seed_warehouse():
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    # 仓内该日 9 行（S1009 为仓内多出；不含 E9001/E9002）
    rows = [("S1001", "C001", 12000), ("S1002", "C001", 8800), ("S1003", "C001", 15000),
            ("S1004", "C001", 23050), ("S1005", "C001", 9900), ("S1006", "C002", 5000),
            ("S1007", "C002", 7200), ("S1008", "C002", 7300), ("S1009", "C003", 4200)]
    for sid, cid, amt in rows:
        conn.execute(
            "INSERT INTO sales (sale_id,customer_id,txn_date,amount_cents,region_version,"
            "region,unattributed_reason,imported_at) VALUES (?,?,'2026-08-20',?,0,NULL,NULL,?)",
            (sid, cid, amt, utcnow()))
    conn.execute("COMMIT")
    conn.close()


def wait(job_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = recon.get_job_detail(job_id)
        if d["status"] in ("succeeded", "failed"):
            return d
        time.sleep(0.05)
    raise AssertionError("timeout")


def launch(job_id):
    recon.try_resume(job_id, recon.http_fetch_page)
    return wait(job_id)


def main():
    env = dict(os.environ, ERP_PORT=str(PORT), ERP_PAGE_SIZE="5", ERP_DELAY_MS="200")
    server = subprocess.Popen(
        [sys.executable, "-I", os.path.join(os.path.dirname(__file__), "..", "erp_source", "server.py")],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        # 等源起来
        for _ in range(50):
            try:
                urllib.request.urlopen(_BASE + "/snapshots", timeout=2)
                break
            except OSError:
                time.sleep(0.1)
        init_db()
        seed_warehouse()

        print("\n[A] 真实 HTTP 正常全量取数（含 200ms/页延迟）")
        jid = recon.accept_job("2026-08-20", "snap-demo-20260820")["job_id"]
        t0 = time.time()
        d = launch(jid)
        elapsed = time.time() - t0
        check("成功取完 11 源行 / 3 页", d["status"] == "succeeded"
              and d["source_rows"] == 11 and d["pages_fetched"] == 3, str(d))
        check("3 页延迟累计约 0.6s（证明真实网络等待发生）", elapsed >= 0.55, f"{elapsed:.2f}s")
        check("漏单 3 / 多单 1 / 字段差异 3",
              d["counts"]["missing_in_warehouse"] == 3
              and d["counts"]["extra_in_warehouse"] == 1
              and d["counts"]["field_diff"] == 3, str(d["counts"]))

        print("\n[B] 真实中页断流（HTTP 500）→ 进度保留 → resume 续跑成功")
        http("POST", "/control/reset")
        code, _ = http("POST", "/control/fail?after_pages=1")
        check("故障注入 200", code == 200)
        jid2 = recon.accept_job("2026-08-21", "snap-demo-20260820")["job_id"]
        d = launch(jid2)
        check("断流后 failed，停在第 1 页（游标 5）",
              d["status"] == "failed" and d["cursor"] == 5 and d["pages_fetched"] == 1
              and "HTTP 500" in d["error"], str(d))
        check("失败时无结论", d["counts"]["missing_in_warehouse"] == 0)
        d = launch(jid2)  # 续跑带 resume=1，源端放行
        check("续跑成功取满 11 行", d["status"] == "succeeded" and d["source_rows"] == 11, str(d))

        print("\n[C] 真实永久坏页：反复续跑始终 failed，绝不发布")
        jid3 = recon.accept_job("2026-08-22", "snapshot-bad")["job_id"]
        for i in range(2):
            d = launch(jid3)
            check(f"第{i+1}次续跑仍 failed", d["status"] == "failed" and d["cursor"] == 5)
        conn = get_conn()
        n = conn.execute("SELECT COUNT(*) AS c FROM recon_result WHERE job_id=?", (jid3,)).fetchone()["c"]
        conn.close()
        check("结论表零行", n == 0, str(n))

        print("\n[D] 网络等待期间不持写锁：取数中并发写销售不被阻塞")
        http("POST", "/control/reset")
        http("POST", "/control/fail?after_pages=5")  # 不会触发（只有3页），仅保证正常
        jid4 = recon.accept_job("2026-08-23", "snap-demo-20260820")["job_id"]
        recon.try_resume(jid4, recon.http_fetch_page)
        time.sleep(0.3)  # 正在某页网络等待中
        t0 = time.time()
        conn = get_conn()
        conn.execute("BEGIN IMMEDIATE")  # 若取数持写锁这里会阻塞
        conn.execute(
            "INSERT INTO sales (sale_id,customer_id,txn_date,amount_cents,region_version,"
            "region,unattributed_reason,imported_at) VALUES ('CONC1','C1','2026-08-23',1,0,NULL,NULL,?)",
            (utcnow(),))
        conn.execute("COMMIT")
        conn.close()
        blocked = time.time() - t0
        check(f"取数网络等待期间写事务立即获得锁（耗时 {blocked:.2f}s）", blocked < 1.0)
        d = wait(jid4)
        check("任务仍正常完成", d["status"] == "succeeded")
    finally:
        try:
            server.terminate()
            server.wait(timeout=10)
        except (PermissionError, ProcessLookupError, OSError):
            pass  # 沙箱可能禁止信号；进程随脚本退出被回收

    print(f"\n==== 真实 HTTP 端到端：{PASS} 通过，{FAIL} 失败 ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
