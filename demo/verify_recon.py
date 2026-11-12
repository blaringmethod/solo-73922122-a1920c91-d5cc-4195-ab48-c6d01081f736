#!/usr/bin/env python3
"""纯标准库验证：不依赖 FastAPI，直接驱动 app.recon 的对账核心逻辑。

桩掉 fastapi（仅用其 HTTPException）与 app.db 的环境，
用真实 SQLite 文件库 + 可编排假取页函数，验证：
  1. 三类差异与双方原值
  2. 受理冻结仓内副本，后续导入不影响
  3. 中页断流：页边界提交、无半页、游标续跑
  4. 永久坏页：反复续跑也不发布结论
  5. 同任务并发续跑只执行一份
  6. 重启恢复（init_db 把 running 转 failed 并保留进度）
  7. 全程不修改 sales
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import types
from datetime import date

# --- 桩掉 fastapi（app.recon 只依赖 HTTPException） ---
fastapi = types.ModuleType("fastapi")


class HTTPException(Exception):
    def __init__(self, status_code: int, detail) -> None:
        self.status_code = status_code
        self.detail = detail


fastapi.HTTPException = HTTPException
sys.modules["fastapi"] = fastapi

_TMP = tempfile.mkdtemp(prefix="recon_verify_")
os.environ["SALES_DB"] = os.path.join(_TMP, "verify.db")
os.environ["ERP_SOURCE_URL"] = "http://fake-erp.test"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import recon  # noqa: E402
from app.db import get_conn, init_db, utcnow  # noqa: E402

PASS, FAIL = 0, 0


def check(name: str, cond: bool, extra: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} {extra}")


def seed_sales() -> None:
    rows = [
        ("S1001", "C001", "2026-08-20", 12000),
        ("S1002", "C001", "2026-08-20", 8800),    # 金额差异
        ("S1003", "C001", "2026-08-20", 15000),
        ("S1004", "C001", "2026-08-20", 23050),
        ("S1005", "C001", "2026-08-20", 9900),    # 客户差异
        ("S1006", "C002", "2026-08-20", 5000),
        ("S1007", "C002", "2026-08-20", 7200),    # 日期差异（仓内 08-20，ERP 05-11）
        ("S1008", "C002", "2026-08-20", 7300),
        ("S1009", "C003", "2026-08-20", 4200),    # 仓内多出
    ]
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        for sid, cid, txn, amt in rows:
            conn.execute(
                "INSERT INTO sales (sale_id, customer_id, txn_date, amount_cents, "
                "region_version, region, unattributed_reason, imported_at) "
                "VALUES (?,?,?,?,0,NULL,NULL,?)",
                (sid, cid, txn, amt, utcnow()))
        conn.execute("COMMIT")
    finally:
        conn.close()


def erp_pages() -> dict:
    sales = [
        {"sale_id": "S1001", "customer_id": "C001", "txn_date": "2026-08-20", "amount_cents": 12000},
        {"sale_id": "S1002", "customer_id": "C001", "txn_date": "2026-08-20", "amount_cents": 8950},
        {"sale_id": "S1003", "customer_id": "C001", "txn_date": "2026-08-20", "amount_cents": 15000},
        {"sale_id": "S1004", "customer_id": "C001", "txn_date": "2026-08-20", "amount_cents": 23050},
        {"sale_id": "S1005", "customer_id": "C009", "txn_date": "2026-08-20", "amount_cents": 9900},
        {"sale_id": "S1006", "customer_id": "C002", "txn_date": "2026-08-20", "amount_cents": 5000},
        {"sale_id": "S1007", "customer_id": "C002", "txn_date": "2026-05-11", "amount_cents": 7200},
        {"sale_id": "S1008", "customer_id": "C002", "txn_date": "2026-08-20", "amount_cents": 7300},
        {"sale_id": "E9001", "customer_id": "C001", "txn_date": "2026-08-20", "amount_cents": 4100},
        {"sale_id": "E9002", "customer_id": "C002", "txn_date": "2026-08-20", "amount_cents": 5200},
    ]
    ps = 3
    out = {}
    for off in range(0, len(sales), ps):
        chunk = sales[off:off + ps]
        nxt = off + len(chunk)
        out[off] = {"cursor": off, "items": chunk,
                    "next_cursor": nxt if nxt < len(sales) else None}
    return out


class FakeFetcher:
    def __init__(self, pages, fail_offsets=(), permanent=False, delay=0.0):
        self.pages, self.fail_offsets, self.permanent, self.delay = pages, set(fail_offsets), permanent, delay
        self.calls, self.lock = [], threading.Lock()

    def __call__(self, base_url, snapshot_id, cursor, resume=False):
        with self.lock:
            self.calls.append((cursor, resume))
        if self.delay:
            time.sleep(self.delay)
        if cursor in self.fail_offsets and not (resume and not self.permanent):
            raise recon.PageFetchError(f"模拟坏页 cursor={cursor}")
        return self.pages[cursor]


def wait(job_id, statuses, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = recon.get_job_detail(job_id)
        if d["status"] in statuses:
            return d
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} 未到达 {statuses}")


def fresh_db() -> None:
    """每个用例独立数据库，避免任务幂等命中与数据串扰。"""
    path = os.path.join(_TMP, f"case_{fresh_db._n}.db")
    fresh_db._n += 1
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    os.environ["SALES_DB"] = path
    init_db()


fresh_db._n = 0


def launch(job_id, fetcher):
    out = recon.try_resume(job_id, fetcher)
    # 等后台守护线程结束
    deadline = time.time() + 15
    while time.time() < deadline:
        d = recon.get_job_detail(job_id)
        if d["status"] in ("succeeded", "failed"):
            return out, d
        time.sleep(0.02)
    raise AssertionError("任务超时未结束")


def case1_happy_path():
    print("\n[1] 正常取完 → 三类差异 + 双方原值")
    fresh_db(); seed_sales()
    fake = FakeFetcher(erp_pages())
    acc = recon.accept_job("2026-08-20", "snap-1")
    _, d = launch(acc["job_id"], fake)
    check("任务 succeeded", d["status"] == "succeeded")
    check("取到 10 源行 / 4 页", d["source_rows"] == 10 and d["pages_fetched"] == 4)
    check("漏单 2（E9001/E9002）", d["counts"]["missing_in_warehouse"] == 2)
    check("多单 1（S1009）", d["counts"]["extra_in_warehouse"] == 1)
    check("字段差异 3（金额/客户/日期）", d["counts"]["field_diff"] == 3)
    res = recon.get_results(acc["job_id"])["items"]
    by = {(x["kind"], x["sale_id"], x["field_name"]): x for x in res}
    check("漏单附 ERP 原值", by[("missing_in_warehouse", "E9001", None)]["erp_raw"]["amount_cents"] == 4100)
    check("多单附仓内原值", by[("extra_in_warehouse", "S1009", None)]["warehouse_raw"]["customer_id"] == "C003")
    da = by[("field_diff", "S1002", "amount_cents")]
    check("金额差异双方原值 8950 vs 8800", da["erp_value"] == "8950" and da["warehouse_value"] == "8800")
    dc = by[("field_diff", "S1005", "customer_id")]
    check("客户差异双方原值 C009 vs C001", dc["erp_value"] == "C009" and dc["warehouse_value"] == "C001")
    dd = by[("field_diff", "S1007", "txn_date")]
    check("日期差异双方原值", dd["erp_value"] == "2026-05-11" and dd["warehouse_value"] == "2026-08-20")


def case2_frozen_snapshot():
    print("\n[2] 受理冻结仓内副本，失败后新增导入不影响对账")
    fresh_db(); seed_sales()
    fake = FakeFetcher(erp_pages(), fail_offsets=(6,))
    acc = recon.accept_job("2026-08-20", "snap-freeze")
    jid = acc["job_id"]
    _, d = launch(jid, fake)
    check("断流后 failed 且可续跑", d["status"] == "failed" and d["resumable"])
    # 失败后新增一条该日销售
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO sales (sale_id, customer_id, txn_date, amount_cents, "
            "region_version, region, unattributed_reason, imported_at) "
            "VALUES ('LATE1','C001','2026-08-20',1,0,NULL,NULL,?)", (utcnow(),))
        conn.execute("COMMIT")
    finally:
        conn.close()
    fake.fail_offsets.clear()
    _, d = launch(jid, fake)
    check("续跑成功", d["status"] == "succeeded")
    check("冻结副本仍为 9 行（LATE1 不入副本）", d["warehouse_rows"] == 9)
    extras = [x["sale_id"] for x in recon.get_results(jid, "extra_in_warehouse")["items"]]
    check("多单仍只有 S1009", extras == ["S1009"], str(extras))


def case3_resume_cursor():
    print("\n[3] 中页断流：无半页、游标续跑、resume 标记")
    fresh_db(); seed_sales()
    fake = FakeFetcher(erp_pages(), fail_offsets=(3,))
    acc = recon.accept_job("2026-08-20", "snap-broken")
    jid = acc["job_id"]
    _, d = launch(jid, fake)
    check("失败时仅提交完整第 1 页（3 行，游标 3）",
          d["pages_fetched"] == 1 and d["cursor"] == 3 and d["source_rows"] == 3)
    try:
        recon.get_results(jid)
        check("失败时禁止查看结论", False)
    except HTTPException as e:
        check("失败时禁止查看结论(409 not_published)",
              e.status_code == 409 and e.detail["error"] == "not_published")
    fake.fail_offsets.clear()
    _, d = launch(jid, fake)
    check("续跑后取满 10 行 4 页", d["source_rows"] == 10 and d["pages_fetched"] == 4 and d["cursor"] == 10)
    check("失败页带 resume=1 重试", (3, True) in fake.calls)
    check("首页只取一次", sum(1 for c, _ in fake.calls if c == 0) == 1)


def case4_permanent_bad():
    print("\n[4] 永久坏页：反复续跑也不发布、结论表为空")
    fresh_db(); seed_sales()
    fake = FakeFetcher(erp_pages(), fail_offsets=(3,), permanent=True)
    acc = recon.accept_job("2026-08-20", "snapshot-bad")
    jid = acc["job_id"]
    launch(jid, fake)
    for _ in range(2):
        launch(jid, fake)
    d = recon.get_job_detail(jid)
    check("始终 failed", d["status"] == "failed")
    conn = get_conn()
    try:
        n = conn.execute("SELECT COUNT(*) AS c FROM recon_result WHERE job_id=?", (jid,)).fetchone()["c"]
    finally:
        conn.close()
    check("recon_result 零行（不完整绝不发布）", n == 0, f"实际 {n}")


def case5_concurrent():
    print("\n[5] 同任务并发续跑：跨线程只执行一份")
    fresh_db(); seed_sales()
    fake = FakeFetcher(erp_pages(), fail_offsets=(3,), delay=0.05)
    acc = recon.accept_job("2026-08-20", "snap-conc")
    jid = acc["job_id"]
    launch(jid, fake)
    fake.fail_offsets.clear()
    barrier = threading.Barrier(2)
    outs = []

    def resume():
        barrier.wait()
        outs.append(recon.try_resume(jid, fake))

    t1, t2 = threading.Thread(target=resume), threading.Thread(target=resume)
    t1.start(); t2.start(); t1.join(); t2.join()
    check("一个 accepted 一个被拒", sorted(o["accepted"] for o in outs) == [False, True], str(outs))
    d = wait(jid, {"succeeded"})
    check("任务成功且 10 行", d["status"] == "succeeded" and d["source_rows"] == 10)
    conn = get_conn()
    try:
        n = conn.execute("SELECT COUNT(*) AS c FROM recon_source_row WHERE job_id=?", (jid,)).fetchone()["c"]
    finally:
        conn.close()
    check("源行无重复写入", n == 10, f"实际 {n}")


def case6_restart():
    print("\n[6] 进程中断重启：running → failed，进度保留，可续跑")
    fresh_db(); seed_sales()
    fake = FakeFetcher(erp_pages(), fail_offsets=(6,))
    acc = recon.accept_job("2026-08-20", "snap-restart")
    jid = acc["job_id"]
    launch(jid, fake)
    # 手动把任务置 running 并上锁，模拟进程被 kill 在取数中
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE recon_job SET status='running' WHERE id=?", (jid,))
        conn.execute("INSERT OR REPLACE INTO recon_mutex (job_id, host, locked_at) VALUES (?,?,?)",
                     (jid, "ghost", utcnow()))
        conn.execute("COMMIT")
    finally:
        conn.close()
    init_db()  # 启动恢复
    conn = get_conn()
    try:
        st = conn.execute("SELECT status FROM recon_job WHERE id=?", (jid,)).fetchone()["status"]
        lk = conn.execute("SELECT COUNT(*) AS c FROM recon_mutex WHERE job_id=?", (jid,)).fetchone()["c"]
        rows = conn.execute("SELECT COUNT(*) AS c FROM recon_source_row WHERE job_id=?", (jid,)).fetchone()["c"]
    finally:
        conn.close()
    check("残留 running 被恢复为 failed", st == "failed", st)
    check("互斥锁已释放", lk == 0)
    check("已取 6 行完整保留", rows == 6, str(rows))
    fake.fail_offsets.clear()
    _, d = launch(jid, fake)
    check("恢复后续跑成功取满", d["status"] == "succeeded" and d["source_rows"] == 10)


def case7_sales_untouched_and_dedup():
    print("\n[7] 对账不修改 sales；重复受理幂等")
    fresh_db(); seed_sales()
    fake = FakeFetcher(erp_pages())
    a1 = recon.accept_job("2026-08-20", "snap-dup")
    a2 = recon.accept_job("2026-08-20", "snap-dup")
    check("同交易日+快照幂等命中", a2["deduped"] and a2["job_id"] == a1["job_id"])
    launch(a1["job_id"], fake)
    conn = get_conn()
    try:
        amt = conn.execute("SELECT amount_cents FROM sales WHERE sale_id='S1002'").fetchone()["amount_cents"]
        cnt = conn.execute("SELECT COUNT(*) AS c FROM sales").fetchone()["c"]
    finally:
        conn.close()
    check("S1002 仓内金额保持 8800 未被改写", amt == 8800, str(amt))
    check("sales 行数保持 9 行未增减", cnt == 9, str(cnt))


def case8_bad_page_validation():
    print("\n[8] 坏页（结构非法/金额非整数）按失败处理、不入库")
    fresh_db(); seed_sales()
    pages = erp_pages()
    bad = dict(pages)
    # 第 2 页（游标 3）结构非法：交易日格式错误
    bad[3] = {"cursor": 3, "items": [
        {"sale_id": "X1", "customer_id": "C1", "txn_date": "2026-08-20", "amount_cents": 100},
        {"sale_id": "X2", "customer_id": "C1", "txn_date": "bad-date", "amount_cents": 200}],
        "next_cursor": 6}
    fake = FakeFetcher(bad)
    acc = recon.accept_job("2026-08-20", "snap-bad-struct")
    jid = acc["job_id"]
    _, d = launch(jid, fake)
    check("结构非法 → failed", d["status"] == "failed")
    check("停在第 1 页之后游标 3", d["cursor"] == 3 and d["source_rows"] == 3)
    # 修正后可续跑
    fake.pages = erp_pages()
    _, d = launch(jid, fake)
    check("修正后续跑成功", d["status"] == "succeeded" and d["source_rows"] == 10)


def main() -> int:
    for fn in (case1_happy_path, case2_frozen_snapshot, case3_resume_cursor,
               case4_permanent_bad, case5_concurrent, case6_restart,
               case7_sales_untouched_and_dedup, case8_bad_page_validation):
        fn()
    print(f"\n==== 结果：{PASS} 通过，{FAIL} 失败 ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
