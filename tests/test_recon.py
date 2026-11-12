"""ERP 日结对账端到端测试。

ERP HTTP 源用可编排的假取页函数替代（注入到 recon 模块），
覆盖：正常三类差异、受理后冻结仓内副本、断流续跑、坏页不发布、
同任务并发只执行一份、重启恢复、结论附双方原值且不修改 sales。
"""
import os
import threading
import time

import pytest

_TMP_DB = os.path.join(os.path.dirname(__file__), "_test_recon.db")
os.environ["SALES_DB"] = _TMP_DB
os.environ["ERP_SOURCE_URL"] = "http://fake-erp.test"

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app import recon, service  # noqa: E402


@pytest.fixture()
def client():
    for suffix in ("", "-wal", "-shm"):
        p = _TMP_DB + suffix
        if os.path.exists(p):
            os.remove(p)
    with TestClient(app) as c:
        yield c


def _seed_warehouse(client):
    """仓内该日销售：与 ERP 快照构造漏单/多单/字段差异。"""
    rows = [
        # 与 ERP 完全一致
        ("S1001", "C001", "2026-08-20", 12000),
        # 金额差异：仓内 8800，ERP 8950
        ("S1002", "C001", "2026-08-20", 8800),
        ("S1003", "C001", "2026-08-20", 15000),
        ("S1004", "C001", "2026-08-20", 23050),
        # 客户差异：仓内 C001，ERP C009
        ("S1005", "C001", "2026-08-20", 9900),
        ("S1006", "C002", "2026-08-20", 5000),
        # 交易日差异：仓内 2026-08-20，ERP 2026-05-11（同 ID 字段比对）
        ("S1007", "C002", "2026-08-20", 7200),
        ("S1008", "C002", "2026-08-20", 7300),
        # 仓内多出（ERP 无此 ID）
        ("S1009", "C003", "2026-08-20", 4200),
    ]
    client.post("/api/sales/import-json", json={"rows": [
        {"sale_id": r[0], "customer_id": r[1], "txn_date": r[2], "amount_cents": r[3]}
        for r in rows]})


def _erp_pages():
    """模拟 ERP 分页数据（页大小 3），含仓内缺失 E9001/E9002。"""
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
    page_size = 3
    pages = {}
    for off in range(0, len(sales), page_size):
        chunk = sales[off:off + page_size]
        nxt = off + len(chunk)
        pages[off] = {"cursor": off, "items": chunk,
                      "next_cursor": nxt if nxt < len(sales) else None}
    return pages


class FakeFetcher:
    """可编排假源：fail_offsets 中的游标抛坏页；resume 后放行；可加延迟。"""
    def __init__(self, pages, fail_offsets=(), permanent=False, delay=0.0):
        self.pages = pages
        self.fail_offsets = set(fail_offsets)
        self.permanent = permanent
        self.delay = delay
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, base_url, snapshot_id, cursor, resume):
        with self.lock:
            self.calls.append((cursor, resume))
        if self.delay:
            time.sleep(self.delay)
        if cursor in self.fail_offsets and not (resume and not self.permanent):
            raise recon.PageFetchError(f"模拟坏页 cursor={cursor}", retryable=True)
        return self.pages[cursor]


def _wait_status(client, job_id, expected, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = client.get(f"/api/recon/jobs/{job_id}").json()
        if d["status"] in expected:
            return d
        time.sleep(0.03)
    raise AssertionError(f"job {job_id} 未到达 {expected}，实际 {d['status']}")


def test_happy_path_three_diff_kinds_with_raw_values(client, monkeypatch):
    _seed_warehouse(client)
    fake = FakeFetcher(_erp_pages())
    monkeypatch.setattr(recon, "http_fetch_page", fake)

    r = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08-20", "erp_snapshot_id": "snap-1"})
    assert r.status_code == 200
    job_id = r.json()["job_id"]
    d = _wait_status(client, job_id, {"succeeded"})
    assert d["source_rows"] == 10 and d["pages_fetched"] == 4
    assert d["counts"]["missing_in_warehouse"] == 2   # E9001 E9002
    assert d["counts"]["extra_in_warehouse"] == 1     # S1009
    assert d["counts"]["field_diff"] == 3             # S1002 金额 + S1005 客户 + S1007 日期

    res = client.get(f"/api/recon/jobs/{job_id}/results").json()["items"]
    by = {(x["kind"], x["sale_id"], x["field_name"]): x for x in res}
    # 漏单附 ERP 原值
    m = by[("missing_in_warehouse", "E9001", None)]
    assert m["erp_raw"]["amount_cents"] == 4100 and m["warehouse_raw"] is None
    # 多单附仓内原值
    e = by[("extra_in_warehouse", "S1009", None)]
    assert e["warehouse_raw"]["customer_id"] == "C003" and e["erp_raw"] is None
    # 字段差异双方原值
    diff_amount = by[("field_diff", "S1002", "amount_cents")]
    assert diff_amount["erp_value"] == "8950" and diff_amount["warehouse_value"] == "8800"
    diff_cust = by[("field_diff", "S1005", "customer_id")]
    assert diff_cust["erp_value"] == "C009" and diff_cust["warehouse_value"] == "C001"
    diff_date = by[("field_diff", "S1007", "txn_date")]
    assert diff_date["erp_value"] == "2026-05-11" and diff_date["warehouse_value"] == "2026-08-20"


def test_warehouse_snapshot_frozen_after_accept(client, monkeypatch):
    _seed_warehouse(client)
    fake = FakeFetcher(_erp_pages(), fail_offsets=(6,))  # 第 3 页先失败
    monkeypatch.setattr(recon, "http_fetch_page", fake)

    r = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08-20", "erp_snapshot_id": "snap-freeze"})
    job_id = r.json()["job_id"]
    _wait_status(client, job_id, {"failed"})

    # 取数失败后：再导入一条该日销售 + 做地区回填，均不得改变冻结副本
    client.post("/api/sales/import-json", json={"rows": [
        {"sale_id": "LATE1", "customer_id": "C001", "txn_date": "2026-08-20",
         "amount_cents": 1}]})
    client.post("/api/corrections", json={
        "customer_id": "C001", "expected_version": 0,
        "entries": [{"region": "华东", "effective_from": "2026-01-01",
                     "effective_to": None}], "backfill_delay_ms": 0})
    # 等更正结束
    deadline = time.time() + 15
    while time.time() < deadline:
        jobs = client.get("/api/corrections").json()["jobs"]
        if jobs and all(j["status"] in ("succeeded", "failed") for j in jobs):
            break
        time.sleep(0.05)

    # 源端恢复后续跑
    fake.fail_offsets.clear()
    rr = client.post(f"/api/recon/jobs/{job_id}/resume")
    assert rr.json()["accepted"] is True
    d = _wait_status(client, job_id, {"succeeded"})
    # 冻结副本仍是受理时的 9 行，LATE1 不进副本；多单仍只有 S1009
    assert d["warehouse_rows"] == 9
    res = client.get(f"/api/recon/jobs/{job_id}/results",
                     params={"kind": "extra_in_warehouse"}).json()["items"]
    assert [x["sale_id"] for x in res] == ["S1009"]


def test_broken_stream_keeps_progress_and_resumes_from_cursor(client, monkeypatch):
    _seed_warehouse(client)
    pages = _erp_pages()
    fake = FakeFetcher(pages, fail_offsets=(3,))  # 第 2 页（游标 3）断流
    monkeypatch.setattr(recon, "http_fetch_page", fake)

    r = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08-20", "erp_snapshot_id": "snap-broken"})
    job_id = r.json()["job_id"]
    d = _wait_status(client, job_id, {"failed"})
    # 第一页（3 行）已完整提交，游标停在 3，无半页
    assert d["pages_fetched"] == 1 and d["cursor"] == 3 and d["source_rows"] == 3
    assert d["resumable"] is True and "可续跑" in d["error"]
    # 失败时结论禁止查看
    rr = client.get(f"/api/recon/jobs/{job_id}/results")
    assert rr.status_code == 409 and rr.json()["detail"]["error"] == "not_published"

    # 源端恢复：续跑从游标 3 开始，且对失败页带 resume=1
    fake.fail_offsets.clear()
    client.post(f"/api/recon/jobs/{job_id}/resume")
    d = _wait_status(client, job_id, {"succeeded"})
    assert d["cursor"] == 10 and d["source_rows"] == 10 and d["pages_fetched"] == 4
    # 失败页以 resume 重试；第一页只请求一次，无重复取页
    assert (3, True) in fake.calls
    assert sum(1 for c, _ in fake.calls if c == 0) == 1


def test_permanent_bad_page_never_publishes(client, monkeypatch):
    _seed_warehouse(client)
    fake = FakeFetcher(_erp_pages(), fail_offsets=(3,), permanent=True)
    monkeypatch.setattr(recon, "http_fetch_page", fake)

    r = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08-20", "erp_snapshot_id": "snapshot-bad"})
    job_id = r.json()["job_id"]
    _wait_status(client, job_id, {"failed"})
    # 多次续跑都失败，始终不发布
    for _ in range(2):
        client.post(f"/api/recon/jobs/{job_id}/resume")
        _wait_status(client, job_id, {"failed"})
    rr = client.get(f"/api/recon/jobs/{job_id}/results")
    assert rr.status_code == 409
    # 结论表始终为空
    from app.db import get_conn
    conn = get_conn()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM recon_result WHERE job_id=?", (job_id,)
        ).fetchone()["c"] == 0
    finally:
        conn.close()


def test_concurrent_resume_only_one_runs(client, monkeypatch):
    _seed_warehouse(client)
    # 每页加延迟，制造续跑并发窗口
    fake = FakeFetcher(_erp_pages(), fail_offsets=(3,), delay=0.05)
    monkeypatch.setattr(recon, "http_fetch_page", fake)
    r = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08-20", "erp_snapshot_id": "snap-conc"})
    job_id = r.json()["job_id"]
    _wait_status(client, job_id, {"failed"})
    fake.fail_offsets.clear()

    barrier = threading.Barrier(2)
    outs = []

    def resume():
        barrier.wait()
        outs.append(client.post(f"/api/recon/jobs/{job_id}/resume").json())

    t1 = threading.Thread(target=resume)
    t2 = threading.Thread(target=resume)
    t1.start(); t2.start(); t1.join(); t2.join()
    accepted = sorted(o["accepted"] for o in outs)
    assert accepted == [False, True]
    d = _wait_status(client, job_id, {"succeeded"})
    assert d["status"] == "succeeded" and d["source_rows"] == 10
    # 数据没有被两个执行者重复插入（主键幂等 + 单实例）
    from app.db import get_conn
    conn = get_conn()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM recon_source_row WHERE job_id=?", (job_id,)
        ).fetchone()["c"] == 10
    finally:
        conn.close()


def test_restart_recovers_interrupted_job_and_resumes(client, monkeypatch):
    _seed_warehouse(client)
    fake = FakeFetcher(_erp_pages(), fail_offsets=(6,))
    monkeypatch.setattr(recon, "http_fetch_page", fake)
    r = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08-20", "erp_snapshot_id": "snap-restart"})
    job_id = r.json()["job_id"]
    d = _wait_status(client, job_id, {"failed"})
    assert d["cursor"] == 6  # 前两页已提交

    # 模拟进程重启：init_db 恢复逻辑把残留 running/pending 转 failed 并清锁
    from app.db import init_db, get_conn
    init_db()
    conn = get_conn()
    try:
        row = conn.execute("SELECT status FROM recon_job WHERE id=?", (job_id,)).fetchone()
        assert row["status"] == "failed"
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM recon_mutex WHERE job_id=?", (job_id,)
        ).fetchone()["c"] == 0
        # 已取页面与游标保留
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM recon_source_row WHERE job_id=?", (job_id,)
        ).fetchone()["c"] == 6
    finally:
        conn.close()

    fake.fail_offsets.clear()
    client.post(f"/api/recon/jobs/{job_id}/resume")
    d = _wait_status(client, job_id, {"succeeded"})
    assert d["source_rows"] == 10


def test_dedup_accept_and_sales_never_modified(client, monkeypatch):
    _seed_warehouse(client)
    fake = FakeFetcher(_erp_pages())
    monkeypatch.setattr(recon, "http_fetch_page", fake)

    r1 = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08-20", "erp_snapshot_id": "snap-dup"})
    j1 = r1.json()["job_id"]
    r2 = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08-20", "erp_snapshot_id": "snap-dup"})
    assert r2.json()["deduped"] is True and r2.json()["job_id"] == j1
    _wait_status(client, j1, {"succeeded"})

    # 对账全过程不修改 sales：金额差异的 S1002 仓内值保持 8800
    s = client.get("/api/sales/S1002").json()
    assert s["amount_cents"] == 8800


def test_invalid_inputs_rejected(client):
    r = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08", "erp_snapshot_id": "x"})
    assert r.status_code == 400
    r = client.post("/api/recon/jobs", json={
        "txn_date": "2026-08-20", "erp_snapshot_id": "  "})
    assert r.status_code == 422  # pydantic min_length
