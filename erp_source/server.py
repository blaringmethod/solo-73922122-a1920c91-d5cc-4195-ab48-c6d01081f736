"""本地 ERP 分页快照源（仅标准库，供 docker compose 演示对账）。

设计目标
--------
- 快照内容不变：同一 snapshot_id 的分页结果完全确定，重复请求/续跑拿到相同数据。
- HTTP 分页：游标式，GET /snapshots/{snapshot_id}/sales?cursor=...
  返回 {items: [...], next_cursor: ...}，next_cursor 为 null 表示取完。
- 支持「中页断流」与「延迟」的故障注入，验证对账任务的可续跑语义：
  * fail_after_pages=N：前 N 页正常，第 N+1 页返回 500（坏页）；收到带
    resume=1 的请求后不再故障，模拟人工确认后源端恢复，任务可续跑。
  * delay_ms：每页固定延迟，模拟网络等待（Web 进程此时不持数据库写锁）。
  * reset：POST /control/reset 清除一次性故障状态，方便重复演示。
- 另有一个稳定坏页快照 snapshot-bad：永远在第 2 页返回 500，用于验证
  「取数失败不得发布不完整结论」。

环境变量
--------
ERP_PAGE_SIZE   每页条数（默认 5）
ERP_DELAY_MS    每页延迟毫秒（默认 300，compose 中可配）
ERP_PORT        监听端口（默认 9000）
"""
from __future__ import annotations

import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PAGE_SIZE = int(os.environ.get("ERP_PAGE_SIZE", "5"))
DELAY_MS = int(os.environ.get("ERP_DELAY_MS", "300"))
PORT = int(os.environ.get("ERP_PORT", "9000"))

# ---------------------------------------------------------------------------
# 内置快照：与仓内样例销售故意制造三类差异
#   - 仓内缺失（ERP 有、仓内没有）：E9001 / E9002
#   - 仓内多出（仓内有、ERP 没有）：S1009 / S1010（C003 两笔不在快照里）
#   - 同 ID 字段差异：
#       S1002 金额 8800 -> 8950；S1005 客户 C001 -> C009；
#       S1007 交易日 2026-05-01 -> 2026-05-11
# 交易日统一为 2026-08-20，便于与仓内按日副本对齐。
# ---------------------------------------------------------------------------
DEMO_SNAPSHOT_ID = "snap-demo-20260820"
DEMO_TXN_DATE = "2026-08-20"

DEMO_SALES = [
    {"sale_id": "S1001", "customer_id": "C001", "txn_date": DEMO_TXN_DATE, "amount_cents": 12000},
    {"sale_id": "S1002", "customer_id": "C001", "txn_date": DEMO_TXN_DATE, "amount_cents": 8950},
    {"sale_id": "S1003", "customer_id": "C001", "txn_date": DEMO_TXN_DATE, "amount_cents": 15000},
    {"sale_id": "S1004", "customer_id": "C001", "txn_date": DEMO_TXN_DATE, "amount_cents": 23050},
    {"sale_id": "S1005", "customer_id": "C009", "txn_date": DEMO_TXN_DATE, "amount_cents": 9900},
    {"sale_id": "S1006", "customer_id": "C002", "txn_date": DEMO_TXN_DATE, "amount_cents": 5000},
    {"sale_id": "S1007", "customer_id": "C002", "txn_date": "2026-05-11", "amount_cents": 7200},
    {"sale_id": "S1008", "customer_id": "C002", "txn_date": DEMO_TXN_DATE, "amount_cents": 7300},
    {"sale_id": "S1010", "customer_id": "C003", "txn_date": DEMO_TXN_DATE, "amount_cents": 6800},
    {"sale_id": "E9001", "customer_id": "C001", "txn_date": DEMO_TXN_DATE, "amount_cents": 4100},
    {"sale_id": "E9002", "customer_id": "C002", "txn_date": DEMO_TXN_DATE, "amount_cents": 5200},
]

# 可通过环境变量追加自定义快照：JSON 字符串
#   {"snap-x": {"txn_date": "...", "sales": [...]}}
_EXTRA = json.loads(os.environ.get("ERP_SNAPSHOTS", "{}"))

# 永久坏页快照 ID：第 2 页（offset=PAGE_SIZE）永远 500，即使带 resume 也不恢复
PERMANENT_BAD = "snapshot-bad"

SNAPSHOTS: dict[str, dict] = {
    DEMO_SNAPSHOT_ID: {"txn_date": DEMO_TXN_DATE, "sales": DEMO_SALES},
    # 永久坏页快照：内容与演示快照相同，但第 2 页始终 500（见 _page_for），
    # 用于验证「取数失败不得发布不完整结论、可反复续跑」。
    PERMANENT_BAD: {"txn_date": DEMO_TXN_DATE, "sales": list(DEMO_SALES)},
    **_EXTRA,
}

# 一次性故障状态（内存态，重启即恢复）：
#   armed[(snapshot_id)] = fail_after_pages
#   tripped 集合记录已触发过的快照；带 resume=1 的请求放行
_armed: dict[str, int] = {}
_tripped: set[str] = set()


def _page_for(snapshot_id: str, offset: int, resume: bool) -> tuple[int, dict]:
    snap = SNAPSHOTS.get(snapshot_id)
    if snap is None:
        return 404, {"error": "snapshot_not_found",
                     "message": f"快照 {snapshot_id} 不存在"}
    sales = snap["sales"]
    if offset < 0 or offset > len(sales):
        return 400, {"error": "bad_cursor", "message": f"游标越界：{offset}"}

    # 永久坏页：第 2 页始终 500，不受 resume 影响（验证不完整绝不发布）
    if snapshot_id == PERMANENT_BAD and offset == PAGE_SIZE:
        return 500, {"error": "bad_page",
                     "message": "模拟永久坏页（第 2 页），resume 也无法恢复，用于验证不完整不发布"}

    # 一次性中页断流
    if not resume and snapshot_id not in _tripped and snapshot_id in _armed:
        fail_after = _armed[snapshot_id]
        # 第 fail_after+1 页（offset = fail_after*PAGE_SIZE）触发
        if offset == fail_after * PAGE_SIZE:
            _tripped.add(snapshot_id)
            return 500, {"error": "stream_broken",
                         "message": f"模拟中页断流：已成功 {fail_after} 页，本页失败；"
                                    f"携带 resume=1 续跑可恢复"}

    if DELAY_MS:
        time.sleep(DELAY_MS / 1000.0)

    items = sales[offset:offset + PAGE_SIZE]
    next_offset = offset + len(items)
    return 200, {
        "snapshot_id": snapshot_id,
        "txn_date": snap["txn_date"],
        "cursor": offset,
        "items": items,
        "next_cursor": next_offset if next_offset < len(sales) else None,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "LocalERP/1.0"

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:  # 简洁日志
        print(f"[erp] {self.address_string()} {fmt % args}", flush=True)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        qs = parse_qs(parsed.query)
        # /snapshots -> 列表
        if parts == ["snapshots"]:
            self._send(200, {"snapshots": [
                {"snapshot_id": sid, "txn_date": s["txn_date"], "rows": len(s["sales"])}
                for sid, s in sorted(SNAPSHOTS.items())]})
            return
        # /snapshots/{id}/sales?cursor=0&resume=1
        if len(parts) == 3 and parts[0] == "snapshots" and parts[2] == "sales":
            try:
                offset = int(qs.get("cursor", ["0"])[0])
            except ValueError:
                self._send(400, {"error": "bad_cursor", "message": "cursor 必须是非负整数"})
                return
            resume = qs.get("resume", ["0"])[0] in ("1", "true", "yes")
            status, payload = _page_for(parts[1], offset, resume)
            self._send(status, payload)
            return
        self._send(404, {"error": "not_found", "path": parsed.path})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        # /control/fail?snapshot_id=...&after_pages=1  注入一次性断流
        if parts == ["control", "fail"]:
            qs = parse_qs(parsed.query)
            sid = qs.get("snapshot_id", [DEMO_SNAPSHOT_ID])[0]
            if sid not in SNAPSHOTS:
                self._send(404, {"error": "snapshot_not_found"})
                return
            after = int(qs.get("after_pages", ["1"])[0])
            _armed[sid] = after
            _tripped.discard(sid)
            self._send(200, {"armed": True, "snapshot_id": sid,
                             "fail_after_pages": after,
                             "hint": "故障页携带 resume=1 即恢复"})
            return
        # /control/reset 清除故障注入
        if parts == ["control", "reset"]:
            _armed.clear()
            _tripped.clear()
            self._send(200, {"reset": True})
            return
        self._send(404, {"error": "not_found", "path": parsed.path})


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[erp] 本地 ERP 分页源启动 :{PORT}，页大小 {PAGE_SIZE}，每页延迟 {DELAY_MS}ms",
          flush=True)
    print(f"[erp] 内置快照：{sorted(SNAPSHOTS)}（含永久坏页 {PERMANENT_BAD}）", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
