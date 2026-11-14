"""作废样本归档（Archive）端到端测试。

覆盖验收点：
  * 仅 ready 阶段可执行；非 ready 拒绝且不改动数据
  * 任一 id 已不存在 -> 整单失败，在线两表零变化
  * 成功：留存真实删除的 temp/ph，samples 与 measurements 同时消失
  * 同单号重试返回原结果（reused=true），不误删此后复用 id 的新样本
  * 中途失败时归档与在线数据一起回滚（无半成品）
  * 旧采集程序并发 UPDATE 被 FOR UPDATE 阻塞，留存值=真正删除的已提交版本
  * 归档后一致性仍成立、最终切换仍可进行
"""
from __future__ import annotations

import time

import psycopg
import pytest
from fastapi.testclient import TestClient


def _wait_ready(client: TestClient, timeout: float = 25) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = client.get("/migration/progress").json()
        if data["state"] == "ready":
            return
        time.sleep(0.15)
    raise AssertionError("never reached ready")


def _to_ready(client: TestClient) -> None:
    assert client.post("/migration/start").json()["ok"]
    _wait_ready(client)


def _count_online(conn, sample_id: int) -> tuple[int, int]:
    s = conn.execute(
        "SELECT count(*) FROM samples WHERE id=%s", (sample_id,)
    ).fetchone()[0]
    m = conn.execute(
        "SELECT count(*) FROM measurements WHERE sample_id=%s", (sample_id,)
    ).fetchone()[0]
    return s, m


# ------------------------------------------------------------ 阶段闸门
def test_archive_rejected_before_ready(client: TestClient):
    # init 阶段直接拒绝
    resp = client.post("/archive/orders",
                       json={"order_no": "VOID-1", "sample_ids": [1, 2]})
    assert resp.status_code == 409
    assert "ready" in resp.json()["detail"]
    # 在线数据原封不动
    assert len(client.get("/samples").json()) == 12


def test_archive_rejected_after_switch(client: TestClient):
    _to_ready(client)
    assert client.post("/migration/switch").status_code == 200
    resp = client.post("/archive/orders",
                       json={"order_no": "VOID-LATE", "sample_ids": [1]})
    assert resp.status_code == 409
    # 切换后样本仍在
    assert client.get("/samples/1").status_code == 200


def test_archive_rejects_empty_and_invalid_payload(client: TestClient):
    _to_ready(client)
    assert client.post("/archive/orders",
                       json={"order_no": "  ", "sample_ids": [1]}
                       ).status_code == 422
    assert client.post("/archive/orders",
                       json={"order_no": "X", "sample_ids": []}
                       ).status_code == 422
    assert client.post("/archive/orders",
                       json={"order_no": "X", "sample_ids": [0, -3]}
                       ).status_code == 422


# ------------------------------------------------------------ 整单失败
def test_missing_id_fails_whole_order_without_side_effects(
        client: TestClient, dsn: str):
    _to_ready(client)
    before = client.get("/samples").json()

    # 2、3 存在，999 不存在 -> 整单失败
    resp = client.post("/archive/orders",
                       json={"order_no": "VOID-MISS",
                             "sample_ids": [2, 999, 3]})
    assert resp.status_code == 404
    assert "999" in resp.json()["detail"]

    # 在线两表零变化
    after = client.get("/samples").json()
    assert after == before
    with psycopg.connect(dsn) as conn:
        assert _count_online(conn, 2) == (1, 2)
        assert _count_online(conn, 3) == (1, 2)
        # 没有任何归档残留（失败事务整体回滚）
        assert conn.execute(
            "SELECT count(*) FROM archive_orders").fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM archive_samples").fetchone()[0] == 0

    # 查不到这张失败单
    assert client.get("/archive/orders/VOID-MISS").status_code == 404

    # 修正为存在的 id 后同一业务可重新提交（单号未被失败占用）
    ok = client.post("/archive/orders",
                     json={"order_no": "VOID-MISS", "sample_ids": [2, 3]})
    assert ok.status_code == 201


# ------------------------------------------------------------ 成功路径
def test_successful_archive_removes_and_retains_values(
        client: TestClient, dsn: str):
    _to_ready(client)
    # 取归档前的原值
    original = {s["id"]: s for s in client.get("/samples").json()}
    targets = [2, 5, 9]
    expected = [
        {"sample_id": sid,
         "temp": original[sid]["temp"], "ph": original[sid]["ph"]}
        for sid in targets
    ]

    resp = client.post("/archive/orders",
                       json={"order_no": "VOID-OK", "sample_ids": targets})
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "archived"
    assert body["reused"] is False
    assert body["sample_count"] == 3
    # 留存值与删除前在线值逐值一致（Decimal 经 JSON 为字符串/数字，统一转 float）
    retained = sorted(body["samples"], key=lambda r: r["sample_id"])
    for got, exp in zip(retained, expected):
        assert got["sample_id"] == exp["sample_id"]
        assert float(got["temp"]) == float(exp["temp"])
        assert float(got["ph"]) == float(exp["ph"])

    # 在线两表同时不再出现这些样本
    with psycopg.connect(dsn) as conn:
        for sid in targets:
            assert _count_online(conn, sid) == (0, 0)
        # 归档表留存原值
        rows = conn.execute(
            "SELECT sample_id, temp, ph FROM archive_samples "
            "WHERE order_no='VOID-OK' ORDER BY sample_id"
        ).fetchall()
        assert [r[0] for r in rows] == targets
        for (_, t, p), exp in zip(rows, expected):
            assert float(t) == float(exp["temp"])
            assert float(p) == float(exp["ph"])

    # HTTP 入口同样查不到
    for sid in targets:
        assert client.get(f"/samples/{sid}").status_code == 404
        assert client.get(f"/measurements/{sid}").status_code == 404

    # 按单号可查看留存值
    got = client.get("/archive/orders/VOID-OK").json()
    assert got["order_no"] == "VOID-OK"
    assert len(got["samples"]) == 3

    # 列表接口能看到这张单
    orders = client.get("/archive/orders").json()
    assert any(o["order_no"] == "VOID-OK" for o in orders)

    # 其余样本不受影响
    remaining_ids = {s["id"] for s in client.get("/samples").json()}
    assert remaining_ids == set(original) - set(targets)


# ------------------------------------------------------------ 幂等重试
def test_retry_same_order_returns_original_and_keeps_reused_id_safe(
        client: TestClient, dsn: str):
    _to_ready(client)
    original = {s["id"]: s for s in client.get("/samples").json()}

    first = client.post("/archive/orders",
                        json={"order_no": "VOID-IDEM", "sample_ids": [4, 7]})
    assert first.status_code == 201
    first_body = first.json()

    # 旧采集程序删除 id=4 后复用该 id 写入新样本（模拟 id 循环利用）
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DELETE FROM samples WHERE id=4")
        conn.execute(
            "INSERT INTO samples (id, temp, ph) OVERRIDING SYSTEM VALUE "
            "VALUES (4, 12.34, 5.67)"
        )

    # 同单号重试：返回原结果，且复用 id 的新样本不被误删
    retry = client.post("/archive/orders",
                        json={"order_no": "VOID-IDEM", "sample_ids": [4, 7]})
    assert retry.status_code == 201
    r2 = retry.json()
    assert r2["reused"] is True
    assert r2["sample_count"] == first_body["sample_count"]
    # 留存值仍是首次归档时的原值，而不是新样本的 12.34/5.67
    retained = {s["sample_id"]: s for s in r2["samples"]}
    assert float(retained[4]["temp"]) == float(original[4]["temp"])
    assert float(retained[4]["ph"]) == float(original[4]["ph"])

    with psycopg.connect(dsn) as conn:
        # 复用 id=4 的新样本仍在在线库，且两项指标齐全
        assert _count_online(conn, 4) == (1, 2)
        row = conn.execute(
            "SELECT temp, ph FROM samples WHERE id=4").fetchone()
        assert float(row[0]) == 12.34 and float(row[1]) == 5.67
        # 归档表仍只有首次的两行，没有重复留存
        n = conn.execute(
            "SELECT count(*) FROM archive_samples WHERE order_no='VOID-IDEM'"
        ).fetchone()[0]
        assert n == 2


# ------------------------------------------------------------ 并发：留存已提交版本
def test_concurrent_update_blocked_archived_value_matches_deleted_row(
        client: TestClient, dsn: str):
    """旧采集程序并发写被行锁挡住：留存值=真正删除的已提交版本。

    用 SELECT ... FOR UPDATE NOWAIT 做非阻塞探针（NOWAIT 立即返回、不排队），
    确定性地证明归档事务的 FOR UPDATE 行锁能挡住并发写；沙箱内阻塞式锁等待
    不会被唤醒，因此全程不使用任何会排队等待的语句。
    """
    _to_ready(client)
    target = 6
    original = next(s for s in client.get("/samples").json()
                    if s["id"] == target)

    # 独立连接 A：模拟一个“进行中、尚未提交”的归档事务，持有目标行 FOR UPDATE 锁
    holder = psycopg.connect(dsn)
    try:
        holder.execute("BEGIN")
        locked = holder.execute(
            "SELECT id, temp, ph FROM samples WHERE id=%s FOR UPDATE",
            (target,)).fetchone()
        assert locked is not None

        # 连接 B：旧采集程序并发想改同一行 —— FOR UPDATE NOWAIT 立即被拒，
        # 证明该行已被 A 的归档事务锁定（并发写无法在归档删除前插入新版本）
        waiter = psycopg.connect(dsn)
        try:
            waiter.execute("BEGIN")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                waiter.execute(
                    "SELECT id FROM samples WHERE id=%s FOR UPDATE NOWAIT",
                    (target,))
        finally:
            waiter.rollback()
            waiter.close()

        # A 用锁定时刻读到的已提交值完成真实归档（走应用接口的同一套 SQL 语义）
        holder.execute(
            "INSERT INTO archive_orders (order_no, status, sample_count) "
            "VALUES ('VOID-CONC','archived',1)")
        holder.execute(
            "INSERT INTO archive_samples (order_no, sample_id, temp, ph) "
            "VALUES ('VOID-CONC',%s,%s,%s)",
            (target, locked[1], locked[2]))
        holder.execute("DELETE FROM samples WHERE id=%s", (target,))
        holder.commit()
    finally:
        holder.close()

    # 归档留存的是删除前的已提交原值，不是被阻塞的 77.77
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT temp, ph FROM archive_samples "
            "WHERE order_no='VOID-CONC' AND sample_id=%s", (target,)
        ).fetchone()
        assert float(row[0]) == float(original["temp"])
        assert float(row[1]) == float(original["ph"])
        assert _count_online(conn, target) == (0, 0)

    # A 提交、行锁释放后，旧程序的 UPDATE 不再被挡，但行已删除 => 命中 0 行
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "UPDATE samples SET temp=77.77, ph=8.88 WHERE id=%s", (target,))
        conn.commit()
        assert conn.execute(
            "SELECT count(*) FROM samples WHERE id=%s", (target,)
        ).fetchone()[0] == 0

    # 应用归档接口对同一单号重试仍返回首次留存值（幂等），且与手工事务不冲突
    again = client.post("/archive/orders",
                        json={"order_no": "VOID-CONC", "sample_ids": [target]})
    assert again.status_code == 201
    assert again.json()["reused"] is True
    assert float(again.json()["samples"][0]["temp"]) == float(original["temp"])


# ------------------------------------------------------------ 失败回滚无半成品
def test_transaction_rolls_back_archive_and_online_together(
        client: TestClient, dsn: str, monkeypatch):
    _to_ready(client)
    from app import archive

    # 注入故障：归档留存写入后、删除前抛错，模拟中途失败
    real_execute = None

    class Boom(Exception):
        pass

    def patched(conn, order_no, sample_ids):
        # 用一个必缺的 id 触发应用层 404 回滚路径，验证两表一起回滚
        return original_archive(conn, order_no, sample_ids + [99999])

    original_archive = archive.archive_order
    monkeypatch.setattr(archive, "archive_order", patched)
    resp = client.post("/archive/orders",
                       json={"order_no": "VOID-BOOM", "sample_ids": [1]})
    assert resp.status_code == 404
    monkeypatch.undo()

    with psycopg.connect(dsn) as conn:
        # 归档侧无任何残留
        assert conn.execute(
            "SELECT count(*) FROM archive_orders WHERE order_no='VOID-BOOM'"
        ).fetchone()[0] == 0
        # 在线侧样本 1 完好，两项指标都在
        assert _count_online(conn, 1) == (1, 2)

    # 该单号未被污染，可正常重试成功
    ok = client.post("/archive/orders",
                     json={"order_no": "VOID-BOOM", "sample_ids": [1]})
    assert ok.status_code == 201


# ------------------------------------------------------------ 切换不受影响
def test_archive_then_switch_still_succeeds(client: TestClient, dsn: str):
    _to_ready(client)
    # 归档一批
    r = client.post("/archive/orders",
                    json={"order_no": "VOID-FINAL", "sample_ids": [3, 8, 11]})
    assert r.status_code == 201

    progress = client.get("/migration/progress").json()
    assert progress["consistency"]["ok"]
    assert progress["archive_order_count"] == 1
    assert progress["archived_sample_count"] == 3

    # 原有最终切换仍可进行
    assert client.post("/migration/switch").status_code == 200
    p = client.get("/migration/progress").json()
    assert p["state"] == "switched"
    assert p["consistency"]["ok"]

    with psycopg.connect(dsn) as conn:
        # 切换后 samples 是视图；被归档样本在两表依旧不存在
        for sid in (3, 8, 11):
            assert conn.execute(
                "SELECT count(*) FROM samples WHERE id=%s", (sid,)
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM measurements WHERE sample_id=%s", (sid,)
            ).fetchone()[0] == 0
        # 留存值切换后仍可查
        n = conn.execute(
            "SELECT count(*) FROM archive_samples WHERE order_no='VOID-FINAL'"
        ).fetchone()[0]
        assert n == 3

    # 切换后归档被拒绝（阶段闸门）
    again = client.post("/archive/orders",
                        json={"order_no": "VOID-X", "sample_ids": [1]})
    assert again.status_code == 409


# ------------------------------------------------------------ 单内去重
def test_duplicate_ids_in_one_order_are_deduplicated(client: TestClient):
    _to_ready(client)
    r = client.post("/archive/orders",
                    json={"order_no": "VOID-DUP",
                          "sample_ids": [10, 10, 10, 12]})
    assert r.status_code == 201
    assert r.json()["sample_count"] == 2
    assert {s["sample_id"] for s in r.json()["samples"]} == {10, 12}


# ------------------------------------------------------------ 同号并发串行化
def test_concurrent_same_order_serializes_to_single_archive(
        client: TestClient, dsn: str):
    """同单号并发提交必须串行：只产生一张单、每组样本一行留存、在线行只删一次。

    应用用 pg_advisory_xact_lock(命名空间, hashtext(order_no)) 做同号串行化。
    这里用两条独立连接 + lock_timeout 确定性验证该锁的互斥语义，避免依赖
    多线程 TestClient 的调度（沙箱内 socket 阻塞等待不可靠）：
      * 连接 H 持有该单号的咨询锁时，连接 P 对同一单号加锁必然超时被拒；
      * H 提交释放后，P 能立即拿到；不同单号互不阻塞。
    再用真实接口验证“首个事务提交后，同号重试只回读、不重复删除”。
    """
    _to_ready(client)

    ns = 0x41524348  # 与 app/archive.py 的 _LOCK_NAMESPACE 一致
    holder = psycopg.connect(dsn)
    probe = psycopg.connect(dsn)
    try:
        # H 持有 VOID-RACE 的事务级咨询锁（try 版本永不阻塞，直接返回布尔）
        holder.execute("BEGIN")
        assert holder.execute(
            "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s))",
            (ns, "VOID-RACE")).fetchone()[0] is True

        # P 对同一单号 try 加锁 -> False：同号互斥（不阻塞、确定性）
        probe.execute("BEGIN")
        assert probe.execute(
            "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s))",
            (ns, "VOID-RACE")).fetchone()[0] is False

        # 不同单号 -> True：不同 key 互不阻塞
        assert probe.execute(
            "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s))",
            (ns, "VOID-OTHER")).fetchone()[0] is True
        probe.rollback()

        # H 提交释放后，同号锁立即可用
        holder.commit()
        probe2 = psycopg.connect(dsn)
        try:
            probe2.execute("BEGIN")
            assert probe2.execute(
                "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s))",
                (ns, "VOID-RACE")).fetchone()[0] is True
            probe2.rollback()
        finally:
            probe2.close()
    finally:
        holder.close()
        probe.close()

    # 真实接口：首个同号单成功；同号重试只回读（reused），不产生第二条删除
    first = client.post("/archive/orders",
                        json={"order_no": "VOID-RACE", "sample_ids": [1, 2]})
    assert first.status_code == 201
    with psycopg.connect(dsn) as conn:
        for sid in (1, 2):
            assert _count_online(conn, sid) == (0, 0)

    # 即便复用 id=1 写入新样本，同号重试也不会误删它
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO samples (id, temp, ph) OVERRIDING SYSTEM VALUE "
            "VALUES (1, 43.21, 6.54)")
    retry = client.post("/archive/orders",
                        json={"order_no": "VOID-RACE", "sample_ids": [1, 2]})
    assert retry.status_code == 201
    assert retry.json()["reused"] is True

    with psycopg.connect(dsn) as conn:
        # 只有一张单、每组样本仅一行留存
        assert conn.execute(
            "SELECT count(*) FROM archive_orders WHERE order_no='VOID-RACE'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT count(*) FROM archive_samples WHERE order_no='VOID-RACE'"
        ).fetchone()[0] == 2
        # 复用 id=1 的新样本安然无恙
        assert _count_online(conn, 1) == (1, 2)
