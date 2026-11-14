r"""作废样本归档（Archive）：最终切换前把分析员确认作废的样本移出在线库。

一张作废单（order_no + 一组 sample id）的全部处理都在**一个数据库事务**内：

    咨询锁(单号) -> 锁迁移状态行 -> 校验阶段=ready
                 -> 按 id 锁定并读取样本当前已提交值（FOR UPDATE）
                 -> 任一 id 不存在：整体回滚，在线数据零变化
                 -> 留存原值 -> 删除 samples（触发器级联清 measurements）
                 -> 写入作废单头并提交

并发与幂等语义：
    * pg_advisory_xact_lock 按单号串行化同号并发请求；首个事务提交后，
      后来者在同事务内读到 archive_orders 已存在的记录，原样返回首次结果，
      不会再次删除——因此此后复用该 id 的新样本绝不会被误删。
    * 对 migration_state 取 FOR UPDATE：与最终切换互斥。切换事务也要更新
      状态行，故归档进行中切换会等待；切换完成后归档读到 switched 并拒绝。
    * 对 samples 取 FOR UPDATE：旧采集程序并发的 UPDATE/DELETE 会被阻塞到
      归档事务提交，归档留存的 temp/ph 与真正删除的行是同一个已提交版本。
    * 删除走 samples（切换前是表+双写触发器，切换后是视图+INSTEAD OF），
      measurements 由触发器同步删除，在线两表同时不再出现这些样本。
"""
from __future__ import annotations

from fastapi import HTTPException
from psycopg.rows import dict_row

from . import db

# 咨询锁“类别”常量：本功能专用命名空间，避免与其它咨询锁冲突。
# 键由 hashtext(order_no) 派生；哈希碰撞仅造成额外等待，正确性仍由
# archive_orders.order_no 唯一约束与事务内复查保证。
_LOCK_NAMESPACE = 0x41524348  # 'ARCH'


def _dict_cursor(conn):
    return conn.cursor(row_factory=dict_row)


def get_order(conn, order_no: str) -> dict | None:
    """读取一张已提交作废单及其留存明细（不存在返回 None）。"""
    with _dict_cursor(conn) as cur:
        cur.execute(
            "SELECT order_no, status, sample_count, detail, created_at "
            "FROM archive_orders WHERE order_no = %s",
            (order_no,),
        )
        order = cur.fetchone()
        if order is None:
            return None
        cur.execute(
            "SELECT sample_id, temp, ph, archived_at "
            "FROM archive_samples WHERE order_no = %s ORDER BY sample_id",
            (order_no,),
        )
        order = dict(order)
        order["samples"] = [dict(r) for r in cur.fetchall()]
    return order


def list_orders(conn) -> list[dict]:
    with _dict_cursor(conn) as cur:
        cur.execute(
            "SELECT order_no, status, sample_count, detail, created_at "
            "FROM archive_orders ORDER BY id"
        )
        return [dict(r) for r in cur.fetchall()]


def archive_order(conn, order_no: str, sample_ids: list[int]) -> dict:
    """提交一张作废单。成功返回留存详情；任一 id 不存在则抛 404 并回滚。

    复用调用方传入的连接，但在本函数内显式管理事务边界。
    """
    if not order_no or not order_no.strip():
        raise HTTPException(422, "order_no 不能为空")
    # 去重保序：同一单内重复 id 视为一个样本
    unique_ids = list(dict.fromkeys(sample_ids))
    if not unique_ids:
        raise HTTPException(422, "sample_ids 至少包含一个样本 id")

    try:
        with conn.transaction():
            cur = conn.cursor()

            # 1) 按单号取事务级咨询锁：同单号并发提交在此串行化
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s, hashtext(%s))",
                (_LOCK_NAMESPACE, order_no),
            )

            # 2) 幂等复查：该单号已成功处理过 -> 原结果返回，不再删除任何行
            existing = get_order(conn, order_no)
            if existing is not None:
                existing["reused"] = True
                return existing

            # 3) 锁定迁移状态行并校验阶段：仅 ready 可执行，且与切换互斥
            cur.execute(
                "SELECT state FROM migration_state WHERE id = 1 FOR UPDATE"
            )
            phase = cur.fetchone()[0]
            if phase != "ready":
                raise HTTPException(
                    409,
                    f"作废归档仅在迁移阶段 ready 可执行，当前为 '{phase}'",
                )

            # 4) 锁定并读取目标样本的当前已提交值。
            #    FOR UPDATE 阻塞旧采集程序的并发写，保证留存值即真正删除的值；
            #    任一行缺失（从未存在/已被删/已被其它单归档）=> 整单失败。
            with _dict_cursor(conn) as dcur:
                dcur.execute(
                    "SELECT id, temp, ph FROM samples "
                    "WHERE id = ANY(%s) FOR UPDATE",
                    (unique_ids,),
                )
                found = {r["id"]: dict(r) for r in dcur.fetchall()}
            missing = [sid for sid in unique_ids if sid not in found]
            if missing:
                # 不写入任何归档记录：事务随异常回滚，在线数据保持原样
                raise HTTPException(
                    404,
                    f"以下样本 id 已不存在，整单失败（未做任何修改）: {missing}",
                )

            # 5) 写入作废单头与留存原值（留存的是删除前一刻的已提交版本）
            with _dict_cursor(conn) as dcur:
                dcur.execute(
                    "INSERT INTO archive_orders "
                    "(order_no, status, sample_count, detail) "
                    "VALUES (%s, 'archived', %s, %s) "
                    "RETURNING order_no, status, sample_count, detail, created_at",
                    (order_no, len(unique_ids),
                     f"archived {len(unique_ids)} samples before switch"),
                )
                header = dict(dcur.fetchone())

                dcur.executemany(
                    "INSERT INTO archive_samples (order_no, sample_id, temp, ph) "
                    "VALUES (%s, %s, %s, %s)",
                    [(order_no, sid, found[sid]["temp"], found[sid]["ph"])
                     for sid in unique_ids],
                )

            # 6) 从在线库移除：DELETE 经触发器级联清理 measurements，
            #    在线 samples 与 measurements 在同一事务里同时消失。
            cur.execute(
                "DELETE FROM samples WHERE id = ANY(%s)", (unique_ids,)
            )
            if cur.rowcount != len(unique_ids):
                # 理论上被 FOR UPDATE 挡住不可能发生；作为防御性不变量
                raise HTTPException(
                    500,
                    f"删除行数 {cur.rowcount} 与预期 {len(unique_ids)} 不符，已回滚",
                )

            header["samples"] = [
                {"sample_id": sid, "temp": found[sid]["temp"],
                 "ph": found[sid]["ph"]}
                for sid in unique_ids
            ]
            header["reused"] = False
        conn.commit()
        return header
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise


def archive_stats(conn) -> dict:
    with _dict_cursor(conn) as cur:
        cur.execute(
            "SELECT count(*) AS order_count, "
            "COALESCE(sum(sample_count), 0) AS archived_sample_count "
            "FROM archive_orders"
        )
        return dict(cur.fetchone())


def get_pool():
    """供路由层延迟取用当前连接池（与 migration 模块的动态取用风格一致）。"""
    return db.pool
