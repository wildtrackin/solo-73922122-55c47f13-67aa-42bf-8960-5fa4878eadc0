-- 04 作废归档（Archive）：分析员确认作废的样本在最终切换前移出在线库并留存原值
-- 由应用首次启动时在单个事务里幂等执行（与 02_expand.sql 同一引导路径）。
--
-- 设计要点：
--   * 只在迁移阶段 ready 允许执行（应用层在事务内读取 migration_state 并加状态行锁）；
--   * 一张作废单对应一组样本，整单在一个事务里成功或一起回滚；
--   * archive_samples 留存“真正删除那一刻”的 temp/ph 原值；
--   * 删除仍走 samples 表（或切换后同名视图），双写/INSTEAD OF 触发器负责让
--     measurements 同步消失，在线两表永远不再出现这些样本；
--   * 单号唯一：同单号重试只回读首次结果，不重复删除、不触碰此后复用 id 的新样本。

CREATE TABLE IF NOT EXISTS archive_orders (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_no     TEXT NOT NULL UNIQUE,
    status       TEXT NOT NULL CHECK (status IN ('archived', 'failed')),
    sample_count INT  NOT NULL DEFAULT 0,
    detail       TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS archive_samples (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_no    TEXT NOT NULL REFERENCES archive_orders(order_no),
    sample_id   BIGINT NOT NULL,
    temp        NUMERIC(10, 2) NOT NULL,
    ph          NUMERIC(10, 2) NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- 一张单内同一被归档样本只留存一行原值
    UNIQUE (order_no, sample_id)
);

CREATE INDEX IF NOT EXISTS idx_archive_samples_order
    ON archive_samples (order_no);
