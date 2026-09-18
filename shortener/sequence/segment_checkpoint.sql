-- 发号器号段 checkpoint 表
-- 记录每个业务(biz_tag)已持久化分配的最大号，作为服务重启后恢复的安全下界。
-- 由号段发号器在"申请号段时"写入：Redis INCRBY 拿到号段终点后、备用号段激活前写入，
-- 保证 max_id >= 任何已实际发放的号，崩溃恢复只跳号、绝不重发。
CREATE TABLE `sequence_checkpoint` (
  `biz_tag`   VARCHAR(64) NOT NULL COMMENT '业务标识',
  `max_id`    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '已持久化的最大号(安全下界)',
  `step`      INT UNSIGNED NOT NULL DEFAULT 10000 COMMENT '号段长度',
  `update_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`biz_tag`)
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COMMENT='发号器号段checkpoint';
