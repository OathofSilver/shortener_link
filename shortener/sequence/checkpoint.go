package sequence

import (
	"context"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// CheckpointStore 发号器号段 checkpoint 存储抽象。
// checkpoint 语义：max_id 记录"已持久化分配的最大号"，永远 >= 任何已实际发放的号，
// 服务重启后从 max_id+1 恢复只跳号、绝不重发。
// 抽象成接口便于单元测试中用 fake 实现 mock DB 故障场景。
type CheckpointStore interface {
	// Load 读取指定业务的 checkpoint；记录不存在时返回 (0, 0, nil)
	Load(ctx context.Context, bizTag string) (maxID uint64, step int, err error)
	// Save 幂等写入 checkpoint（GREATEST 保证只增不减，多实例并发写安全）
	Save(ctx context.Context, bizTag string, maxID uint64, step int) error
}

type mySQLCheckpointStore struct {
	conn sqlx.SqlConn
}

var _ CheckpointStore = (*mySQLCheckpointStore)(nil)

// NewMySQLCheckpointStore 基于 MySQL 的 checkpoint 存储
func NewMySQLCheckpointStore(conn sqlx.SqlConn) CheckpointStore {
	return &mySQLCheckpointStore{conn: conn}
}

// Save 使用 INSERT ... ON DUPLICATE KEY UPDATE + GREATEST 实现幂等只增写入：
// 多实例各自 checkpoint 自己的号段终点时，乱序到达也不会让 max_id 回退
func (m *mySQLCheckpointStore) Save(ctx context.Context, bizTag string, maxID uint64, step int) error {
	query := `insert into sequence_checkpoint (biz_tag, max_id, step) values (?, ?, ?)
		on duplicate key update
		max_id = greatest(max_id, values(max_id)),
		step = values(step)`
	_, err := m.conn.ExecCtx(ctx, query, bizTag, maxID, step)
	return err
}

func (m *mySQLCheckpointStore) Load(ctx context.Context, bizTag string) (uint64, int, error) {
	var row struct {
		MaxID uint64 `db:"max_id"`
		Step  int    `db:"step"`
	}
	query := `select max_id, step from sequence_checkpoint where biz_tag = ? limit 1`
	err := m.conn.QueryRowCtx(ctx, &row, query, bizTag)
	if err == sqlx.ErrNotFound {
		return 0, 0, nil // 首次部署，尚无 checkpoint
	}
	if err != nil {
		return 0, 0, err
	}
	return row.MaxID, row.Step, nil
}
