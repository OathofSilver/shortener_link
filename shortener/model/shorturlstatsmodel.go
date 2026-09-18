package model

import (
	"context"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

// 短链跳转计数表模型
// 注意：计数行更新极其频繁，不适合走 go-zero 的行缓存(sqlc)，
// 因此这里使用原生 sqlx 直连，读写均直接操作 MySQL。

type ShortUrlStats struct {
	Id         int64  `db:"id"`
	Surl       string `db:"surl"`
	TotalCount int64  `db:"total_count"`
}

type ShortUrlStatsModel interface {
	// IncrClick 累加指定短链的访问次数，记录不存在时自动插入(初始值1)
	IncrClick(ctx context.Context, surl string) error
	// GetTotalCount 查询指定短链的累计访问次数，记录不存在时返回0
	GetTotalCount(ctx context.Context, surl string) (int64, error)
}

type shortUrlStatsModel struct {
	conn sqlx.SqlConn
}

var _ ShortUrlStatsModel = (*shortUrlStatsModel)(nil)

func NewShortUrlStatsModel(conn sqlx.SqlConn) ShortUrlStatsModel {
	return &shortUrlStatsModel{conn: conn}
}

// IncrClick 利用 INSERT ... ON DUPLICATE KEY UPDATE 在数据库层原子累加，
// 并发消费多实例同时写同一短链也不会丢失计数(依赖 uniq_surl 唯一键)
func (m *shortUrlStatsModel) IncrClick(ctx context.Context, surl string) error {
	query := `insert into short_url_stats (surl, total_count) values (?, 1)
		on duplicate key update total_count = total_count + 1`
	_, err := m.conn.ExecCtx(ctx, query, surl)
	return err
}

func (m *shortUrlStatsModel) GetTotalCount(ctx context.Context, surl string) (int64, error) {
	var total int64
	query := `select total_count from short_url_stats where surl = ? limit 1`
	err := m.conn.QueryRowCtx(ctx, &total, query, surl)
	if err == sqlx.ErrNotFound {
		return 0, nil // 该短链还没有任何访问记录
	}
	return total, err
}
