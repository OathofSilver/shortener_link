package logic

import (
	"context"
	"fmt"
	"strconv"

	"shortener/shortener/internal/svc"
	"shortener/shortener/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type StatsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewStatsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StatsLogic {
	return &StatsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// Stats 查询指定短链的累计访问总次数
// 读取顺序：Redis 计数器(热数据) → MySQL 计数表(真源)，MySQL 读到后回填 Redis
func (l *StatsLogic) Stats(req *types.StatsRequest) (resp *types.StatsResponse, err error) {
	key := fmt.Sprintf("stats:click:count:%s", req.ShortURL)
	// 1. 先查 Redis 计数器(消费端 INCR 维护的热点值)
	if v, err := l.svcCtx.Redis.Get(key); err == nil && v != "" {
		if n, perr := strconv.ParseInt(v, 10, 64); perr == nil {
			return &types.StatsResponse{Total: n}, nil
		}
	}
	// 2. 未命中回源 MySQL(计数真源)
	total, err := l.svcCtx.StatsModel.GetTotalCount(l.ctx, req.ShortURL)
	if err != nil {
		logx.Errorw("stats_ShortUrlStatsModel.GetTotalCount failed",
			logx.Field("err", err.Error()),
			logx.Field("surl", req.ShortURL))
		return nil, err
	}
	// 3. 回填 Redis，使用 SETNX 而不是 SET：
	// 避免"回填旧值覆盖消费端 INCR 出的新值"的竞态(消费延迟时 MySQL 落后于 Redis)
	_, _ = l.svcCtx.Redis.Setnx(key, strconv.FormatInt(total, 10))
	return &types.StatsResponse{Total: total}, nil
}
