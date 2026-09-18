package sequence

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// 号段模式分布式发号器（Redis 号段 + 本地双缓冲 + DB 异步 checkpoint）
//
// 架构：
//   - 热路径无锁：Next() 在当前内存号段上 atomic 自增，纯内存操作，微秒级
//   - 双缓冲：当前号段用量达 80% 时，后台 goroutine 通过 Redis INCRBY 批量申请
//     下一号段填入备用槽；当前号段耗尽后无缝切换到备用槽
//   - checkpoint：申请号段时（备用槽激活前）先把号段终点写入 DB（GREATEST 幂等），
//     保证 DB 记录值 >= 任何已发放的号，服务重启后恢复只跳号、绝不重发
//   - 降级：Redis 不可用 → 新号段无法申请 → 当前号段耗尽后 fail fast 返回错误；
//     DB 不可用 → 只阻塞新号段激活（checkpoint 重试耗尽即视为申请失败），
//     不影响已激活号段的正常发号
//
// 多实例安全：Redis INCRBY 是号段分配的唯一协调点（原子操作），各实例拿到的号段
// 天然不重叠；DB checkpoint 用 GREATEST 只增写入，乱序到达也不会回退。

const (
	segmentRedisKeyPrefix = "sequence:segment:" // Redis 计数器 key 前缀
	defaultStep           = 10000               // 默认号段长度
	defaultThreshold      = 80                  // 默认预加载触发阈值(%)
	ckptBaseInterval      = 200 * time.Millisecond // checkpoint 重试基础间隔
	ckptMaxInterval       = 3 * time.Second        // checkpoint 重试最大间隔
	maxCkptRetry          = 10                     // checkpoint 单号段最大重试次数
	switchMaxWait         = 2 * time.Second        // 切换时等待备用号段的最大时长
	switchPollInterval    = 5 * time.Millisecond   // 切换等待轮询间隔
	startupLoadRetry      = 3                      // 启动加载 checkpoint 的重试次数
)

// ErrSegmentNotReady 备用号段未就绪（Redis/DB 故障导致预加载失败），fail fast
var ErrSegmentNotReady = errors.New("sequence: segment not ready")

// segment 单个号段，闭区间 [start, end]
type segment struct {
	start uint64
	end   uint64
	cur   atomic.Uint64 // 已发放到的号（初始为 start-1）
}

func newSegment(start, end uint64) *segment {
	s := &segment{start: start, end: end}
	s.cur.Store(start - 1)
	return s
}

// SegmentRedis 号段模式发号器，实现 Sequence 接口
type SegmentRedis struct {
	redis     *redis.Redis
	ckpt      CheckpointStore
	bizTag    string
	key       string
	step      uint64
	threshold int

	segs   [2]atomic.Pointer[segment] // 双缓冲槽位
	curIdx atomic.Int32               // 当前使用的槽位下标(0/1)
	loading atomic.Bool               // CAS 标记：保证同一时刻只有一个预加载在执行
	switchMu sync.Mutex               // 仅在号段耗尽切换时使用，均摊开销趋近于零

	// checkpoint 重试参数（测试可注入更小值加速用例）
	ckptMaxRetry     int
	ckptBaseInterval time.Duration
	ckptMaxInterval  time.Duration
}

var _ Sequence = (*SegmentRedis)(nil)

// NewSegmentRedis 创建号段模式发号器。
// 构造时完成恢复：读 DB checkpoint 作为安全下界，对齐 Redis 计数器（只跳号不重发），
// 并同步加载首个号段（首段激活前 checkpoint 必须落盘，否则启动失败）。
// 参数 step <= 0 / threshold 非法时使用默认值。
func NewSegmentRedis(r *redis.Redis, ckpt CheckpointStore, bizTag string, step, threshold int) (*SegmentRedis, error) {
	if r == nil {
		return nil, errors.New("sequence: nil redis client")
	}
	if ckpt == nil {
		return nil, errors.New("sequence: nil checkpoint store")
	}
	if bizTag == "" {
		bizTag = "shortener"
	}
	if step <= 0 {
		step = defaultStep
	}
	if threshold <= 0 || threshold >= 100 {
		threshold = defaultThreshold
	}
	s := &SegmentRedis{
		redis:            r,
		ckpt:             ckpt,
		bizTag:           bizTag,
		key:              segmentRedisKeyPrefix + bizTag,
		step:             uint64(step),
		threshold:        threshold,
		ckptMaxRetry:     maxCkptRetry,
		ckptBaseInterval: ckptBaseInterval,
		ckptMaxInterval:  ckptMaxInterval,
	}

	// 1. 加载 DB checkpoint（重试后仍失败则拒绝启动：带风险启动优于启动失败）
	var dbMax uint64
	var err error
	for i := 0; i < startupLoadRetry; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		dbMax, _, err = s.ckpt.Load(ctx, s.bizTag)
		cancel()
		if err == nil {
			break
		}
		logx.Errorw("load sequence checkpoint failed",
			logx.Field("err", err.Error()),
			logx.Field("bizTag", s.bizTag),
			logx.Field("attempt", i+1))
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("sequence: load checkpoint after %d retries: %w", startupLoadRetry, err)
	}

	// 2. 对齐 Redis 计数器：以 max(dbMax, redisVal) 为恢复位点
	//    - Redis 数据完好(>= dbMax)：沿用 Redis 值，无跳号
	//    - Redis 数据丢失/落后(< dbMax)：对齐到 dbMax，跳号但不重发（正确性优先）
	curVal, err := s.getRedisMax()
	if err != nil {
		return nil, fmt.Errorf("sequence: read redis counter: %w", err)
	}
	if curVal < dbMax {
		if err := s.redis.Set(s.key, strconv.FormatUint(dbMax, 10)); err != nil {
			return nil, fmt.Errorf("sequence: align redis counter to checkpoint: %w", err)
		}
		logx.Infow("redis counter behind db checkpoint, aligned (gap skipped)",
			logx.Field("redisVal", curVal),
			logx.Field("dbMax", dbMax),
			logx.Field("bizTag", s.bizTag))
	}

	// 3. 同步加载首个号段（激活前必须 checkpoint 成功，失败则启动失败）
	firstSeg, err := s.loadSegment()
	if err != nil {
		return nil, fmt.Errorf("sequence: load first segment: %w", err)
	}
	s.segs[0].Store(firstSeg)
	logx.Infow("segment sequence initialized",
		logx.Field("bizTag", s.bizTag),
		logx.Field("step", s.step),
		logx.Field("threshold", s.threshold))
	return s, nil
}

// Next 取下一个号（无锁热路径，仅在号段边界走慢路径）
func (s *SegmentRedis) Next() (uint64, error) {
	for {
		seg := s.segs[s.curIdx.Load()].Load()
		next := seg.cur.Add(1)
		if next <= seg.end {
			s.maybePreload(seg, next)
			return next, nil
		}
		// 当前号段耗尽，切换到备用号段；备用未就绪时在慢路径内等待/同步申请，
		// 超时(如 Redis/DB 故障)则 fail fast 返回错误
		if err := s.trySwitch(); err != nil {
			return 0, err
		}
	}
}

// maybePreload 用量达到阈值时触发一次异步预加载（CAS 去重，多 goroutine 竞争只有一个生效）
func (s *SegmentRedis) maybePreload(seg *segment, next uint64) {
	// 未达阈值（used/threshold% 以下不触发）
	if (next-seg.start+1)*100 < s.step*uint64(s.threshold) {
		return
	}
	other := s.curIdx.Load() ^ 1
	if s.segs[other].Load() != nil {
		return // 备用号段已就绪
	}
	// CAS 抢占预加载权，避免多个请求重复触发
	if s.loading.CompareAndSwap(false, true) {
		go s.preload(int(other))
	}
}

// preload 异步预加载备用号段：INCRBY → checkpoint(带重试) → 激活
// 失败则不激活（保持 fail-fast 语义），下次达到阈值时重新尝试
func (s *SegmentRedis) preload(slot int) {
	defer s.loading.Store(false)
	// double-check：进入时备用槽可能已被别的路径填充
	if s.segs[slot].Load() != nil {
		return
	}
	seg, err := s.loadSegment()
	if err != nil {
		logx.Errorw("preload segment failed, will retry on next threshold trigger",
			logx.Field("err", err.Error()),
			logx.Field("bizTag", s.bizTag))
		return
	}
	s.segs[slot].Store(seg)
	logx.Infow("preload segment ready",
		logx.Field("bizTag", s.bizTag),
		logx.Field("start", seg.start),
		logx.Field("end", seg.end))
}

// loadSegment 申请一个新号段：
// 1) Redis INCRBY 批量取号 → [newMax-step+1, newMax]
// 2) checkpoint 写 DB（指数退避重试），失败则整个申请失败（不产出号段）
// 顺序不可颠倒：checkpoint 成功先于号段可用，是"绝不重发"的根本保证
func (s *SegmentRedis) loadSegment() (*segment, error) {
	newMax, err := s.redis.Incrby(s.key, int64(s.step))
	if err != nil {
		return nil, fmt.Errorf("redis incrby: %w", err)
	}
	if newMax < int64(s.step) {
		return nil, fmt.Errorf("redis counter corrupted, incrby returned %d", newMax)
	}
	end := uint64(newMax)

	// checkpoint 重试：指数退避，基础间隔起，封顶 ckptMaxInterval
	var ckptErr error
	interval := s.ckptBaseInterval
	for i := 0; i < s.ckptMaxRetry; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ckptErr = s.ckpt.Save(ctx, s.bizTag, end, int(s.step))
		cancel()
		if ckptErr == nil {
			break
		}
		logx.Errorw("save sequence checkpoint failed, retrying",
			logx.Field("err", ckptErr.Error()),
			logx.Field("bizTag", s.bizTag),
			logx.Field("maxID", end),
			logx.Field("attempt", i+1))
		time.Sleep(interval)
		interval *= 2
		if interval > s.ckptMaxInterval {
			interval = s.ckptMaxInterval
		}
	}
	if ckptErr != nil {
		return nil, fmt.Errorf("save checkpoint after %d retries: %w", s.ckptMaxRetry, ckptErr)
	}
	return newSegment(end-s.step+1, end), nil
}

// trySwitch 号段耗尽后的切换（慢路径，持锁串行化）：
// 1) 备用槽就绪 → 原子切换当前槽下标
// 2) 备用槽未就绪且无人在预加载 → 当前 goroutine 同步申请一个号段（fail fast：失败即报错）
// 3) 已有异步预加载在进行 → 有限自旋等待，超时返回 ErrSegmentNotReady
func (s *SegmentRedis) trySwitch() error {
	s.switchMu.Lock()
	defer s.switchMu.Unlock()

	old := int(s.curIdx.Load())
	other := old ^ 1
	if s.segs[other].Load() != nil {
		s.curIdx.Store(int32(other))
		s.segs[old].Store(nil) // 清空旧槽，便于后续预加载复用
		return nil
	}
	// 无人在预加载：同步申请（此时是阻塞恢复的最后机会）
	if s.loading.CompareAndSwap(false, true) {
		seg, err := s.loadSegment()
		s.loading.Store(false)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrSegmentNotReady, err)
		}
		s.segs[other].Store(seg)
		s.curIdx.Store(int32(other))
		s.segs[old].Store(nil)
		logx.Infow("switch to segment (sync loaded)",
			logx.Field("bizTag", s.bizTag),
			logx.Field("start", seg.start),
			logx.Field("end", seg.end))
		return nil
	}
	// 异步预加载进行中：短暂等待其完成
	deadline := time.Now().Add(switchMaxWait)
	for time.Now().Before(deadline) {
		time.Sleep(switchPollInterval)
		if s.segs[other].Load() != nil {
			s.curIdx.Store(int32(other))
			s.segs[old].Store(nil)
			return nil
		}
	}
	return ErrSegmentNotReady
}

// getRedisMax 读取 Redis 计数器当前值，key 不存在时返回 0
func (s *SegmentRedis) getRedisMax() (uint64, error) {
	exists, err := s.redis.Exists(s.key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	val, err := s.redis.Get(s.key)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid redis counter value %q: %w", val, err)
	}
	return v, nil
}
