package sequence

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// fake CheckpointStore：可注入故障、记录写入顺序，用于测试各种异常场景
type fakeCkpt struct {
	mu       sync.Mutex
	maxID    uint64
	step     int
	saveErr  error   // 非 nil 时 Save 返回该错误
	loadErr  error   // 非 nil 时 Load 返回该错误
	savedIDs []uint64 // 记录每次 Save 的 maxID（顺序）
}

func (f *fakeCkpt) Load(ctx context.Context, bizTag string) (uint64, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return 0, 0, f.loadErr
	}
	return f.maxID, f.step, nil
}

func (f *fakeCkpt) Save(ctx context.Context, bizTag string, maxID uint64, step int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	if maxID > f.maxID {
		f.maxID = maxID
	}
	f.step = step
	f.savedIDs = append(f.savedIDs, maxID)
	return nil
}

func (f *fakeCkpt) savedSnapshot() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.savedIDs...)
}

// newTestSeq 构造基于 miniredis 的发号器（独立 miniredis 实例，互不影响）
func newTestSeq(t *testing.T, ckpt CheckpointStore, step, threshold int) (*SegmentRedis, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	seq, err := NewSegmentRedis(redis.New(mr.Addr()), ckpt, "test", step, threshold)
	if err != nil {
		t.Fatalf("NewSegmentRedis: %v", err)
	}
	return seq, mr
}

// TestConcurrentUnique 并发唯一性：多 goroutine 并发取号必须零重复且严格递增分配
func TestConcurrentUnique(t *testing.T) {
	const (
		goroutines = 50
		perG       = 200
		step       = 1000
	)
	seq, _ := newTestSeq(t, &fakeCkpt{}, step, 80)

	var wg sync.WaitGroup
	results := make([][]uint64, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ids := make([]uint64, 0, perG)
			for i := 0; i < perG; i++ {
				id, err := seq.Next()
				if err != nil {
					t.Errorf("Next() error: %v", err)
					return
				}
				ids = append(ids, id)
			}
			results[g] = ids
		}(g)
	}
	wg.Wait()

	seen := make(map[uint64]struct{}, goroutines*perG)
	for _, ids := range results {
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				t.Fatalf("duplicate id %d", id)
			}
			seen[id] = struct{}{}
		}
	}
	if len(seen) != goroutines*perG {
		t.Fatalf("expected %d unique ids, got %d", goroutines*perG, len(seen))
	}
}

// TestPreloadTriggeredAtThreshold 用量达到 80% 时备用号段应被异步预加载就绪
func TestPreloadTriggeredAtThreshold(t *testing.T) {
	const step = 100
	seq, _ := newTestSeq(t, &fakeCkpt{}, step, 80)

	// 发 79 个号不应触发（79/100 < 80%）
	for i := 0; i < 79; i++ {
		if _, err := seq.Next(); err != nil {
			t.Fatalf("Next() error: %v", err)
		}
	}
	if seq.segs[1].Load() != nil {
		t.Fatal("backup segment should not be loaded before threshold")
	}
	// 第 80 个号达到阈值，异步预加载应在窗口内完成
	if _, err := seq.Next(); err != nil {
		t.Fatalf("Next() error: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return seq.segs[1].Load() != nil })
}

// TestSwitchSeamless 跨越多个号段连续取号：切换必须无缝且不重复
func TestSwitchSeamless(t *testing.T) {
	const step = 50
	seq, _ := newTestSeq(t, &fakeCkpt{}, step, 80)

	last := uint64(0)
	for i := 0; i < step*3+17; i++ {
		id, err := seq.Next()
		if err != nil {
			t.Fatalf("Next() at %d error: %v", i, err)
		}
		if id <= last {
			t.Fatalf("id %d not greater than previous %d", id, last)
		}
		last = id
	}
	// 3+ 个号段全部 checkpoint 落盘
	if got := len(fakeCkptOf(seq).savedSnapshot()); got < 4 {
		t.Fatalf("expected >=4 checkpoints, got %d", got)
	}
}

// TestDBFailFailFast DB 不可用（checkpoint 持续失败）时：备用号段不激活，号段耗尽后 fail fast；
// DB 恢复后可自动继续服务
func TestDBFailFailFast(t *testing.T) {
	ck := &fakeCkpt{}
	const step = 10
	seq, _ := newTestSeq(t, ck, step, 50)
	useFastRetry(seq)

	// 注入 DB 故障
	ck.mu.Lock()
	ck.saveErr = errors.New("db down")
	ck.mu.Unlock()
	// 消耗完当前号段（预加载在第 5 个号触发但会失败）
	for i := 0; i < step; i++ {
		if _, err := seq.Next(); err != nil {
			t.Fatalf("Next() within current segment error: %v", err)
		}
	}
	// 号段耗尽后必须 fail fast
	_, err := seq.Next()
	if !errors.Is(err, ErrSegmentNotReady) {
		t.Fatalf("expected ErrSegmentNotReady, got %v", err)
	}
	// DB 恢复后应能继续取号（同步申请路径）
	// 注意：故障期间预加载的 INCRBY 已在 Redis 分配了 [11,20]（checkpoint 失败被丢弃），
	// 这些号被正确跳过而不会重发，恢复后首号应为 21 —— 这正是"绝不重发"语义的体现
	ck.mu.Lock()
	ck.saveErr = nil
	ck.mu.Unlock()
	id, err := seq.Next()
	if err != nil {
		t.Fatalf("Next() after db recovery error: %v", err)
	}
	if id != 2*step+1 {
		t.Fatalf("expected id %d after recovery, got %d", 2*step+1, id)
	}
}

// TestRecoverFromDBCheckpoint Redis 数据丢失时从 DB checkpoint 恢复：跳号但绝不重发
func TestRecoverFromDBCheckpoint(t *testing.T) {
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	// DB checkpoint = 5000，Redis 无数据 → 首个号应为 5001
	seq, err := NewSegmentRedis(redis.New(mr.Addr()), &fakeCkpt{maxID: 5000, step: 1000}, "test", 1000, 80)
	if err != nil {
		t.Fatalf("NewSegmentRedis: %v", err)
	}
	id, err := seq.Next()
	if err != nil {
		t.Fatalf("Next() error: %v", err)
	}
	if id != 5001 {
		t.Fatalf("expected first id 5001, got %d", id)
	}

	// Redis 值(8000) > DB checkpoint(5000) → 沿用 Redis 位点，首号 8001
	if err := mr.Set(segmentRedisKeyPrefix+"test2", strconv.FormatUint(8000, 10)); err != nil {
		t.Fatalf("seed redis: %v", err)
	}
	seq2, err := NewSegmentRedis(redis.New(mr.Addr()), &fakeCkpt{maxID: 5000, step: 1000}, "test2", 1000, 80)
	if err != nil {
		t.Fatalf("NewSegmentRedis: %v", err)
	}
	id2, err := seq2.Next()
	if err != nil {
		t.Fatalf("Next() error: %v", err)
	}
	if id2 != 8001 {
		t.Fatalf("expected first id 8001, got %d", id2)
	}
}

// TestCheckpointBeforeActivate 核心正确性：号段激活前 checkpoint 必须已落盘
func TestCheckpointBeforeActivate(t *testing.T) {
	const step = 100
	ck := &fakeCkpt{}
	seq, _ := newTestSeq(t, ck, step, 80)

	// 构造完成后：首段已激活，其终点 step 必然已落盘
	saved := ck.savedSnapshot()
	if len(saved) != 1 || saved[0] != step {
		t.Fatalf("expected checkpoint [%d] after init, got %v", step, saved)
	}
	// 跨号段取号后：每个已激活号段的终点都应已落盘（[1,100] 和 [101,200]）
	// 第三个号段的预加载在第 180 个号异步触发，等待其 checkpoint 落盘后再断言
	for i := 0; i < step*2; i++ {
		if _, err := seq.Next(); err != nil {
			t.Fatalf("Next() error: %v", err)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return len(ck.savedSnapshot()) >= 3 })
	saved = ck.savedSnapshot()
	// 前两个 checkpoint 必须严格对应两个已激活号段的终点
	if saved[0] != step || saved[1] != 2*step {
		t.Fatalf("expected checkpoints [%d %d ...], got %v", step, 2*step, saved)
	}
	// Save 记录值必须单调不减（GREATEST 语义由 fake 实现，此处验证调用方传值正确）
	for i := 1; i < len(saved); i++ {
		if saved[i] < saved[i-1] {
			t.Fatalf("checkpoint values not monotonic: %v", saved)
		}
	}
}

// TestStartupDBDown 启动时 DB 不可用必须拒绝启动（优于带重发风险启动）
func TestStartupDBDown(t *testing.T) {
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()
	ck := &fakeCkpt{loadErr: errors.New("db down")}
	_, err := NewSegmentRedis(redis.New(mr.Addr()), ck, "test", 1000, 80)
	if err == nil {
		t.Fatal("expected error when db unavailable at startup")
	}
}

// TestRedisFailFast Redis 不可用时 fail fast：当前号段可用期间正常发号，耗尽后报错
func TestRedisFailFast(t *testing.T) {
	const step = 10
	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()
	seq, err := NewSegmentRedis(redis.New(mr.Addr()), &fakeCkpt{}, "test", step, 50)
	if err != nil {
		t.Fatalf("NewSegmentRedis: %v", err)
	}
	// 停掉 Redis 模拟故障
	mr.SetError("redis down")
	// 当前号段内仍可发号（纯内存操作）
	for i := 0; i < step; i++ {
		if _, err := seq.Next(); err != nil {
			t.Fatalf("Next() within current segment error: %v", err)
		}
	}
	// 耗尽后 fail fast
	_, err = seq.Next()
	if !errors.Is(err, ErrSegmentNotReady) {
		t.Fatalf("expected ErrSegmentNotReady, got %v", err)
	}
}

// waitFor 轮询等待条件成立，超时则测试失败
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

// fakeCkptOf 取回测试中注入的 fake CheckpointStore（仅测试内部使用）
func fakeCkptOf(seq *SegmentRedis) *fakeCkpt {
	return seq.ckpt.(*fakeCkpt)
}

// useFastRetry 注入极小的 checkpoint 重试参数，避免 DB 故障用例等待过长的退避时间
func useFastRetry(seq *SegmentRedis) {
	seq.ckptMaxRetry = 2
	seq.ckptBaseInterval = 5 * time.Millisecond
	seq.ckptMaxInterval = 10 * time.Millisecond
}
