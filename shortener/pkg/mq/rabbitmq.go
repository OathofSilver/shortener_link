// Package mq 封装 RabbitMQ 生产端能力：
// 1. 拓扑声明：direct 交换机 + 持久化点击队列 + 死信交换机/队列（承接格式错误或重试耗尽的消息）
// 2. 发布确认（publisher confirm）：确保消息真正到达 broker，未确认视为投递失败
// 3. 失败重试 + 本地补偿：投递失败重试多次仍失败时消息落盘，后台定时重发
// 4. 断线自动重连：连接断开后后台重建连接与拓扑
package mq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"
	"github.com/zeromicro/go-zero/core/logx"
)

// RabbitMQ 拓扑常量，消费端也会复用
const (
	ExchangeClick   = "shortener.stats.exchange"  // 点击事件交换机(direct)
	QueueClick      = "shortener.stats.click.queue" // 点击事件队列(持久化,带死信参数)
	RoutingKeyClick = "click"                     // 点击事件路由键

	ExchangeDead   = "shortener.stats.dlx"    // 死信交换机
	QueueDead      = "shortener.stats.click.dlq" // 死信队列
	RoutingKeyDead = "click.dead"             // 死信路由键

	HeaderRetryCount = "x-retry-count" // 消息头：消费端处理失败的重试次数

	publishRetry   = 3               // 单条消息发布重试次数
	confirmTimeout = 5 * time.Second // 等待发布确认的超时时间
	reconnectDelay = 3 * time.Second // 断线重连间隔
	fallbackScan   = time.Minute     // 补偿文件扫描周期
)

// ClickEvent 点击事件消息体
type ClickEvent struct {
	MsgID     string `json:"msgId"`     // 消息唯一ID，消费端幂等去重的依据
	Surl      string `json:"surl"`      // 短链接
	ClickedAt int64  `json:"clickedAt"` // 点击时间戳(毫秒)
	Retry     int    `json:"retry"`     // 消费端处理失败后的重试次数
}

// DeclareTopology 在指定通道上声明交换机/队列/绑定关系，幂等可重复执行
func DeclareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ExchangeClick, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", ExchangeClick, err)
	}
	if err := ch.ExchangeDeclare(ExchangeDead, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead exchange %s: %w", ExchangeDead, err)
	}
	// 点击队列开启死信转发：消费端 Reject 的消息进入死信队列，便于排查与人工补偿
	args := amqp.Table{
		"x-dead-letter-exchange":    ExchangeDead,
		"x-dead-letter-routing-key": RoutingKeyDead,
	}
	if _, err := ch.QueueDeclare(QueueClick, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare queue %s: %w", QueueClick, err)
	}
	if err := ch.QueueBind(QueueClick, RoutingKeyClick, ExchangeClick, false, nil); err != nil {
		return fmt.Errorf("bind queue %s: %w", QueueClick, err)
	}
	if _, err := ch.QueueDeclare(QueueDead, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead queue %s: %w", QueueDead, err)
	}
	if err := ch.QueueBind(QueueDead, RoutingKeyDead, ExchangeDead, false, nil); err != nil {
		return fmt.Errorf("bind dead queue %s: %w", QueueDead, err)
	}
	return nil
}

// RabbitMQ 生产端：内部维护带 confirm 模式的发布通道，断线自动重连
type RabbitMQ struct {
	url          string
	fallbackFile string

	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel        // 发布通道(confirm 模式)
	confirms chan amqp.Confirmation // broker 确认回执
	closed  bool

	fmu sync.Mutex // 补偿文件读写锁
}

// NewRabbitMQ 创建生产端。初始连接失败不返回错误(如 broker 未启动)，
// 会在后台持续重连；期间发布的消息走重试+落盘补偿，保证不丢。
func NewRabbitMQ(url, fallbackFile string) *RabbitMQ {
	r := &RabbitMQ{url: url, fallbackFile: fallbackFile}
	if err := r.connect(context.Background()); err != nil {
		logx.Errorw("rabbitmq initial connect failed, will retry in background",
			logx.Field("err", err.Error()))
	}
	go r.reconnectLoop()
	go r.republishFallbackLoop()
	return r
}

func (r *RabbitMQ) connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connectLocked(ctx)
}

// 调用方需持有 r.mu
func (r *RabbitMQ) connectLocked(ctx context.Context) error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}
	if err := DeclareTopology(ch); err != nil {
		conn.Close()
		return err
	}
	// 开启发布确认模式，broker 落盘后回执 ack，保证消息不丢
	if err := ch.Confirm(false); err != nil {
		conn.Close()
		return fmt.Errorf("enable confirm mode: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 256))

	r.closeLocked() // 关闭旧连接(如果有)
	r.conn = conn
	r.channel = ch
	r.confirms = confirms

	// 监听连接断开，由 reconnectLoop 负责重建
	go func(c *amqp.Connection) {
		closeErr, ok := <-c.NotifyClose(make(chan *amqp.Error, 1))
		if ok && closeErr != nil {
			logx.Errorw("rabbitmq connection closed",
				logx.Field("reason", closeErr.Reason))
		}
	}(conn)
	return nil
}

// 调用方需持有 r.mu
func (r *RabbitMQ) closeLocked() {
	if r.channel != nil {
		r.channel.Close()
		r.channel = nil
	}
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
}

func (r *RabbitMQ) reconnectLoop() {
	for {
		if r.closed {
			return
		}
		r.mu.Lock()
		needRetry := r.conn == nil || r.conn.IsClosed()
		r.mu.Unlock()
		if needRetry {
			if err := r.connect(context.Background()); err != nil {
				logx.Errorw("rabbitmq reconnect failed", logx.Field("err", err.Error()))
			} else {
				logx.Info("rabbitmq reconnected")
			}
		}
		time.Sleep(reconnectDelay)
	}
}

// Publish 发布持久化消息并等待 broker 确认，重试 publishRetry 次仍失败则返回错误
func (r *RabbitMQ) Publish(ctx context.Context, routingKey string, body []byte) error {
	var lastErr error
	for attempt := 0; attempt < publishRetry; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		ch, confirms, err := r.getChannel(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		err = ch.PublishWithContext(ctx, ExchangeClick, routingKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // 持久化消息，broker 重启不丢
			Timestamp:    time.Now(),
			Body:         body,
		})
		if err != nil {
			lastErr = fmt.Errorf("publish: %w", err)
			_ = r.connect(context.Background()) // 通道可能已损坏，重建后重试
			continue
		}
		select {
		case c, ok := <-confirms:
			if ok && c.Ack {
				return nil
			}
			if !ok {
				_ = r.connect(context.Background())
			}
			lastErr = errors.New("publisher confirm not acked")
		case <-time.After(confirmTimeout):
			lastErr = errors.New("publisher confirm timeout")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return lastErr
}

func (r *RabbitMQ) getChannel(ctx context.Context) (*amqp.Channel, chan amqp.Confirmation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, errors.New("rabbitmq producer closed")
	}
	if r.conn == nil || r.conn.IsClosed() || r.channel == nil || r.channel.IsClosed() {
		if err := r.connectLocked(ctx); err != nil {
			return nil, nil, err
		}
	}
	return r.channel, r.confirms, nil
}

// PublishClickAsync 异步投递一条点击事件，绝不阻塞重定向主流程。
// 投递失败时消息落盘到补偿文件，由后台任务重发，保证最终不丢。
func (r *RabbitMQ) PublishClickAsync(ctx context.Context, surl string) {
	event := ClickEvent{
		MsgID:     newMsgID(),
		Surl:      surl,
		ClickedAt: time.Now().UnixMilli(),
	}
	body, err := json.Marshal(event)
	if err != nil {
		logx.Errorw("marshal click event failed", logx.Field("err", err.Error()))
		return
	}
	go func() {
		defer func() {
			if e := recover(); e != nil {
				logx.Errorw("publish click event panic", logx.Field("panic", e))
			}
		}()
		pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.Publish(pctx, RoutingKeyClick, body); err != nil {
			logx.Errorw("publish click event failed, save to fallback file",
				logx.Field("err", err.Error()),
				logx.Field("msgId", event.MsgID),
				logx.Field("surl", event.Surl))
			if ferr := r.saveFallback(body); ferr != nil {
				logx.Errorw("save fallback click event failed",
					logx.Field("err", ferr.Error()),
					logx.Field("msgId", event.MsgID))
			}
		}
	}()
}

// saveFallback 发布失败的消息按行落盘，作为投递失败的补偿
func (r *RabbitMQ) saveFallback(body []byte) error {
	if r.fallbackFile == "" {
		return errors.New("fallback file not configured")
	}
	if err := os.MkdirAll(filepath.Dir(r.fallbackFile), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(r.fallbackFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(body, '\n'))
	return err
}

// republishFallbackLoop 定时扫描补偿文件，重发成功后移除对应行
func (r *RabbitMQ) republishFallbackLoop() {
	for {
		time.Sleep(fallbackScan)
		if r.closed {
			return
		}
		r.republishFallbackOnce()
	}
}

func (r *RabbitMQ) republishFallbackOnce() {
	r.fmu.Lock()
	defer r.fmu.Unlock()
	data, err := os.ReadFile(r.fallbackFile)
	if err != nil {
		return // 文件不存在或读取失败，跳过本次
	}
	var remaining bytes.Buffer
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := r.Publish(pctx, RoutingKeyClick, line)
		cancel()
		if err != nil {
			logx.Errorw("republish fallback click event failed", logx.Field("err", err.Error()))
			remaining.Write(append(line, '\n'))
		} else {
			logx.Info("republish fallback click event success")
		}
	}
	// 用未成功的内容覆盖写回
	if err := os.WriteFile(r.fallbackFile, remaining.Bytes(), 0o644); err != nil {
		logx.Errorw("rewrite fallback file failed", logx.Field("err", err.Error()))
	}
}

// Close 优雅关闭
func (r *RabbitMQ) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.closeLocked()
}

// newMsgID 生成消息唯一ID
func newMsgID() string {
	return uuid.NewString()
}
