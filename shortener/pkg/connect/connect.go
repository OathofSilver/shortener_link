package connect

import (
	"github.com/zeromicro/go-zero/core/logx"
	"net/http"
	"time"
)

// client 全局的HTTP客户端
// 使用连接池复用 TCP 连接：探测在转链请求路径上，若每次新建连接(DisableKeepAlives)，
// 高并发下客户端 TIME_WAIT 会耗尽本机动态端口(Windows 默认约 1.6 万个)，导致探测批量失败
var client = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 2000,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 2 * time.Second,
}

// Get 判断url是否能请求通
func Get(url string) bool {
	resp, err := client.Get(url)
	if err != nil {
		logx.Errorw("connect client.Get failed", logx.LogField{Key: "err", Value: err.Error()})
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK //别人给我发一个跳转响应这里也不算过
}
