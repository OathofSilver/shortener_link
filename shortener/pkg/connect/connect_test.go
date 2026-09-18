package connect

import (
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

// GoConvey是一个非常非常好用的Go测试框架
func TestGet(t *testing.T) {
	convey.Convey("基础用例", t, func() {
		url := "https://www.liwenzhou.com"
		got := Get(url)
		// 断言
		convey.So(got, convey.ShouldEqual, true) //断言got是否为true 相等判断

	})
	convey.Convey("url请求不通示例", t, func() {
		url := "https://www.wenzhou.com/posts/Go/unit-test-5/"
		got := Get(url)
		// 断言
		convey.So(got, convey.ShouldEqual, false)
	})
}
