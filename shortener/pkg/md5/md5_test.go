package md5

import "testing"

func TestSum(t *testing.T) {
	type args struct {
		data []byte
	}
	// 测试数据
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "基本示例", args: args{data: []byte("https://www.baidu.com")}, want: "f9751de431104b125f48dd79cc55822a"},
		{name: "示例", args: args{data: []byte("11111111")}, want: "1bbd886460827015e5d605ed44252251"},
	}
	for _, tt := range tests {
		// 每个数据 run
		t.Run(tt.name, func(t *testing.T) {
			if got := Sum(tt.args.data); got != tt.want {
				t.Errorf("Sum() = %v, want %v", got, tt.want)
			}
		})
	}
}
