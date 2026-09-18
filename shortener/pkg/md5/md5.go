package md5

import (
	"crypto/md5"
	"encoding/hex"
)

// md5 可以加盐 增加安全性
// Sum 对传入的参数求md5值
func Sum(data []byte) string {
	h := md5.New()
	h.Write(data)                         // 写入待哈希的数据
	return hex.EncodeToString(h.Sum(nil)) //32位16进制数
}
