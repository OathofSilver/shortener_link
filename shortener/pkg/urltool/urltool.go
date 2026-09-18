package urltool

import (
	"errors"
	"net/url"
	"path"
)

func GetBasePath(targetUrl string) (string, error) {
	myUrl, err := url.Parse(targetUrl) //基本都能解析通过
	if err != nil {
		return "", err
	}
	if len(myUrl.Host) == 0 {
		return "", errors.New("no host in targetUrl")
	}
	//  URL 路径中提取最后一个元素的方法
	// 提取最后一个元素之前，将删除尾随斜杠。
	return path.Base(myUrl.Path), nil
}
