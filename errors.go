// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

var (
	ErrBadOptionsDataNodeCannotNull = fmt.Errorf("非法参数, DataNodes为空")
	ErrPathIsNotDirectory           = fmt.Errorf("传入路径不是目录")
)

func GetErrFromBody(response *http.Response) error {
	all, _ := ioutil.ReadAll(response.Body)
	return fmt.Errorf("%s: %s", response.Status, all)
}
