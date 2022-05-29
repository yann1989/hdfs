// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestClient_Mkdir(t *testing.T) {
	err := cli.Mkdir("/test", os.ModePerm)
	assert.Equal(t, err, nil, "创建目录失败")
}

func TestClient_GetContentSummary(t *testing.T) {
	ret, err := cli.GetContentSummary("/")
	assert.Equal(t, err, nil, "获取目录信息失败")
	t.Log(ret)
}
