// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClient_ReadFile(t *testing.T) {
	data, err := cli.ReadFile("/test/测试数据.txt", 0, 1024, 1024)
	assert.Equal(t, err, nil)
	t.Logf("数据: %s", data)
}

func TestClient_Read(t *testing.T) {
	data, err := cli.Read("/test/2.txt")
	assert.Equal(t, err, nil)
	t.Logf("数据: %s", data)
}
