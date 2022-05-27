// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClient_Create(t *testing.T) {
	err := cli.Create("/test/2.txt", []byte("abcdefg"))
	assert.Equal(t, err, nil, "创建文件失败")
}

func TestClient_Append(t *testing.T) {
	err := cli.Append("/test/2.txt", 0, []byte("1111111111"))
	assert.Equal(t, err, nil, "创建文件失败")
}
