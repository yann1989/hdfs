// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestClient_ListStatus(t *testing.T) {
	data, err := cli.ListStatus("/")
	assert.Equal(t, err, nil)
	t.Logf("状态列表: %v", data)
}

func TestClient_GetFileStatus(t *testing.T) {
	data, err := cli.GetFileStatus("/test/2.txt")
	assert.Equal(t, err, nil)
	t.Logf("状态: %v", data)
}

func TestClient_Walk(t *testing.T) {
	err := cli.Walk("/", func(prefixPath string, status FileStatus) {
		t.Log(filepath.Join(prefixPath, status.PathSuffix))
	})
	assert.Equal(t, err, nil)
}

func TestClient_GetFileChecksum(t *testing.T) {
	data, err := cli.GetFileChecksum("/test/2.txt")
	assert.Equal(t, err, nil)
	t.Logf("md5: %v", data)
}
