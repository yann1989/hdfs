// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"
)

func TestClient_Create(t *testing.T) {
	err := cli.Create("/2.txt", []byte("aaaaaaaaaaaaaa"))
	assert.Equal(t, err, nil, "创建文件失败")
}

func TestClient_CreateFile(t *testing.T) {
	err := cli.CreateFile("/1.txt", true, 0, 0, 0, 0, []byte("dsfsdfdsfdsfd"))
	assert.Equal(t, err, nil, "创建文件失败")
}

func TestClient_Append(t *testing.T) {
	err := cli.Append("/test/2.txt", 0, []byte("1111111111"))
	assert.Equal(t, err, nil, "创建文件失败")
}

func TestClient_CreateFileWithGoodBlock(t *testing.T) {
	//var wg sync.WaitGroup
	//
	list, _ := ioutil.ReadDir("/Users/yann/Desktop/test")
	for i := 0; i < len(list); i++ {
		go func(i int) {
			all, _ := ioutil.ReadFile(filepath.Join("/Users/yann/Desktop/test", list[i].Name()))
			err := cli.CreateFileWithGoodBlock(filepath.Join("/xxx/133", list[i].Name()), all)
			if err != nil {
				panic(err)
			}
		}(i)
	}
	go func() {
		all, _ := ioutil.ReadFile("/Users/yann/Desktop/aaa.sql")
		err := cli.CreateFileWithGoodBlock("/syn_data/lf/lfrd/text_data/bbb.sql", all)
		if err != nil {
			panic(err)
		}
	}()

	time.Sleep(time.Hour)
}
