// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"path/filepath"
	"sync"
	"testing"
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
	var wg sync.WaitGroup

	list, _ := ioutil.ReadDir("/Users/yann/Desktop/test")
	for i := 0; i < len(list); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			all, _ := ioutil.ReadFile(filepath.Join("/Users/yann/Desktop/test", list[i].Name()))
			err := cli.CreateFileWithGoodBlock(filepath.Join("/xxx/133", list[i].Name()), all)
			if err != nil {
				panic(err)
			}
		}(i)
	}

	wg.Wait()

	all, _ := ioutil.ReadFile("/Users/yann/Desktop/test/影音-全量数据贯通说明-202209232的副本9.docx")
	err := cli.CreateFileWithGoodBlock("/ddd/影音-全量数据贯通说明-202209232的副本9", all)
	if err != nil {
		panic(err)
	}
}
