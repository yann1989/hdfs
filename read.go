// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

const (
	ReadFileFormat        = "%s/webhdfs/v1%s?op=OPEN&length=%d&offset=%d&buffersize=%d&user.name=%s"
	DefaultReadFileFormat = "%s/webhdfs/v1%s?op=OPEN&user.name=%s"
	DefaultReadLength     = 4096
)

// ReadFile 读取指定文件
func (c *Client) Read(path string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(DefaultReadFileFormat, c.addr, path, c.user), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, GetErrFromBody(resp)
	}
	return ioutil.ReadAll(resp.Body)
}

// ReadFile 读取指定文件
func (c *Client) ReadFile(path string, offset, length uint64, bufferSize uint) ([]byte, error) {
	if offset < 0 {
		offset = 0
	}
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	if length <= 0 {
		length = DefaultReadLength
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(ReadFileFormat, c.addr, path, length, offset, bufferSize, c.user), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, GetErrFromBody(resp)
	}
	return ioutil.ReadAll(resp.Body)
}
