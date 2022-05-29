// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
)

const (
	DefaultCreateFileFormat = "%s/webhdfs/v1%s?op=CREATE&blocksize=134217728&overwrite=false&permission=644&buffersize=4096&replication=%d&user.name=%s"
	CreateFileFormat        = "%s/webhdfs/v1%s?op=CREATE&overwrite=%v&blocksize=%d&permission=%s&buffersize=%d&replication=%d&user.name=%s"
	AppendFileFormat        = "%s/webhdfs/v1%s?op=APPEND&buffersize=%d&user.name=%s"
	DefaultBlockSize        = 134217728 //默认块大小
	DefaultBufferSize       = 4096
)

// Create 使用默认配置创建文件
func (c *Client) Create(path string, data []byte) error {
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(DefaultCreateFileFormat, c.addr, path, c.replication, c.user), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return GetErrFromBody(resp)
	}
	return nil
}

// CreateFile 自定义配置创建文件
func (c *Client) CreateFile(path string, overwrite bool, blockSize uint64, permission os.FileMode, bufferSize uint, replication uint16, data []byte) error {
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}
	if bufferSize == 0 {
		bufferSize = DefaultBufferSize
	}
	if replication == 0 {
		replication = DefaultReplication
	}
	if permission == 0 {
		permission = 0644
	}

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(CreateFileFormat, c.addr, path, overwrite, blockSize, permission, bufferSize, replication, c.user), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return GetErrFromBody(resp)
	}
	return nil
}

func (c *Client) Append(path string, bufferSize uint, data []byte) error {
	if bufferSize == 0 {
		bufferSize = DefaultBufferSize
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf(AppendFileFormat, c.addr, path, bufferSize, c.user), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return GetErrFromBody(resp)
	}
	return nil
}
