// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"strconv"
)

const (
	DefaultCreateFileFormat = "%s/webhdfs/v1%s?op=CREATE&overwrite=false&permission=644&buffersize=4096&replication=%d&user.name=%s"
	CreateFileFormat        = "%s/webhdfs/v1%s?op=CREATE&overwrite=%v&blocksize=%d&permission=%s&buffersize=%d&replication=%d&user.name=%s"
	AppendFileFormat        = "%s/webhdfs/v1%s?op=APPEND&buffersize=%d&user.name=%s"
	DefaultBufferSize       = 4096
	DefaultFilePerm         = 0644
	ContentTypeKey          = "Content-Type"
	ContentTypeValue        = "application/octet-stream"
)

// Create 使用默认配置创建文件
func (c *Client) Create(path string, data []byte) error {
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(DefaultCreateFileFormat, c.addr, path, c.replication, c.user), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set(ContentTypeKey, ContentTypeValue)
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
func (c *Client) CreateFile(path string, overwrite bool, blockSize uint64, perm os.FileMode, bufferSize uint, replication uint16, data []byte) error {
	if blockSize == 0 {
		blockSize = c.blockSize
	}
	if bufferSize == 0 {
		bufferSize = DefaultBufferSize
	}
	if replication == 0 {
		replication = DefaultReplication
	}
	if perm == 0 {
		perm = DefaultFilePerm
	}

	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(CreateFileFormat, c.addr, path, overwrite, blockSize, strconv.FormatInt(int64(perm), 8), bufferSize, replication, c.user), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set(ContentTypeKey, ContentTypeValue)
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

// CreateFileWithGoodBlock 使用合适的块大小存放数据
func (c *Client) CreateFileWithGoodBlock(path string, data []byte) error {
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(CreateFileFormat, c.addr, path, true, getBlockSize(len(data), c.blockSize), strconv.FormatInt(int64(DefaultFilePerm), 8), DefaultBufferSize, c.replication, c.user), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set(ContentTypeKey, ContentTypeValue)
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
	req.Header.Set(ContentTypeKey, ContentTypeValue)
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

const MB = 1024 * 1024

func getBlockSize(size int, minBlockSize uint64) uint64 {
	dataLength := uint64(size)
	switch {
	case dataLength < minBlockSize:
		return minBlockSize
	case dataLength >= 512*MB:
		dataLength = 512 * MB
	case dataLength >= 256*MB:
		dataLength = 256 * MB
	case dataLength >= 128*MB:
		dataLength = 128 * MB
	case dataLength >= 64*MB:
		dataLength = 64 * MB
	case dataLength >= 32*MB:
		dataLength = 32 * MB
	case dataLength >= 16*MB:
		dataLength = 16 * MB
	case dataLength >= 8*MB:
		dataLength = 8 * MB
	case dataLength >= 4*MB:
		dataLength = 4 * MB
	case dataLength >= 2*MB:
		dataLength = 2 * MB
	default:
		dataLength = 1 * MB
	}
	if dataLength < minBlockSize {
		dataLength = minBlockSize
	}
	return dataLength
}
