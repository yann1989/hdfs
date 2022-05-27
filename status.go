// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"encoding/json"
	"fmt"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"net/http"
	"path/filepath"
)

const (
	ListStatus      = "%s/webhdfs/v1%s?op=LISTSTATUS&user.name=%s"
	GetFileStatus   = "%s/webhdfs/v1%s?op=GETFILESTATUS&user.name=%s"
	GetFileChecksum = "%s/webhdfs/v1%s?op=GETFILECHECKSUM&user.name=%s"
	TypeFile        = "FILE"
	TypeDir         = "DIRECTORY"
)

type FileStatus struct {
	AccesTime        int64
	BlockSize        int64
	Group            string
	Length           int64
	ModificationTime int64
	Owner            string
	PathSuffix       string
	Permission       string
	Replication      int64
	Type             string
}

func (f FileStatus) IsDir() bool {
	if f.Type == TypeDir {
		return true
	}
	return false
}

// ListStatus 获取指定路径下列表
func (c *Client) ListStatus(path string) ([]FileStatus, error) {
	node, err := c.getDataNode()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(ListStatus, node, path, c.user), nil)
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
	all, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var ret []FileStatus
	return ret, json.Unmarshal([]byte(gjson.Get(string(all), "FileStatuses.FileStatus").Raw), &ret)
}

// GetFileStatus 获取指定文件的信息
func (c *Client) GetFileStatus(path string) (*FileStatus, error) {
	node, err := c.getDataNode()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(GetFileStatus, node, path, c.user), nil)
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
	all, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var ret FileStatus
	return &ret, json.Unmarshal([]byte(gjson.Get(string(all), "FileStatus").Raw), &ret)
}

// Walk 遍历某个路径下的目录
func (c *Client) Walk(path string, callback func(prefixPath string, status FileStatus)) error {
	status, err := c.GetFileStatus(path)
	if err != nil {
		return err
	}
	if status.Type != TypeDir {
		return ErrPathIsNotDirectory
	}
	c.recursionDir(path, callback)
	return nil
}

func (c *Client) recursionDir(path string, callback func(prefixPath string, status FileStatus)) {
	ls, _ := c.ListStatus(path)
	if len(ls) == 0 {
		return
	}
	for i := 0; i < len(ls); i++ {
		callback(path, ls[i])
		if ls[i].Type != TypeDir {
			continue
		}
		c.recursionDir(filepath.Join(path, ls[i].PathSuffix), callback)
	}
}

type FileChecksum struct {
	Algorithm string
	Bytes     string
	Length    int64
}

// GetFileChecksum 获取指定文件的md5值
func (c *Client) GetFileChecksum(path string) (*FileChecksum, error) {
	node, err := c.getDataNode()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(GetFileChecksum, node, path, c.user), nil)
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
	all, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var ret FileChecksum
	return &ret, json.Unmarshal([]byte(gjson.Get(string(all), "FileChecksum").Raw), &ret)
}
