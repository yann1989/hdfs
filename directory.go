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
	"os"
	"strconv"
)

const (
	MkdirPathFormat         = "%s/webhdfs/v1%s?op=MKDIRS&permission=%s&user.name=%s"
	GetContentSummaryFormat = "%s/webhdfs/v1%s?op=GETCONTENTSUMMARY&user.name=%s"
)

// Mkdir 创建目录并设置目录权限
func (c *Client) Mkdir(dirname string, perm os.FileMode) error {
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(MkdirPathFormat, c.addr, dirname, strconv.FormatInt(int64(perm), 8), c.user), nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return GetErrFromBody(resp)
	}
	return nil
}

// MkdirAll 创建目录并设置目录权限
func (c *Client) MkdirAll(dirname string, perm os.FileMode) error {
	return c.Mkdir(dirname, perm)
}

type ContentSummary struct {
	DirectoryCount int64
	FileCount      int64
	Length         int64
	Quota          int64
	SpaceConsumed  int64
	SpaceQuota     int64
}

// GetContentSummary 获取目录的内容摘要
func (c *Client) GetContentSummary(dirname string) (*ContentSummary, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(GetContentSummaryFormat, c.addr, dirname, c.user), nil)
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
	var ret ContentSummary
	return &ret, json.Unmarshal([]byte(gjson.Get(string(all), "ContentSummary").Raw), &ret)
}
