// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

import (
	"fmt"
	"net/http"
)

const (
	RemoveAllFormat = "%s/webhdfs/v1%s?op=DELETE&user.name=%s"
)

func (c *Client) RemoveAll(path string) error {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(RemoveAllFormat, c.addr, path, c.user), nil)
	if err != nil {
		return err
	}
	//req.Header.Set(ContentTypeKey, ContentTypeValue)
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
