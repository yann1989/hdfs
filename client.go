// Author: yann
// Date: 2022/5/20
// Desc: hdfs_rest

package hdfs

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFoundDataNode = fmt.Errorf("没有找到可用的节点信息")
	DefaultTransport    = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
)

const (
	DefaultUser        = "root" //默认用户
	DefaultReplication = 1      //默认复制数量
)

type Client struct {
	*http.Client
	dataNodes []string
	sync.RWMutex
	user        string
	replication int
}

type ClientOption struct {
	DataNodes   []string
	User        string
	Replication int
	Timeout     time.Duration
	Transport   *http.Transport
}

func New(opt *ClientOption) *Client {
	if len(opt.DataNodes) == 0 {
		panic(ErrBadOptionsDataNodeCannotNull)
	}
	for i := 0; i < len(opt.DataNodes); i++ {
		if !strings.HasPrefix(opt.DataNodes[i], "http") {
			opt.DataNodes[i] = fmt.Sprintf("http://%s", opt.DataNodes[i])
		}
	}
	if opt.Replication == 0 {
		opt.Replication = DefaultReplication
	}

	if len(opt.User) == 0 {
		opt.User = DefaultUser
	}

	if opt.Transport == nil {
		opt.Transport = DefaultTransport
	}

	return &Client{
		Client: &http.Client{
			Transport: opt.Transport,
			Timeout:   opt.Timeout,
		},
		dataNodes:   opt.DataNodes,
		user:        opt.User,
		replication: DefaultReplication,
	}
}

func (c *Client) getDataNode() (string, error) {
	c.RLock()
	defer c.RUnlock()
	length := len(c.dataNodes)
	if length == 0 {
		return "", ErrNotFoundDataNode
	}
	return c.dataNodes[rand.New(rand.NewSource(time.Now().Unix())).Intn(length)], nil
}
