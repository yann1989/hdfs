// Author: yann
// Date: 2022/5/20
// Desc: hdfs_rest

package hdfs

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

var (
	DefaultTransport = &http.Transport{
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
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
)

const (
	DefaultUser        = "root" //默认用户
	DefaultReplication = 1      //默认复制数量
	DefaultProtocol    = "http"
	DefaultHost        = "127.0.0.1:14000"
	DefaultPort        = 14000
)

type Client struct {
	*http.Client
	addr        string
	host        string
	port        int
	protocol    string
	user        string
	replication int
}

func New(opts ...Option) *Client {
	var client = new(Client)
	client.Client = new(http.Client)
	for _, opt := range opts {
		opt(client)
	}

	if len(client.host) == 0 {
		client.host = DefaultHost
	}

	if client.port == 0 {
		client.port = DefaultPort
	}

	if len(client.protocol) == 0 {
		client.protocol = DefaultProtocol
	}

	if len(client.user) == 0 {
		client.user = DefaultUser
	}

	if client.replication == 0 {
		client.replication = DefaultReplication
	}

	if !strings.HasPrefix(client.host, DefaultProtocol) {
		client.addr = fmt.Sprintf("%s://%s:%d", client.protocol, client.host, client.port)
	}

	if client.Transport == nil {
		client.Transport = DefaultTransport
	}

	return client
}
