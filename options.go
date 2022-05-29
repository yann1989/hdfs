// Author: yann
// Date: 2022/5/29
// Desc: hdfs

package hdfs

import (
	"net/http"
	"time"
)

type Option func(client *Client)

//HostOption 主机
func HostOption(host string) Option {
	return func(client *Client) {
		client.host = host
	}
}

//PortOption 端口
func PortOption(port int) Option {
	return func(client *Client) {
		client.port = port
	}
}

//SSLOption use http or https.  ture is https.
func SSLOption(SSL bool) Option {
	return func(client *Client) {
		if SSL {
			client.protocol = "https"
		} else {
			client.protocol = "http"
		}
	}
}

//UserOption HDFS User
func UserOption(user string) Option {
	return func(client *Client) {
		client.user = user
	}
}

//ReplicationOption 复制份数
func ReplicationOption(replication int) Option {
	return func(client *Client) {
		client.replication = replication
	}
}

//TimeoutOption 设置超时时间
func TimeoutOption(timeout time.Duration) Option {
	return func(client *Client) {
		client.Timeout = timeout
	}
}

//TransportOption 设置传输
func TransportOption(transport *http.Transport) Option {
	return func(client *Client) {
		client.Transport = transport
	}
}

//MinBlockSizeOption 设置最小块大小 单位字节
func MinBlockSizeOption(blockSize uint64) Option {
	return func(client *Client) {
		client.blockSize = blockSize
	}
}
