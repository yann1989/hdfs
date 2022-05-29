// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

var cli *Client

func init() {
	cli = New(HostOption("10.1.141.215"))
}
