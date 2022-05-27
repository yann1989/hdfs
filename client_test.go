// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

var cli *Client

func init() {
	cli = New(&ClientOption{
		DataNodes: []string{"10.1.141.215:14000"},
		User:      "root",
	})
}
