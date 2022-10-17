// Author: yann
// Date: 2022/5/22
// Desc: hdfsrest

package hdfs

var cli *Client

func init() {
	cli = New(HostOption("192.168.10.13"), MinBlockSizeOption(DefaultBlockSize/128), PortOption(DefaultPort), UserOption("hdfs"))
}
