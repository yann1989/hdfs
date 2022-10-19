package hdfs

import "testing"

func TestRemoveAll(t *testing.T) {
	err := cli.RemoveAll("/abcdef/02.mp4")
	if err != nil {
		panic(err)
	}
}
