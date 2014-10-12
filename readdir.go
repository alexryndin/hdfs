package hdfs

import (
	"code.google.com/p/goprotobuf/proto"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"os"
	"strings"
)

// ReadDir reads the directory named by dirname and returns a list of sorted
// directory entries.
func (c *Client) ReadDir(dirname string) ([]os.FileInfo, error) {
	return c.getDirList(dirname, "", 0)
}

func (c *Client) getDirList(dirname string, after string, max int) ([]os.FileInfo, error) {
	res := make([]os.FileInfo, 0)
	last := after
	for max <= 0 || len(res) < max {
		partial, remaining, err := c.getPartialDirList(dirname, last)
		if err != nil {
			return nil, err
		}

		res = append(res, partial...)
		if remaining == 0 {
			break
		}
	}

	if max > 0 && len(res) > max {
		res = res[:max]
	}

	return res, nil
}

func (c *Client) getPartialDirList(dirname string, after string) ([]os.FileInfo, int, error) {
	dirname = strings.TrimSuffix(dirname, "/")

	req := &hdfs.GetListingRequestProto{
		Src:          proto.String(dirname),
		StartAfter:   []byte(after),
		NeedLocation: proto.Bool(false),
	}
	resp := &hdfs.GetListingResponseProto{}

	err := c.namenode.Execute("getListing", req, resp)
	if err != nil {
		return nil, 0, err
	}

	list := resp.GetDirList().GetPartialListing()
	res := make([]os.FileInfo, 0, len(list))
	for _, status := range list {
		res = append(res, newFileInfo(status, "", dirname))
	}

	remaining := int(resp.GetDirList().GetRemainingEntries())
	return res, remaining, nil
}