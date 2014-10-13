package hdfs

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"testing"
)

const (
	// many grep -b died to bring us this information...
	testStr    = "Abominable are the tumblers into which he pours his poison."
	testStrOff = 48847

	testStr2            = "http://www.gutenberg.org"
	testStr2Off         = 1256988
	testStr2NegativeOff = -288
)

func TestFileRead(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/foo.txt")
	require.Nil(t, err)

	bytes, err := ioutil.ReadAll(file)
	assert.Nil(t, err)
	assert.Equal(t, "bar\n", string(bytes))
}

func TestReadEmptyFile(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/emptyfile")

	file, err := client.Open("/_test/emptyfile")
	require.Nil(t, err)

	bytes, err := ioutil.ReadAll(file)
	assert.Nil(t, err)
	assert.Equal(t, "", string(bytes))
}

func TestFileBigReadN(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/mobydick.txt")
	require.Nil(t, err)

	n, err := io.CopyN(ioutil.Discard, file, 1000000)
	assert.Nil(t, err)
	assert.Equal(t, n, 1000000)
}

func TestFileReadAt(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/mobydick.txt")
	require.Nil(t, err)

	buf := make([]byte, len(testStr))
	off := 0
	for off < len(buf) {
		n, err := file.ReadAt(buf[off:], int64(testStrOff+off))
		assert.Nil(t, err)
		assert.True(t, n > 0)
		off += n
	}

	assert.Equal(t, string(buf), testStr)

	buf = make([]byte, len(testStr2))
	off = 0
	for off < len(buf) {
		n, err := file.ReadAt(buf[off:], int64(testStr2Off+off))
		assert.Nil(t, err)
		assert.True(t, n > 0)
		off += n
	}

	assert.Equal(t, testStr2, string(buf))
}

func TestFileSeek(t *testing.T) {
	client := getClient(t)

	file, err := client.Open("/_test/mobydick.txt")
	require.Nil(t, err)

	buf := new(bytes.Buffer)

	off, err := file.Seek(testStrOff, 0)
	assert.Nil(t, err)
	assert.Equal(t, testStrOff, off)

	n, err := io.CopyN(buf, file, int64(len(testStr)))
	assert.Nil(t, err)
	assert.Equal(t, len(testStr), n)
	assert.Equal(t, testStr, string(buf.Bytes()))

	// seek backwards and read it again
	off, err = file.Seek(-1*int64(len(testStr)), 1)
	assert.Nil(t, err)
	assert.Equal(t, testStrOff, off)

	buf.Reset()
	n, err = io.CopyN(buf, file, int64(len(testStr)))
	assert.Nil(t, err)
	assert.Equal(t, len(testStr), n)
	assert.Equal(t, testStr, string(buf.Bytes()))

	// now seek forward to another block and read a string
	off, err = file.Seek(testStr2NegativeOff, 2)
	assert.Nil(t, err)
	assert.Equal(t, testStr2Off, off)

	buf.Reset()
	n, err = io.CopyN(buf, file, int64(len(testStr2)))
	assert.Nil(t, err)
	assert.Equal(t, len(testStr2), n)
	assert.Equal(t, testStr2, string(buf.Bytes()))
}

func TestFileReadDir(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/fulldir3")
	mkdirp(t, "/_test/fulldir3/dir")
	touch(t, "/_test/fulldir3/1")
	touch(t, "/_test/fulldir3/2")
	touch(t, "/_test/fulldir3/3")

	file, err := client.Open("/_test/fulldir3")
	require.Nil(t, err)

	res, err := file.Readdir(2)
	require.Equal(t, 2, len(res))
	assert.Equal(t, "/_test/fulldir3/1", res[0].Name())
	assert.Equal(t, "/_test/fulldir3/2", res[1].Name())

	res, err = file.Readdir(5)
	require.Equal(t, 2, len(res))
	assert.Equal(t, "/_test/fulldir3/3", res[0].Name())
	assert.Equal(t, "/_test/fulldir3/dir", res[1].Name())

	res, err = file.Readdir(0)
	require.Equal(t, 4, len(res))
	assert.Equal(t, "/_test/fulldir3/1", res[0].Name())
	assert.Equal(t, "/_test/fulldir3/2", res[1].Name())
	assert.Equal(t, "/_test/fulldir3/3", res[2].Name())
	assert.Equal(t, "/_test/fulldir3/dir", res[3].Name())
}

func TestFileReadDirnames(t *testing.T) {
	client := getClient(t)

	mkdirp(t, "/_test/fulldir4")
	mkdirp(t, "/_test/fulldir4/dir")
	touch(t, "/_test/fulldir4/1")
	touch(t, "/_test/fulldir4/2")
	touch(t, "/_test/fulldir4/3")

	file, err := client.Open("/_test/fulldir4")
	require.Nil(t, err)

	res, err := file.Readdirnames(0)
	require.Equal(t, 4, len(res))
	assert.Equal(t, []string{
		"/_test/fulldir4/1",
		"/_test/fulldir4/2",
		"/_test/fulldir4/3",
		"/_test/fulldir4/dir",
	}, res)
}
