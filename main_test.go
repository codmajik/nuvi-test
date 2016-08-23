package main

import (
	"archive/zip"
	"bytes"
	"strings"
	"sync"
	"testing"
)

type MockContent struct {
	Name, Content string
}

type MockRange struct {
	t        *testing.T
	pending  bool
	fileName string
	files    []MockContent
}

func (c *MockRange) Next() (string, bool) {
	return c.fileName, c.pending
}

func (c *MockRange) Reader(fileName string) (*zip.Reader, error) {
	buf := new(bytes.Buffer)

	// Create a new zip archive.
	w := zip.NewWriter(buf)

	// Add some files to the archive.

	for _, file := range c.files {
		f, err := w.Create(file.Name)
		if err != nil {
			c.t.Error("add item to zip failed", file.Name, err)
			return nil, err
		}
		_, err = f.Write([]byte(file.Content))
		if err != nil {
			c.t.Error("write content of zip item ", file.Name, "failed", err)
			return nil, err
		}
	}

	// Make sure to check the error on Close.
	err := w.Close()
	if err != nil {
		c.t.Error("Close Buffer Failed", err)
		return nil, err
	}

	return zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
}

func (c *MockRange) CleanUp(fileName string) {
	c.pending = false
}

func TestExtractFileList(t *testing.T) {
	testcases := []struct {
		html   string
		count  int
		values []string
	}{
		{"<a href='ksdfjlsdjfsldfkjsd'></a>", 0, nil},
		{"<a href='dsfjsdlfjskdl.zip'></a>", 1, []string{"dsfjsdlfjskdl.zip"}},
		{"<a href='first.zip'></a>", 1, []string{"first.zip"}},
		{"<a href='first.zip'></a><a href='another.zip'></a>", 2, []string{"first.zip", "another.zip"}},
	}

	for _, c := range testcases {

		res := extractFileList(strings.NewReader(c.html))
		if len(res) != c.count {
			t.Fail()
		}

		for idx, val := range c.values {
			if res[idx] != val {
				t.Errorf("expected %s at index %d got %s", val, idx, res[idx])
			}
		}
	}
}

func TestUnZipAndStore(t *testing.T) {

	content := []MockContent{
		{"test_content1.xml", "<a>content of xml 1 inside zip</a>"},
		{"test_content2.xml", "<a>content of xml 2 inside zip</a>"},
		{"test_content3.xml", "<a>content of xml 3 inside zip</a>"},
	}

	mrange := &MockRange{t, true, "TestUnzipAndStoreFile.zip", content}

	redisConn := NewMockRedis(t)
	extractNstore(redisConn, mrange, &sync.WaitGroup{})

	redisConn.CheckActAndCommand(0, "SEND", "MULTI")
	redisConn.CheckActAndCommand(1, "SEND", "DEL", REDIS_LIST_NAME)

	for idx, file := range mrange.files {
		redisConn.CheckActAndCommand(idx+2, "SEND", "LPUSH", REDIS_LIST_NAME, file.Content)
	}

	redisConn.CheckActAndCommand(len(mrange.files)+2, "DO", "EXEC")
}
