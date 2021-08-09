package utils

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

// Gunzip unzips a byte array
func Gunzip(in []byte) ([]byte, error) {
	b := bytes.NewReader(in)
	reader, err := gzip.NewReader(b)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}
