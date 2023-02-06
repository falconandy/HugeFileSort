package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"strconv"
)

type SeekReader struct {
	f  *os.File
	br *bufio.Reader
}

func NewSeekReader(filePath string) (*SeekReader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return &SeekReader{
		f:  f,
		br: bufio.NewReader(f),
	}, nil
}

func (r *SeekReader) Line(offset int) (Line, error) {
	_, err := r.f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return Line{}, err
	}

	r.br.Reset(r.f)
	_, err = r.br.ReadBytes('\n')
	if err != nil {
		return Line{}, err
	}

	data, err := r.br.ReadBytes('\n')
	if err != nil {
		if !errors.Is(err, io.EOF) || len(data) == 0 {
			return Line{}, err
		}
	}

	dotIndex := bytes.IndexByte(data, '.')
	index, _ := strconv.Atoi(string(data[:dotIndex]))
	return Line{
		index: index,
		text:  data[dotIndex+1 : len(data)-1],
	}, nil
}

func (r *SeekReader) Close() error {
	return r.f.Close()
}
