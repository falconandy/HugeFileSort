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
	f        *os.File
	br       *bufio.Reader
	position int64
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

func (r *SeekReader) Line(offset int) (*Line, error) {
	_, err := r.f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	r.br.Reset(r.f)
	skipData, err := r.br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	data, err := r.br.ReadBytes('\n')
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
		if len(data) == 0 {
			return nil, nil
		}
	}

	r.position += int64(len(skipData) + len(data))
	dotIndex := bytes.IndexByte(data, '.')
	index, _ := strconv.ParseInt(string(data[:dotIndex]), 10, 64)
	text := data[dotIndex+1 : len(data)-1]
	return &Line{
		index:    index,
		text:     text,
		position: r.position,
	}, nil
}

func (r *SeekReader) Close() error {
	return r.f.Close()
}
