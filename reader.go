package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"strconv"
)

type Reader struct {
	f        *os.File
	br       *bufio.Reader
	position int64
}

func NewReader(filePath string) (*Reader, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return &Reader{
		f:  f,
		br: bufio.NewReader(f),
	}, nil
}

func (r *Reader) NextLine() (*Line, error) {
	data, err := r.br.ReadBytes('\n')
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
		if len(data) == 0 {
			return nil, nil
		}
	}

	r.position += int64(len(data))
	dotIndex := bytes.IndexByte(data, '.')
	index, _ := strconv.ParseInt(string(data[:dotIndex]), 10, 64)
	text := data[dotIndex+1:]
	if len(text) > 0 && text[len(text)-1] == '\n' {
		text = text[:len(text)-1]
	}
	return &Line{
		index:    index,
		text:     text,
		position: r.position,
	}, nil
}
