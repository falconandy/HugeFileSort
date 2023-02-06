package main

import (
	"bufio"
	"os"
	"strconv"
)

type Writer struct {
	FilePath string
	Size     int

	f  *os.File
	bw *bufio.Writer
}

func NewWriter(filePath string) (*Writer, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	return &Writer{
		FilePath: filePath,
		f:        f,
		bw:       bufio.NewWriter(f),
	}, nil
}

func (w *Writer) WriteLines(lines []Line) error {
	for _, line := range lines {
		err := w.WriteLine(line)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) WriteLine(line Line) error {
	n, err := w.bw.WriteString(strconv.Itoa(line.index))
	if err != nil {
		return err
	}
	w.Size += n

	err = w.bw.WriteByte('.')
	if err != nil {
		return err
	}
	w.Size += 1

	n, err = w.bw.Write(line.text)
	if err != nil {
		return err
	}
	w.Size += n

	err = w.bw.WriteByte('\n')
	if err != nil {
		return err
	}
	w.Size += 1

	return nil
}

func (w *Writer) Close() error {
	err := w.bw.Flush()
	if err != nil {
		return err
	}
	return w.f.Close()
}
