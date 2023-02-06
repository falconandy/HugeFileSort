package main

import (
	"bytes"
)

type Line struct {
	index int
	text  []byte
}

func (line Line) Equal(other Line) bool {
	return line.index == other.index && bytes.Equal(line.text, other.text)
}

func (line Line) Less(other Line) bool {
	textCompare := bytes.Compare(line.text, other.text)
	if textCompare != 0 {
		return textCompare < 0
	}

	return line.index < other.index
}
