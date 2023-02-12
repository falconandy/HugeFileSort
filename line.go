package main

import (
	"bytes"
	"unicode/utf8"

	"golang.org/x/text/collate"
)

type Line struct {
	text     []byte
	index    int64
	position int64
}

func (line *Line) Same(other *Line) bool {
	return line.position == other.position
}

func (line *Line) Less(other *Line, collator *collate.Collator) bool {
	if collator == nil {
		return line.LessBytes(other)
	}

	text1, text2 := line.text, other.text

	if &text1[0] != &text2[0] {
		n := 0
		for n < len(text1) && n < len(text2) && text1[n] == text2[n] {
			n++
		}

		switch {
		case n == len(text1) && n == len(text2):
			other.text = line.text
		case n == len(text1) && n < len(text2):
			return true
		case n < len(text1) && n == len(text2):
			return false
		default:
			if n > 0 {
				for {
					r, size := utf8.DecodeLastRune(text1[:n])
					n -= size
					if r != utf8.RuneError {
						break
					}
					if size == 0 {
						break
					}
				}
			}

			textCompare := collator.Compare(text1[n:], text2[n:])

			if textCompare != 0 {
				return textCompare < 0
			}
		}
	}

	if line.index != other.index {
		return line.index < other.index
	}

	return line.position < other.position
}

func (line *Line) LessBytes(other *Line) bool {
	textCompare := bytes.Compare(line.text, other.text)

	if textCompare != 0 {
		return textCompare < 0
	}

	if line.index != other.index {
		return line.index < other.index
	}

	return line.position < other.position
}
