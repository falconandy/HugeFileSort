package main

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	f, err := os.CreateTemp("", "")
	require.Nil(t, err)

	_, err = f.WriteString(`111111.abc
22222.xyz
3.q
4444444.qwerty`)
	require.Nil(t, err)

	err = f.Close()
	require.Nil(t, err)

	defer func() { _ = os.Remove(f.Name()) }()

	r, err := NewReader(f.Name())
	require.Nil(t, err)

	line, err := r.NextLine()
	assert.Nil(t, err)
	assert.Equal(t, 111111, line.index)
	assert.Equal(t, "abc", string(line.text))

	line, err = r.NextLine()
	assert.Nil(t, err)
	assert.Equal(t, 22222, line.index)
	assert.Equal(t, "xyz", string(line.text))

	line, err = r.NextLine()
	assert.Nil(t, err)
	assert.Equal(t, 3, line.index)
	assert.Equal(t, "q", string(line.text))

	line, err = r.NextLine()
	assert.Nil(t, err)
	assert.Equal(t, 4444444, line.index)
	assert.Equal(t, "qwerty", string(line.text))

	line, err = r.NextLine()
	assert.True(t, errors.Is(err, io.EOF))
}
