package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeekReader(t *testing.T) {
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

	r, err := NewSeekReader(f.Name())
	require.Nil(t, err)

	line, err := r.Line(1)
	assert.Nil(t, err)
	assert.Equal(t, 22222, line.index)
	assert.Equal(t, []byte("xyz"), line.text)

	line, err = r.Line(20)
	assert.Nil(t, err)
	assert.Equal(t, 3, line.index)
	assert.Equal(t, []byte("q"), line.text)

	line, err = r.Line(0)
	assert.Nil(t, err)
	assert.Equal(t, 22222, line.index)
	assert.Equal(t, []byte("xyz"), line.text)
}
