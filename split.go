package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	lineBatchSize = 1000
)

type Splitter struct {
	MaxFileSize int
	ChunkSize   int
	TempDir     string
	Seed        int64
	WorkerCount int

	randGenerator     *rand.Rand
	nextTempFileIndex int
}

func (s *Splitter) Split(sourcePath string) ([]File, error) {
	stat, err := os.Stat(sourcePath)
	if err != nil {
		return nil, err
	}
	sourceSize := int(stat.Size())

	seed := s.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	randSource := rand.NewSource(seed)
	s.randGenerator = rand.New(randSource)

	return s.splitFile(File{
		Path: sourcePath,
		Size: sourceSize,
		Temp: false,
	})
}

func (s *Splitter) splitFile(f File) ([]File, error) {
	if f.Size <= s.MaxFileSize {
		return []File{f}, nil
	}

	chunkStarts, err := s.getChunkStarts(f)
	if err != nil {
		return nil, err
	}

	reader, err := NewReader(f.Path)
	if err != nil {
		return nil, err
	}

	writersInput := make([]chan []Line, len(chunkStarts)+1)
	writers := make([]*Writer, len(chunkStarts)+1)

	lineBatchCh := make(chan []Line, s.WorkerCount)

	var writersWG sync.WaitGroup
	for i := range writers {
		writerPath := filepath.Join(s.TempDir, fmt.Sprintf("%d.txt", s.nextTempFileIndex))
		s.nextTempFileIndex++

		writers[i], err = NewWriter(writerPath)
		writersInput[i] = make(chan []Line, lineBatchSize)

		writersWG.Add(1)
		go func(w *Writer, linesCh <-chan []Line) {
			defer writersWG.Done()
			s.writeLines(w, linesCh)
		}(writers[i], writersInput[i])
	}

	var splitterWG sync.WaitGroup
	for i := 0; i < s.WorkerCount; i++ {
		splitterWG.Add(1)
		go func() {
			defer splitterWG.Done()
			s.splitLines(chunkStarts, lineBatchCh, writersInput)
		}()
	}

	lineBatch := make([]Line, 0, lineBatchSize)
	for {
		line, err := reader.NextLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		lineBatch = append(lineBatch, line)

		if len(lineBatch) >= lineBatchSize {
			lineBatchCh <- lineBatch
			lineBatch = make([]Line, 0, lineBatchSize)
		}
	}

	if len(lineBatch) > 0 {
		lineBatchCh <- lineBatch
	}

	close(lineBatchCh)

	splitterWG.Wait()

	for _, ch := range writersInput {
		close(ch)
	}

	writersWG.Wait()

	splitFiles, err := s.getSplitFiles(writers)
	if err != nil {
		return nil, err
	}

	return splitFiles, nil
}

func (s *Splitter) getChunkStarts(f File) ([]Line, error) {
	chunkCount := (f.Size-1)/s.ChunkSize + 1
	seekReader, err := NewSeekReader(f.Path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = seekReader.Close() }()

	offsets := make([]int, chunkCount-1)
	for i := range offsets {
		offsets[i] = s.randGenerator.Intn(f.Size)
	}
	sort.Ints(offsets)

	chunkStarts := make([]Line, 0, len(offsets))
	for _, offset := range offsets {
		line, err := seekReader.Line(offset)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			continue
		}

		chunkStarts = append(chunkStarts, line)
	}

	err = seekReader.Close()
	if err != nil {
		return nil, err
	}

	sort.Slice(chunkStarts, func(i, j int) bool {
		return chunkStarts[i].Less(chunkStarts[j])
	})

	// Remove possible duplicates
	for i := len(chunkStarts) - 1; i > 0; i-- {
		if chunkStarts[i].Equal(chunkStarts[i-1]) {
			copy(chunkStarts[i+1:], chunkStarts[i:])
			chunkStarts = chunkStarts[:len(chunkStarts)-1]
		}
	}

	return chunkStarts, nil
}

func (s *Splitter) splitLines(chunkStarts []Line, lineBatchCh <-chan []Line, writersInput []chan []Line) {
	for lines := range lineBatchCh {
		chunkIndexes := make(map[int][]Line)
		for _, line := range lines {
			chunkIndex := sort.Search(len(chunkStarts), func(i int) bool {
				return !chunkStarts[i].Less(line)
			})

			if chunkIndex < len(chunkStarts) && line.Equal(chunkStarts[chunkIndex]) {
				chunkIndex++
			}

			chunkIndexes[chunkIndex] = append(chunkIndexes[chunkIndex], line)
		}

		for chunkIndex, lines := range chunkIndexes {
			writersInput[chunkIndex] <- lines
		}
	}
}

func (s *Splitter) writeLines(w *Writer, linesCh <-chan []Line) {
	for lines := range linesCh {
		err := w.WriteLines(lines)
		if err != nil {
			log.Println(err)
		}
	}
	err := w.Close()
	if err != nil {
		log.Println(err)
	}
}

func (s *Splitter) getSplitFiles(writers []*Writer) ([]File, error) {
	var splitFiles []File
	for _, writer := range writers {
		f := File{
			Path: writer.FilePath,
			Size: writer.Size,
			Temp: true,
		}

		if writer.Size > s.MaxFileSize {
			files, err := s.splitFile(f)
			if err != nil {
				return nil, err
			}
			splitFiles = append(splitFiles, files...)

			if f.Temp {
				_ = os.Remove(f.Path)
			}
		} else {
			splitFiles = append(splitFiles, f)
		}
	}

	return splitFiles, nil
}
