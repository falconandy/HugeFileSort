package main

import (
	"log"
	"os"
	"sort"
)

type Sorter struct {
	OutputPath   string
	Files        []File
	MaxFileSize  int
	CollatorPool *CollatorPool

	currentSize      int
	currentFileIndex int
}

type SortedFileLines struct {
	SortedLines []*Line
	FileIndex   int
}

func (s *Sorter) Sort() error {
	s.currentSize = 0
	s.currentFileIndex = 0

	w, err := NewWriter(s.OutputPath)
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()

	sortedFileLinesCh := make(chan SortedFileLines)

	writtenFileIndex := -1
	pendingLines := make(map[int][]*Line)

	s.scheduleNextFiles(sortedFileLinesCh)

	for lines := range sortedFileLinesCh {
		if lines.FileIndex == writtenFileIndex+1 {
			err := w.WriteLines(lines.SortedLines)
			if err != nil {
				log.Println(err)
			}
			writtenFileIndex++
			s.currentSize -= s.Files[writtenFileIndex].Size
			s.scheduleNextFiles(sortedFileLinesCh)

			for {
				if sortedLines, ok := pendingLines[writtenFileIndex+1]; ok {
					err := w.WriteLines(sortedLines)
					if err != nil {
						log.Println(err)
					}
					writtenFileIndex++
					delete(pendingLines, writtenFileIndex)
					s.currentSize -= s.Files[writtenFileIndex].Size
					s.scheduleNextFiles(sortedFileLinesCh)
				} else {
					break
				}
			}

			if writtenFileIndex == len(s.Files)-1 {
				break
			}
		} else {
			pendingLines[lines.FileIndex] = lines.SortedLines
		}
	}

	close(sortedFileLinesCh)

	return nil
}

func (s *Sorter) scheduleNextFiles(sortedFileLinesCh chan<- SortedFileLines) {
	for s.currentFileIndex < len(s.Files) && s.currentSize+s.Files[s.currentFileIndex].Size < s.MaxFileSize {
		currentFile := s.Files[s.currentFileIndex]

		go func(file File, fileIndex int) {
			lines, err := s.sortFileLines(file.Path)
			if err != nil {
				log.Println(err)
			}

			sortedFileLinesCh <- SortedFileLines{
				SortedLines: lines,
				FileIndex:   fileIndex,
			}

			if file.Temp {
				_ = os.Remove(file.Path)
			}
		}(currentFile, s.currentFileIndex)

		s.currentSize += currentFile.Size
		s.currentFileIndex++
	}
}

func (s *Sorter) sortFileLines(sourcePath string) ([]*Line, error) {
	collator := s.CollatorPool.Get()
	defer s.CollatorPool.Put(collator)

	reader, err := NewReader(sourcePath)
	if err != nil {
		return nil, err
	}

	var allLines []*Line
	for {
		line, err := reader.NextLine()
		if err != nil {
			return nil, err
		}
		if line == nil {
			break
		}

		allLines = append(allLines, line)
	}

	sort.Slice(allLines, func(i, j int) bool {
		return allLines[i].Less(allLines[j], collator)
	})

	return allLines, nil
}
