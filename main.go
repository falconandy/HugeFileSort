package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type File struct {
	Path string
	Size int
	Temp bool
}

func main() {
	var checkSorted bool
	var maxFileSize, chunkSize int
	var seed int64
	var tempDir string
	flag.BoolVar(&checkSorted, "check", false, "check SOURCE_FILE is sorted")
	flag.IntVar(&maxFileSize, "maxSize", 1*1024*1024*1024, "max file size to sort in-memory (bytes)")
	flag.IntVar(&chunkSize, "chunkSize", 0, "chunk size (bytes)")
	flag.Int64Var(&seed, "seed", 0, "seed")
	flag.StringVar(&tempDir, "tempDir", "", "temp dir")

	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Printf("Usage: %s [FLAGS] SOURCE_FILE [OUTPUT_FILE]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	sourcePath := args[0]
	var outputPath string
	if len(args) > 1 {
		outputPath = args[1]
	} else {
		ext := filepath.Ext(sourcePath)
		outputPath = strings.TrimSuffix(sourcePath, ext) + ".sorted" + ext
	}

	var err error
	if checkSorted {
		err = checkIsSorted(sourcePath)
	} else {
		if chunkSize == 0 {
			chunkSize = maxFileSize / 100
		}

		if seed == 0 {
			seed = time.Now().UnixNano()
			fmt.Println("seed:", seed)
		}

		if tempDir == "" {
			tempDir, err = os.MkdirTemp("", "hugesort-*")
			if err != nil {
				log.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(tempDir) }()
		} else {
			err := os.MkdirAll(tempDir, 0777)
			if err != nil {
				log.Fatal(err)
			}
		}

		err = splitAndSort(sourcePath, outputPath, maxFileSize, chunkSize, seed, tempDir)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func splitAndSort(sourcePath, outputPath string, maxFileSize, chunkSize int, seed int64, tempDir string) error {
	splitter := Splitter{
		MaxFileSize: maxFileSize,
		ChunkSize:   chunkSize,
		TempDir:     tempDir,
		Seed:        seed,
		WorkerCount: runtime.NumCPU(),
	}

	nowSplit := time.Now()
	splitFiles, err := splitter.Split(sourcePath)
	if err != nil {
		return err
	}
	fmt.Println("Split done in", time.Since(nowSplit).Round(time.Millisecond))

	sorter := Sorter{
		OutputPath:  outputPath,
		Files:       splitFiles,
		MaxFileSize: maxFileSize,
	}

	nowSort := time.Now()
	err = sorter.Sort()
	if err != nil {
		return err
	}
	fmt.Println("Sort/Write done in", time.Since(nowSort).Round(time.Millisecond))

	fmt.Println("TOTAL done in", time.Since(nowSplit).Round(time.Millisecond))

	return nil
}

func checkIsSorted(sourcePath string) error {
	reader, err := NewReader(sourcePath)
	if err != nil {
		return err
	}

	var currentLine Line
	for {
		line, err := reader.NextLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if line.Less(currentLine) {
			fmt.Println("not sorted")
			return nil
		}
		currentLine = line
	}

	fmt.Println("sorted")
	return nil
}
