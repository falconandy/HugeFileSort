# HugeFileSort

Черновая реализация сортировки большого файла

https://habr.com/ru/post/714524/

```shell
go build
```

```
Usage: ./HugeFileSort [FLAGS] SOURCE_FILE [OUTPUT_FILE]
  -check
        check SOURCE_FILE is sorted
  -chunkSize int
        chunk size (bytes) (default maxSize/10)
  -maxSize int
        max file size to sort in-memory (bytes) (default 1073741824)
  -seed int
        seed
  -tempDir string
        temp dir
```
