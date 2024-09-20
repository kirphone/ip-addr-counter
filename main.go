package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math/bits"
	"os"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	start := time.Now()

	numWorkers := runtime.NumCPU()
	log.Printf("using %d workers\n", numWorkers)

	fileName := "ip_addresses"
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("failed to open input file: %v", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("failed to stat input file: %v", err)
	}
	fileSize := fileInfo.Size()

	chunkSize := fileSize / int64(numWorkers)
	offsets := make([]int64, 0, numWorkers+1)

	for i := 0; i < numWorkers; i++ {
		offsets = append(offsets, int64(i)*chunkSize)
	}

	offsets = append(offsets, fileSize)

	bitmaps := make([][]uint64, numWorkers)
	var g errgroup.Group

	for i := 0; i < numWorkers; i++ {
		i := i
		g.Go(func() error {
			bitmap, err := processChunk(fileName, offsets[i], offsets[i+1])
			if err != nil {
				return fmt.Errorf("worker %d failed: %v", i, err)
			}

			bitmaps[i] = bitmap
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("processing failed: %v", err)
	}

	finalBitmap := mergeBitmaps(bitmaps, len(bitmaps[0]))

	totalUniqueIPs := 0
	for _, word := range finalBitmap {
		totalUniqueIPs += bits.OnesCount64(word)
	}

	log.Printf("total unique IP addresses: %d\n", totalUniqueIPs)

	totalElapsed := time.Since(start)
	log.Printf("total time elapsed: %v\n", totalElapsed)
}

func processChunk(fileName string, startOffset, endOffset int64) ([]uint64, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	_, err = file.Seek(startOffset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek in file: %v", err)
	}

	reader := bufio.NewReader(file)

	if startOffset != 0 {
		_, err = readLine(reader)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to discard partial line: %v", err)
		}
	}

	const bitmapSize = 1 << 32
	const uint64Size = 64
	const arraySize = bitmapSize / uint64Size
	bitmap := make([]uint64, arraySize)

	currentOffset := startOffset

	for currentOffset < endOffset {
		line, err := readLine(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading line: %v", err)
		}
		currentOffset += int64(len(line)) + 1

		ipUint32, err := parseIPv4(line)
		if err != nil {
			continue
		}

		idx := ipUint32 / uint32(uint64Size)
		pos := ipUint32 % uint32(uint64Size)
		bitmap[idx] |= 1 << pos
	}

	return bitmap, nil
}

func readLine(reader *bufio.Reader) ([]byte, error) {
	line, isPrefix, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}
	if isPrefix {
		return nil, fmt.Errorf("line too long")
	}
	return line, nil
}

func mergeBitmaps(bitmaps [][]uint64, bitmapSize int) []uint64 {
	finalBitmap := make([]uint64, bitmapSize)
	numWorkers := len(bitmaps)

	for i := 0; i < bitmapSize; i++ {
		var word uint64
		for j := 0; j < numWorkers; j++ {
			word |= bitmaps[j][i]
		}
		finalBitmap[i] = word
	}

	return finalBitmap
}

func parseIPv4(ipStr []byte) (uint32, error) {
	var ip uint32
	var octet uint32
	var shift uint
	parts := 0

	for i := 0; i < len(ipStr); i++ {
		c := ipStr[i]
		if c >= '0' && c <= '9' {
			octet = octet*10 + uint32(c-'0')
			if octet > 255 {
				return 0, fmt.Errorf("invalid octet value")
			}
		} else if c == '.' {
			if parts >= 3 {
				return 0, fmt.Errorf("too many octets")
			}
			ip |= octet << (24 - shift)
			octet = 0
			shift += 8
			parts++
		} else {
			return 0, fmt.Errorf("invalid character in IP")
		}
	}
	ip |= octet << (24 - shift)
	if parts != 3 {
		return 0, fmt.Errorf("not enough octets")
	}
	return ip, nil
}
