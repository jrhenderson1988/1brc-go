package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"slices"
	"time"
)

const fileReadBufSize = 1024 * 1024 * 4

func main() {
	startProfile()
	defer stopProfile()

	start := time.Now()

	execute("./measurements.txt", os.Stdout)

	end := time.Now()
	elapsed := end.Sub(start)
	fmt.Printf("\n%s\n", elapsed)
}

func withFileReadParallel(path string) *stationData {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	totalChunks := 0
	resultCh := make(chan *stationData)
	buf := make([]byte, fileReadBufSize)
	leftOver := make([]byte, fileReadBufSize)
	leftOverSize := 0
	for {
		n, err := f.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}

		lastNewlinePos := -1
		for i := n - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				lastNewlinePos = i
				break
			}
		}

		chunk := make([]byte, lastNewlinePos+leftOverSize)
		copy(chunk[0:leftOverSize], leftOver[0:leftOverSize])
		copy(chunk[leftOverSize:leftOverSize+lastNewlinePos], buf[0:lastNewlinePos])

		go func(data []byte, ch chan *stationData) {
			ch <- consumeChunk(data)
		}(chunk, resultCh)

		leftOverSize = n - lastNewlinePos
		copy(leftOver[0:leftOverSize], buf[lastNewlinePos:lastNewlinePos+leftOverSize])
		totalChunks++
	}

	data := makeStationData()
	for i := 0; i < totalChunks; i++ {
		other := <-resultCh
		data.Merge(other)
	}
	close(resultCh)

	return data
}

func execute(path string, w io.Writer) {
	data := withFileReadParallel(path)
	output(data, w)
}

func consumeChunk(chunk []byte) *stationData {
	data := makeStationData()

	for _, line := range bytes.Split(chunk, []byte{'\n'}) {
		if len(line) < 1 {
			continue
		}
		data.ConsumeLine(line)
	}

	return data
}

func startProfile() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}

	if err = pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
}

func stopProfile() {
	pprof.StopCPUProfile()
}

func makeStationData() *stationData {
	return newStationData()
}

func getMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k, _ := range m {
		keys = append(keys, k)
	}

	return keys
}

func output(data *stationData, w io.Writer) {
	keys := data.StationNames()
	slices.Sort(keys)

	fmt.Fprint(w, "{")
	for i, k := range keys {
		if k == "" {
			panic("Empty key")
		}
		if i != 0 {
			fmt.Fprint(w, ", ")
		}

		d := data.ValuesOf(k)
		fmt.Fprintf(w, "%s=%.1f/%.1f/%.1f", k, d.Min(), d.Mean(), d.Max())
	}
	fmt.Fprint(w, "}")
}
