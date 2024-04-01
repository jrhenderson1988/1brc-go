package main

import (
	"bufio"
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

type stationData interface {
	StationNames() []string
	ConsumeLine(line []byte)
	ValuesOf(name string) values
}

type values interface {
	Min() float64
	Mean() float64
	Max() float64
}

func main() {
	startProfile()
	defer stopProfile()

	start := time.Now()

	//////////////////////////////////
	//benchmarkFileRead()
	//benchmarkBufioScanner()
	//////////////////////////////////
	const useInt64 = false
	const useBufio = false

	execute("./measurements.txt", useBufio, useInt64, os.Stdout)

	end := time.Now()
	elapsed := end.Sub(start)
	fmt.Printf("\n%s\n", elapsed)
}

func withBufferedScanner(path string, fn func(line []byte)) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := makeScanner(f)
	for scanner.Scan() {
		fn(scanner.Bytes())
	}
}

func withFileRead(path string, fn func(line []byte)) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	buf := make([]byte, fileReadBufSize)
	leftOver := make([]byte, fileReadBufSize)
	leftOverSize := 0
	for {
		n, err := f.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}

		start := 0
		for i := start; i < n; i++ {
			if buf[i] == '\n' {
				if start == 0 {
					// include leftover
					copy(leftOver[leftOverSize:leftOverSize+i], buf[0:i])
					line := leftOver[0 : leftOverSize+i]
					fn(line)
				} else {
					line := buf[start:i]
					fn(line)
				}
				start = i + 1
			}
		}

		if start < n {
			leftOverSize = n - start
			copy(leftOver[0:leftOverSize], buf[n-leftOverSize:])
		} else {
			leftOverSize = 0
		}
	}

	if leftOverSize > 0 {
		line := leftOver[0:leftOverSize]
		fn(line)
	}
}

func execute(path string, useBufio, useInt64 bool, w io.Writer) {
	data := makeStationData(useInt64)

	fn := func(line []byte) {
		data.ConsumeLine(line)
	}

	if useBufio {
		withBufferedScanner(path, fn)
	} else {
		withFileRead(path, fn)
	}

	output(data, w)
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

func makeScanner(file *os.File) *bufio.Scanner {
	scanner := bufio.NewScanner(file)
	const bufferSize = 1024 * 1024 * 8
	buf := make([]byte, bufferSize)
	scanner.Buffer(buf, bufferSize)

	return scanner
}

func makeStationData(useInt64 bool) stationData {
	if useInt64 {
		return newStationDataInt64()
	} else {
		return newStationDataFloat64()
	}
}

func getMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k, _ := range m {
		keys = append(keys, k)
	}

	return keys
}

func output(data stationData, w io.Writer) {
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

// int64 + bufio = 38.277s
// int64 + fileread = 35.982s
// float64 + bufio = 40.643s
// float64 + fileread = 39.208s
