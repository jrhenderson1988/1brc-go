package main

import (
	"bytes"
	"os"
	"testing"
)

func TestExecute(t *testing.T) {
	expected, err := os.ReadFile("./expected.txt")
	if err != nil {
		panic(err)
	}

	for _, useBufio := range []bool{true, false} {
		for _, useInt64 := range []bool{true, false} {
			buf := bytes.NewBufferString("")
			execute("./test.txt", useBufio, useInt64, buf)
			result := buf.String()
			if string(expected) != result {
				t.Fatalf("Content did not match.\n  actual   = %s,\n  expected = %s", result, expected)
			}
		}
	}
}
