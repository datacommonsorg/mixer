package main

import (
	"flag"
	"log"

	"github.com/datacommonsorg/mixer/sqlite/writer"
)

var (
	inputDir = flag.String("input_dir",
		"",
		"Input directory.")
	outputDir = flag.String("output_dir",
		"",
		"Output directory.")
)

func main() {
	w := writer.New(*inputDir, *outputDir)
	if err := w.Write(); err != nil {
		log.Fatalf("writer.Write() = %v", err)
	}
}
