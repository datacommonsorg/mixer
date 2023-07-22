package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/datacommonsorg/mixer/internal/server/resource"
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
	if err := writer.Write(*inputDir,
		*outputDir,
		&resource.Metadata{
			RemoteMixerDomain: "https://api.datacommons.org",
			RemoteMixerAPIKey: "AIzaSyCTI4Xz-UW_G2Q2RfknhcfdAnTHq5X5XuI",
		},
		&http.Client{}); err != nil {
		log.Fatalf("writer.Write() = %v", err)
	}
}
