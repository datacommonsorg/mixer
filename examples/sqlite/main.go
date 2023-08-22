package main

import (
	"flag"
	"log"

	_ "github.com/mattn/go-sqlite3" // SQLite driver

	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/sqlite/writer"
)

var (
	sqlite_dir = flag.String("sqlite_dir", "", "SQLite directory.")
)

func main() {
	flag.Parse()
	if err := writer.Write(
		&resource.Metadata{
			SQLitePath:        *sqlite_dir,
			RemoteMixerDomain: "https://autopush.api.datacommons.org",
			RemoteMixerAPIKey: "AIzaSyBCybF1COkc05kj5n5FHpXOnH3EdGBnUz0",
		}); err != nil {
		log.Fatalf("writer.Write() = %v", err)
	}
}
