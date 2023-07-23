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
			RemoteMixerDomain: "https://api.datacommons.org",
			RemoteMixerAPIKey: "AIzaSyCTI4Xz-UW_G2Q2RfknhcfdAnTHq5X5XuI",
		}); err != nil {
		log.Fatalf("writer.Write() = %v", err)
	}
}
