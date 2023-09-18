package main

import (
	"database/sql"
	"flag"
	"log"

	_ "github.com/mattn/go-sqlite3" // SQLite driver

	"github.com/datacommonsorg/mixer/internal/server/resource"
	"github.com/datacommonsorg/mixer/internal/sqldb"
)

var (
	sqliteDir = flag.String("sqlite_dir", "", "SQLite directory.")
)

func main() {
	flag.Parse()
	sqlClient, err := sql.Open("sqlite3", *sqliteDir)
	if err != nil {
		log.Fatalf("Can not open SQL client %v", err)
	}
	if err := sqldb.Write(
		sqlClient,
		&resource.Metadata{
			SQLitePath:        *sqliteDir,
			RemoteMixerDomain: "https://autopush.api.datacommons.org",
			RemoteMixerAPIKey: "AIzaSyBCybF1COkc05kj5n5FHpXOnH3EdGBnUz0",
		}); err != nil {
		log.Fatalf("writer.Write() = %v", err)
	}
}
