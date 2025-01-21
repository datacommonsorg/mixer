// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A SQL client wrapper.

package sqldb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	_ "modernc.org/sqlite" // import the sqlite driver
)

const (
	// Drivers
	sqliteDriver = "sqlite"
	mysqlDriver  = "mysql"

	// Cloud SQL constants
	cloudSQLConnectionIdentifier = "cloudsqlconn"
	cloudSQLDefaultPort          = "3306"
	cloudSQLDefaultDbName        = "datacommons"
	cloudSQLDbUserKey            = "DB_USER"
	cloudSQLDbPassKey            = "DB_PASS"
	cloudSQLDbNameKey            = "DB_NAME"
	cloudSQLDbPortKey            = "DB_PORT"
)

// SQLClient encapsulates a SQL DB connection.
type SQLClient struct {
	dbx *sqlx.DB
}

// UseConnections uses connections from the src client to this client.
// This method is to workaround the fact that we currently need to maintain the client by value in the store but connections by reference.
// This method should be removed once the store maintains the client by reference.
func (sc *SQLClient) UseConnections(src *SQLClient) {
	sc.dbx = src.dbx
}

// Close closes the underlying database connection
func (sc *SQLClient) Close() error {
	if sc.dbx != nil {
		return sc.dbx.Close()
	}
	return nil
}

func IsConnected(sqlClient *SQLClient) bool {
	return sqlClient != nil && sqlClient.dbx != nil
}

func NewSQLiteClient(sqlitePath string) (*SQLClient, error) {
	db, err := newSQLiteConnection(sqlitePath)
	if err != nil {
		return nil, err
	}
	return newSQLClient(db, sqliteDriver), nil
}

func NewCloudSQLClient(instanceName string) (*SQLClient, error) {
	db, err := newCloudSQLConnection(instanceName)
	if err != nil {
		return nil, err
	}
	return newSQLClient(db, mysqlDriver), nil
}

func newSQLClient(db *sql.DB, driver string) *SQLClient {
	return &SQLClient{
		dbx: sqlx.NewDb(db, driver),
	}
}

func newSQLiteConnection(dbPath string) (*sql.DB, error) {
	_, err := os.Stat(dbPath)
	if err != nil {
		return nil, fmt.Errorf("error accessing sqlite db file: %s (%w)", dbPath, err)
	}

	db, err := sql.Open(sqliteDriver, dbPath)
	if err != nil {
		return nil, fmt.Errorf("error opening sqlite db: %s (%w)", dbPath, err)
	}
	log.Printf("Connected to sqlite db: %s", dbPath)
	return db, err
}

func newCloudSQLConnection(instanceName string) (*sql.DB, error) {
	mustGetenv := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			log.Fatalf("environment variable not set: %s", k)
		}
		return v
	}
	getenv := func(key, fallback string) string {
		value := os.Getenv(key)
		if len(value) == 0 {
			return fallback
		}
		return value
	}
	var (
		dbUser = mustGetenv(cloudSQLDbUserKey)
		dbPwd  = mustGetenv(cloudSQLDbPassKey)
		dbName = getenv(cloudSQLDbNameKey, cloudSQLDefaultDbName)
		dbPort = getenv(cloudSQLDbPortKey, cloudSQLDefaultPort)
	)

	d, err := cloudsqlconn.NewDialer(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cloudsqlconn.NewDialer: %w", err)
	}
	var opts []cloudsqlconn.DialOption
	mysql.RegisterDialContext(cloudSQLConnectionIdentifier,
		func(ctx context.Context, addr string) (net.Conn, error) {
			return d.Dial(ctx, instanceName, opts...)
		})

	dbURI := fmt.Sprintf(
		"%s:%s@%s(localhost:%s)/%s?parseTime=true",
		dbUser, dbPwd, cloudSQLConnectionIdentifier, dbPort, dbName)

	db, err := sql.Open(mysqlDriver, dbURI)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("db.Ping: %w", err)
	}

	log.Printf("Connected to Cloud SQL instance: %s", instanceName)
	return db, nil
}
