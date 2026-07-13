// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package emulator

import (
	"context"
	"crypto/rand"
	_ "embed"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	databaseadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instanceadmin "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	mixerspanner "github.com/datacommonsorg/mixer/internal/server/spanner"
	"github.com/datacommonsorg/mixer/test"
)

const emulatorProjectID = "mixer-emulator-test"

//go:embed testdata/schema.sql
var emulatorSchema string

//go:embed testdata/seed.sql
var emulatorSeed string

var suite *emulatorSuite

type emulatorSuite struct {
	instanceAdmin *instanceadmin.InstanceAdminClient
	databaseAdmin *databaseadmin.DatabaseAdminClient
	spannerClient mixerspanner.SpannerClient
	instanceName  string
	databaseNames []string
	provisionMu   sync.Mutex
}

func TestMain(m *testing.M) {
	if test.RunSpannerEmulatorTests {
		if err := validateSpannerEmulatorHost(os.Getenv("SPANNER_EMULATOR_HOST")); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		var err error
		suite, err = newEmulatorSuite(ctx)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "set up Spanner emulator tests: %v\n", err)
			os.Exit(1)
		}
	}

	code := m.Run()
	if suite != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		if err := suite.close(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "clean up Spanner emulator tests: %v\n", err)
			code = 1
		}
		cancel()
	}
	os.Exit(code)
}

func requireSuite(t *testing.T) *emulatorSuite {
	t.Helper()
	if suite == nil {
		t.Skip("set RUN_SPANNER_EMULATOR_TESTS=true to run Spanner emulator tests")
	}
	return suite
}

func validateSpannerEmulatorHost(value string) error {
	host, port, err := net.SplitHostPort(value)
	if err != nil || host != "localhost" || port == "" {
		return fmt.Errorf("RUN_SPANNER_EMULATOR_TESTS=true requires SPANNER_EMULATOR_HOST=localhost:<port>")
	}
	return nil
}

func newEmulatorSuite(ctx context.Context) (_ *emulatorSuite, err error) {
	instanceID, err := randomResourceID("mixer-test")
	if err != nil {
		return nil, err
	}
	databaseID, err := randomResourceID("test")
	if err != nil {
		return nil, err
	}

	instanceClient, err := instanceadmin.NewInstanceAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	databaseClient, err := databaseadmin.NewDatabaseAdminClient(ctx)
	if err != nil {
		_ = instanceClient.Close()
		return nil, err
	}

	resources := &emulatorSuite{
		instanceAdmin: instanceClient,
		databaseAdmin: databaseClient,
		instanceName:  fmt.Sprintf("projects/%s/instances/%s", emulatorProjectID, instanceID),
	}
	defer func() {
		if err != nil {
			cleanupContext, cancel := context.WithTimeout(context.Background(), time.Minute)
			_ = resources.close(cleanupContext)
			cancel()
		}
	}()

	createInstance, err := instanceClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + emulatorProjectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Name:        resources.instanceName,
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", emulatorProjectID),
			DisplayName: instanceID,
			NodeCount:   1,
		},
	})
	if err != nil {
		return nil, err
	}
	if _, err = createInstance.Wait(ctx); err != nil {
		return nil, err
	}

	statements, err := emulatorSchemaStatements()
	if err != nil {
		return nil, err
	}
	if _, err = resources.createDatabase(ctx, databaseID, statements, seedEmulatorDatabase); err != nil {
		return nil, err
	}

	config := fmt.Sprintf("project: %s\ninstance: %s\ndatabase: %s\n", emulatorProjectID, instanceID, databaseID)
	resources.spannerClient, err = mixerspanner.NewSpannerClient(ctx, config, "", true, false)
	if err != nil {
		return nil, err
	}
	return resources, nil
}

func (s *emulatorSuite) close(ctx context.Context) error {
	if s.spannerClient != nil {
		s.spannerClient.Close()
	}
	var cleanupErr error
	for _, databaseName := range s.databaseNames {
		if err := s.databaseAdmin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: databaseName}); err != nil && cleanupErr == nil {
			cleanupErr = err
		}
	}
	if s.instanceName != "" {
		if err := s.instanceAdmin.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{Name: s.instanceName}); err != nil && cleanupErr == nil {
			cleanupErr = err
		}
	}
	if s.databaseAdmin != nil {
		_ = s.databaseAdmin.Close()
	}
	if s.instanceAdmin != nil {
		_ = s.instanceAdmin.Close()
	}
	return cleanupErr
}

// createDatabase serializes provisioning because the emulator supports only one
// schema change or read-write transaction at a time. Tests share the seeded
// database unless they need to mutate data or use an incompatible schema.
func (s *emulatorSuite) createDatabase(
	ctx context.Context,
	databaseID string,
	statements []string,
	seed func(context.Context, string) error,
) (string, error) {
	s.provisionMu.Lock()
	defer s.provisionMu.Unlock()

	operation, err := s.databaseAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          s.instanceName,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
		ExtraStatements: statements,
	})
	if err != nil {
		return "", err
	}
	database, err := operation.Wait(ctx)
	if err != nil {
		return "", err
	}
	databaseName := database.GetName()
	s.databaseNames = append(s.databaseNames, databaseName)
	if seed != nil {
		if err := seed(ctx, databaseName); err != nil {
			return "", err
		}
	}
	return databaseName, nil
}

func randomResourceID(prefix string) (string, error) {
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%x", prefix, random), nil
}

func emulatorSchemaStatements() ([]string, error) {
	return splitEmulatorSchemaStatements(emulatorSchema)
}

func splitEmulatorSchemaStatements(schema string) ([]string, error) {
	schema = strings.TrimSpace(withoutSQLCommentLines(schema))
	if !strings.HasSuffix(schema, ";") {
		return nil, fmt.Errorf("emulator schema must end with a semicolon")
	}

	parts := strings.Split(schema, ";")
	statements := make([]string, 0, len(parts)-1)
	for i, part := range parts[:len(parts)-1] {
		statement := strings.TrimSpace(part)
		fields := strings.Fields(statement)
		if len(fields) == 0 || !strings.EqualFold(fields[0], "CREATE") {
			return nil, fmt.Errorf("invalid emulator schema statement %d: semicolons may only terminate CREATE statements", i+1)
		}
		statements = append(statements, statement)
	}
	return statements, nil
}

func emulatorSeedStatements() ([]string, error) {
	return splitEmulatorSeedStatements(emulatorSeed)
}

func splitEmulatorSeedStatements(seed string) ([]string, error) {
	lines := strings.Split(withoutSQLCommentLines(seed), "\n")
	statements := []string{}
	current := []string{}
	appendStatement := func() error {
		statement := strings.TrimSpace(strings.Join(current, "\n"))
		current = current[:0]
		if statement == "" {
			return nil
		}
		statement = strings.TrimSpace(strings.TrimSuffix(statement, ";"))
		fields := strings.Fields(statement)
		if len(fields) == 0 || !strings.EqualFold(fields[0], "INSERT") {
			return fmt.Errorf("invalid emulator seed statement %d: each paragraph must contain one INSERT statement", len(statements)+1)
		}
		statements = append(statements, statement)
		return nil
	}
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			if err := appendStatement(); err != nil {
				return nil, err
			}
			continue
		}
		current = append(current, line)
	}
	if err := appendStatement(); err != nil {
		return nil, err
	}
	if len(statements) == 0 {
		return nil, fmt.Errorf("emulator seed must contain at least one INSERT statement")
	}
	return statements, nil
}

func withoutSQLCommentLines(sql string) string {
	lines := strings.Split(strings.ReplaceAll(sql, "\r\n", "\n"), "\n")
	lines = slices.DeleteFunc(lines, func(line string) bool {
		return strings.HasPrefix(strings.TrimSpace(line), "--")
	})
	return strings.Join(lines, "\n")
}

func seedEmulatorDatabase(ctx context.Context, databaseName string) error {
	statements, err := emulatorSeedStatements()
	if err != nil {
		return err
	}
	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		return err
	}
	defer client.Close()

	if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, transaction *spanner.ReadWriteTransaction) error {
		for i, sql := range statements {
			if _, err := transaction.Update(ctx, spanner.Statement{SQL: sql}); err != nil {
				return fmt.Errorf("execute emulator seed statement %d: %w", i+1, err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("seed emulator data: %w", err)
	}

	_, err = client.Apply(ctx, []*spanner.Mutation{spanner.Insert("IngestionHistory",
		[]string{"WorkflowExecutionID", "CreationTimestamp", "CompletionTimestamp", "IngestionFailure", "Status"},
		[]interface{}{"emulator-seed", spanner.CommitTimestamp, spanner.CommitTimestamp, false, "SUCCESS"},
	)})
	if err != nil {
		return fmt.Errorf("record emulator ingestion: %w", err)
	}
	return nil
}

func TestValidateSpannerEmulatorHost(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "localhost", value: "localhost:9010"},
		{name: "missing", wantErr: true},
		{name: "missing port", value: "localhost", wantErr: true},
		{name: "loopback IP", value: "127.0.0.1:9010", wantErr: true},
		{name: "remote", value: "spanner.googleapis.com:443", wantErr: true},
		{name: "localhost suffix", value: "localhost.example.com:9010", wantErr: true},
		{name: "URL", value: "http://localhost:9010", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSpannerEmulatorHost(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateSpannerEmulatorHost(%q) error = %v, wantErr %t", test.value, err, test.wantErr)
			}
		})
	}
}

func TestSplitEmulatorSchemaStatements(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		want      []string
		wantError string
	}{
		{
			name:   "inline terminators",
			schema: "CREATE TABLE A (id INT64) PRIMARY KEY(id); CREATE INDEX AByID ON A(id);",
			want: []string{
				"CREATE TABLE A (id INT64) PRIMARY KEY(id)",
				"CREATE INDEX AByID ON A(id)",
			},
		},
		{
			name:   "terminator on separate line",
			schema: "CREATE TABLE A (id INT64) PRIMARY KEY(id)\n;\n",
			want:   []string{"CREATE TABLE A (id INT64) PRIMARY KEY(id)"},
		},
		{
			name:   "trailing whitespace",
			schema: "\n CREATE TABLE A (id INT64) PRIMARY KEY(id); \n\t",
			want:   []string{"CREATE TABLE A (id INT64) PRIMARY KEY(id)"},
		},
		{
			name:   "comment lines",
			schema: "-- schema comment; ignored\nCREATE TABLE A (id INT64) PRIMARY KEY(id);\n  -- another; comment\nCREATE INDEX AByID ON A(id);",
			want: []string{
				"CREATE TABLE A (id INT64) PRIMARY KEY(id)",
				"CREATE INDEX AByID ON A(id)",
			},
		},
		{
			name:      "missing final terminator",
			schema:    "CREATE TABLE A (id INT64) PRIMARY KEY(id)",
			wantError: "must end with a semicolon",
		},
		{
			name:      "embedded semicolon",
			schema:    "CREATE TABLE A (value STRING(MAX) CHECK (value != ';')) PRIMARY KEY(value);",
			wantError: "invalid emulator schema statement 2",
		},
		{
			name:      "non create statement",
			schema:    "ALTER TABLE A ADD COLUMN value STRING(MAX);",
			wantError: "invalid emulator schema statement 1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := splitEmulatorSchemaStatements(test.schema)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("splitEmulatorSchemaStatements() error = %v, want substring %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitEmulatorSchemaStatements() error = %v", err)
			}
			if !slices.Equal(got, test.want) {
				t.Errorf("splitEmulatorSchemaStatements() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestEmulatorSchemaStatements(t *testing.T) {
	statements, err := emulatorSchemaStatements()
	if err != nil {
		t.Fatalf("emulatorSchemaStatements() error = %v", err)
	}
	if len(statements) != 10 {
		t.Fatalf("emulatorSchemaStatements() returned %d statements, want 10", len(statements))
	}
	if !strings.HasPrefix(statements[0], "CREATE TABLE Node") {
		t.Errorf("first schema statement = %q, want CREATE TABLE Node", statements[0])
	}
	if !strings.HasPrefix(statements[len(statements)-1], "CREATE PROPERTY GRAPH DCGraph") {
		t.Errorf("last schema statement = %q, want CREATE PROPERTY GRAPH DCGraph", statements[len(statements)-1])
	}
}

func TestSplitEmulatorSeedStatements(t *testing.T) {
	tests := []struct {
		name      string
		seed      string
		want      []string
		wantError string
	}{
		{
			name: "paragraphs",
			seed: "INSERT INTO A (id)\nVALUES (1);\n\n  \nINSERT INTO B (id) VALUES (2)",
			want: []string{
				"INSERT INTO A (id)\nVALUES (1)",
				"INSERT INTO B (id) VALUES (2)",
			},
		},
		{
			name: "comment lines",
			seed: "-- seed comment; ignored\nINSERT INTO A (id) VALUES (1);\n\n  -- another; comment\nINSERT INTO B (id) VALUES (2);",
			want: []string{
				"INSERT INTO A (id) VALUES (1)",
				"INSERT INTO B (id) VALUES (2)",
			},
		},
		{
			name:      "empty",
			seed:      " \n\t",
			wantError: "at least one INSERT statement",
		},
		{
			name:      "non insert statement",
			seed:      "UPDATE A SET id = 2",
			wantError: "invalid emulator seed statement 1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := splitEmulatorSeedStatements(test.seed)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("splitEmulatorSeedStatements() error = %v, want substring %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitEmulatorSeedStatements() error = %v", err)
			}
			if !slices.Equal(got, test.want) {
				t.Errorf("splitEmulatorSeedStatements() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestEmulatorSeedStatements(t *testing.T) {
	statements, err := emulatorSeedStatements()
	if err != nil {
		t.Fatalf("emulatorSeedStatements() error = %v", err)
	}
	if len(statements) != 4 {
		t.Fatalf("emulatorSeedStatements() returned %d statements, want 4", len(statements))
	}
	if !strings.HasPrefix(statements[0], "INSERT INTO Node") {
		t.Errorf("first seed statement = %q, want INSERT INTO Node", statements[0])
	}
	if !strings.HasPrefix(statements[len(statements)-1], "INSERT INTO Observation") {
		t.Errorf("last seed statement = %q, want INSERT INTO Observation", statements[len(statements)-1])
	}
}
