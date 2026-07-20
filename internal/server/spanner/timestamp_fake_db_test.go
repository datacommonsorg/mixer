// Copyright 2026 Google LLC
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

package spanner

import (
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
	_ "modernc.org/sqlite"
)

type fakeSpannerServer struct {
	spannerpb.UnimplementedSpannerServer
	db *sql.DB
}

func (f *fakeSpannerServer) CreateSession(ctx context.Context, req *spannerpb.CreateSessionRequest) (*spannerpb.Session, error) {
	return &spannerpb.Session{Name: req.Database + "/sessions/s1"}, nil
}

func (f *fakeSpannerServer) BatchCreateSessions(ctx context.Context, req *spannerpb.BatchCreateSessionsRequest) (*spannerpb.BatchCreateSessionsResponse, error) {
	return &spannerpb.BatchCreateSessionsResponse{
		Session: []*spannerpb.Session{{Name: req.Database + "/sessions/s1"}},
	}, nil
}

func (f *fakeSpannerServer) ExecuteStreamingSql(req *spannerpb.ExecuteSqlRequest, stream spannerpb.Spanner_ExecuteStreamingSqlServer) error {
	rows, err := f.db.QueryContext(stream.Context(), req.Sql)
	if err != nil {
		return err
	}
	defer rows.Close()

	meta := &spannerpb.ResultSetMetadata{
		RowType: &spannerpb.StructType{
			Fields: []*spannerpb.StructType_Field{
				{
					Name: "StalenessTimestamp",
					Type: &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP},
				},
			},
		},
	}

	var values []*structpb.Value
	if rows.Next() {
		var ts sql.NullString
		if err := rows.Scan(&ts); err != nil {
			return err
		}
		if ts.Valid && ts.String != "" {
			values = append(values, structpb.NewStringValue(ts.String))
		} else {
			values = append(values, structpb.NewNullValue())
		}
	} else {
		values = append(values, structpb.NewNullValue())
	}

	return stream.Send(&spannerpb.PartialResultSet{
		Metadata: meta,
		Values:   values,
	})
}

type ingestionHistoryRow struct {
	workflowID          string
	creationTimestamp   string
	completionTimestamp *string
	status              string
	stage               *string
}

func strPtr(s string) *string {
	return &s
}

func setupFakeSpannerClient(t *testing.T, rows []ingestionHistoryRow) (*spannerDatabaseClient, func()) {
	t.Helper()
	ctx := context.Background()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	_, err = sqliteDB.Exec(`
		CREATE TABLE IngestionHistory (
			WorkflowExecutionID TEXT PRIMARY KEY,
			CreationTimestamp TEXT,
			CompletionTimestamp TEXT,
			Status TEXT,
			Stage TEXT
		);`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	for _, r := range rows {
		var completionVal, stageVal interface{}
		if r.completionTimestamp != nil {
			completionVal = *r.completionTimestamp
		}
		if r.stage != nil {
			stageVal = *r.stage
		}
		_, err = sqliteDB.Exec(`
			INSERT INTO IngestionHistory (WorkflowExecutionID, CreationTimestamp, CompletionTimestamp, Status, Stage)
			VALUES (?, ?, ?, ?, ?);
		`, r.workflowID, r.creationTimestamp, completionVal, r.status, stageVal)
		if err != nil {
			t.Fatalf("failed to insert row: %v", err)
		}
	}

	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	spannerpb.RegisterSpannerServer(server, &fakeSpannerServer{db: sqliteDB})
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create gRPC client conn: %v", err)
	}

	client, err := spanner.NewClient(ctx, "projects/test-proj/instances/test-inst/databases/test-db", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("failed to create spanner client: %v", err)
	}

	sc := &spannerDatabaseClient{
		client:                       client,
		useNewIngestionHistorySchema: true,
	}

	return sc, func() {
		client.Close()
		_ = conn.Close()
		server.Stop()
		_ = sqliteDB.Close()
	}
}

func TestStalenessTimestamp_EmptyIngestionHistoryTable(t *testing.T) {
	sc, cleanup := setupFakeSpannerClient(t, nil)
	defer cleanup()

	_ = sc.fetchAndUpdateTimestamp(context.Background())
	ts, _ := sc.getStalenessTimestamp()
	if !ts.IsZero() {
		t.Fatalf("got %v, want zero timestamp for empty table", ts)
	}
}

func TestStalenessTimestamp_NoActiveIngestions(t *testing.T) {
	sc, cleanup := setupFakeSpannerClient(t, []ingestionHistoryRow{
		{"run-1", "2026-07-20T10:00:00Z", strPtr("2026-07-20T10:10:00Z"), "SUCCESS", strPtr("dataflow")},
	})
	defer cleanup()

	_ = sc.fetchAndUpdateTimestamp(context.Background())
	ts, _ := sc.getStalenessTimestamp()
	if !ts.IsZero() {
		t.Fatalf("got %v, want zero timestamp when no active runs exist", ts)
	}
}

func TestStalenessTimestamp_IgnorePreprocessingStage(t *testing.T) {
	sc, cleanup := setupFakeSpannerClient(t, []ingestionHistoryRow{
		{"run-1", "2026-07-20T10:00:00Z", strPtr("2026-07-20T10:10:00Z"), "SUCCESS", strPtr("dataflow")},
		{"run-2", "2026-07-20T10:30:00Z", nil, "RUNNING", strPtr("preprocessing")},
	})
	defer cleanup()

	_ = sc.fetchAndUpdateTimestamp(context.Background())
	ts, _ := sc.getStalenessTimestamp()
	if !ts.IsZero() {
		t.Fatalf("got %v, want zero timestamp (preprocessing stage should be ignored)", ts)
	}
}

func TestStalenessTimestamp_ConcurrentPreprocessingAndDataflow(t *testing.T) {
	sc, cleanup := setupFakeSpannerClient(t, []ingestionHistoryRow{
		{"run-1", "2026-07-20T10:00:00Z", strPtr("2026-07-20T10:10:00Z"), "SUCCESS", strPtr("dataflow")},
		{"run-2", "2026-07-20T10:30:00Z", nil, "RUNNING", strPtr("preprocessing")},
		{"run-3", "2026-07-20T10:40:00Z", nil, "RUNNING", strPtr("dataflow")},
	})
	defer cleanup()

	_ = sc.fetchAndUpdateTimestamp(context.Background())
	ts, _ := sc.getStalenessTimestamp()
	want, _ := time.Parse(time.RFC3339, "2026-07-20T10:40:00Z")
	if !ts.Equal(want) {
		t.Fatalf("got %v, want %v (dataflow run-3 timestamp)", ts, want)
	}
}

func TestStalenessTimestamp_SupportLegacyNullStage(t *testing.T) {
	sc, cleanup := setupFakeSpannerClient(t, []ingestionHistoryRow{
		{"run-1", "2026-07-20T10:00:00Z", strPtr("2026-07-20T10:10:00Z"), "SUCCESS", strPtr("dataflow")},
		{"run-4", "2026-07-20T10:35:00Z", nil, "RUNNING", nil},
	})
	defer cleanup()

	_ = sc.fetchAndUpdateTimestamp(context.Background())
	ts, _ := sc.getStalenessTimestamp()
	want, _ := time.Parse(time.RFC3339, "2026-07-20T10:35:00Z")
	if !ts.Equal(want) {
		t.Fatalf("got %v, want %v (legacy null stage run-4)", ts, want)
	}
}

func TestStalenessTimestamp_IncludePostprocessingStage(t *testing.T) {
	sc, cleanup := setupFakeSpannerClient(t, []ingestionHistoryRow{
		{"run-1", "2026-07-20T10:00:00Z", strPtr("2026-07-20T10:10:00Z"), "SUCCESS", strPtr("dataflow")},
		{"run-5", "2026-07-20T10:50:00Z", nil, "RUNNING", strPtr("postprocessing")},
	})
	defer cleanup()

	_ = sc.fetchAndUpdateTimestamp(context.Background())
	ts, _ := sc.getStalenessTimestamp()
	want, _ := time.Parse(time.RFC3339, "2026-07-20T10:50:00Z")
	if !ts.Equal(want) {
		t.Fatalf("got %v, want %v (postprocessing stage run-5)", ts, want)
	}
}

func TestStalenessTimestamp_FailedPreprocessingStage(t *testing.T) {
	sc, cleanup := setupFakeSpannerClient(t, []ingestionHistoryRow{
		{"run-1", "2026-07-20T10:00:00Z", strPtr("2026-07-20T10:10:00Z"), "SUCCESS", strPtr("dataflow")},
		{"run-2", "2026-07-20T10:30:00Z", strPtr("2026-07-20T10:31:00Z"), "FAILURE", strPtr("preprocessing")},
	})
	defer cleanup()

	_ = sc.fetchAndUpdateTimestamp(context.Background())
	ts, _ := sc.getStalenessTimestamp()
	if !ts.IsZero() {
		t.Fatalf("got %v, want zero timestamp (failed preprocessing stage should be ignored)", ts)
	}
}

