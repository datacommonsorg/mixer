// Copyright 2024 Google LLC
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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/datacommonsorg/mixer/internal/metrics"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// SpannerConnector handles the low-level details of connecting to Spanner and executing queries.
type SpannerConnector struct {
	client          *spanner.Client
	timestamp       atomic.Int64
	ticker          Ticker
	stopCh          chan struct{}
	startOnce       sync.Once
	stopOnce        sync.Once
	wg              sync.WaitGroup
	updateTimestamp func(context.Context) error
}

// NewSpannerConnector creates a new SpannerConnector.
func NewSpannerConnector(ctx context.Context, cfg *SpannerConfig) (*SpannerConnector, error) {
	client, err := newDBClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	se := &SpannerConnector{
		client: client,
	}

	// Set an initial timestamp synchronously before starting the background loop.
	se.ticker = NewTimestampTicker()
	se.stopCh = make(chan struct{})
	se.updateTimestamp = se.fetchAndUpdateTimestamp
	if err := se.updateTimestamp(ctx); err != nil {
		slog.Error("Error initializing Spanner staleness timestamp", "error", err.Error())
		return nil, err
	}
	return se, nil
}

// Id returns the database name.
func (se *SpannerConnector) Id() string {
	return se.client.DatabaseName()
}

// Start starts the background goroutine to periodically fetch the timestamp.
func (se *SpannerConnector) Start() {
	se.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())

		se.wg.Add(1)
		go func() {
			defer se.wg.Done()
			defer cancel()
			defer se.ticker.Stop()

			for {
				select {
				case <-se.stopCh:
					return
				case <-se.ticker.C():
					err := se.updateTimestamp(ctx)
					if err != nil {
						slog.Error("Error updating Spanner staleness timestamp", "error", err)
					}
				}
			}
		}()
	})
}

// Close closes the Spanner client and stops the background goroutine.
func (se *SpannerConnector) Close() {
	se.stopOnce.Do(func() {
		close(se.stopCh)
		se.wg.Wait()
		if se.client != nil {
			se.client.Close()
		}
	})
}

// fetchAndUpdateTimestamp queries Spanner and updates the timestamp.
func (se *SpannerConnector) fetchAndUpdateTimestamp(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, timestampPollingTimeout)
	defer cancel()

	iter := se.client.Single().Query(queryCtx, *GetCompletionTimestampQuery())
	defer iter.Stop()

	row, err := iter.Next()

	var warnMsg string
	if err == iterator.Done {
		warnMsg = "No valid rows found in IngestionHistory."
	} else if code := spanner.ErrCode(err); code == codes.NotFound ||
		(code == codes.InvalidArgument && strings.Contains(err.Error(), "Table not found: IngestionHistory")) {
		warnMsg = "IngestionHistory table not found."
	}

	if warnMsg != "" {
		slog.Warn(warnMsg + " Falling back to strong reads.")
		return nil
	}

	if err != nil {
		if isTimeoutError(err) {
			slog.ErrorContext(queryCtx, "Spanner timestamp polling timed out",
				"timeout_duration", timestampPollingTimeout.String(),
				"error", err.Error(),
			)
		}
		return fmt.Errorf("failed to fetch row: %w", err)
	}

	var timestamp time.Time
	if err := row.Column(0, &timestamp); err != nil {
		return fmt.Errorf("failed to read CompletionTimestamp column: %w", err)
	}

	se.timestamp.Store(timestamp.UnixNano())
	return nil
}

func (se *SpannerConnector) getStalenessTimestamp() (time.Time, error) {
	val := se.timestamp.Load()
	if val != 0 {
		return time.Unix(0, val).UTC(), nil
	}
	slog.Error("Spanner staleness timestamp not available")
	return time.Time{}, fmt.Errorf("error getting staleness timestamp")
}

func (se *SpannerConnector) executeQuery(
	ctx context.Context,
	stmt spanner.Statement,
	handleRows func(*spanner.RowIterator) error,
) error {
	var queryCtx context.Context
	var cancel context.CancelFunc

	if _, ok := ctx.Deadline(); ok {
		queryCtx, cancel = context.WithCancel(ctx)
	} else {
		slog.Warn("Parent context has no deadline; using default API timeout", "timeout", ApiTimeout.String())
		queryCtx, cancel = context.WithTimeout(ctx, ApiTimeout)
	}
	defer cancel()

	runQuery := func(tb spanner.TimestampBound) error {
		metrics.RecordSpannerQuery(queryCtx)
		startTime := time.Now()
		iter := se.client.Single().WithTimestampBound(tb).Query(queryCtx, stmt)
		defer iter.Stop()
		err := handleRows(iter)
		duration := time.Since(startTime)

		if shouldLogSQL(queryCtx) {
			interpolatedSQL := InterpolateSQL(&stmt)
			schema := getSchemaName(queryCtx)
			fmt.Printf("\n=== [%s] Spanner Query (Took %v) ===\n", schema, duration)
			fmt.Println("[Parameterized Query]")
			for k, v := range stmt.Params {
				jsonVal, _ := json.Marshal(v)
				fmt.Printf("SET @%s = %s;\n", k, string(jsonVal))
			}
			fmt.Println()
			fmt.Println(stmt.SQL)
			fmt.Println("\n[Interpolated Query]")
			fmt.Println(interpolatedSQL)
			fmt.Println("================================================")
		}

		if isTimeoutError(err) {
			slog.ErrorContext(queryCtx, "Spanner query timed out",
				"sql", stmt.SQL,
				"error", err.Error(),
			)
		}

		return err
	}

	ts, err := se.getStalenessTimestamp()
	if err != nil {
		return runQuery(spanner.StrongRead())
	}
	err = runQuery(spanner.ReadTimestamp(ts))

	if spanner.ErrCode(err) == codes.FailedPrecondition {
		slog.Error("Stale read timestamp expired. Falling back to StrongRead.",
			"expiredTimestamp", ts.String())
		return runQuery(spanner.StrongRead())
	}
	return err
}

// queryStructs executes a query and maps the results to an input struct.
func (se *SpannerConnector) queryStructs(
	ctx context.Context,
	stmt spanner.Statement,
	newStruct func() interface{},
	withStruct func(interface{}),
) error {
	return se.executeQuery(ctx, stmt, func(iter *spanner.RowIterator) error {
		return processRows(iter, newStruct, withStruct)
	})
}

// queryDynamic executes a dynamically constructed query and returns the results as a slice of string slices.
func (se *SpannerConnector) queryDynamic(
	ctx context.Context,
	stmt spanner.Statement,
) ([][]string, error) {
	var rowData [][]string
	err := se.executeQuery(ctx, stmt, func(iter *spanner.RowIterator) error {
		result, err := processDynamicRows(iter)
		rowData = result
		return err
	})
	return rowData, err
}

// queryCache executes a query and maps the results to an input cache proto.
func queryCache[T proto.Message](
	ctx context.Context,
	se *SpannerConnector,
	stmt spanner.Statement,
	newProto func() T,
) (map[string]map[string]T, error) {
	var data map[string]map[string]T
	err := se.executeQuery(ctx, stmt, func(iter *spanner.RowIterator) error {
		result, err := processCacheRows(iter, newProto)
		data = result
		return err
	})
	return data, err
}

func processRows(iter *spanner.RowIterator, newStruct func() interface{}, withStruct func(interface{})) error {
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to fetch row: %w", err)
		}

		rowStruct := newStruct()
		if err := row.ToStructLenient(rowStruct); err != nil {
			return fmt.Errorf("failed to parse row: %w", err)
		}
		withStruct(rowStruct)
	}
	return nil
}

func processDynamicRows(iter *spanner.RowIterator) ([][]string, error) {
	rowData := [][]string{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return rowData, err
		}

		data := []string{}
		for i := 0; i < row.Size(); i++ {
			var val spanner.GenericColumnValue
			if err := row.Column(i, &val); err != nil {
				return rowData, err
			}
			data = append(data, val.Value.GetStringValue())
		}
		rowData = append(rowData, data)
	}
	return rowData, nil
}

func processCacheRows[T proto.Message](iter *spanner.RowIterator, newProto func() T) (map[string]map[string]T, error) {
	results := make(map[string]map[string]T)
	unmarshaler := protojson.UnmarshalOptions{DiscardUnknown: true}

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to fetch row: %w", err)
		}

		var key string
		if err := row.ColumnByName("key", &key); err != nil {
			return nil, fmt.Errorf("failed to read key column: %w", err)
		}

		var provenance string
		if err := row.ColumnByName("provenance", &provenance); err != nil {
			return nil, fmt.Errorf("failed to read provenance column: %w", err)
		}

		var jsonStr spanner.NullString
		if err := row.ColumnByName("value", &jsonStr); err != nil {
			return nil, fmt.Errorf("failed to read value column: %w", err)
		}

		if jsonStr.Valid {
			msg := newProto()
			if err := unmarshaler.Unmarshal([]byte(jsonStr.StringVal), msg); err != nil {
				return nil, fmt.Errorf("failed to unmarshal proto: %w", err)
			}

			if results[key] == nil {
				results[key] = make(map[string]T)
			}
			results[key][provenance] = msg
		}
	}
	return results, nil
}

func isTimeoutError(err error) bool {
	return spanner.ErrCode(err) == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded)
}

// shouldLogSQL checks whether to log the full interpolated SQL query based on request header.
func shouldLogSQL(ctx context.Context) bool {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		headers := md.Get(util.XLogSQL)
		return len(headers) > 0 && headers[0] == "true"
	}
	return false
}

// getSchemaName returns the name of the schema being used based on context.
func getSchemaName(ctx context.Context) string {
	if useNormalizedSchema(ctx) {
		return "Normalized"
	}
	return "Legacy"
}

// newDBClient creates the database name string and initializes the Spanner client.
func newDBClient(ctx context.Context, cfg *SpannerConfig) (*spanner.Client, error) {
	// Construct the database name string
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.Project, cfg.Instance, cfg.Database)

	// Create the Spanner client
	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %w", err)
	}

	return client, nil
}
