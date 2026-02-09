// Copyright 2025 Google LLC
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

package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"net/http"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const (
	BigtableReadOutcomeOK    = "ok"    // Read was successful and returned data.
	BigtableReadOutcomeEmpty = "empty" // Read was successful, but had no data.
	BigtableReadOutcomeError = "error"
)

type (
	rpcMethodKey string
)

const (
	// Configuration values
	prometheusPort         = 2223
	shutdownTimeoutSeconds = 60
	meterName              = "github.com/datacommonsorg/mixer/internal/metrics"

	// Context keys
	rpcMethodContextKey = rpcMethodKey("rpc.method")

	// Bigtable metric attributes
	btCachePrefixAttr = "bigtable.cache.prefix"
	btReadOutcomeAttr = "bigtable.read.outcome"

	// Cache data metric attributes
	cacheDataTypeAttr = "cachedata.type"

	// Common metric attributes
	rpcMethodAttr     = "rpc.method"
	rpcStatusCodeAttr = "rpc.grpc.status_code"

	unknownMethodName = "UnknownMethod"
)

var (
	once  sync.Once
	meter metric.Meter

	// Bigtable metrics
	btReadLatencyHistogram metric.Int64Histogram

	// Cache data metrics
	cachedataReadCounter metric.Int64Counter

	// V3 mirroring metrics
	v3LatencyDiffHistogram    metric.Int64Histogram
	v3ResponseMismatchCounter metric.Int64Counter
	v3MirrorErrorCounter      metric.Int64Counter

	// V2 diversion metrics
	v2DiversionCounter metric.Int64Counter

	// Spanner metrics
	spannerQueryCounter metric.Int64Counter
)

// ResetForTest resets the metrics package for testing.
// This is not thread-safe and should only be called in serial tests.
func ResetForTest() {
	// TODO: Use dependency injection for meter provider so this isn't needed.
	once = sync.Once{}
	meter = nil
	btReadLatencyHistogram = nil
	cachedataReadCounter = nil
	v3LatencyDiffHistogram = nil
	v3ResponseMismatchCounter = nil
	v3MirrorErrorCounter = nil
	v2DiversionCounter = nil
	spannerQueryCounter = nil
}

func initMetrics() {
	meter = otel.GetMeterProvider().Meter(meterName)
	var err error

	btReadLatencyHistogram, err = meter.Int64Histogram(
		"datacommons.mixer.bigtable.read.duration",
		metric.WithUnit("ms"),
	)
	if err != nil {
		slog.Error("Failed to create bigtable read duration histogram", "error", err)
	}

	cachedataReadCounter, err = meter.Int64Counter("datacommons.mixer.cachedata.reads")
	if err != nil {
		slog.Error("Failed to create cachedata read counter", "error", err)
	}

	v3LatencyDiffHistogram, err = meter.Int64Histogram(
		"datacommons.mixer.v3_latency_diff",
		metric.WithDescription(
			"Difference in latency between mirrored V3 API calls in milliseconds (v3 minus original)",
		),
		metric.WithUnit("ms"),
	)
	if err != nil {
		slog.Error("Failed to create v3 latency diff histogram", "error", err)
	}

	v3ResponseMismatchCounter, err = meter.Int64Counter(
		"datacommons.mixer.v3_response_mismatches",
		metric.WithDescription("Count of V3 mirrored response mismatches"),
	)
	if err != nil {
		slog.Error("Failed to create v3 mismatch counter", "error", err)
	}

	v3MirrorErrorCounter, err = meter.Int64Counter(
		"datacommons.mixer.v3_mirror_errors",
		metric.WithDescription("Count of errors encountered during V3 mirroring"),
	)
	if err != nil {
		slog.Error("Failed to create v3 mirror error counter", "error", err)
	}

	v2DiversionCounter, err = meter.Int64Counter(
		"datacommons.mixer.v2_diversion",
		metric.WithDescription("Count of V2 API calls that were diverted to the new backend"),
	)
	if err != nil {
		slog.Error("Failed to create v2 diversion counter", "error", err)
	}

	spannerQueryCounter, err = meter.Int64Counter(
		"datacommons.mixer.spanner_query",
		metric.WithDescription("Count of Spanner queries from Mixer APIs"),
	)
	if err != nil {
		slog.Error("Failed to create Spanner query counter", "error", err)
	}
}

// getShortMethodName extracts the short method name from a full gRPC method string.
// For example, "/datacommons.Mixer/V2Node" becomes "V2Node".
func getShortMethodName(fullMethod string) string {
	// If fullMethod is unset, return UnknownMethod
	if fullMethod == "" {
		return unknownMethodName
	}
	// If fullMethod has no slash, return the full value
	if !strings.Contains(fullMethod, "/") {
		return fullMethod
	}
	// If fullMethod is "/datacommons.Mixer/V2Node", shortMethodName will be "V2Node".
	shortMethodName := fullMethod[strings.LastIndex(fullMethod, "/")+1:]
	return shortMethodName
}

// Retrieves the RPC method name from the context.
func getRpcMethod(ctx context.Context) string {
	methodName, ok := ctx.Value(rpcMethodContextKey).(string)
	if !ok {
		return unknownMethodName
	}
	return methodName
}

// NewContext creates a new context for mirroring, copying over the RPC method
// from an existing context.
func NewContext(baseCtx context.Context) context.Context {
	return context.WithValue(context.Background(), rpcMethodContextKey, getRpcMethod(baseCtx))
}

// For non-streaming RPC endpoints, extracts the RPC method name and adds it to the context.
func InjectMethodNameUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	newCtx := context.WithValue(ctx, rpcMethodContextKey, getShortMethodName(info.FullMethod))
	return handler(newCtx, req)
}

// For streaming RPC endpoints, extracts the RPC method name and adds it to the context.
func InjectMethodNameStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	newStream := grpc_middleware.WrapServerStream(ss)
	newStream.WrappedContext = context.WithValue(
		ss.Context(),
		rpcMethodContextKey,
		getShortMethodName(info.FullMethod),
	)
	return handler(srv, newStream)
}

// Sets up an HTTP endpoint serving metrics that can be scraped by Prometheus.
func ExportPrometheusOverHttp() error {
	exporter, err := prometheus.New()
	if err != nil {
		return err
	}
	mp := sdk.NewMeterProvider(sdk.WithReader(exporter))
	otel.SetMeterProvider(mp)

	prometheusHost := fmt.Sprintf(":%d", prometheusPort)
	lis, err := net.Listen("tcp", prometheusHost)
	if err != nil {
		return err
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		err := http.Serve(lis, mux)
		if err != nil {
			slog.Error("Failed to serve prometheus", "error", err)
		}
	}()
	return nil
}

// Sets up an OTLP exporter that pushes metrics to an OTLP collector over gRPC.
func ExportOtlpOverGrpc(ctx context.Context) error {
	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithInsecure())
	if err != nil {
		return err
	}

	reader := sdk.NewPeriodicReader(exporter, sdk.WithInterval(10*time.Minute))
	mp := sdk.NewMeterProvider(sdk.WithReader(reader))
	otel.SetMeterProvider(mp)
	return nil
}

// ExportToConsole sets up an exporter that prints metrics to the console.
// This is useful for local development and debugging.
func ExportToConsole() {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	exp, err := stdoutmetric.New(
		stdoutmetric.WithEncoder(enc),
		stdoutmetric.WithoutTimestamps(),
	)
	if err != nil {
		panic(err)
	}

	// Register the exporter with an SDK via a periodic reader.
	mp := sdk.NewMeterProvider(
		sdk.WithReader(sdk.NewPeriodicReader(exp)),
	)
	otel.SetMeterProvider(mp)
}

// Gracefully shuts down the meter provider with a timeout. Should be called
// before server shutdown.
func ShutdownWithTimeout() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeoutSeconds*time.Second)
	defer cancel()
	mp := otel.GetMeterProvider().(*sdk.MeterProvider)
	err := mp.Shutdown(ctx)
	if err != nil {
		slog.Error("Failed to shutdown MeterProvider", "error", err)
	}
}

// Adds the given duration to the histogram of Bigtable read latency.
// Outcome should be one of ok, empty, or error.
// Prefix is the cache prefix, e.g. "d/1/".
func RecordBigtableReadDuration(
	ctx context.Context,
	readDuration time.Duration,
	outcome string,
	prefix string,
) {
	once.Do(initMetrics)
	if btReadLatencyHistogram == nil {
		return
	}
	btReadLatencyHistogram.Record(ctx, readDuration.Milliseconds(),
		metric.WithAttributes(
			attribute.String(btCachePrefixAttr, prefix),
			attribute.String(btReadOutcomeAttr, outcome),
			attribute.String(rpcMethodAttr, getRpcMethod(ctx)),
		))
}

// Increments a counter of local cachedata reads broken down by type.
func RecordCachedataRead(ctx context.Context, cacheType string) {
	once.Do(initMetrics)
	if cachedataReadCounter == nil {
		return
	}
	cachedataReadCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(cacheDataTypeAttr, cacheType),
			attribute.String(rpcMethodAttr, getRpcMethod(ctx)),
		))
}

// RecordV3LatencyDiff records the latency difference between an API call and a
// mirrored equivalent V3 call.
func RecordV3LatencyDiff(ctx context.Context, diff time.Duration, skipCache bool) {
	once.Do(initMetrics)
	if v3LatencyDiffHistogram == nil {
		return
	}
	v3LatencyDiffHistogram.Record(ctx, diff.Milliseconds(),
		metric.WithAttributes(
			attribute.String(rpcMethodAttr, getRpcMethod(ctx)),
			attribute.Bool("rpc.headers.skip_cache", skipCache),
		))
}

// RecordV3Mismatch increments a counter for how many times a mirrored V3 call
// returns a different value from the original call.
func RecordV3Mismatch(ctx context.Context) {
	once.Do(initMetrics)
	if v3ResponseMismatchCounter == nil {
		return
	}
	v3ResponseMismatchCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(rpcMethodAttr, getRpcMethod(ctx)),
		))
}

// RecordV3MirrorError increments a counter for mirrored V3 requests that
// returned an error.
func RecordV3MirrorError(ctx context.Context, err error) {
	once.Do(initMetrics)
	if v3MirrorErrorCounter == nil {
		return
	}
	st, _ := status.FromError(err)
	v3MirrorErrorCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(rpcMethodAttr, getRpcMethod(ctx)),
			attribute.String(rpcStatusCodeAttr, st.Code().String()),
		))
}

// RecordV2Diversion increments a counter for V2 API calls that were diverted
// to the new backend.
func RecordV2Diversion(ctx context.Context) {
	once.Do(initMetrics)
	if v2DiversionCounter == nil {
		return
	}
	v2DiversionCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(rpcMethodAttr, getRpcMethod(ctx)),
		))
}

// RecordSpannerQuery increments a counter for Spanner queries from Mixer APIs.
func RecordSpannerQuery(ctx context.Context) {
	once.Do(initMetrics)
	if spannerQueryCounter == nil {
		return
	}
	spannerQueryCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(rpcMethodAttr, getRpcMethod(ctx)),
		))
}
