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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

var (
	method               = flag.String("method", "GetObservations", "Method to test: GetObservations, CheckVariableExistence, GetObservationsContainedInPlace, GetStatVarGroupNode, GetFilteredStatVarGroupNode, GetFilteredTopic")
	variables            = flag.String("variables", "", "Comma-separated list of variables")
	entities             = flag.String("entities", "", "Comma-separated list of entities")
	nodes                = flag.String("nodes", "", "Comma-separated list of StatVarGroup or Topic nodes")
	constrainedEntities  = flag.String("constrained_entities", "", "Comma-separated list of constrained entities for filtered variable group info methods")
	ancestor             = flag.String("ancestor", "", "Ancestor place for contained-in queries")
	childType            = flag.String("child_type", "", "Child place type for contained-in queries")
	config               = flag.String("config", "deploy/storage/spanner_graph_info.yaml", "Path to spanner graph info yaml")
	date                 = flag.String("date", "", "Optional date filter")
	numEntitiesExistence = flag.Int("num_entities_existence", 0, "Minimum number of constrained entities that must have observations")
	includeDefinitions   = flag.Bool("include_definitions", false, "Include definitions for StatVarGroup nodes")
	iterations           = flag.Int("iterations", 1, "Number of times to run the method")
	logSQL               = flag.Bool("log_sql", true, "Print each executed SQL statement with its Spanner execution time")
)

const (
	datasetPrefix = "dc/d/"
	sourcePrefix  = "dc/s/"
)

func main() {
	flag.Parse()

	if err := validateInputs(*method, *variables, *entities, *ancestor, *childType, *nodes, *constrainedEntities, *numEntitiesExistence); err != nil {
		log.Fatal(err)
	}
	if *iterations < 1 {
		log.Fatal("iterations must be at least 1")
	}
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// run times the selected method and prints a latency summary.
func run(ctx context.Context) error {
	client, err := initSpannerClient(ctx, *config)
	if err != nil {
		return fmt.Errorf("failed to initialize Spanner client: %w", err)
	}
	defer client.Close()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	op, err := newOperation(client, *method)
	if err != nil {
		return err
	}
	durations, err := runTimed(ctx, *iterations, *logSQL, op)
	printSummary(*method, durations)
	return err
}

// newOperation returns a function that calls the selected Spanner client method with the flag values.
func newOperation(client spanner.SpannerClient, method string) (func(context.Context) error, error) {
	vars := splitList(*variables)
	ents := splitList(*entities)
	nodeIDs := splitList(*nodes)

	switch method {
	case "GetObservations":
		return func(ctx context.Context) error {
			_, err := client.GetObservations(ctx, vars, ents, *date)
			return err
		}, nil
	case "CheckVariableExistence":
		return func(ctx context.Context) error {
			_, err := client.CheckVariableExistence(ctx, vars, ents)
			return err
		}, nil
	case "GetObservationsContainedInPlace":
		containedInPlace := &v2.ContainedInPlace{Ancestor: *ancestor, ChildPlaceType: *childType}
		return func(ctx context.Context) error {
			_, err := client.GetObservationsContainedInPlace(ctx, vars, containedInPlace, *date)
			return err
		}, nil
	case "GetStatVarGroupNode":
		return func(ctx context.Context) error {
			_, err := client.GetStatVarGroupNode(ctx, nodeIDs, *includeDefinitions)
			return err
		}, nil
	case "GetFilteredStatVarGroupNode":
		constrainedPlaces, constrainedImport, err := parseConstrainedEntities(*constrainedEntities)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) error {
			_, err := client.GetFilteredStatVarGroupNode(ctx, nodeIDs, constrainedPlaces, constrainedImport, *numEntitiesExistence, *includeDefinitions)
			return err
		}, nil
	case "GetFilteredTopic":
		constrainedPlaces, constrainedImport, err := parseConstrainedEntities(*constrainedEntities)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) error {
			_, err := client.GetFilteredTopic(ctx, nodeIDs, constrainedPlaces, constrainedImport, *numEntitiesExistence)
			return err
		}, nil
	default:
		return nil, fmt.Errorf("unsupported method: %s", method)
	}
}

// validateInputs ensures that the required flags are provided for the selected method.
func validateInputs(method string, variables string, entities string, ancestor string, childType string, nodes string, constrainedEntities string, numEntitiesExistence int) error {
	if numEntitiesExistence < 0 {
		return fmt.Errorf("num_entities_existence must be non-negative")
	}

	switch method {
	case "GetObservations", "CheckVariableExistence":
		if len(splitList(variables)) == 0 {
			return fmt.Errorf("at least one variable is required for method %s", method)
		}
	case "GetObservationsContainedInPlace":
		if len(splitList(variables)) == 0 || strings.TrimSpace(ancestor) == "" || strings.TrimSpace(childType) == "" {
			return fmt.Errorf("variables, ancestor, and child_type are required for method %s", method)
		}
	case "GetStatVarGroupNode":
		if len(splitList(nodes)) == 0 {
			return fmt.Errorf("nodes are required for method %s", method)
		}
	case "GetFilteredStatVarGroupNode", "GetFilteredTopic":
		if len(splitList(nodes)) == 0 || len(splitList(constrainedEntities)) == 0 {
			return fmt.Errorf("nodes and constrained_entities are required for method %s", method)
		}
		if _, _, err := parseConstrainedEntities(constrainedEntities); err != nil {
			return err
		}
	// TODO: Support GetSdmxObservations later.
	// case "GetSdmxObservations":
	default:
		return fmt.Errorf("unsupported method: %s", method)
	}
	return nil
}

func splitList(value string) []string {
	if value == "" {
		return []string{}
	}
	items := strings.Split(value, ",")
	result := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			result = append(result, item)
		}
	}
	return result
}

func parseConstrainedEntities(value string) ([]string, string, error) {
	var constrainedPlaces []string
	constrainedImport := ""
	for _, entity := range splitList(value) {
		if strings.HasPrefix(entity, datasetPrefix) || strings.HasPrefix(entity, sourcePrefix) {
			if constrainedImport != "" {
				return nil, "", fmt.Errorf("only one import or source constraint can be specified")
			}
			constrainedImport = entity
			continue
		}
		constrainedPlaces = append(constrainedPlaces, entity)
	}
	return constrainedPlaces, constrainedImport, nil
}

// initSpannerClient reads the config and initializes the Spanner client.
func initSpannerClient(ctx context.Context, configPath string) (spanner.SpannerClient, error) {
	yamlFile, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// We reuse the logic from client.go by passing the YAML string directly.
	// The client.go NewSpannerClient expects the YAML content.
	return spanner.NewSpannerClient(ctx, string(yamlFile), nil)
}

// runTimed runs op the given number of times in sequence and returns the
// wall-clock duration of each successful run. With logSQL, it injects
// X-Log-SQL=true so the Spanner client prints each statement and its execution time.
func runTimed(ctx context.Context, iterations int, logSQL bool, op func(context.Context) error) ([]time.Duration, error) {
	if logSQL {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(util.XLogSQL, "true"))
	}
	durations := make([]time.Duration, 0, iterations)
	for i := 1; i <= iterations; i++ {
		start := time.Now()
		if err := op(ctx); err != nil {
			return durations, fmt.Errorf("iteration %d: %w", i, err)
		}
		durations = append(durations, time.Since(start))
	}
	return durations, nil
}

// latencySummary summarizes the durations of repeated runs.
type latencySummary struct {
	runs   int
	min    time.Duration
	median time.Duration
	mean   time.Duration
	max    time.Duration
}

func summarizeDurations(durations []time.Duration) latencySummary {
	if len(durations) == 0 {
		return latencySummary{}
	}
	sorted := slices.Clone(durations)
	slices.Sort(sorted)
	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	mid := len(sorted) / 2
	median := sorted[mid]
	if len(sorted)%2 == 0 {
		median = (sorted[mid-1] + sorted[mid]) / 2
	}
	return latencySummary{
		runs:   len(sorted),
		min:    sorted[0],
		median: median,
		mean:   total / time.Duration(len(sorted)),
		max:    sorted[len(sorted)-1],
	}
}

// printSummary prints the duration of each successful run and their summary.
func printSummary(method string, durations []time.Duration) {
	fmt.Printf("\n=== %s latency ===\n", method)
	for i, d := range durations {
		fmt.Printf("run %d: %v\n", i+1, d)
	}
	if len(durations) > 0 {
		s := summarizeDurations(durations)
		fmt.Printf("runs=%d min=%v median=%v mean=%v max=%v\n", s.runs, s.min, s.median, s.mean, s.max)
	}
}
