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
	"strings"
	"sync"
	"time"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/grpc/metadata"
)

var (
	method    = flag.String("method", "GetObservations", "Method to test: GetObservations, CheckVariableExistence, GetObservationsContainedInPlace, GetSdmxObservations")
	variables = flag.String("variables", "", "Comma-separated list of variables")
	entities  = flag.String("entities", "", "Comma-separated list of entities")
	ancestor  = flag.String("ancestor", "", "Ancestor place for contained-in queries")
	childType = flag.String("child_type", "", "Child place type for contained-in queries")
	config    = flag.String("config", "deploy/storage/spanner_graph_info.yaml", "Path to spanner graph info yaml")
)

func main() {
	flag.Parse()

	if err := validateInputs(*method, *variables, *entities, *ancestor, *childType); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	client, err := initSpannerClient(ctx, *config)
	if err != nil {
		log.Fatalf("Failed to initialize Spanner client: %v", err)
	}
	defer client.Close()

	vars := strings.Split(*variables, ",")
	ents := strings.Split(*entities, ",")
	if *entities == "" {
		ents = []string{}
	}

	// Setup logging to Info by default, but enable extraction via header.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	switch *method {
	case "GetObservations":
		runParallelTest(ctx, "Legacy", client, func(c context.Context) error {
			_, err := client.GetObservations(c, vars, ents)
			return err
		}, "Normalized", client, func(c context.Context) error {
			_, err := client.GetObservations(c, vars, ents)
			return err
		})
	case "CheckVariableExistence":
		runParallelTest(ctx, "Legacy", client, func(c context.Context) error {
			_, err := client.CheckVariableExistence(c, vars, ents)
			return err
		}, "Normalized", client, func(c context.Context) error {
			_, err := client.CheckVariableExistence(c, vars, ents)
			return err
		})
	case "GetObservationsContainedInPlace":
		runParallelTest(ctx, "Legacy", client, func(c context.Context) error {
			_, err := client.GetObservationsContainedInPlace(c, vars, &v2.ContainedInPlace{
				Ancestor:       *ancestor,
				ChildPlaceType: *childType,
			})
			return err
		}, "Normalized", client, func(c context.Context) error {
			_, err := client.GetObservationsContainedInPlace(c, vars, &v2.ContainedInPlace{
				Ancestor:       *ancestor,
				ChildPlaceType: *childType,
			})
			return err
		})
	default:
		log.Fatalf("Unsupported method: %s", *method)
	}
}

// validateInputs ensures that the required flags are provided for the selected method.
func validateInputs(method string, variables string, entities string, ancestor string, childType string) error {
	switch method {
	case "GetObservations", "CheckVariableExistence":
		if variables == "" {
			return fmt.Errorf("at least one variable is required for method %s", method)
		}
	case "GetObservationsContainedInPlace":
		if variables == "" || ancestor == "" || childType == "" {
			return fmt.Errorf("variables, ancestor, and child_type are required for method %s", method)
		}
	// TODO: Support GetSdmxObservations later.
	// case "GetSdmxObservations":
	default:
		return fmt.Errorf("unsupported method: %s", method)
	}
	return nil
}

// initSpannerClient reads the config and initializes the Spanner client.
func initSpannerClient(ctx context.Context, configPath string) (spanner.SpannerClient, error) {
	yamlFile, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// We reuse the logic from client.go by passing the YAML string directly.
	// The client.go NewSpannerClient expects the YAML content.
	return spanner.NewSpannerClient(ctx, string(yamlFile), "")
}

// runParallelTest runs two operations in parallel and compares their execution time.
// It injects X-Log-SQL=true for both, and X-Use-Normalized-Schema=true for the second operation.
func runParallelTest(
	ctx context.Context,
	name1 string, client1 spanner.SpannerClient, op1 func(context.Context) error,
	name2 string, client2 spanner.SpannerClient, op2 func(context.Context) error,
) {
	var wg sync.WaitGroup
	wg.Add(2)

	var time1, time2 time.Duration
	var err1, err2 error

	// Run Operation 1 (Legacy usually)
	go func() {
		defer wg.Done()
		// Inject X-Log-SQL to see the query
		c1 := metadata.NewIncomingContext(ctx, metadata.Pairs(util.XLogSQL, "true"))
		start := time.Now()
		err1 = op1(c1)
		time1 = time.Since(start)
	}()

	// Run Operation 2 (Normalized usually)
	go func() {
		defer wg.Done()
		// Inject X-Log-SQL and X-Use-Normalized-Schema
		c2 := metadata.NewIncomingContext(ctx, metadata.Pairs(
			util.XLogSQL, "true",
			util.XUseNormalizedSchema, "true",
		))
		start := time.Now()
		err2 = op2(c2)
		time2 = time.Since(start)
	}()

	wg.Wait()

	fmt.Println("\n=== Performance Results ===")
	if err1 != nil {
		fmt.Printf("%s: Failed with error: %v\n", name1, err1)
	} else {
		fmt.Printf("%s: Took %v\n", name1, time1)
	}

	if err2 != nil {
		fmt.Printf("%s: Failed with error: %v\n", name2, err2)
	} else {
		fmt.Printf("%s: Took %v\n", name2, time2)
	}
}
