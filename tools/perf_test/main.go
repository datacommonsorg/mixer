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

	ctx := context.Background()
	client, err := initSpannerClient(ctx, *config)
	if err != nil {
		log.Fatalf("Failed to initialize Spanner client: %v", err)
	}
	defer client.Close()

	vars := splitList(*variables)
	ents := splitList(*entities)
	nodeIDs := splitList(*nodes)

	// Setup logging to Info by default, but enable extraction via header.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	switch *method {
	case "GetObservations":
		runParallelTest(ctx, "Legacy", client, func(c context.Context) error {
			_, err := client.GetObservations(c, vars, ents, *date)
			return err
		}, "MultiEntity", client, func(c context.Context) error {
			_, err := client.GetObservations(c, vars, ents, *date)
			return err
		})
	case "CheckVariableExistence":
		runParallelTest(ctx, "Legacy", client, func(c context.Context) error {
			_, err := client.CheckVariableExistence(c, vars, ents)
			return err
		}, "MultiEntity", client, func(c context.Context) error {
			_, err := client.CheckVariableExistence(c, vars, ents)
			return err
		})
	case "GetObservationsContainedInPlace":
		runParallelTest(ctx, "Legacy", client, func(c context.Context) error {
			_, err := client.GetObservationsContainedInPlace(c, vars, &v2.ContainedInPlace{
				Ancestor:       *ancestor,
				ChildPlaceType: *childType,
			}, *date)
			return err
		}, "MultiEntity", client, func(c context.Context) error {
			_, err := client.GetObservationsContainedInPlace(c, vars, &v2.ContainedInPlace{
				Ancestor:       *ancestor,
				ChildPlaceType: *childType,
			}, *date)
			return err
		})
	case "GetStatVarGroupNode":
		runParallelTest(ctx, "Legacy", client, func(c context.Context) error {
			_, err := client.GetStatVarGroupNode(c, nodeIDs, *includeDefinitions)
			return err
		}, "MultiEntity", client, func(c context.Context) error {
			_, err := client.GetStatVarGroupNode(c, nodeIDs, *includeDefinitions)
			return err
		})
	case "GetFilteredStatVarGroupNode":
		constrainedPlaces, constrainedImport, err := parseConstrainedEntities(*constrainedEntities)
		if err != nil {
			log.Fatal(err)
		}
		runParallelTest(ctx, "Legacy", client, func(c context.Context) error {
			_, err := client.GetFilteredStatVarGroupNode(c, nodeIDs, constrainedPlaces, constrainedImport, *numEntitiesExistence, *includeDefinitions)
			return err
		}, "MultiEntity", client, func(c context.Context) error {
			_, err := client.GetFilteredStatVarGroupNode(c, nodeIDs, constrainedPlaces, constrainedImport, *numEntitiesExistence, *includeDefinitions)
			return err
		})
	case "GetFilteredTopic":
		constrainedPlaces, constrainedImport, err := parseConstrainedEntities(*constrainedEntities)
		if err != nil {
			log.Fatal(err)
		}
		runParallelTest(ctx, "Legacy", client, func(c context.Context) error {
			_, err := client.GetFilteredTopic(c, nodeIDs, constrainedPlaces, constrainedImport, *numEntitiesExistence)
			return err
		}, "MultiEntity", client, func(c context.Context) error {
			_, err := client.GetFilteredTopic(c, nodeIDs, constrainedPlaces, constrainedImport, *numEntitiesExistence)
			return err
		})
	default:
		log.Fatalf("Unsupported method: %s", *method)
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

// runParallelTest runs two operations in parallel and injects metadata headers.
// It injects X-Log-SQL=true for both, and X-Use-Multi-Entity-Schema=true for the second operation.
func runParallelTest(
	ctx context.Context,
	name1 string, client1 spanner.SpannerClient, op1 func(context.Context) error,
	name2 string, client2 spanner.SpannerClient, op2 func(context.Context) error,
) {
	var wg sync.WaitGroup
	wg.Add(2)

	var err1, err2 error

	// Run Operation 1 (Legacy usually)
	go func() {
		defer wg.Done()
		// Inject X-Log-SQL to see the query
		c1 := metadata.NewIncomingContext(ctx, metadata.Pairs(util.XLogSQL, "true"))
		err1 = op1(c1)
	}()

	// Run Operation 2 (MultiEntity usually)
	go func() {
		defer wg.Done()
		// Inject X-Log-SQL and X-Use-Multi-Entity-Schema
		c2 := metadata.NewIncomingContext(ctx, metadata.Pairs(
			util.XLogSQL, "true",
			util.XUseMultiEntitySchema, "true",
		))
		err2 = op2(c2)
	}()

	wg.Wait()

	fmt.Println("\n=== Execution Status ===")
	if err1 != nil {
		fmt.Printf("%s: Failed with error: %v\n", name1, err1)
	} else {
		fmt.Printf("%s: Succeeded\n", name1)
	}

	if err2 != nil {
		fmt.Printf("%s: Failed with error: %v\n", name2, err2)
	} else {
		fmt.Printf("%s: Succeeded\n", name2)
	}
}
