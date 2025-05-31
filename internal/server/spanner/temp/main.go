package main

import (
	"context"
	"fmt"
	"log"

	"github.com/datacommonsorg/mixer/internal/server/spanner"
)

const spannerInfoYaml = `
project: datcom-store
instance: dc-kg-test
database: dc_graph_stable
`

// This is a temporary program to test proto fields in spanner.
// Usage: go run internal/server/spanner/temp/main.go

func main() {
	ctx := context.Background()
	client, err := spanner.NewSpannerClient(ctx, spannerInfoYaml)
	if err != nil {
		log.Fatalf("Failed to create SpannerClient: %v", err)
	}
	variables := []string{"AirPollutant_Cancer_Risk"}
	entities := []string{"geoId/01001", "geoId/02013"}
	obs, err := client.GetObservations(ctx, variables, entities, "", false)
	if err != nil {
		log.Fatalf("Failed to get observations: %v", err)
	}
	for _, o := range obs {
		fmt.Printf("Observations for %s %s:\n", o.VariableMeasured, o.ObservationAbout)
		for _, o2 := range o.Observations {
			fmt.Printf("  %v %v\n", o2.Date, o2.Value)
		}

	}
}
