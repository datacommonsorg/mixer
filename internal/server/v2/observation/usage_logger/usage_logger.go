package usagelogger

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/store"
)

type FacetLog struct {
	Facet     pb.Facet `json:"facet"`
	NumSeries int      `json:"count"`
	Earliest  string   `json:"earliest"`
	Latest    string   `json:"latest"`
}

type StatVarLog struct {
	StatVarDCID string      `json:"stat_var_dcid"`
	Facets      []*FacetLog `json:"facets"`
	// Places      []string    `json:"places"`
}

type UsageLog struct {
	Timestamp time.Time     `json:"timestamp"`
	PlaceType string        `json:"place_type"`
	Feature   string        `json:"feature"`
	StatVars  []*StatVarLog `json:"stat_vars"`
}

// LogValue implements slog.LogValuer.
// It returns a group containing the fields of
// the UsageLog, so that they appear together in the log output.
func (u UsageLog) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("feature", u.Feature),
		slog.Time("timestamp", u.Timestamp),
		slog.String("place_type", u.PlaceType),
		slog.Any("stat_vars", u.StatVars),
	)
}

// standardizeToYear takes a date string and returns a string with only the year.
func standardizeToYear(dateStr string) (string, error) {
	var t time.Time
	var err error

	// Determine the format based on the string length.
	switch len(dateStr) {
	case 0: // many entries have no earliest or latest
		return "", nil
	case 4: // YYYY
		t, err = time.Parse("2006", dateStr)
	case 7: // YYYY-MM
		t, err = time.Parse("2006-01", dateStr)
	case 10: // YYYY-MM-DD
		t, err = time.Parse("2006-01-02", dateStr)
	default:
		return "", fmt.Errorf("unsupported date format: %s", dateStr)
	}

	if err != nil {
		return "", err
	}

	// Format the time object to just the year.
	return t.Format("2006"), err
}

func MakeStatVarAndPlaceLogs(store *store.Store, observations []*pbv2.ObservationResponse) []*StatVarLog {
	// statVarDCID -> log with a list of facets
	statVarLogs := make(map[string]*StatVarLog)

	for _, resp := range observations {
		if resp == nil {
			continue
		}
		for variable, varObs := range resp.ByVariable {
			facetLogMaps := make(map[string]*FacetLog)
			// adding stat var to statVarLogs list of stat vars if it hasn't been already
			if _, ok := statVarLogs[variable]; !ok {
				statVarLogs[variable] = &StatVarLog{
					StatVarDCID: variable,
				}
			}

			// entities := []string{}
			// for entity := range varObs.ByEntity {
			// 	entities = append(entities, entity)
			// }

			// Run the entity type fetch and logging in the background.
			// Use a new context to allow logging to complete even if the original request is cancelled.
			// entityDCID_to_placeType, err := obs.LogEntityTypes(context.Background(), store, entities)

			// if err != nil{

			// }

			// for each entity in the variableResponse
			for _, entityObs := range varObs.ByEntity {
				// placeType := entityDCID_to_placeType[entityDcid]
				for _, facetObs := range entityObs.OrderedFacets {
					facetID := facetObs.FacetId

					if facetLog, ok := facetLogMaps[facetID]; ok {
						// incrementing the number of entities queried and earliest/latest to account if we've seen this facet before
						facetLog.NumSeries++
						// formatting year dates -- some stats only have year, so we only consider granularity to the year
						earliest, err := standardizeToYear(facetObs.EarliestDate)
						if err != nil {
							fmt.Printf("Error processing %s: %v\n", facetObs.EarliestDate, err)
						}
					latest, err := standardizeToYear(facetObs.LatestDate)
						if err != nil {
							fmt.Printf("Error processing %s: %v\n", facetObs.LatestDate, err)
						}

							if earliest != "" && (facetLog.Earliest == "" || earliest < facetLog.Earliest) {
							facetLog.Earliest = earliest
						}
						if latest != "" && (facetLog.Latest == "" || latest > facetLog.Latest) {
							facetLog.Latest = latest
						}
					} else {
						// setting facet inf o as if we've only seen one entity
						if facetData, ok := resp.Facets[facetID]; ok {
							facetLogMaps[facetID] = &FacetLog{
								Facet:     *facetData,
								NumSeries: 1,
								Earliest:  facetObs.EarliestDate,
								Latest:    facetObs.LatestDate,
							}
						}
					}
				}
			}

			// setting facet info
			facetLogs := []*FacetLog{}
			for _, facetLog := range facetLogMaps {
				facetLogs = append(facetLogs, facetLog)
			}
			statVarLogs[variable].Facets = facetLogs
		}
	}

	resultLogs := []*StatVarLog{}
	// matching facet logs and list of places to the correct statvar
	for _, svLog := range statVarLogs {
		// facetLogs := []*FacetLog{}
		// for _, facetLog := range facetLogMaps[variable] {
		// 	facetLogs = append(facetLogs, facetLog)
		// }
		// svLog.Facets = facetLogs

		resultLogs = append(resultLogs, svLog)
	}

	return resultLogs
}

/**
Writes a structured log to stdout, which is ingested by GCP cloud logging automatically

Includes the following:
- timestamp at the time of logging
- placeType -- the type submitted for a "within" query or the types of whatever places were queried directly
	- TODO: make this a list to account for multiple places in one query or a place with multiple types
- feature: which product (website, custom DC, client libraries, etc.) made this call to mixer
- statVars: all variables queried
	- statvarDCID
	- list of facets that were used in the result for this stat var
		- then these include the facet details (import name, measurement method, etc.) and
		earliest/latest date used and the number of entities that used the particular facet
*/
func UsageLogger(feature string, placeType string, store *store.Store, observations []*pbv2.ObservationResponse) {

	statVars := MakeStatVarAndPlaceLogs(store, observations)

	logEntry := UsageLog{
		Timestamp: time.Now(),
		PlaceType: placeType,
		Feature:   feature,
		StatVars:  statVars,
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("query_result", slog.Any("log_data", logEntry))
}