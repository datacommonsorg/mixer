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

type Feature struct {
	fromRemote bool `json:"from_remote"`
	surface string `json:"surface"`
}

// facet-specific information, separated into the facet info (import name, measurement method, etc.)
// and the query-specific number of series that came from this facet in the query result and the
// earliest and latest dates that the facet was used for across these series
type FacetLog struct {
	Facet     pb.Facet `json:"facet"`
	NumSeries int      `json:"count"`
	Earliest  string   `json:"earliest"`
	Latest    string   `json:"latest"`
}

type StatVarLog struct {
	StatVarDCID string      `json:"stat_var_dcid"`
	Facets      []*FacetLog `json:"facets"`
}

// Full log with all query-level info
type UsageLog struct {
	// time that log is written
	Timestamp time.Time     `json:"timestamp"`
	// TODO: placeType (city, county, etc.) requested for collection queries
	// See discussion here https://docs.google.com/document/d/1ETB3dj4y1rKcSrgCMcc6c2n-sQ-t8IzHWxZL0tMevkI/edit?tab=t.sy6cgv7mofcp#bookmark=id.4y4aq6f7jnmt
	// may expand this to be a list of the number of series queried for each placeType
	PlaceType string        `json:"place_type"`
	// the DC product (website, MCP server, client libraries, etc.) that the query originates from
	Feature   Feature        `json:"feature"`
	// whether the query is requesting values for a statvar, facet information, or checking existence
	// value, facet, or existence
	QueryType string		`json:"query_type"`
	// all stat vars queried in this query, with each including a list of facets used in that particular variable
	StatVars  []*StatVarLog `json:"stat_vars"`
}

// Handles formatting the structured log to correctly break down the structs as JSON objects in Cloud Logger
func (u UsageLog) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Any("feature", u.Feature),
		slog.Time("timestamp", u.Timestamp),
		slog.String("place_type", u.PlaceType),
		slog.String("query_type", u.QueryType),
		slog.Any("stat_vars", u.StatVars),
	)
}

// Takes a date string and returns a string with only the year
// used to compute earliest and latest dates for facets
func standardizeToYear(dateStr string) (string, error) {
	var t time.Time
	var err error

	// raw dates can be year, m-year, or d-m-year
	switch len(dateStr) {
	case 0: // many entries have no earliest or latest date at all - we ignore those cases
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

	// because a lot of our data only has the year, we only use that
	return t.Format("2006"), err
}

// Formats logs for the stat vars and facets
func MakeStatVarLogs(store *store.Store, observations []*pbv2.ObservationResponse) []*StatVarLog {
	// statVarDCID -> list of facets
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

			// we get all of the facets used for each entity
			for _, entityObs := range varObs.ByEntity {
				// the entity observation contains a list of the most relevant facets -- we include all of them
				for _, facetObs := range entityObs.OrderedFacets {
					facetID := facetObs.FacetId

					if facetLog, ok := facetLogMaps[facetID]; ok {
						facetLog.NumSeries++
						// some stats only have year, so we only consider granularity to the year
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

			// all facets used across the result for this stat var
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
		resultLogs = append(resultLogs, svLog)
	}

	return resultLogs
}

/**
Writes a structured log to stdout, which is ingested by GCP cloud logging automatically

Includes the following:
- timestamp at the time of logging
- (WIP) placeType -- the type submitted for a "within" query or the types of whatever places were queried directly
	- TODO: make this a list to account for multiple places in one query or a place with multiple types
- feature: which product (website, client libraries, etc.) made this call to mixer. It includes a fromRemote
	boolean that indicates if the surface was acessed via remote mixer domain, aka from a custom DC instance.
- statVars: all variables queried
	- statvarDCID
	- list of facets that were used in the result for this stat var
		- then these include the facet details (import name, measurement method, etc.) and
		earliest/latest date used and the number of entities that used the particular facet
*/
func UsageLogger(feature string, fromRemote string, placeType string, store *store.Store, observations []*pbv2.ObservationResponse, queryType string) {

	statVars := MakeStatVarLogs(store, observations)

	logEntry := UsageLog{
		Timestamp: time.Now(),
		PlaceType: placeType,
		Feature: Feature{
			surface:    feature,
			fromRemote: fromRemote != "",
		},
		QueryType: queryType,
		StatVars:  statVars,
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("query_result", slog.Any("usage_log_data", logEntry))
}