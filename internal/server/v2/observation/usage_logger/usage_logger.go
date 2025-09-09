package usagelogger

import (
	"log/slog"
	"os"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
)

type FacetLog struct {
    Facet    pb.Facet `json:"facet"`
    Count    int      `json:"count"`
    Earliest string   `json:"earliest"`
    Latest   string   `json:"latest"`
}

type StatVarLog struct {
    StatVarDCID string      `json:"stat_var_dcid"`
    Facets      []*FacetLog `json:"facets"`
    Places      []string    `json:"places"`
}

type UsageLog struct {
    Timestamp time.Time     `json:"timestamp"`
    PlaceType string        `json:"place_type"`
    Feature   string        `json:"feature"`
    StatVars  []*StatVarLog `json:"stat_vars"`
}

// LogValue implements slog.LogValuer.
// It returns a group containing the fields of
// the Name, so that they appear together in the log output.
func (u UsageLog) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("feature", u.Feature),
		slog.Time("timestamp", u.Timestamp),
		slog.String("place_type", u.PlaceType),
		slog.Any("stat_vars", u.StatVars),
	)
}

func MakeStatVarAndPlaceLogs(observations []*pbv2.ObservationResponse) []*StatVarLog {
	// statVarDCID -> log with a list of facets and places
	statVarLogs := make(map[string]*StatVarLog)
	// var -> facetId -> FacetLog
	facetLogMaps := make(map[string]map[string]*FacetLog)
	// var id -> list of place DCIDs
	placeSet := make(map[string][]string)

	for _, resp := range observations {
		if resp == nil {
			continue
		}
		for variable, varObs := range resp.ByVariable {
			// adding stat var to statVarLogs list of stat vars if it hasn't been already
			if _, ok := statVarLogs[variable]; !ok {
				statVarLogs[variable] = &StatVarLog{
					StatVarDCID: variable,
				}
				facetLogMaps[variable] = make(map[string]*FacetLog)
				placeSet[variable] = make([]string, 0)
			}

			// for each entity in the variableResponse
			// tempFacetLogs := facetLogMaps[variable]
			for entity, entityObs := range varObs.ByEntity {
				// add the place ID to the placeSet
				placeSet[variable] = append(placeSet[variable], entity)
				for _, facetObs := range entityObs.OrderedFacets {
					facetID := facetObs.FacetId

					if facetLog, ok := facetLogMaps[variable][facetID]; ok {
						// incrementing count and earliest/latest to account if we've seen this facet before
						facetLog.Count++
						if facetObs.EarliestDate != "" && (facetLog.Earliest == "" || facetObs.EarliestDate < facetLog.Earliest) {
							facetLog.Earliest = facetObs.EarliestDate
						}
						if facetObs.LatestDate != "" && (facetLog.Latest == "" || facetObs.LatestDate > facetLog.Latest) {
							facetLog.Latest = facetObs.LatestDate
						}
					} else {
						// setting facet info as if we've only seen one entity
						if facetData, ok := resp.Facets[facetID]; ok {
							facetLogMaps[variable][facetID] = &FacetLog{
								Facet:    *facetData,
								Count:    1,
								Earliest: facetObs.EarliestDate,
								Latest:   facetObs.LatestDate,
							}
						}
					}
				}
			}
		}
	}

	resultLogs := []*StatVarLog{}
	// matching facet logs and list of places to the correct statvar
	for variable, svLog := range statVarLogs {
		facetLogs := []*FacetLog{}
		for _, facetLog := range facetLogMaps[variable] {
			facetLogs = append(facetLogs, facetLog)
		}
		svLog.Facets = facetLogs

		svLog.Places = placeSet[variable]
		resultLogs = append(resultLogs, svLog)
	}

	return resultLogs
}

func UsageLogger(feature string, placeType string, observations []*pbv2.ObservationResponse) {
	/*
	- feature
	- placeType or null
	- observation response (by var and entity) []*pbv2.ObservationResponse
	- facet response (facet, )
		- earliest and latest dates, which is in the facet information

	to generate:
	- timestamp

	https://pkg.go.dev/log/slog@master#example-LogValuer-Group
	*/

	statVars := MakeStatVarAndPlaceLogs(observations)

	logEntry := UsageLog{
		Timestamp: time.Now(),
		PlaceType: placeType,
		Feature: feature,
		StatVars: statVars,
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("query_result", slog.Any("log_data", logEntry))
}