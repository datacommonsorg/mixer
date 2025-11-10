package log

import (
	"fmt"
	"log/slog"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
)

// The source of the logged query -- because features like the MCP
// server can be used via custom DC, we also track if a call is to a remote mixer,
// which often indicates that it was made via a custom DC.
type Feature struct {
	// Indicates if query came from a call to a remote mixer.
	IsRemote bool `json:"is_remote"`
	// The DC product that the query came from, e.g. website, datagemma, etc.
	Surface string `json:"surface"`
}

type FacetLog struct {
	// Import name, measurement method, etc.
	Facet     *pb.Facet `json:"facet"`
	// The number of series that used this facet for the current request.
	NumSeries int      `json:"count"`
	// The earliest date across all series using this facet.
	Earliest  string   `json:"earliest"`
	// The latest date across all series using this facet.
	Latest    string   `json:"latest"`
}

// Used so we can compile one list of all facets used for each statVar
// across all of the series fetched.
type StatVarLog struct {
	StatVarDCID string      `json:"stat_var_dcid"`
	// List of all facets that provided results for the given statVar.
	Facets      []*FacetLog `json:"facets"`
}

// Full log with all information for the current query.
type UsageLog struct {
	// TODO: placeType (city, county, etc.) requested for collection queries
	// See discussion here https://docs.google.com/document/d/1ETB3dj4y1rKcSrgCMcc6c2n-sQ-t8IzHWxZL0tMevkI/edit?tab=t.sy6cgv7mofcp#bookmark=id.4y4aq6f7jnmt.
	// May expand this to be a list of the number of series queried for each placeType.
	PlaceTypes []string        `json:"place_types"`
	// The DC product (website, MCP server, client libraries, etc.) that the query originates from,
	// and a flag indicating if the call comes via a custom DC to remote mixer.
	Feature   Feature        `json:"feature"`
	// Whether the query is requesting values for a statvar, facet information, or checking existence.
	// Options: value, facet, or existence.
    QueryType string         `json:"query_type"`
	// All stat vars queried in this request, with each including a list of facets used in that particular variable.
	StatVars  []*StatVarLog `json:"stat_vars"`
	// A unique ID for this request generated in handler_v2. 
	// This is used to match mixer calls with cached requests in the website.
	ResponseId string `json:"response_id"`
}

// Breaks down the log structs to be read as JSON objects in Cloud Logger.
func (u UsageLog) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Any("feature", u.Feature),
		slog.Any("place_types", u.PlaceTypes),
		slog.String("query_type", u.QueryType),
		slog.Any("stat_vars", u.StatVars),
		slog.String("response_id", u.ResponseId),
	)
}

// Takes a date string and returns a string with only the year.
// This is used to compute earliest and latest dates for facets because some data only include year-level granularity.
func standardizeToYear(dateStr string) (string, error) {
	var t time.Time
	var err error

	// Raw dates can be year, m-year, or d-m-year.
	switch len(dateStr) {
	case 0: // Many entries have no earliest or latest date at all - we ignore those cases.
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

	// Because a lot of our data only has the year, we only use that.
    return t.Format("2006"), nil
}

// Formats logs for the stat vars and facets.
// 
// Parameters:
// 	observationResponse: The response from an observation query. This contains all results 
//	for each entity and variable requested.
// Returns:
//  A list of StatVarLog, each containing the stat var DCID and a list of facets used for that stat var.
//  A list of all place types in the response. This is separate because it isn't broken down per stat var.
func MakeStatVarLogs(observationResponse *pbv2.ObservationResponse) ([]*StatVarLog, []string) {
	// statVarLogs is a map statVarDCID -> list of facets.
	statVarsByDcid := make(map[string]*StatVarLog)
	resultLogs := make([]*StatVarLog, 0)
	placeTypesSet := make(map[string]struct{})

	if observationResponse == nil {
		return resultLogs, []string{}
	}

	// Iterate through each response's variables, collecting the facets used in that resp into our
	// cumulative list of facets used for the given variable.
	for variable, varObs := range observationResponse.ByVariable {
		// A map of facetId -> FacetLog
		facetsByFacetId := make(map[string]*FacetLog)
		if _, ok := statVarsByDcid[variable]; !ok {
			statVarsByDcid[variable] = &StatVarLog{
				StatVarDCID: variable,
			}
		}

		// Get all of the facets used for each entity.
		for _, entityObs := range varObs.ByEntity {
			for _, placeType := range entityObs.PlaceTypes {
				placeTypesSet[placeType] = struct{}{}
			}

			// The entity observation contains a list of the most relevant facets -- we include all of them.
			for _, facetObs := range entityObs.OrderedFacets {
				facetId := facetObs.FacetId

				// Some stats only have year, so we only consider granularity to the year.
				earliest, err := standardizeToYear(facetObs.EarliestDate)
				if err != nil {
					slog.Error("Error processing date", "date", facetObs.EarliestDate, "error", err)
				}
				latest, err := standardizeToYear(facetObs.LatestDate)
				if err != nil {
					slog.Error("Error processing date", "date", facetObs.LatestDate, "error", err)
				}

				// If we have a map for this facet, we add to it.
				if facetLog, ok := facetsByFacetId[facetId]; ok {
					facetLog.NumSeries++
					if earliest != "" && (facetLog.Earliest == "" || earliest < facetLog.Earliest) {
						facetLog.Earliest = earliest
					}
					if latest != "" && (facetLog.Latest == "" || latest > facetLog.Latest) {
						facetLog.Latest = latest
					}
				} else {
					// If this is the first time we see the facet, create a map entry.
					if facetData, ok := observationResponse.Facets[facetId]; ok {
						facetsByFacetId[facetId] = &FacetLog{
							Facet:     facetData,
							NumSeries: 1,
							Earliest:  earliest,
							Latest:    latest,
						}
					}
				}
			}
		}

		// All facets used for this stat var across all of the responses.
		facetLogs := []*FacetLog{}
		for _, facetLog := range facetsByFacetId {
			facetLogs = append(facetLogs, facetLog)
		}
		statVarsByDcid[variable].Facets = facetLogs
	}

	// Moving statVarLogs from a map keyed by statVarDcid to a list.
	for _, svLog := range statVarsByDcid {
		resultLogs = append(resultLogs, svLog)
	}

	placeTypes := make([]string, 0, len(placeTypesSet))
	for placeType := range placeTypesSet {
		placeTypes = append(placeTypes, placeType)
	}
	return resultLogs, placeTypes
}


// Writes a structured log to stdout, which is ingested by GCP cloud logging to track mixer usage.
// Currently only used by the v2/observation endpoint.
func WriteUsageLog(surface string, isRemote bool, observationResponse *pbv2.ObservationResponse, queryType shared.QueryType, responseId string) {

	statVars, placeTypes := MakeStatVarLogs(observationResponse)

	logEntry := UsageLog{
		PlaceTypes: placeTypes,
		Feature: Feature{
			Surface:    surface,
			IsRemote: isRemote,
		},
		QueryType: string(queryType),
		StatVars:  statVars,
		ResponseId: responseId,
	}

	slog.Info("new_query", slog.Any("usage_log", logEntry))
}