package sqlquery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv2 "github.com/datacommonsorg/mixer/internal/proto/v2"
	"github.com/datacommonsorg/mixer/internal/server/v2/shared"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/protobuf/proto"
)

// GetObservations fetches observations from the specified SQL database.
func GetObservations(
	ctx context.Context,
	sqlClient *sql.DB,
	sqlProvenances map[string]*pb.Facet,
	variables []string,
	entities []string,
	queryDate string,
	filter *pbv2.FacetFilter,
) (*pbv2.ObservationResponse, error) {
	if sqlClient == nil || len(variables) == 0 {
		return newObservationResponse(variables), nil
	}

	// Query SQL.
	rows, err := sqlClient.Query(getObservationsSQLQuery(variables, entities, queryDate))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Generate intermediate response.
	intermediateResponse, err := generateIntermediateResponse(rows, sqlProvenances)
	if err != nil {
		return nil, err
	}

	// Generate ObservationResponse.
	return generateObservationResponse(intermediateResponse, variables, queryDate), nil
}

func generateObservationResponse(
	intermediate *intermediateObservationResponse,
	variables []string,
	queryDate string,
) *pbv2.ObservationResponse {
	response := newObservationResponse(variables)

	for _, byFacetKey := range intermediate.orderedKeys {
		byFacetValue := intermediate.byFacet[*byFacetKey]
		variable, entity, facetId := byFacetKey.variable, byFacetKey.entity, byFacetKey.facetId
		observations, facet := byFacetValue.observations, byFacetValue.facet
		if response.ByVariable[variable].ByEntity[entity] == nil {
			response.ByVariable[variable].ByEntity[entity] = &pbv2.EntityObservation{
				OrderedFacets: []*pbv2.FacetObservation{},
			}
		}
		if queryDate == shared.LATEST {
			observations = observations[len(observations)-1:]
		}
		response.ByVariable[variable].ByEntity[entity].OrderedFacets = append(
			response.ByVariable[variable].ByEntity[entity].OrderedFacets,
			&pbv2.FacetObservation{
				FacetId:      facetId,
				Observations: observations,
				ObsCount:     int32(len(observations)),
				EarliestDate: observations[0].Date,
				LatestDate:   observations[len(observations)-1].Date,
			},
		)
		response.Facets[facetId] = facet
	}

	return response
}

func generateIntermediateResponse(
	rows *sql.Rows,
	cachedProvenances map[string]*pb.Facet,
) (*intermediateObservationResponse, error) {
	intermediate := intermediateObservationResponse{byFacet: make(map[byFacetKey]*byFacetValue), orderedKeys: []*byFacetKey{}}
	for rows.Next() {
		var entity, variable, date, provenance, unit, scalingFactor, measurementMethod, observationPeriod, properties string
		var value float64
		if err := rows.Scan(&entity, &variable, &date, &value, &provenance, &unit, &scalingFactor, &measurementMethod, &observationPeriod, &properties); err != nil {
			return nil, err
		}
		observation := &pb.PointStat{
			Date:  date,
			Value: proto.Float64(value),
		}

		facetId, facet := toFacet(cachedProvenances, provenance, unit, scalingFactor, measurementMethod, observationPeriod, properties)
		intermediateByFacetKey := byFacetKey{variable: variable, entity: entity, facetId: facetId}
		intermediateByFacetValue := intermediate.byFacet[intermediateByFacetKey]
		if intermediateByFacetValue == nil {
			intermediateByFacetValue = &byFacetValue{facetId: facetId, facet: facet}
			intermediate.byFacet[intermediateByFacetKey] = intermediateByFacetValue
			intermediate.orderedKeys = append(intermediate.orderedKeys, &intermediateByFacetKey)
		}
		intermediateByFacetValue.observations = append(intermediateByFacetValue.observations, observation)
	}

	return &intermediate, rows.Err()
}

func newObservationResponse(variables []string) *pbv2.ObservationResponse {
	result := &pbv2.ObservationResponse{
		ByVariable: map[string]*pbv2.VariableObservation{},
		Facets:     map[string]*pb.Facet{},
	}
	for _, variable := range variables {
		result.ByVariable[variable] = &pbv2.VariableObservation{
			ByEntity: map[string]*pbv2.EntityObservation{},
		}
	}
	return result
}

// toFacet returns a facet ID and facet based on the specific observation properties.
// Note that the "properties" argument which includes custom properties is not currently used.
// But it can be used in the future, if we add a provision of custom properties to the Facet proto.
func toFacet(
	cachedProvenances map[string]*pb.Facet,
	provenance, unit, scalingFactor, measurementMethod, observationPeriod, _ string,
) (string, *pb.Facet) {
	cachedFacet := cachedProvenances[provenance]
	if cachedFacet == nil {
		cachedFacet = &pb.Facet{}
	}
	facet := proto.Clone(cachedFacet).(*pb.Facet)
	facet.Unit = unit
	facet.ScalingFactor = scalingFactor
	facet.MeasurementMethod = measurementMethod
	facet.ObservationPeriod = observationPeriod
	return util.GetFacetID(facet), facet
}

func getObservationsSQLQuery(variables []string,
	entities []string, queryDate string) string {
	entitiesStr := "'" + strings.Join(entities, "', '") + "'"
	variablesStr := "'" + strings.Join(variables, "', '") + "'"
	query := fmt.Sprintf(
		`
			SELECT entity, variable, date, value, provenance, unit, scaling_factor, measurement_method, observation_period, properties FROM observations
			WHERE entity IN (%s)
			AND variable IN (%s)
			AND value != ''
		`,
		entitiesStr,
		variablesStr,
	)
	if queryDate != "" && queryDate != shared.LATEST {
		query += fmt.Sprintf("AND date = (%s) ", queryDate)
	}
	query += "ORDER BY date ASC;"
	return query
}

// The internal structs below are for generating an intermediate response from the SQL response to simplify generating the final ObservationResponse.
type intermediateObservationResponse struct {
	byFacet map[byFacetKey]*byFacetValue
	// Ordered using insertion order for now.
	// Can update based on a ranking config if we decide to support that.
	orderedKeys []*byFacetKey
}

type byFacetKey struct {
	variable string
	entity   string
	facetId  string
}

type byFacetValue struct {
	observations []*pb.PointStat
	facetId      string
	facet        *pb.Facet
}
