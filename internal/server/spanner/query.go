// Copyright 2024 Google LLC
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

// Queries executed by the SpannerClient.
package spanner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/datacommonsorg/mixer/internal/metrics"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/server/datasources"
	v2 "github.com/datacommonsorg/mixer/internal/server/v2"
	"github.com/datacommonsorg/mixer/internal/translator/types"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	// Maximum number of events to return for an event collection.
	maxEvents = 100

	// Use a large page size for the batch to ensure we get properties for all events.
	// Since we have up to maxEvents (100) events, and each event might have multiple edges,
	// DefaultPageSize (500) is too small and truncates the results for events at the end of the batch.
	eventBatchPageSize = 10000

	// Maximum number of edge hops to traverse for chained properties.
	maxHops = 10
	where   = "\n\t\tWHERE\n\t\t\t"
	and     = "\n\t\t\tAND "

	// Default timeout for timestamp polling.
	timestampPollingTimeout = 10 * time.Second

	// Default timeout for API requests.
	ApiTimeout = 60 * time.Second

	// Special edge predicates.
	predAffectedPlace      = "affectedPlace"
	predStartDate          = "startDate"
	predStartLocation      = "startLocation"
	predProvenance         = "provenance"
	predTypeOf             = "typeOf"
	predGeoJsonCoordinates = "geoJsonCoordinates"
	predName               = "name"
	predUrl                = "url"
	predDomain             = "domain"
)

// GetNodeProps retrieves node properties from Spanner given a list of IDs and a direction and returns a map.
func (sc *spannerDatabaseClient) GetNodeProps(ctx context.Context, ids []string, out bool) (map[string][]*Property, error) {
	props := map[string][]*Property{}
	if len(ids) == 0 {
		return props, nil
	}
	for _, id := range ids {
		props[id] = []*Property{}
	}

	err := queryStructs(
		ctx,
		sc,
		*GetNodePropsQuery(ids, out),
		func() interface{} {
			return &Property{}
		},
		func(rowStruct interface{}) {
			prop := rowStruct.(*Property)
			subjectID := prop.SubjectID
			props[subjectID] = append(props[subjectID], prop)
		},
	)
	if err != nil {
		return props, err
	}

	return props, nil
}

// GetNodeEdgesByID retrieves node edges from Spanner and returns a map of subjectID to Edges.
func (sc *spannerDatabaseClient) GetNodeEdgesByID(ctx context.Context, ids []string, arc *v2.Arc, pageSize, offset int) (map[string][]*Edge, error) {
	edges := make(map[string][]*Edge)
	if len(ids) == 0 {
		return edges, nil
	}
	for _, id := range ids {
		edges[id] = []*Edge{}
	}

	err := queryStructs(
		ctx,
		sc,
		*GetNodeEdgesByIDQuery(ids, arc, pageSize, offset),
		func() interface{} {
			return &Edge{}
		},
		func(rowStruct interface{}) {
			edge := rowStruct.(*Edge)
			subjectID := edge.SubjectID
			edges[subjectID] = append(edges[subjectID], edge)
		},
	)
	if err != nil {
		return nil, err
	}

	return edges, nil
}

// GetObservations retrieves observations from Spanner given a list of variables and entities.
func (sc *spannerDatabaseClient) GetObservations(ctx context.Context, variables []string, entities []string) ([]*Observation, error) {
	var observations []*Observation
	if len(entities) == 0 {
		return nil, fmt.Errorf("entity must be specified")
	}

	err := queryStructs(
		ctx,
		sc,
		*GetObservationsQuery(variables, entities),
		func() interface{} {
			return &Observation{}
		},
		func(rowStruct interface{}) {
			observation := rowStruct.(*Observation)
			observations = append(observations, observation)
		},
	)
	if err != nil {
		return observations, err
	}

	return observations, nil
}

// CheckVariableExistence checks for the existence of observations for the given variables and entities.
// Returns a slice of rows, where each row contains [variable, entity] that has at least one observation.
func (sc *spannerDatabaseClient) CheckVariableExistence(ctx context.Context, variables []string, entities []string) ([][]string, error) {
	stmt, err := FilterStatVarsByEntityQuery(variables, entities)
	if err != nil {
		return nil, err
	}
	return queryDynamic(ctx, sc, *stmt)
}

// GetObservationsContainedInPlace retrieves observations from Spanner given a list of variables and an entity expression.
func (sc *spannerDatabaseClient) GetObservationsContainedInPlace(ctx context.Context, variables []string, containedInPlace *v2.ContainedInPlace) ([]*Observation, error) {
	var observations []*Observation
	if len(variables) == 0 || containedInPlace == nil {
		return observations, nil
	}

	err := queryStructs(
		ctx,
		sc,
		*GetObservationsContainedInPlaceQuery(variables, containedInPlace),
		func() interface{} {
			return &Observation{}
		},
		func(rowStruct interface{}) {
			observation := rowStruct.(*Observation)
			observations = append(observations, observation)
		},
	)
	if err != nil {
		return observations, err
	}

	return observations, nil
}

// SearchNodes searches nodes in the graph based on the query and optionally the types.
// If the types array is empty, it searches across nodes of all types.
// A maximum of 100 results are returned.
func (sc *spannerDatabaseClient) SearchNodes(ctx context.Context, query string, types []string) ([]*SearchNode, error) {
	var nodes []*SearchNode
	if query == "" {
		return nodes, nil
	}

	err := queryStructs(
		ctx,
		sc,
		*SearchNodesQuery(query, types),
		func() interface{} {
			return &SearchNode{}
		},
		func(rowStruct interface{}) {
			node := rowStruct.(*SearchNode)
			nodes = append(nodes, node)
		},
	)
	if err != nil {
		return nodes, err
	}

	return nodes, nil
}

// ResolveByID fetches ID resolution candidates for a list of input nodes and in and out properties and returns a map of node to candidates.
func (sc *spannerDatabaseClient) ResolveByID(ctx context.Context, nodes []string, in, out string) (map[string][]string, error) {
	nodeToCandidates := make(map[string][]string)
	if len(nodes) == 0 {
		return nodeToCandidates, nil
	}

	// Create a map of Spanner node value to dcid to decode encoded values.
	valueMap := map[string]string{}
	for _, node := range nodes {
		value := generateObjectValue(node)
		valueMap[node] = node
		valueMap[value] = node
	}

	err := queryStructs(
		ctx,
		sc,
		*ResolveByIDQuery(nodes, in, out),
		func() interface{} {
			return &ResolutionCandidate{}
		},
		func(rowStruct interface{}) {
			resolutionCandidate := rowStruct.(*ResolutionCandidate)
			node := valueMap[resolutionCandidate.Node]
			nodeToCandidates[node] = append(nodeToCandidates[node], resolutionCandidate.Candidate)
		},
	)
	if err != nil {
		return nil, err
	}

	return nodeToCandidates, nil
}

// GetEventCollectionDate retrieves event collection dates from Spanner.
func (sc *spannerDatabaseClient) GetEventCollectionDate(ctx context.Context, placeID, eventType string) ([]string, error) {
	stmt := GetEventCollectionDateQuery(placeID, eventType)
	rows, err := queryDynamic(ctx, sc, *stmt)
	if err != nil {
		return nil, err
	}

	var res []string
	for _, row := range rows {
		if len(row) > 0 {
			res = append(res, row[0])
		}
	}
	return res, nil
}

// GetEventCollection retrieves and filters event collection from Spanner.
func (sc *spannerDatabaseClient) GetEventCollection(ctx context.Context, req *pbv1.EventCollectionRequest) (*pbv1.EventCollection, error) {
	// Get event DCIDs
	eventRows, err := sc.GetEventCollectionDcids(ctx, req.AffectedPlaceDcid, req.EventType, req.Date)
	if err != nil {
		return nil, fmt.Errorf("failed to get event dcids: %w", err)
	}
	if len(eventRows) == 0 {
		return &pbv1.EventCollection{}, nil
	}

	dcids := parseAndSortEvents(eventRows, req.EventType)

	// Get properties for all DCIDs
	arc := &v2.Arc{
		Out:        true,
		SingleProp: "*",
	}
	edgesMap, err := sc.GetNodeEdgesByID(ctx, dcids, arc, eventBatchPageSize, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get node edges: %w", err)
	}

	// Filter and Assemble
	res := assembleEventCollection(dcids, edgesMap, req)

	// Fetch and populate provenance info.
	if err := sc.populateProvenanceInfo(ctx, res); err != nil {
		return nil, err
	}

	return res, nil
}
func (sc *spannerDatabaseClient) populateProvenanceInfo(ctx context.Context, res *pbv1.EventCollection) error {
	provDcids := []string{}
	seen := map[string]bool{}
	for _, event := range res.Events {
		if event.ProvenanceId != "" && !seen[event.ProvenanceId] {
			seen[event.ProvenanceId] = true
			provDcids = append(provDcids, event.ProvenanceId)
		}
	}

	if len(provDcids) == 0 {
		return nil
	}

	provArc := &v2.Arc{
		Out:          true,
		BracketProps: []string{predUrl, predName, predDomain},
	}
	provEdgesMap, err := sc.GetNodeEdgesByID(ctx, provDcids, provArc, datasources.DefaultPageSize, 0)
	if err != nil {
		return fmt.Errorf("failed to get provenance info: %w", err)
	}

	for provDcid, edges := range provEdgesMap {
		info := &pbv1.EventCollection_ProvenanceInfo{}
		res.ProvenanceInfo[provDcid] = info
		for _, edge := range edges {
			switch edge.Predicate {
			case predUrl:
				info.ProvenanceUrl = edge.Value
			case predName:
				info.ImportName = edge.Value
			case predDomain:
				info.Domain = edge.Value
			}
		}
		// Fallback if domain still empty.
		if info.Domain == "" && info.ProvenanceUrl != "" {
			info.Domain = parseDomain(info.ProvenanceUrl)
		}
	}

	return nil
}

// parseDomain parses the URL to get the host domain.
func parseDomain(provUrl string) string {
	u, err := url.Parse(provUrl)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	parts := strings.Split(host, ".")
	if len(parts) >= 2 {
		return strings.Join(parts[len(parts)-2:], ".")
	}
	return host
}

func assembleEventCollection(dcids []string, edgesMap map[string][]*Edge, req *pbv1.EventCollectionRequest) *pbv1.EventCollection {
	res := &pbv1.EventCollection{
		Events:         []*pbv1.EventCollection_Event{},
		ProvenanceInfo: make(map[string]*pbv1.EventCollection_ProvenanceInfo),
	}

	for _, dcid := range dcids {
		if edges, ok := edgesMap[dcid]; ok {
			event := assembleAndFilterEvent(dcid, edges, req)
			if event != nil {
				res.Events = append(res.Events, event)
			}
		}
	}

	return res
}

// assembleAndFilterEvent assembles an event from its edges and filters it based on the request.
// Returns nil if the event should be filtered out.
func assembleAndFilterEvent(dcid string, edges []*Edge, req *pbv1.EventCollectionRequest) *pbv1.EventCollection_Event {
	event := &pbv1.EventCollection_Event{
		Dcid:         dcid,
		Places:       []string{},
		Dates:        []string{},
		GeoLocations: []*pbv1.EventCollection_GeoLocation{}, // Initialize
		PropVals:     make(map[string]*pbv1.EventCollection_ValList),
	}

	for _, edge := range edges {
		populateSpecialFields(event, edge)
		populatePropVals(event, edge)
	}

	// Filter events.
	// We must filter AFTER populating because we need the full event data
	// (e.g. PropVals) for filtering logic (keepEvent).
	if !keepEvent(event, req) {
		return nil
	}

	cleanUpPropVals(event)

	return event
}

func populateSpecialFields(event *pbv1.EventCollection_Event, edge *Edge) {
	switch edge.Predicate {
	case predAffectedPlace:
		// Exclude S2Cell places as per proto contract (proto/v1/event.proto).
		if !strings.HasPrefix(edge.Value, "s2CellId/") {
			event.Places = append(event.Places, edge.Value)
		}
	case predStartDate:
		// Do NOT trim date.
		event.Dates = append(event.Dates, edge.Value)
	case predStartLocation:
		// Populate GeoLocations from startLocation value.
		populateGeoLocation(event, edge.Value)
	}
}

func populateGeoLocation(event *pbv1.EventCollection_Event, value string) {
	// Note: The startLocation value in Spanner is usually a latLong/ DCID (e.g. latLong/577521_-958960).
	// We parse it here for performance to avoid an extra database roundtrip.
	//
	// TODO(task): Revisit this optimization if we encounter valid startLocation values
	// that are NOT latLong/ DCIDs but still need to be resolved to points, or if the
	// assumption that dcids always contain coordinates is not true.
	if strings.HasPrefix(value, "latLong/") {
		parts := strings.Split(strings.TrimPrefix(value, "latLong/"), "_")
		if len(parts) == 2 {
			lat, err1 := strconv.ParseFloat(parts[0], 64)
			lon, err2 := strconv.ParseFloat(parts[1], 64)
			if err1 == nil && err2 == nil {
				event.GeoLocations = append(event.GeoLocations, &pbv1.EventCollection_GeoLocation{
					Geo: &pbv1.EventCollection_GeoLocation_Point_{
						Point: &pbv1.EventCollection_GeoLocation_Point{
							Latitude:  proto.Float64(lat / 100000.0),
							Longitude: proto.Float64(lon / 100000.0),
						},
					},
				})
				return // Success
			}
		}
	}

	slog.Warn("startLocation is not a valid latLong/ DCID, skipping parsing optimization", "value", value)
}

func populatePropVals(event *pbv1.EventCollection_Event, edge *Edge) {
	val := edge.Value
	if edge.Predicate == predGeoJsonCoordinates && len(edge.Bytes) > 0 {
		if len(edge.Bytes) > 2 && edge.Bytes[0] == 0x1f && edge.Bytes[1] == 0x8b {
			decompressed, err := util.Unzip(edge.Bytes)
			if err == nil {
				val = string(decompressed)
			} else {
				slog.Error("failed to decompress geoJsonCoordinates", "err", err, "dcid", event.Dcid)
				val = ""
			}
		} else {
			val = string(edge.Bytes)
		}
	}
	if _, ok := event.PropVals[edge.Predicate]; !ok {
		event.PropVals[edge.Predicate] = &pbv1.EventCollection_ValList{Vals: []string{}}
	}
	event.PropVals[edge.Predicate].Vals = append(event.PropVals[edge.Predicate].Vals, val)

	if edge.Provenance != "" {
		event.ProvenanceId = edge.Provenance
	}
}

func cleanUpPropVals(event *pbv1.EventCollection_Event) {
	// Clean up PropVals (remove specialized fields to match V2).
	delete(event.PropVals, predAffectedPlace)
	delete(event.PropVals, predStartDate)
	delete(event.PropVals, predStartLocation)
	delete(event.PropVals, predProvenance)
	delete(event.PropVals, predTypeOf)
}

func keepEvent(event *pbv1.EventCollection_Event, req *pbv1.EventCollectionRequest) bool {
	if req.FilterProp == "" {
		return true
	}
	for prop, vals := range event.GetPropVals() {
		if prop == req.FilterProp {
			if len(vals.Vals) == 0 {
				return false
			}
			valStr := strings.TrimSpace(strings.TrimPrefix(vals.Vals[0], req.FilterUnit))
			v, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				return false
			}
			return v >= req.FilterLowerLimit && v <= req.FilterUpperLimit
		}
	}
	return false
}

// GetEventCollectionDcids retrieves event DCIDs from Spanner.
func (sc *spannerDatabaseClient) GetEventCollectionDcids(ctx context.Context, placeID, eventType, date string) ([]EventIdWithMagnitudeDcid, error) {
	stmt := GetEventCollectionDcidsQuery(placeID, eventType, date)
	rows, err := queryDynamic(ctx, sc, *stmt)
	if err != nil {
		return nil, err
	}

	var res []EventIdWithMagnitudeDcid
	for _, row := range rows {
		if len(row) == 0 {
			continue
		}
		item := EventIdWithMagnitudeDcid{EventID: row[0]}
		if len(row) > 1 {
			item.MagnitudeDcid = row[1]
		}
		res = append(res, item)
	}
	return res, nil
}

type parsedEvent struct {
	dcid      string
	magnitude float64
}

// parseMagnitudeDcid parses the numeric magnitude value from a DCID string.
//
// Background:
// In the Spanner graph, quantity nodes have a `value` property that is identical to their DCID
// (e.g. `SquareKilometer91.57871`). Since we'd still receive a string with a prefix even after
// another jump, we can bypass the redundant join and parse the numeric value directly from the
// `object_id` of the edge in-memory.
// Ideally the value in Spanner should be stored as just the value and not this awkward string.
// If that happens, we can remove this function and just use the value directly.
func parseMagnitudeDcid(magnitudeDcid, unit string) float64 {
	if magnitudeDcid == "" || unit == "" {
		return 0.0
	}
	valStr := strings.TrimPrefix(magnitudeDcid, unit)
	v, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		slog.Error("failed to parse magnitude DCID", "err", err, "valStr", valStr, "magnitudeDcid", magnitudeDcid)
		return 0.0
	}
	return v
}

// parseAndSortEvents parses magnitude DCIDs, sorts events by magnitude then DCID alphabetical, and truncates to top 100.
func parseAndSortEvents(rows []EventIdWithMagnitudeDcid, eventType string) []string {
	cfg, hasCfg := EventConfigs[eventType]

	var events []parsedEvent
	for _, r := range rows {
		mag := 0.0
		if hasCfg {
			mag = parseMagnitudeDcid(r.MagnitudeDcid, cfg.MagnitudeValUnit)
		}
		events = append(events, parsedEvent{dcid: r.EventID, magnitude: mag})
	}

	// Stable sort: magnitude descending (or ascending per config), DCID ascending tie-breaker
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].magnitude != events[j].magnitude {
			if hasCfg && cfg.Order == ASC {
				return events[i].magnitude < events[j].magnitude
			}
			return events[i].magnitude > events[j].magnitude
		}
		return events[i].dcid < events[j].dcid
	})

	var res []string
	for i := 0; i < len(events) && i < maxEvents; i++ {
		res = append(res, events[i].dcid)
	}
	return res
}

func (sc *spannerDatabaseClient) Sparql(ctx context.Context, nodes []types.Node, queries []*types.Query, opts *types.QueryOptions) ([][]string, error) {
	query, err := SparqlQuery(nodes, queries, opts)
	if err != nil {
		return nil, fmt.Errorf("error building sparql query: %v", err)
	}

	return queryDynamic(ctx, sc, *query)
}

func (sc *spannerDatabaseClient) GetProvenanceSummary(ctx context.Context, variables []string) (map[string]map[string]*pb.StatVarSummary_ProvenanceSummary, error) {
	if len(variables) == 0 {
		return map[string]map[string]*pb.StatVarSummary_ProvenanceSummary{},
			nil
	}

	results, err := queryCache(
		ctx,
		sc,
		*GetCacheDataQuery(TypeProvenanceSummary, variables),
		func() *pb.StatVarSummary_ProvenanceSummary {
			return &pb.StatVarSummary_ProvenanceSummary{}
		},
	)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// GetTermEmbeddingQuery retrieves embeddings from Spanner for a given query.
func (sc *spannerDatabaseClient) GetTermEmbeddingQuery(ctx context.Context, modelName, searchLabel, taskType string) ([]float64, error) {
	var embeddings []float64
	err := sc.executeQuery(ctx, *GetTermEmbeddingQuery(modelName, searchLabel, taskType), func(iter *spanner.RowIterator) error {
		row, err := iter.Next()
		if err == iterator.Done {
			return fmt.Errorf("no embedding returned for model %s and label %s", modelName, searchLabel)
		}
		if err != nil {
			return err
		}
		return row.Column(0, &embeddings)
	})
	return embeddings, err
}

// VectorSearchQuery performs vector similarity search in Spanner.
func (sc *spannerDatabaseClient) VectorSearchQuery(ctx context.Context, limit int, embeddings []float64, numLeaves int, threshold float64) ([]*VectorSearchResult, error) {
	var results []*VectorSearchResult
	err := queryStructs(
		ctx,
		sc,
		*VectorSearchQuery(limit, embeddings, numLeaves, threshold),
		func() interface{} {
			return &VectorSearchResult{}
		},
		func(rowStruct interface{}) {
			res := rowStruct.(*VectorSearchResult)
			results = append(results, res)
		},
	)
	return results, err
}
// GetStatVarGroupNode fetches StatVarGroupNode info from Spanner.
func (sc *spannerDatabaseClient) GetStatVarGroupNode(ctx context.Context, nodes []string) ([]*StatVarGroupNode, error) {
	var svgNodes []*StatVarGroupNode
	if len(nodes) == 0 {
		return svgNodes, nil
	}

	err := queryStructs(
		ctx,
		sc,
		*GetStatVarGroupNodeQuery(nodes),
		func() interface{} {
			return &StatVarGroupNode{}
		},
		func(rowStruct interface{}) {
			svgNodes = append(svgNodes, rowStruct.(*StatVarGroupNode))
		},
	)
	if err != nil {
		return svgNodes, err
	}

	return svgNodes, nil
}

// GetFilteredStatVarGroupNode fetches the relevant info to build a filtered StatVarGroupNode from Spanner.
func (sc *spannerDatabaseClient) GetFilteredStatVarGroupNode(ctx context.Context, node string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int) (*FilteredStatVarGroupNode, error) {
	filteredStatVarGroupNode := &FilteredStatVarGroupNode{}
	errGroup, errCtx := errgroup.WithContext(ctx)
	svgChildChan := make(chan []*SVGChild, 1)
	childSVChan := make(chan []*ChildSV, 1)
	childSVGChan := make(chan []*ChildSVG, 1)

	errGroup.Go(func() error {
		var svgChildren []*SVGChild
		err := queryStructs(
			errCtx,
			sc,
			*GetSVGChildrenQuery(node),
			func() interface{} {
				return &SVGChild{}
			},
			func(rowStruct interface{}) {
				svgChildren = append(svgChildren, rowStruct.(*SVGChild))
			},
		)
		if err != nil {
			return err
		}
		svgChildChan <- svgChildren
		return nil
	})

	errGroup.Go(func() error {
		var childSVs []*ChildSV
		err := queryStructs(
			errCtx,
			sc,
			*GetFilteredSVGChildrenQuery(templateSV, node, constrainedPlaces, constrainedImport, numEntitiesExistence),
			func() interface{} {
				return &ChildSV{}
			},
			func(rowStruct interface{}) {
				childSVs = append(childSVs, rowStruct.(*ChildSV))
			},
		)
		if err != nil {
			return err
		}
		childSVChan <- childSVs
		return nil
	})

	errGroup.Go(func() error {
		var childSVGs []*ChildSVG
		err := queryStructs(
			errCtx,
			sc,
			*GetFilteredSVGChildrenQuery(templateSVG, node, constrainedPlaces, constrainedImport, numEntitiesExistence),
			func() interface{} {
				return &ChildSVG{}
			},
			func(rowStruct interface{}) {
				childSVGs = append(childSVGs, rowStruct.(*ChildSVG))
			},
		)
		if err != nil {
			return err
		}
		childSVGChan <- childSVGs
		return nil
	})

	if err := errGroup.Wait(); err != nil {
		return filteredStatVarGroupNode, err
	}

	close(svgChildChan)
	close(childSVChan)
	close(childSVGChan)

	filteredStatVarGroupNode.SVGChild = <-svgChildChan
	filteredStatVarGroupNode.ChildSV = <-childSVChan
	filteredStatVarGroupNode.ChildSVG = <-childSVGChan

	return filteredStatVarGroupNode, nil
}

// GetFilteredTopic fetches the relevant info to build a filtered Topic response from Spanner.
func (sc *spannerDatabaseClient) GetFilteredTopic(ctx context.Context, node string, constrainedPlaces []string, constrainedImport string, numEntitiesExistence int) (int, error) {
	stmt := GetFilteredSVGChildrenQuery(templateTopic, node, constrainedPlaces, constrainedImport, numEntitiesExistence)
	rows, err := queryDynamic(ctx, sc, *stmt)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		// No child SVs.
		return 0, nil
	}
	if len(rows[0]) == 0 {
		return 0, fmt.Errorf("malformed response when fetching count of Topic children")
	}
	count, err := strconv.Atoi(rows[0][0])
	if err != nil {
		return 0, fmt.Errorf("error converting Topic children count to int")
	}
	return count, nil
}

// fetchAndUpdateTimestamp queries Spanner and updates the timestamp.
func (sc *spannerDatabaseClient) fetchAndUpdateTimestamp(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, timestampPollingTimeout)
	defer cancel()

	iter := sc.client.Single().Query(queryCtx, *GetCompletionTimestampQuery())
	defer iter.Stop()

	row, err := iter.Next()

	// Handle missing or empty table cases gracefully
	var warnMsg string
	if err == iterator.Done {
		warnMsg = "No valid rows found in IngestionHistory."
	} else if code := spanner.ErrCode(err); code == codes.NotFound ||
		(code == codes.InvalidArgument && strings.Contains(err.Error(), "Table not found: IngestionHistory")) {
		warnMsg = "IngestionHistory table not found."
	}

	if warnMsg != "" {
		slog.Warn(warnMsg + " Falling back to strong reads.")
		return nil
	}

	if err != nil {
		if isTimeoutError(err) {
			slog.ErrorContext(queryCtx, "Spanner timestamp polling timed out",
				"timeout_duration", timestampPollingTimeout.String(),
				"error", err.Error(),
			)
		}
		return fmt.Errorf("failed to fetch row: %w", err)
	}

	var timestamp time.Time
	if err := row.Column(0, &timestamp); err != nil {
		return fmt.Errorf("failed to read CompletionTimestamp column: %w", err)
	}

	sc.timestamp.Store(timestamp.UnixNano())
	return nil
}

func (sc *spannerDatabaseClient) getStalenessTimestamp() (time.Time, error) {
	val := sc.timestamp.Load()
	if val != 0 {
		return time.Unix(0, val).UTC(), nil
	}
	slog.Error("Spanner staleness timestamp not available")
	return time.Time{}, fmt.Errorf("error getting staleness timestamp")
}

func (sc *spannerDatabaseClient) executeQuery(
	ctx context.Context,
	stmt spanner.Statement,
	handleRows func(*spanner.RowIterator) error,
) error {
	var queryCtx context.Context
	var cancel context.CancelFunc

	if _, ok := ctx.Deadline(); ok {
		queryCtx, cancel = context.WithCancel(ctx)
	} else {
		// Fallback if the parent context surprisingly has no deadline.
		// Using the default API timeout.
		slog.Warn("Parent context has no deadline; using default API timeout", "timeout", ApiTimeout.String())
		queryCtx, cancel = context.WithTimeout(ctx, ApiTimeout)
	}
	defer cancel()

	runQuery := func(tb spanner.TimestampBound) error {
		metrics.RecordSpannerQuery(queryCtx)
		iter := sc.client.Single().WithTimestampBound(tb).Query(queryCtx, stmt)
		defer iter.Stop()
		err := handleRows(iter)

		// Log slow Spanner queries that timed out.
		if isTimeoutError(err) {
			slog.ErrorContext(queryCtx, "Spanner query timed out",
				"sql", stmt.SQL,
				"error", err.Error(),
			)
		}

		return err
	}

	ts, err := sc.getStalenessTimestamp()
	if err != nil {
		return runQuery(spanner.StrongRead())
	}
	err = runQuery(spanner.ReadTimestamp(ts))

	// Log error if timestamp is older than retention and fall back to strong read.
	if spanner.ErrCode(err) == codes.FailedPrecondition {
		slog.Error("Stale read timestamp expired. Falling back to StrongRead.",
			"expiredTimestamp", ts.String())
		return runQuery(spanner.StrongRead())
	}
	return err
}

// queryStructs executes a query and maps the results to an input struct.
func queryStructs(
	ctx context.Context,
	sc *spannerDatabaseClient,
	stmt spanner.Statement,
	newStruct func() interface{},
	withStruct func(interface{}),
) error {
	return sc.executeQuery(ctx, stmt, func(iter *spanner.RowIterator) error {
		return processRows(iter, newStruct, withStruct)
	})
}

// queryDynamic executes a dynamically constructed query and returns the results as a slice of string slices.
func queryDynamic(
	ctx context.Context,
	sc *spannerDatabaseClient,
	stmt spanner.Statement,
) ([][]string, error) {
	var rowData [][]string
	err := sc.executeQuery(ctx, stmt, func(iter *spanner.RowIterator) error {
		result, err := processDynamicRows(iter)
		rowData = result
		return err
	})
	return rowData, err
}

// queryCache executes a query and maps the results to an input cache proto.
func queryCache[T proto.Message](
	ctx context.Context,
	sc *spannerDatabaseClient,
	stmt spanner.Statement,
	newProto func() T,
) (map[string]map[string]T, error) {
	var data map[string]map[string]T
	err := sc.executeQuery(ctx, stmt, func(iter *spanner.RowIterator) error {
		result, err := processCacheRows(iter, newProto)
		data = result
		return err
	})
	return data, err
}

func processRows(iter *spanner.RowIterator, newStruct func() interface{}, withStruct func(interface{})) error {
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to fetch row: %w", err)
		}

		rowStruct := newStruct()
		if err := row.ToStructLenient(rowStruct); err != nil {
			return fmt.Errorf("failed to parse row: %w", err)
		}
		withStruct(rowStruct)
	}

	return nil
}

// processDynamicRows processes rows from dynamically constructed queries.
func processDynamicRows(iter *spanner.RowIterator) ([][]string, error) {
	rowData := [][]string{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return rowData, err
		}

		data := []string{}
		for i := 0; i < row.Size(); i++ {
			var val spanner.GenericColumnValue
			if err := row.Column(i, &val); err != nil {
				return rowData, err
			}
			data = append(data, val.Value.GetStringValue())
		}
		rowData = append(rowData, data)
	}
	return rowData, nil
}

// processCacheRows processes rows and maps them to a proto struct.
func processCacheRows[T proto.Message](iter *spanner.RowIterator, newProto func() T) (map[string]map[string]T, error) {
	results := make(map[string]map[string]T)
	unmarshaler := protojson.UnmarshalOptions{DiscardUnknown: true}

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to fetch row: %w", err)
		}

		var key string
		if err := row.ColumnByName("key", &key); err != nil {
			return nil, fmt.Errorf("failed to read key column: %w", err)
		}

		var provenance string
		if err := row.ColumnByName("provenance", &provenance); err != nil {
			return nil, fmt.Errorf("failed to read provenance column: %w", err)
		}

		var jsonStr spanner.NullString
		if err := row.ColumnByName("value", &jsonStr); err != nil {
			return nil, fmt.Errorf("failed to read value column: %w", err)
		}

		if jsonStr.Valid {
			msg := newProto()
			if err := unmarshaler.Unmarshal([]byte(jsonStr.StringVal), msg); err != nil {
				return nil, fmt.Errorf("failed to unmarshal proto: %w", err)
			}

			if results[key] == nil {
				results[key] = make(map[string]T)
			}
			results[key][provenance] = msg
		}
	}

	return results, nil
}

// isTimeoutError checks if an error is a timeout error from Spanner or context.
func isTimeoutError(err error) bool {
	return spanner.ErrCode(err) == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded)
}
