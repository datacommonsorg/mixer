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
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	// Maximum number of edge hops to traverse for chained properties.
	maxHops = 10
	where   = "\n\t\tWHERE\n\t\t\t"
	and     = "\n\t\t\tAND "

	// Default timeout for timestamp polling.
	timestampPollingTimeout = 10 * time.Second

	// Default timeout for RPC requests.
	rpcTimeout = 60 * time.Second

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
	dcids, err := sc.GetEventCollectionDcids(ctx, req.AffectedPlaceDcid, req.EventType, req.Date)
	if err != nil {
		return nil, fmt.Errorf("failed to get event dcids: %w", err)
	}
	if len(dcids) == 0 {
		return &pbv1.EventCollection{}, nil
	}

	// Get properties for all DCIDs
	arc := &v2.Arc{
		Out:        true,
		SingleProp: "*",
	}
	edgesMap, err := sc.GetNodeEdgesByID(ctx, dcids, arc, datasources.DefaultPageSize, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get node edges: %w", err)
	}

	// Filter and Assemble
	res := assembleEventCollection(edgesMap, req)

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

func assembleEventCollection(edgesMap map[string][]*Edge, req *pbv1.EventCollectionRequest) *pbv1.EventCollection {
	res := &pbv1.EventCollection{
		Events:         []*pbv1.EventCollection_Event{},
		ProvenanceInfo: make(map[string]*pbv1.EventCollection_ProvenanceInfo),
	}

	for dcid, edges := range edgesMap {
		event := assembleAndFilterEvent(dcid, edges, req)
		if event != nil {
			res.Events = append(res.Events, event)
		}
	}

	// Sort events by DCID for determinism.
	sort.Slice(res.Events, func(i, j int) bool {
		return res.Events[i].Dcid < res.Events[j].Dcid
	})

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
func (sc *spannerDatabaseClient) GetEventCollectionDcids(ctx context.Context, placeID, eventType, date string) ([]string, error) {
	stmt := GetEventCollectionDcidsQuery(placeID, eventType, date)
	rows, err := queryDynamic(ctx, sc, *stmt)
	if err != nil {
		return nil, err
	}

	var dcids []string
	for _, row := range rows {
		if len(row) > 0 {
			dcids = append(dcids, row[0])
		}
	}
	return dcids, nil
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

// fetchAndUpdateTimestamp queries Spanner and updates the timestamp.
func (sc *spannerDatabaseClient) fetchAndUpdateTimestamp(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, timestampPollingTimeout)
	defer cancel()

	iter := sc.client.Single().Query(queryCtx, *GetCompletionTimestampQuery())
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return fmt.Errorf("no valid rows found in IngestionHistory")
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
	var timeout time.Duration

	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	} else {
		// Fallback if the parent context surprisingly has no deadline.
		// Using the default API timeout of 60 seconds.
		slog.Warn("Parent context has no deadline; using default API timeout of 60 seconds")
		timeout = rpcTimeout
	}
	queryCtx, cancel = context.WithTimeout(ctx, timeout)
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
				"timeout", timeout.String(),
				"error", err.Error(),
			)
		}

		return err
	}

	if sc.useStaleReads {
		ts, err := sc.getStalenessTimestamp()
		if err != nil {
			return err
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
	return runQuery(spanner.StrongRead())
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
