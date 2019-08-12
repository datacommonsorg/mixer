// Copyright 2019 Google LLC
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

package store

import (
	"context"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"

	"encoding/json"

	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/translator"
	"github.com/datacommonsorg/mixer/util"

	"golang.org/x/sync/errgroup"

	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --------------------------------- STRUCTS ----------------------------------

// Triple represents a triples entry in the BT triples cache.
type Triple struct {
	SubjectID    string   `json:"subjectId,omitempty"`
	SubjectName  string   `json:"subjectName,omitempty"`
	SubjectTypes []string `json:"subjectTypes,omitempty"`
	Predicate    string   `json:"predicate,omitempty"`
	ObjectID     string   `json:"objectId,omitempty"`
	ObjectName   string   `json:"objectName,omitempty"`
	ObjectValue  string   `json:"objectValue,omitempty"`
	ObjectTypes  string   `json:"objectTypes,omitempty"`
}

// Node represents a information about a node.
type Node struct {
	Dcid   string   `json:"dcid,omitempty"`
	Name   string   `json:"name,omitempty"`
	ProvID string   `json:"provenanceId,omitempty"`
	Value  string   `json:"value,omitempty"`
	Types  []string `json:"types,omitempty"`
}

// TriplesCache represents the json structure returned by the BT triples cache
type TriplesCache struct {
	Triples []Triple `json:"triples"`
}

// PropValueCache represents the json structure returned by the BT PropVal cache
type PropValueCache struct {
	Nodes []Node `json:"entities,omitempty"`
}

// PropLabelCache represents the json structure returned by the BT Prop cache
type PropLabelCache struct {
	InLabels  []string `json:"inLabels"`
	OutLabels []string `json:"outLabels"`
}

func (s *store) GetPropertyValues(ctx context.Context,
	in *pb.GetPropertyValuesRequest, out *pb.GetPropertyValuesResponse) error {
	if in.GetLimit() == 0 {
		in.Limit = util.BtCacheLimit
	}

	var err error
	var inRes, outRes map[string]map[string][]Node
	if in.GetLimit() > util.BtCacheLimit {
		inRes, err = s.bqGetPropertyValues(ctx, in, false)
		if err != nil {
			return err
		}
		outRes, err = s.bqGetPropertyValues(ctx, in, true)
		if err != nil {
			return err
		}
	} else {
		inRes, err = s.btGetPropertyValues(ctx, in, false)
		if err != nil {
			return err
		}
		outRes, err = s.btGetPropertyValues(ctx, in, true)
		if err != nil {
			return err
		}
	}

	nodeRes := make(map[string]map[string][]Node)
	for _, r := range []map[string]map[string][]Node{inRes, outRes} {
		for k1, v1 := range r {
			if _, ok := nodeRes[k1]; !ok {
				nodeRes[k1] = v1
			} else {
				for k2, v2 := range v1 {
					nodeRes[k1][k2] = v2
				}
			}
		}
	}

	jsonRaw, err := json.Marshal(nodeRes)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return err
}

func (s *store) bqGetPropertyValues(ctx context.Context,
	in *pb.GetPropertyValuesRequest, arcOut bool) (map[string]map[string][]Node, error) {
	// TODO(antaresc): Fix the ValueType not being used in the triple query
	dcids := in.GetDcids()

	// Get request parameters
	valueType := in.GetValueType()
	prop := in.GetProperty()
	var direction string
	if arcOut {
		direction = "out"
	} else {
		direction = "in"
	}
	limit := in.GetLimit()
	triples := []*pb.Triple{}

	// Get triples from the triples table
	var srcIDCol, valIDCol string
	if arcOut {
		srcIDCol = "t.subject_id"
		valIDCol = "t.object_id"
	} else {
		srcIDCol = "t.object_id"
		valIDCol = "t.subject_id"
	}

	// Perform the SQL query
	// POTENTIAL BUG: If the value is not a DCID then this joins may return an
	// empty result. Hopefully, users will only specify a type when the predicate
	// does not point to a leaf node.
	var qStr string
	if valueType != "" {
		qStr = fmt.Sprintf("SELECT t.prov_id, t.subject_id, t.predicate, t.object_id, t.object_value "+
			"FROM `%s.Triple` AS t JOIN `%s.Instance` AS i ON %s = i.id "+
			"WHERE %s IN (%s) "+
			"AND t.predicate = \"%s\" "+
			"AND i.type = \"%s\" "+
			"LIMIT %d",
			s.bqDb, s.bqDb, valIDCol, srcIDCol, util.StringList(dcids), prop, valueType, limit)
	} else {
		qStr = fmt.Sprintf("SELECT t.prov_id, t.subject_id, t.predicate, t.object_id, t.object_value "+
			"FROM `%s.Triple` AS t "+
			"WHERE %s IN (%s) "+
			"AND t.predicate = \"%s\" "+
			"LIMIT %d",
			s.bqDb, srcIDCol, util.StringList(dcids), prop, limit*util.LimitFactor)
	}
	queryStrs := []string{qStr}
	tripleRes, err := queryTripleTable(ctx, s.bqClient, queryStrs)
	if err != nil {
		return nil, err
	}
	triples = append(triples, tripleRes...)

	// Get the nodeType to use for special table queries
	nodeType := in.GetValueType()
	if nodeType == "" {
		nodeType, err = getNodeType(ctx, s.bqClient, s.bqDb, dcids[0])
		if err != nil {
			return nil, err
		}
	}

	// Get out in/out arc triples depending on the direction given.
	if arcOut {
		outArcInfo, err := s.bqGetOutArcInfo(nodeType)
		if err != nil {
			return nil, err
		}
		tripleRes, err = getOutArcFromSpecialTable(ctx, s.bqClient, s.bqDb, dcids, outArcInfo, prop)
		if err != nil {
			return nil, err
		}
		triples = append(triples, tripleRes...)
	} else {
		inArcInfo, err := s.bqGetInArcInfo(nodeType)
		if err != nil {
			return nil, err
		}
		tripleRes, err = getInArcFromSpecialTable(ctx, s.bqClient, s.bqDb, dcids, inArcInfo, prop)
		if err != nil {
			return nil, err
		}
		triples = append(triples, tripleRes...)
	}

	// Get node info for all reference dcids. First, build a list of dcids to
	// getNodeInfo for
	nodeIds := make([]string, 0)
	for _, t := range triples {
		if arcOut && t.GetObjectId() != "" {
			nodeIds = append(nodeIds, t.GetObjectId())
		} else if !arcOut {
			nodeIds = append(nodeIds, t.GetSubjectId())
		}
	}
	nodeInfo, err := getNodeInfo(ctx, s.bqClient, s.bqDb, nodeIds)
	if err != nil {
		return nil, err
	}

	// Populate nodes from the final list of triples. First initialize the map
	// with the given parameters
	nodeRes := make(map[string]map[string][]Node, 0)
	for _, dcid := range dcids {
		nodeRes[dcid] = make(map[string][]Node, 0)
		nodeRes[dcid][direction] = make([]Node, 0)
	}

	// Then copy over all triples
	for _, t := range triples {
		// Get the node's contents
		var srcID string
		var node Node
		if arcOut && t.GetObjectValue() != "" {
			srcID = t.GetSubjectId()
			node = Node{
				ProvID: t.GetProvenanceId(),
				Value:  t.GetObjectValue(),
			}
		} else {
			var valID string
			if arcOut {
				srcID = t.GetSubjectId()
				valID = t.GetObjectId()
			} else {
				srcID = t.GetObjectId()
				valID = t.GetSubjectId()
			}
			if valNode, valOk := nodeInfo[valID]; valOk {
				node = *valNode
			} else {
				node = Node{
					Dcid:   valID,
					ProvID: t.GetProvenanceId(),
				}
			}
		}

		// Map the node accordingly
		nodeRes[srcID][direction] = append(nodeRes[srcID][direction], node)
	}

	return nodeRes, nil
}

func (s *store) btGetPropertyValues(ctx context.Context,
	in *pb.GetPropertyValuesRequest, arcOut bool) (map[string]map[string][]Node, error) {
	dcids := in.GetDcids()
	prop := in.GetProperty()
	var direction string
	if arcOut {
		direction = "out"
	} else {
		direction = "in"
	}
	valType := in.GetValueType()

	keyPrefix := map[bool]string{
		true:  util.BtPropValOutPrefix,
		false: util.BtPropValInPrefix,
	}

	rowRangeList := bigtable.RowRangeList{}
	for _, dcid := range dcids {
		rowPrefix := fmt.Sprintf("%s%s-%s", keyPrefix[arcOut], dcid, prop)
		if valType != "" {
			rowPrefix = rowPrefix + "-" + valType
		}
		rowRangeList = append(rowRangeList, bigtable.PrefixRange((rowPrefix)))
	}

	btRawValuesMap := map[string][]string{} // Key: dcid; value: a list of raw row values.
	if err := util.BigTableReadRowsParallel(ctx, s.btTable, rowRangeList,
		func(btRow bigtable.Row) error {
			// Extract DCID from row key.
			rowKey := btRow.Key()
			parts := strings.Split(rowKey, "-")
			dcid := strings.TrimPrefix(parts[0], keyPrefix[arcOut])

			btResult := btRow[util.BtFamily][0]
			if _, ok := btRawValuesMap[dcid]; !ok {
				btRawValuesMap[dcid] = []string{}
			}
			btRawValuesMap[dcid] = append(btRawValuesMap[dcid], string(btResult.Value))

			return nil
		}); err != nil {
		return nil, err
	}

	nodeRes := make(map[string]map[string][]Node, 0)
	for dcid, btRawValues := range btRawValuesMap {
		nodeList := make([]Node, 0)
		for _, btRawValue := range btRawValues {
			btJSONRaw, err := util.UnzipAndDecode(string(btRawValue))
			if err != nil {
				return nil, err
			}

			// Parse the JSON and send the triples to the channel.
			var btPropVals PropValueCache
			json.Unmarshal(btJSONRaw, &btPropVals)

			nodes := btPropVals.Nodes
			if limit := int(in.GetLimit()); len(btPropVals.Nodes) > limit {
				nodes = nodes[:limit]
			}
			nodeList = append(nodeList, nodes...)
		}

		nodeRes[dcid] = map[string][]Node{
			direction: nodeList,
		}
	}

	return nodeRes, nil
}

func (s *store) GetTriples(ctx context.Context,
	in *pb.GetTriplesRequest, out *pb.GetTriplesResponse) error {
	if in.GetLimit() == 0 {
		in.Limit = util.BtCacheLimit
	}

	var err error
	if in.GetLimit() > util.BtCacheLimit {
		err = s.bqGetTriples(ctx, in, out)
	} else {
		err = s.btGetTriples(ctx, in, out)
	}
	return err
}

func (s *store) bqGetTriples(
	ctx context.Context, in *pb.GetTriplesRequest, out *pb.GetTriplesResponse) error {
	// Get parameters from the request and create the triples channel
	dcids := in.GetDcids()
	limit := in.GetLimit()
	resultsChan := make(chan map[string][]*pb.Triple, len(dcids))

	// Perform the BigQuery queries for each dcid asynchronously.
	// TODO(antaresc): replace all instances of ctx.
	errs, errCtx := errgroup.WithContext(ctx)
	for _, dcid := range dcids {
		// Asynchronously handle each dcid
		dcid := dcid
		errs.Go(func() error {
			// Maintain a list of triples to return
			resTrips := make([]*pb.Triple, 0)

			// First get the node type associated with this dcid.
			nodeType, err := getNodeType(errCtx, s.bqClient, s.bqDb, dcid)
			if err != nil {
				return err
			}
			resTrips = append(resTrips, &pb.Triple{
				SubjectId:   dcid,
				Predicate:   "typeOf",
				ObjectValue: nodeType,
			})

			// Get all the in/out arc nodes/values from the Triples table.
			qStrs := []string{
				// Get all the out arc nodes/values from Triples table.
				fmt.Sprintf(
					"SELECT t.prov_id, t.subject_id, t.predicate, t.object_id, t.object_value  "+
						"FROM `%s.Triple` AS t "+
						"WHERE t.subject_id = \"%s\"", s.bqDb, dcid),
				// Get all the in arc nodes from Triples table.
				fmt.Sprintf(
					"SELECT t.prov_id, t.subject_id, t.predicate, t.object_id, t.object_value  "+
						"FROM `%s.Triple` AS t "+
						"WHERE t.predicate != \"typeOf\" AND t.object_id = \"%s\" LIMIT %d",
					s.bqDb, dcid, limit),
			}
			// Send the query and append new triples
			tt, err := queryTripleTable(errCtx, s.bqClient, qStrs)
			if err != nil {
				return err
			}
			resTrips = append(resTrips, tt...)

			// Get all the out arc nodes/values from special tables (Place, etc.).
			outArcInfo, err := s.bqGetOutArcInfo(nodeType)
			if err != nil {
				return err
			}
			outTriples, err := getOutArcFromSpecialTable(errCtx, s.bqClient, s.bqDb, []string{dcid}, outArcInfo)
			if err != nil {
				return err
			}
			resTrips = append(resTrips, outTriples...)

			// Get all the in arc nodes from Special tables (Place etc...).
			inArcInfo, err := s.bqGetInArcInfo(nodeType)
			if err != nil {
				return err
			}
			inTriples, err := getInArcFromSpecialTable(errCtx, s.bqClient, s.bqDb, []string{dcid}, inArcInfo)
			if err != nil {
				return err
			}
			resTrips = append(resTrips, inTriples...)

			// Get typeOf in arc nodes from Instance table.
			if nodeType == "Class" {
				instanceTriples, err := getInstances(errCtx, s.bqClient, s.bqDb, dcid, limit)
				if err != nil {
					return err
				}
				resTrips = append(resTrips, instanceTriples...)
			}

			// Populate all other fields in the triple (i.e. name, type, etc.)
			baseNodeInfo, err := getNodeInfo(errCtx, s.bqClient, s.bqDb, []string{dcid})
			if err != nil {
				return err
			}
			name := baseNodeInfo[dcid].Name
			types := baseNodeInfo[dcid].Types

			allDcids := []string{}
			for _, t := range resTrips {
				if t.GetSubjectId() == dcid {
					if t.GetObjectId() != "" {
						allDcids = append(allDcids, t.GetObjectId())
					}
				} else {
					allDcids = append(allDcids, t.GetSubjectId())
				}
			}
			nodeInfo, err := getNodeInfo(errCtx, s.bqClient, s.bqDb, allDcids)
			if err != nil {
				return err
			}
			for _, t := range resTrips {
				if t.GetSubjectId() == dcid {
					t.SubjectName = name
					t.SubjectTypes = types
					if t.GetObjectId() != "" {
						t.ObjectName = nodeInfo[t.GetObjectId()].Name
						t.ObjectTypes = nodeInfo[t.GetObjectId()].Types
					}
				} else {
					t.SubjectName = nodeInfo[t.GetSubjectId()].Name
					t.SubjectTypes = nodeInfo[t.GetSubjectId()].Types
					t.ObjectName = name
					t.ObjectTypes = types
				}
			}

			// Send the list of triples to the channel
			triplesMap := map[string][]*pb.Triple{dcid: resTrips}
			resultsChan <- triplesMap

			return nil
		})
	}

	// Block on threads performing the cache read.
	err := errs.Wait()
	if err != nil {
		return err
	}

	// Copy over the contents of the results channel
	close(resultsChan)
	resultsMap := map[string][]*pb.Triple{}
	for triplesMap := range resultsChan {
		for dcid := range triplesMap {
			// Need only copy over the dcids because an empty list of triples is
			// created for each dcid given.
			resultsMap[dcid] = triplesMap[dcid]
		}
	}

	// Format the json response and encode it in base64 as necessary.
	jsonRaw, err := json.Marshal(resultsMap)
	if err != nil {
		return err
	}
	jsonStr := string(jsonRaw)
	out.Payload = jsonStr

	return nil
}

// Returns information for inwards facing arcs towards the given node type.
func (s *store) bqGetInArcInfo(nodeType string) ([]translator.InArcInfo, error) {
	// Get parent type
	parentType, exists := s.subTypeMap[nodeType]
	if !exists {
		parentType = nodeType
	}

	// Get in arc info
	var err error
	inArcInfo, exists := s.inArcInfo[parentType]
	if !exists {
		inArcInfo, err = translator.GetInArcInfo(s.bqMapping, parentType)
		if err != nil {
			return nil, err
		}
		s.inArcInfo[parentType] = inArcInfo
	}

	return inArcInfo, nil
}

// Returns information for outwards facing arcs towards the given node type.
func (s *store) bqGetOutArcInfo(nodeType string) (map[string][]translator.OutArcInfo, error) {
	// Get parent type
	parentType, exists := s.subTypeMap[nodeType]
	if !exists {
		parentType = nodeType
	}

	// Get out arc info
	var err error
	outArcInfo, exists := s.outArcInfo[parentType]
	if !exists {
		outArcInfo, err = translator.GetOutArcInfo(s.bqMapping, parentType)
		if err != nil {
			return nil, err
		}
		s.outArcInfo[parentType] = outArcInfo
	}

	return outArcInfo, nil
}

func (s *store) GetPropertyLabels(
	ctx context.Context,
	in *pb.GetPropertyLabelsRequest,
	out *pb.GetPropertyLabelsResponse) error {
	dcids := in.GetDcids()

	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, fmt.Sprintf("%s%s", util.BtArcsPrefix, dcid))
	}

	resultsMap := map[string]*PropLabelCache{}
	if err := util.BigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(btRow bigtable.Row) error {
			// Extract DCID from row key.
			dcid := strings.TrimPrefix(btRow.Key(), util.BtArcsPrefix)

			if len(btRow[util.BtFamily]) > 0 {
				btRawValue := btRow[util.BtFamily][0].Value
				btJSONRaw, err := util.UnzipAndDecode(string(btRawValue))
				if err != nil {
					return err
				}
				var btPropLabels PropLabelCache
				json.Unmarshal(btJSONRaw, &btPropLabels)
				resultsMap[dcid] = &btPropLabels

				// Fill in InLabels / OutLabels with an empty list if not present.
				if resultsMap[dcid].InLabels == nil {
					resultsMap[dcid].InLabels = []string{}
				}
				if resultsMap[dcid].OutLabels == nil {
					resultsMap[dcid].OutLabels = []string{}
				}
			}

			return nil
		}); err != nil {
		return err
	}

	// Iterate through all dcids to make sure they are present in resultsMap.
	for _, dcid := range dcids {
		if _, exists := resultsMap[dcid]; !exists {
			resultsMap[dcid] = &PropLabelCache{
				InLabels:  []string{},
				OutLabels: []string{},
			}
		}
	}

	jsonRaw, err := json.Marshal(resultsMap)
	if err != nil {
		return err
	}
	jsonStr := string(jsonRaw)
	out.Payload = jsonStr

	return nil
}

func (s *store) btGetTriples(
	ctx context.Context, in *pb.GetTriplesRequest, out *pb.GetTriplesResponse) error {
	dcids := in.GetDcids()

	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, fmt.Sprintf("%s%s", util.BtTriplesPrefix, dcid))
	}

	resultsMap := map[string][]Triple{}
	if err := util.BigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(btRow bigtable.Row) error {
			// Extract DCID from row key.
			dcid := strings.TrimPrefix(btRow.Key(), util.BtTriplesPrefix)

			if len(btRow[util.BtFamily]) > 0 {
				btRawValue := btRow[util.BtFamily][0].Value
				btJSONRaw, err := util.UnzipAndDecode(string(btRawValue))
				if err != nil {
					return err
				}

				var btTriples TriplesCache
				json.Unmarshal(btJSONRaw, &btTriples)

				if limit := int(in.GetLimit()); limit < len(btTriples.Triples) {
					btTriples.Triples = btTriples.Triples[:limit]
				}

				if btTriples.Triples == nil {
					resultsMap[dcid] = []Triple{}
				} else {
					resultsMap[dcid] = btTriples.Triples
				}
			}
			return nil
		}); err != nil {
		return err
	}

	// Iterate through all dcids to make sure they are present in resultsMap.
	for _, dcid := range dcids {
		if _, exists := resultsMap[dcid]; !exists {
			resultsMap[dcid] = []Triple{}
		}
	}

	// Format the json response and encode it in base64 as necessary.
	jsonRaw, err := json.Marshal(resultsMap)
	if err != nil {
		return err
	}
	jsonStr := string(jsonRaw)
	out.Payload = jsonStr

	return nil
}

// ----------------------------- HELPER FUNCTIONS -----------------------------

func readTripleFromBq(it *bigquery.RowIterator) ([]*pb.Triple, error) {
	result := []*pb.Triple{}
	for {
		t := pb.Triple{}
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		for idx, cell := range row {
			// Columns are: prov_id, subject_id, predicate, object_id, object_value
			v, _ := cell.(string)
			switch idx {
			case 0:
				t.ProvenanceId = v
			case 1:
				t.SubjectId = v
			case 2:
				t.Predicate = v
			case 3:
				if cell != nil {
					t.ObjectId = v
				}
			case 4:
				if cell != nil {
					t.ObjectValue = v
				}
			default:
				return nil, status.Errorf(codes.InvalidArgument, "Unexpected column index %d", idx)
			}
		}
		result = append(result, &t)
	}
	return result, nil
}

func queryTripleTable(ctx context.Context, client *bigquery.Client, qStrs []string) ([]*pb.Triple, error) {
	tripleChan := make(chan []*pb.Triple, 2)
	errs, errCtx := errgroup.WithContext(ctx)
	for _, qStr := range qStrs {
		qStr := qStr
		errs.Go(func() error {
			log.Printf("Query: %v\n", qStr)
			q := client.Query(qStr)
			it, err := q.Read(errCtx)
			if err != nil {
				return err
			}
			triples, err := readTripleFromBq(it)
			if err != nil {
				return err
			}
			tripleChan <- triples
			return nil
		})
	}
	// Wait for completion and return the first error (if any)
	err := errs.Wait()
	if err != nil {
		return nil, err
	}
	close(tripleChan)

	result := []*pb.Triple{}
	for triples := range tripleChan {
		result = append(result, triples...)
	}
	return result, nil
}

func getOutArcFromSpecialTable(
	ctx context.Context,
	client *bigquery.Client,
	db string,
	dcid []string,
	outArcInfo map[string][]translator.OutArcInfo,
	predicate ...string) ([]*pb.Triple, error) {
	result := []*pb.Triple{}
	for table, pcs := range outArcInfo {
		hasQuery := false
		qStr := "SELECT id"
		for _, pc := range pcs {
			if len(predicate) > 0 && predicate[0] != pc.Pred {
				continue
			}
			hasQuery = true
			qStr += fmt.Sprintf(", t.%s AS %s ", pc.Column, pc.Pred)
		}
		if !hasQuery {
			continue
		}
		qStr += fmt.Sprintf("FROM %s AS t WHERE id IN (%s)", table, util.StringList(dcid))
		log.Printf("Query: %v\n", qStr)
		q := client.Query(qStr)
		it, err := q.Read(ctx)
		if err != nil {
			return nil, err
		}
		for {
			var row []bigquery.Value
			err := it.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}
			var rowDcid string
			for i, cell := range row {
				v, _ := cell.(string)
				if i == 0 {
					rowDcid = v
					continue
				}
				if cell == nil {
					continue
				}
				pcIndex := i - 1
				t := pb.Triple{SubjectId: rowDcid, Predicate: pcs[pcIndex].Pred}
				if pcs[pcIndex].IsNode {
					t.ObjectId = v
				} else {
					t.ObjectValue = v
				}
				result = append(result, &t)
			}
		}
	}
	return result, nil
}

func getInArcFromSpecialTable(
	ctx context.Context,
	client *bigquery.Client,
	db string,
	dcid []string,
	inArcInfo []translator.InArcInfo,
	predicate ...string) ([]*pb.Triple, error) {
	result := []*pb.Triple{}
	for _, info := range inArcInfo {
		if info.ObjCol == "place_key" || info.ObjCol == "observed_node_key" {
			continue
		}
		if len(predicate) > 0 && predicate[0] != info.Pred {
			continue
		}
		qStr := fmt.Sprintf("SELECT id, %s FROM %s WHERE %s IN (%s)", info.SubCol, info.Table, info.ObjCol, util.StringList(dcid))
		log.Printf("Query: %v\n", qStr)
		q := client.Query(qStr)
		it, err := q.Read(ctx)
		if err != nil {
			return nil, err
		}
		for {
			var row []bigquery.Value
			err := it.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}
			var rowDcid string
			for i, cell := range row {
				v, _ := cell.(string)
				if i == 0 {
					rowDcid = v
					continue
				}
				if cell == nil {
					continue
				}
				t := pb.Triple{SubjectId: rowDcid, Predicate: info.Pred}
				t.ObjectId = v
				result = append(result, &t)
			}
		}
	}
	return result, nil
}

func getInstances(
	ctx context.Context,
	client *bigquery.Client,
	db string,
	dcid string,
	limit int32) ([]*pb.Triple, error) {
	result := []*pb.Triple{}
	qStr := fmt.Sprintf("SELECT id From `%s.Instance` where type = \"%s\" LIMIT %d", db, dcid, limit)
	log.Printf("Query: %v\n", qStr)
	q := client.Query(qStr)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		inst := row[0].(string)
		result = append(
			result,
			&pb.Triple{
				SubjectId:   inst,
				Predicate:   "typeOf",
				ObjectValue: dcid,
			})
	}
	return result, nil
}

func getNodeType(ctx context.Context, client *bigquery.Client, db string, dcid string) (string, error) {
	qStr := fmt.Sprintf("SELECT type FROM `%s.Instance` where id = \"%s\"", db, dcid)
	log.Printf("Query: %v\n", qStr)
	q := client.Query(qStr)
	it, err := q.Read(ctx)
	if err != nil {
		return "", err
	}
	var nodeType string
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", err
		}
		nodeType = row[0].(string)
	}
	return nodeType, nil
}

func getNodeInfo(ctx context.Context, client *bigquery.Client, db string, dcid []string) (map[string]*Node, error) {
	result := map[string]*Node{}

	// Return an empty map if no dcids are given
	if len(dcid) == 0 {
		return result, nil
	}

	// Perform a query to the instance table
	qStr := fmt.Sprintf("SELECT id, name, type, prov_id "+
		"FROM `%s.Instance` "+
		"WHERE id IN (%s)",
		db, util.StringList(dcid))
	log.Printf("Query: %v\n", qStr)
	q := client.Query(qStr)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var id, name, ntype, provID string
		for idx, cell := range row {
			v, _ := cell.(string)
			if cell == nil {
				continue
			}
			if idx == 0 {
				id = v
			} else if idx == 1 {
				name = v
			} else if idx == 2 {
				ntype = v
			} else if idx == 3 {
				provID = v
			}
		}
		if _, ok := result[id]; !ok {
			result[id] = &Node{}
		}
		result[id].Dcid = id
		result[id].Name = name
		result[id].ProvID = provID
		result[id].Types = append(result[id].Types, ntype)
	}
	return result, nil
}
