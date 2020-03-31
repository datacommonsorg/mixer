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

	"encoding/json"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigtable"
	pb "github.com/datacommonsorg/mixer/proto"
	"github.com/datacommonsorg/mixer/util"
	"google.golang.org/api/iterator"
)

// ----------------------------- BQ CLIENT METHODS -----------------------------

func (s *store) GetPlacesIn(ctx context.Context,
	in *pb.GetPlacesInRequest, out *pb.GetPlacesInResponse) error {
	return s.btGetPlacesIn(ctx, in, out)
}

func (s *store) btGetPlacesIn(ctx context.Context,
	in *pb.GetPlacesInRequest, out *pb.GetPlacesInResponse) error {
	dcids := in.GetDcids()
	placeType := in.GetPlaceType()

	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, fmt.Sprintf("%s%s^%s", util.BtPlacesInPrefix, dcid, placeType))
	}

	results := []map[string]string{}
	if err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(btRow bigtable.Row) error {
			// Extract DCID from row key.
			parts := strings.Split(btRow.Key(), "^")
			dcid := strings.TrimPrefix(parts[0], util.BtPlacesInPrefix)

			if len(btRow[util.BtFamily]) > 0 {
				btRawValue := btRow[util.BtFamily][0].Value
				btValueRaw, err := util.UnzipAndDecode(string(btRawValue))
				if err != nil {
					return err
				}
				for _, place := range strings.Split(string(btValueRaw), ",") {
					results = append(results, map[string]string{
						"dcid":  dcid,
						"place": place,
					})
				}
			}

			return nil
		}); err != nil {
		return err
	}

	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}

// TODO(*): Deprecate this method once the BT route is stable.
func (s *store) bqGetPlacesIn(ctx context.Context,
	in *pb.GetPlacesInRequest, out *pb.GetPlacesInResponse) error {
	out.Payload = "[]" // By default, return empty list.
	parentIds := in.GetDcids()

	// Get typing information from the first node.
	nodeInfo, err := getNodeInfo(ctx, s.bqClient, s.bqDb, parentIds[:1])
	if err != nil {
		return err
	}
	n, ok := nodeInfo[parentIds[0]]
	if !ok {
		return nil
	}
	parentType := n.Types[0]
	interTypes, ok := s.containedIn[util.TypePair{Child: in.GetPlaceType(), Parent: parentType}]
	if !ok {
		return nil
	}

	// SELECT t1.subject_id FROM `google.com:datcom-store-dev.dc_v3_internal.Triple` as t1
	// JOIN `google.com:datcom-store-dev.dc_v3_internal.Instance` as i
	// ON t1.subject_id = i.id AND i.type = "City"
	// JOIN `google.com:datcom-store-dev.dc_v3_internal.Triple` as t2 ON t1.object_id = t2.subject_id
	// WHERE t2.predicate = "containedInPlace" and t2.object_id = "geoId/06"

	// Build the query string.
	qStr := fmt.Sprintf(
		"SELECT DISTINCT t%d.object_id, t0.subject_id "+
			"FROM `%s.Triple` as t0 "+
			"JOIN `%s.Instance` as i ON t0.subject_id = i.id AND i.type = '%s' ",
		len(interTypes), s.bqDb, s.bqDb, in.GetPlaceType())
	for i := range interTypes {
		qStr += fmt.Sprintf(
			"JOIN `%s.Triple` as t%d ON t%d.object_id = t%d.subject_id AND t%d.predicate = 'containedInPlace'",
			s.bqDb, i+1, i, i+1, i+1)
	}
	qStr += fmt.Sprintf("WHERE t%d.object_id IN (%s)", len(interTypes), util.StringList(parentIds))
	log.Printf("GetPlacesIn: %v\n", qStr)

	// Issue the query and build the resulting json.
	jsonRes := make([]map[string]string, 0)
	query := s.bqClient.Query(qStr)
	it, err := query.Read(ctx)
	if err != nil {
		return err
	}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		jsonRes = append(jsonRes, map[string]string{
			"dcid":  row[0].(string),
			"place": row[1].(string),
		})
	}

	jsonRaw, err := json.Marshal(jsonRes)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}

// RelatedPlacesInfo represents the json structure returned by the RelatedPlaces cache.
type RelatedPlacesInfo struct {
	RelatedPlaces    []string `json:"relatedPlaces,omitempty"`
	RankFromTop      int32    `json:"rankFromTop,omitempty"`
	RankFromBottom   int32    `json:"rankFromBottom,omitempty"`
	AllPlaces        []string `json:"allPlaces,omitempty"`
	Top1000Places    []string `json:"top1000Places,omitempty"`
	Bottom1000Places []string `json:"bottom1000Places,omitempty"`
}

func (s *store) GetRelatedPlaces(ctx context.Context,
	in *pb.GetRelatedPlacesRequest, out *pb.GetRelatedPlacesResponse) error {
	// TODO: Move the default value up to Python API. Consult wsws before moving.
	//
	// The default values ensure for the following 5 cache keys.
	// count^CensusACS5yrSurvey^^^^^measuredValue^Person
	// income^CensusACS5yrSurvey^^^^USDollar^medianValue^Person^age^Years15Onwards^ \
	//   incomeStatus^WithIncome
	// age^CensusACS5yrSurvey^^^^Year^medianValue^Person
	// unemploymentRate^BLSSeasonallyUnadjusted^^^^^measuredValue^Person
	// count^^^^^^measuredValue^CriminalActivities^crimeType^UCR_CombinedCrime
	measuredProperty := in.GetMeasuredProperty()
	populationType := in.GetPopulationType()
	measurementMethod := in.GetMeasurementMethod()
	if measurementMethod == "" && populationType == "Person" {
		if measuredProperty == "unemploymentRate" {
			measurementMethod = "BLSSeasonallyUnadjusted"
		} else {
			measurementMethod = "CensusACS5yrSurvey"
		}
	}
	unit := in.GetUnit()
	if unit == "" {
		if measuredProperty == "age" {
			unit = "Year"
		} else if measuredProperty == "income" {
			unit = "USDollar"
		}
	}

	popObsSignatureItems := []string{measuredProperty, measurementMethod,
		in.GetMeasurementDenominator(), in.GetMeasurementQualifier(), in.GetScalingFactor(),
		unit, in.GetStatType(), in.GetPopulationType()}
	if len(in.GetPvs()) > 0 {
		iterateSortPVs(in.GetPvs(), func(i int, p, v string) {
			popObsSignatureItems = append(popObsSignatureItems, []string{p, v}...)
		})
	}
	popObsSignature := strings.Join(popObsSignatureItems, "^")

	// TODO: Currently same_place_type and within_place options are mutually exclusive.
	// The logic here chooses within_place when both set.
	// Remove the logic when both options can live together.
	withinPlace := in.GetWithinPlace()
	if withinPlace == "country/USA" {
		withinPlace = ""
	}
	samePlaceType := in.GetSamePlaceType()
	isPerCapita := in.GetIsPerCapita()
	var prefix string
	if withinPlace == "" {
		if samePlaceType {
			prefix = util.BtRelatedPlacesSameTypePrefix
			if isPerCapita {
				prefix = util.BtRelatedPlacesSameTypePCPrefix
			}
		} else {
			prefix = util.BtRelatedPlacesPrefix
			if isPerCapita {
				prefix = util.BtRelatedPlacesPCPrefix
			}
		}
	} else {
		if samePlaceType {
			prefix = util.BtRelatedPlacesSameTypeAndAncestorPrefix
			if isPerCapita {
				prefix = util.BtRelatedPlacesSameTypeAndAncestorPCPrefix
			}
		} else {
			prefix = util.BtRelatedPlacesSameAncestorPrefix
			if isPerCapita {
				prefix = util.BtRelatedPlacesSameAncestorPCPrefix
			}
		}
	}

	dcids := in.GetDcids()
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		if withinPlace != "" {
			rowList = append(rowList, fmt.Sprintf("%s%s^%s^%s", prefix, dcid, withinPlace,
				popObsSignature))
		} else {
			rowList = append(rowList, fmt.Sprintf("%s%s^%s", prefix, dcid, popObsSignature))
		}
	}

	results := map[string]*RelatedPlacesInfo{}
	if err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(btRow bigtable.Row) error {
			// Extract DCID from row key.
			parts := strings.Split(btRow.Key(), "^")
			dcid := strings.TrimPrefix(parts[0], prefix)

			if len(btRow[util.BtFamily]) > 0 {
				btRawValue := btRow[util.BtFamily][0].Value
				btJSONRaw, err := util.UnzipAndDecode(string(btRawValue))
				if err != nil {
					return err
				}
				var btRelatedPlacesInfo RelatedPlacesInfo
				json.Unmarshal(btJSONRaw, &btRelatedPlacesInfo)
				results[dcid] = &btRelatedPlacesInfo
			}

			return nil
		}); err != nil {
		return err
	}

	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}

// StatisticalVariable contains key info of population and observation.
type StatisticalVariable struct {
	PopType                string            `json:"popType,omitempty"`
	PVs                    map[string]string `json:"pvs,omitempty"`
	MeasuredProp           string            `json:"measuredProp,omitempty"`
	MeasurementMethod      string            `json:"measurementMethod,omitempty"`
	MeasurementDenominator string            `json:"measurementDeonominator,omitempty"`
	MeasurementQualifier   string            `json:"measurementQualifier,omitempty"`
	ScalingFactor          string            `json:"scalingFactor,omitempty"`
	Unit                   string            `json:"unit,omitempty"`
	StatType               string            `json:"statType,omitempty"`
}

// InterestingPlaceAspect contains info about why a place is interesting.
type InterestingPlaceAspect struct {
	RankFromTop      int32                `json:"rankFromTop,omitempty"`
	RankFromBottom   int32                `json:"rankFromBottom,omitempty"`
	StatVar          *StatisticalVariable `json:"statVar,omitempty"`
	ContainedInPlace string               `json:"containedInPlace,omitempty"`
	PlaceType        string               `json:"placeType,omitempty"`
	PerCapita        bool                 `json:"perCapita,omitempty"`
}

// InterestingPlaceAspects contains a list of InterestingPlaceAspect objects.
type InterestingPlaceAspects struct {
	Aspects []*InterestingPlaceAspect `json:"aspects,omitempty"`
}

func (s *store) GetInterestingPlaceAspects(ctx context.Context,
	in *pb.GetInterestingPlaceAspectsRequest, out *pb.GetInterestingPlaceAspectsResponse) error {
	rowList := bigtable.RowList{}
	for _, dcid := range in.GetDcids() {
		rowList = append(rowList, fmt.Sprintf("%s%s", util.BtInterestingPlaceAspectPrefix, dcid))
	}

	results := map[string]*InterestingPlaceAspects{}
	if err := bigTableReadRowsParallel(ctx, s.btTable, rowList,
		func(btRow bigtable.Row) error {
			// Extract DCID from row key.
			dcid := strings.TrimPrefix(btRow.Key(), util.BtInterestingPlaceAspectPrefix)

			if len(btRow[util.BtFamily]) > 0 {
				btRawValue := btRow[util.BtFamily][0].Value
				btJSONRaw, err := util.UnzipAndDecode(string(btRawValue))
				if err != nil {
					return err
				}
				var btInterestingPlaceAspects InterestingPlaceAspects
				json.Unmarshal(btJSONRaw, &btInterestingPlaceAspects)
				results[dcid] = &btInterestingPlaceAspects
			}

			return nil
		}); err != nil {
		return err
	}

	jsonRaw, err := json.Marshal(results)
	if err != nil {
		return err
	}
	out.Payload = string(jsonRaw)

	return nil
}
