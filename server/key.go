// Copyright 2020 Google LLC
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

package server

import (
	"fmt"
	"sort"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/datacommonsorg/mixer/util"
)

var propValkeyPrefix = map[bool]string{
	true:  util.BtOutPropValPrefix,
	false: util.BtInPropValPrefix,
}

func buildStatsKeySuffix(statsVar *StatisticalVariable) string {
	keySuffix := strings.Join([]string{
		statsVar.MeasuredProp,
		statsVar.StatType,
		statsVar.MeasurementDenominator,
		statsVar.MeasurementQualifier,
		statsVar.PopType},
		"^")
	var cprops []string
	for cprop := range statsVar.PVs {
		cprops = append(cprops, cprop)
	}
	sort.Strings(cprops)
	for _, cprop := range cprops {
		keySuffix += fmt.Sprintf("^%s^%s", cprop, statsVar.PVs[cprop])
	}
	return keySuffix
}

func buildTriplesKey(dcids []string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, fmt.Sprintf("%s%s", util.BtTriplesPrefix, dcid))
	}
	return rowList
}

type placeStatVar struct {
	place   string
	statVar string
}

func buildStatsKey(
	places []string, statVars map[string]*StatisticalVariable) (
	bigtable.RowList, map[string]*placeStatVar) {
	rowList := bigtable.RowList{}
	keyToToken := map[string]*placeStatVar{}
	for sv, svObj := range statVars {
		keySuffix := buildStatsKeySuffix(svObj)
		for _, place := range places {
			rowKey := fmt.Sprintf("%s%s^%s", util.BtChartDataPrefix, place, keySuffix)
			rowList = append(rowList, rowKey)
			keyToToken[rowKey] = &placeStatVar{place, sv}
		}
	}
	return rowList, keyToToken
}

func buildStatCollectionKey(
	parentPlace, childType, date string,
	statVars map[string]*StatisticalVariable, op string) (
	bigtable.RowList, map[string]string) {

	rowList := bigtable.RowList{}
	keyToToken := map[string]string{}
	for sv, svObj := range statVars {
		rowKey := strings.Join([]string{
			util.BtChartDataPrefix + parentPlace,
			childType,
			svObj.MeasuredProp,
			op,
			svObj.StatType,
			svObj.MeasurementDenominator,
			svObj.MeasurementQualifier,
			date,
			svObj.PopType,
		}, "^")
		cprops := []string{}
		for cprop := range svObj.PVs {
			cprops = append(cprops, cprop)
		}
		sort.Strings(cprops)
		for _, cprop := range cprops {
			rowKey += fmt.Sprintf("^%s^%s", cprop, svObj.PVs[cprop])
		}
		rowList = append(rowList, rowKey)
		keyToToken[rowKey] = sv
	}
	return rowList, keyToToken
}

func buildPropertyValuesKey(
	dcids []string, prop string, arcOut bool) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowKey := fmt.Sprintf("%s%s^%s", propValkeyPrefix[arcOut], dcid, prop)
		rowList = append(rowList, rowKey)
	}
	return rowList
}

func buildPropertyLabelKey(dcids []string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, fmt.Sprintf("%s%s", util.BtArcsPrefix, dcid))
	}
	return rowList
}

func buildObservedNodeKey(dcids []string, pred string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList,
			fmt.Sprintf("%s%s^%s", util.BtObsAncestorPrefix, dcid, pred))
	}
	return rowList
}

func buildPopPVKey(dcids []string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(rowList, fmt.Sprintf("%s%s", util.BtPopPVPrefix, dcid))
	}
	return rowList
}

func buildPlaceInKey(dcids []string, placeType string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(
			rowList, fmt.Sprintf("%s%s^%s", util.BtPlacesInPrefix, dcid, placeType))
	}
	return rowList
}

func buildPlaceStatsVarKey(dcids []string) bigtable.RowList {
	rowList := bigtable.RowList{}
	for _, dcid := range dcids {
		rowList = append(
			rowList, fmt.Sprintf("%s%s", util.BtPlaceStatsVarPrefix, dcid))
	}
	return rowList
}
