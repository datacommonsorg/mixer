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

package sqlquery

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// GetStatVarSummaries returns summaries of the specified statvars.
func GetStatVarSummaries(ctx context.Context, sqlClient *sqldb.SQLClient, statvars []string) (map[string]*pb.StatVarSummary, error) {
	defer util.TimeTrack(time.Now(), fmt.Sprintf("SQL: GetStatVarSummaries (%s)", strings.Join(statvars, ", ")))

	summaries := map[string]*pb.StatVarSummary{}
	if len(statvars) == 0 {
		return summaries, nil
	}

	var (
		placeTypeRows []*sqldb.SVSummary
		provRows      []*sqldb.SVProvenanceSummary
	)
	errGroup, errCtx := errgroup.WithContext(ctx)
	errGroup.Go(func() error {
		rows, err := sqlClient.GetSVSummaries(errCtx, statvars)
		if err != nil {
			return err
		}
		placeTypeRows = rows
		return nil
	})
	errGroup.Go(func() error {
		rows, err := sqlClient.GetSVProvenanceSummaries(errCtx, statvars)
		if err != nil {
			return err
		}
		provRows = rows
		return nil
	})
	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	for _, row := range placeTypeRows {
		ensureSummary(summaries, row.Variable).PlaceTypeSummary[row.EntityType] = &pb.StatVarSummary_PlaceTypeSummary{
			PlaceCount: row.EntityCount,
			MinValue:   proto.Float64(row.MinValue),
			MaxValue:   proto.Float64(row.MaxValue),
			TopPlaces:  toPlaces(row.SampleEntityIds),
		}
	}
	pivotProvenanceRows(summaries, provRows)

	return summaries, nil
}

func toPlaces(entityIds []string) []*pb.StatVarSummary_Place {
	places := []*pb.StatVarSummary_Place{}
	for _, entityId := range entityIds {
		places = append(places, &pb.StatVarSummary_Place{
			Dcid: entityId,
			Name: entityId,
		})
	}
	return places
}

// ensureSummary returns the summary for variable, allocating it (with non-nil
// sub-maps) on first use. The nil-map rechecks below guard against callers that
// pass in a pre-existing summary built without the sub-maps initialized.
func ensureSummary(summaries map[string]*pb.StatVarSummary, variable string) *pb.StatVarSummary {
	s, ok := summaries[variable]
	if !ok {
		s = &pb.StatVarSummary{
			PlaceTypeSummary:  map[string]*pb.StatVarSummary_PlaceTypeSummary{},
			ProvenanceSummary: map[string]*pb.StatVarSummary_ProvenanceSummary{},
		}
		summaries[variable] = s
	}
	if s.PlaceTypeSummary == nil {
		s.PlaceTypeSummary = map[string]*pb.StatVarSummary_PlaceTypeSummary{}
	}
	if s.ProvenanceSummary == nil {
		s.ProvenanceSummary = map[string]*pb.StatVarSummary_ProvenanceSummary{}
	}
	return s
}

// pivotProvenanceRows folds per-(7-tuple) rows into the nested ProvenanceSummary shape
// on each *pb.StatVarSummary, computing per-provenance and per-series rollups in Go.
func pivotProvenanceRows(summaries map[string]*pb.StatVarSummary, rows []*sqldb.SVProvenanceSummary) {
	type seriesKey struct {
		measurementMethod, observationPeriod, scalingFactor, unit string
	}
	// seriesByVarProv dedups *SeriesSummary pointers by (variable, provenance, seriesKey)
	// so repeated rows with the same key (one per entity_type) update the same series.
	seriesByVarProv := map[string]map[string]map[seriesKey]*pb.StatVarSummary_SeriesSummary{}

	for _, r := range rows {
		summary := ensureSummary(summaries, r.Variable)

		ps, ok := summary.ProvenanceSummary[r.Provenance]
		if !ok {
			name := r.ProvenanceName
			if name == "" {
				name = r.Provenance
			}
			ps = &pb.StatVarSummary_ProvenanceSummary{
				ImportName:    name,
				SeriesSummary: []*pb.StatVarSummary_SeriesSummary{},
			}
			summary.ProvenanceSummary[r.Provenance] = ps
		}

		sk := seriesKey{r.MeasurementMethod, r.ObservationPeriod, r.ScalingFactor, r.Unit}
		provMap, ok := seriesByVarProv[r.Variable]
		if !ok {
			provMap = map[string]map[seriesKey]*pb.StatVarSummary_SeriesSummary{}
			seriesByVarProv[r.Variable] = provMap
		}
		keyedMap, ok := provMap[r.Provenance]
		if !ok {
			keyedMap = map[seriesKey]*pb.StatVarSummary_SeriesSummary{}
			provMap[r.Provenance] = keyedMap
		}
		series, ok := keyedMap[sk]
		if !ok {
			series = &pb.StatVarSummary_SeriesSummary{
				SeriesKey: &pb.StatVarSummary_SeriesSummary_SeriesKey{
					MeasurementMethod: r.MeasurementMethod,
					ObservationPeriod: r.ObservationPeriod,
					ScalingFactor:     r.ScalingFactor,
					Unit:              r.Unit,
				},
				PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{},
			}
			ps.SeriesSummary = append(ps.SeriesSummary, series)
			keyedMap[sk] = series
		}

		series.PlaceTypeSummary[r.EntityType] = &pb.StatVarSummary_PlaceTypeSummary{
			PlaceCount: r.EntityCount,
			MinValue:   proto.Float64(r.MinValue),
			MaxValue:   proto.Float64(r.MaxValue),
			TopPlaces:  toPlaces(r.SampleEntityIds),
		}

		if series.EarliestDate == "" || r.EarliestDate < series.EarliestDate {
			series.EarliestDate = r.EarliestDate
		}
		if r.LatestDate > series.LatestDate {
			series.LatestDate = r.LatestDate
		}
		if series.MinValue == nil || r.MinValue < *series.MinValue {
			series.MinValue = proto.Float64(r.MinValue)
		}
		if series.MaxValue == nil || r.MaxValue > *series.MaxValue {
			series.MaxValue = proto.Float64(r.MaxValue)
		}
		series.ObservationCount += float64(r.ObservationCount)
		series.TimeSeriesCount += float64(r.EntityCount)

		ps.ObservationCount += float64(r.ObservationCount)
		ps.TimeSeriesCount += float64(r.EntityCount)
	}

	// Sort series deterministically so the API response is stable across runs
	// (Go map iteration order is randomized).
	for _, summary := range summaries {
		for _, ps := range summary.ProvenanceSummary {
			sort.SliceStable(ps.SeriesSummary, func(i, j int) bool {
				a, b := ps.SeriesSummary[i].SeriesKey, ps.SeriesSummary[j].SeriesKey
				if a == nil || b == nil {
					return a == nil && b != nil
				}
				if a.MeasurementMethod != b.MeasurementMethod {
					return a.MeasurementMethod < b.MeasurementMethod
				}
				if a.ObservationPeriod != b.ObservationPeriod {
					return a.ObservationPeriod < b.ObservationPeriod
				}
				if a.ScalingFactor != b.ScalingFactor {
					return a.ScalingFactor < b.ScalingFactor
				}
				return a.Unit < b.Unit
			})
		}
	}
}
