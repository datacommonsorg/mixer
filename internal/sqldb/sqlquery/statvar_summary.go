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
	"strings"
	"time"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/sqldb"
	"github.com/datacommonsorg/mixer/internal/util"
)

// GetStatVarSummaries returns summaries of the specified statvars.
func GetStatVarSummaries(ctx context.Context, sqlClient *sqldb.SQLClient, statvars []string) (map[string]*pb.StatVarSummary, error) {
	defer util.TimeTrack(time.Now(), fmt.Sprintf("SQL: GetStatVarSummaries (%s)", strings.Join(statvars, ", ")))

	summaries := map[string]*pb.StatVarSummary{}

	if len(statvars) == 0 {
		return summaries, nil
	}

	rows, err := sqlClient.GetSVSummaries(ctx, statvars)
	if err != nil {
		return nil, err
	}

	for _, row := range rows {
		if _, ok := summaries[row.Variable]; !ok {
			summaries[row.Variable] = &pb.StatVarSummary{
				PlaceTypeSummary: map[string]*pb.StatVarSummary_PlaceTypeSummary{},
			}
		}

		summaries[row.Variable].PlaceTypeSummary[row.EntityType] = &pb.StatVarSummary_PlaceTypeSummary{
			PlaceCount: row.EntityCount,
			MinValue:   &row.MinValue,
			MaxValue:   &row.MaxValue,
			TopPlaces:  toPlaces(row.SampleEntityIds),
		}
	}

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
