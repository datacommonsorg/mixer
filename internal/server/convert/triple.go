// Copyright 2022 Google LLC
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

// Contains functions to convert between protobuf and golang struct.

package convert

import (
	"sort"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/model"
)

// ToLegacyResult converts pb.GetTriplesResponse to legacy golang struct to
// make the API response backward compatible.
func ToLegacyResult(in *pb.GetTriplesResponse) map[string][]*model.Triple {
	out := map[string][]*model.Triple{}
	for dcid, triplesPb := range in.Triples {
		out[dcid] = []*model.Triple{}
		if triplesPb.Triples != nil {
			// Bigtable data is not protobuf message.
			for _, t := range triplesPb.Triples {
				out[dcid] = append(out[dcid], toLegacyTriple(t))
			}
		} else {
			subjectName := ""
			subjectTypes := []string{}
			if triplesPb.OutNodes != nil {
				if nameEntities, ok := triplesPb.OutNodes["name"]; ok {
					for _, e := range nameEntities.Entities {
						subjectName = e.Value
						break
					}
				}
				if typeEntites, ok := triplesPb.OutNodes["typeOf"]; ok {
					for _, e := range typeEntites.Entities {
						subjectTypes = append(subjectTypes, e.Dcid)
					}
				}
			}
			for pred, nodes := range triplesPb.OutNodes {
				for _, e := range nodes.Entities {
					t := outEntityToTriple(pred, dcid, subjectName, subjectTypes, e)
					out[dcid] = append(out[dcid], t)
				}
			}
			for pred, nodes := range triplesPb.InNodes {
				for _, e := range nodes.Entities {
					t := inEntityToTriple(pred, dcid, subjectName, subjectTypes, e)
					out[dcid] = append(out[dcid], t)
				}
			}
		}
		sort.SliceStable(out[dcid], func(i, j int) bool {
			if out[dcid][i].SubjectID == out[dcid][j].SubjectID {
				if out[dcid][i].Predicate == out[dcid][j].Predicate {
					if out[dcid][i].ObjectID == out[dcid][j].ObjectID {
						return out[dcid][i].ObjectValue < out[dcid][j].ObjectValue
					}
					return out[dcid][i].ObjectID < out[dcid][j].ObjectID
				}
				return out[dcid][i].Predicate < out[dcid][j].Predicate
			}
			if out[dcid][i].SubjectID == dcid {
				return true
			} else if out[dcid][j].SubjectID == dcid {
				return false
			}
			return out[dcid][i].SubjectID < out[dcid][j].SubjectID
		})
	}
	return out
}

func outEntityToTriple(
	pred string,
	subjectID string,
	subjectName string,
	subjectTypes []string,
	e *pb.EntityInfo,
) *model.Triple {
	return &model.Triple{
		SubjectID:    subjectID,
		SubjectName:  subjectName,
		SubjectTypes: subjectTypes,
		Predicate:    pred,
		ObjectID:     e.Dcid,
		ObjectName:   e.Name,
		ObjectValue:  e.Value,
		ObjectTypes:  e.Types,
		ProvenanceID: e.ProvenanceId,
	}
}

func inEntityToTriple(
	pred string,
	objectID string,
	objectName string,
	objectTypes []string,
	e *pb.EntityInfo,
) *model.Triple {
	return &model.Triple{
		ObjectID:     objectID,
		ObjectName:   objectName,
		ObjectTypes:  objectTypes,
		Predicate:    pred,
		SubjectID:    e.Dcid,
		SubjectName:  e.Name,
		SubjectTypes: e.Types,
		ProvenanceID: e.ProvenanceId,
	}
}

func toLegacyTriple(in *pb.Triple) *model.Triple {
	return &model.Triple{
		SubjectID:    in.SubjectId,
		SubjectName:  in.SubjectName,
		SubjectTypes: in.SubjectTypes,
		Predicate:    in.Predicate,
		ObjectID:     in.ObjectId,
		ObjectName:   in.ObjectName,
		ObjectValue:  in.ObjectValue,
		ObjectTypes:  in.ObjectTypes,
		ProvenanceID: in.ProvenanceId,
	}
}
