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

package recon

import (
	"context"
	"encoding/json"
	"fmt"

	pb "github.com/datacommonsorg/mixer/internal/proto"
	"github.com/datacommonsorg/mixer/internal/server/v0/propertyvalue"
	"github.com/datacommonsorg/mixer/internal/store"
	"github.com/datacommonsorg/mixer/internal/store/bigtable"
	"github.com/golang/geo/s2"
	"google.golang.org/protobuf/proto"
)

const (
	gridSize         float64 = 0.2
	geoJSONPredicate string  = "geoJsonCoordinates"
)

// ResolveCoordinates implements API for ReconServer.ResolveCoordinates.
func ResolveCoordinates(
	ctx context.Context, in *pb.ResolveCoordinatesRequest, store *store.Store) (
	*pb.ResolveCoordinatesResponse, error,
) {
	// Map: lat^lng => normalized lat^lng.
	normCoordinateMap := map[string]string{}
	coordinateLookupKeys := map[string]struct{}{}

	filteredTypes := map[string]struct{}{}
	for _, t := range in.GetPlaceTypes() {
		filteredTypes[t] = struct{}{}
	}

	// Read request.
	for _, coordinate := range in.GetCoordinates() {
		nKey := normalizedCoordinateKey(coordinate)
		normCoordinateMap[coordinateKey(coordinate)] = nKey
		coordinateLookupKeys[nKey] = struct{}{}
	}

	// Read coordinate recon cache.
	keyBody := []string{}
	for key := range coordinateLookupKeys {
		keyBody = append(keyBody, key)
	}
	reconDataList, err := bigtable.Read(
		ctx,
		store.BtGroup,
		bigtable.BtCoordinateReconPrefix,
		[][]string{keyBody},
		func(jsonRaw []byte) (interface{}, error) {
			var recon pb.CoordinateRecon
			if err := proto.Unmarshal(jsonRaw, &recon); err != nil {
				return nil, err
			}
			return &recon, nil
		},
	)
	if err != nil {
		return nil, err
	}

	// Collect places that don't fully cover the tiles that the coordinates are in.
	questionablePlaces := map[string]struct{}{}
	for _, reconData := range reconDataList {
		for _, row := range reconData {
			for _, place := range row.Data.(*pb.CoordinateRecon).GetPlaces() {
				if len(filteredTypes) > 0 {
					_, keep := filteredTypes[place.GetDominantType()]
					if !keep {
						continue
					}
				}
				if _, ok := questionablePlaces[place.GetDcid()]; !ok && !place.GetFull() {
					questionablePlaces[place.GetDcid()] = struct{}{}
				}
			}
		}
		// Only process data from one preferred import group.
		if len(reconData) > 0 {
			break
		}
	}

	questionablePlaceList := []string{}
	for place := range questionablePlaces {
		questionablePlaceList = append(questionablePlaceList, place)
	}
	geoJSONData, err := propertyvalue.GetPropertyValuesHelper(
		ctx, store, questionablePlaceList, geoJSONPredicate, true)
	if err != nil {
		return nil, err
	}
	s2PolygonMap := map[string]*s2.Polygon{}
	for place, gjValues := range geoJSONData {
		if len(gjValues) == 0 {
			continue
		}
		s2Polygon, err := parseGeoJSON(gjValues[0].Value)
		if err != nil {
			return nil, err
		}
		s2PolygonMap[place] = s2Polygon
	}

	// Assemble response.
	res := &pb.ResolveCoordinatesResponse{}
	for _, co := range in.GetCoordinates() {
		nKey := normCoordinateMap[coordinateKey(co)]
		placeCoordinates := &pb.ResolveCoordinatesResponse_PlaceCoordinate{
			Latitude:  co.GetLatitude(),
			Longitude: co.GetLongitude(),
		}
		for _, reconData := range reconDataList {
			if len(reconData) == 0 {
				continue
			}
			for _, row := range reconData {
				if fmt.Sprintf("%s^%s", row.Parts[0], row.Parts[1]) != nKey {
					continue
				}
				for _, place := range row.Data.(*pb.CoordinateRecon).GetPlaces() {
					if len(filteredTypes) > 0 {
						_, keep := filteredTypes[place.GetDominantType()]
						if !keep {
							continue
						}
					}
					if place.GetFull() {
						placeCoordinates.PlaceDcids = append(
							placeCoordinates.PlaceDcids,
							place.GetDcid(),
						)
						placeCoordinates.Places = append(placeCoordinates.Places,
							&pb.ResolveCoordinatesResponse_Place{
								Dcid:         place.GetDcid(),
								DominantType: place.GetDominantType(),
							})
					} else { // Not fully cover the tile.
						s2Polygon, ok := s2PolygonMap[place.GetDcid()]
						if !ok {
							continue
						}
						contained, err := isContainedIn(s2Polygon, co.GetLatitude(), co.GetLongitude())
						if err != nil {
							return res, err
						}
						if contained {
							placeCoordinates.PlaceDcids = append(
								placeCoordinates.PlaceDcids,
								place.GetDcid(),
							)
							placeCoordinates.Places = append(placeCoordinates.Places,
								&pb.ResolveCoordinatesResponse_Place{
									Dcid:         place.GetDcid(),
									DominantType: place.GetDominantType(),
								})
						}
					}
				}
			}
			// Only need data from a preferred import group.
			break
		}
		res.PlaceCoordinates = append(res.PlaceCoordinates, placeCoordinates)
	}

	return res, nil
}

func coordinateKey(c *pb.ResolveCoordinatesRequest_Coordinate) string {
	return fmt.Sprintf("%f^%f", c.GetLatitude(), c.GetLongitude())
}

func normalizedCoordinateKey(c *pb.ResolveCoordinatesRequest_Coordinate) string {
	// Normalize to South-West of the grid points.
	lat := float64(int((c.GetLatitude()+90.0)/gridSize))*gridSize - 90
	lng := float64(int((c.GetLongitude()+180.0)/gridSize))*gridSize - 180
	return fmt.Sprintf("%.1f^%.1f", lat, lng)
}

// Polygon represents a polygon shape.
type Polygon struct {
	Loops [][][]float64
}

// MultiPolygon represents a list of polygons.
type MultiPolygon struct {
	Polygons [][][][]float64
}

// GeoJSON represents the geoJson data for a place.
type GeoJSON struct {
	Type         string          `json:"type"`
	Coordinates  json.RawMessage `json:"coordinates"`
	Polygon      Polygon         `json:"-"`
	MultiPolygon MultiPolygon    `json:"-"`
}

func buildS2Loops(loops [][][]float64) ([]*s2.Loop, error) {
	res := []*s2.Loop{}
	for i, loop := range loops {
		if l := len(loop); l < 4 {
			return nil, fmt.Errorf("geoJson requires >= 4 points for a loop, got %d", l)
		}

		s2Points := []s2.Point{}
		// NOTE: We have to skip the last point when constructing the s2Loop.
		// In GeoJson, the last point is the same as the first point for a loop.
		// If not skipping, it sometimes leads to wrong result for containment calculation.
		for _, point := range loop[:len(loop)-1] {
			if len(point) != 2 {
				return nil, fmt.Errorf("wrong point format: %+v", point)
			}
			// NOTE: GeoJson has longitude comes before latitude.
			s2Points = append(s2Points,
				s2.PointFromLatLng(s2.LatLngFromDegrees(point[1], point[0])))
		}
		s2Loop := s2.LoopFromPoints(s2Points)
		if i == 0 {
			// The first ring of a polygon is a shell, it should be normalized to counter-clockwise.
			//
			// This step ensures that the planar polygon loop follows the "right-hand rule"
			// and reverses the orientation when that is not the case. This is specified by
			// RFC 7946 GeoJSON spec (https://tools.ietf.org/html/rfc7946), but is commonly
			// disregarded. Since orientation is easy to deduce on the plane, we assume the
			// obvious orientation is intended. We reverse orientation to ensure that all
			// loops follow the right-hand rule. This corresponds to S2's "interior-on-the-
			// left rule", and allows us to create these polygon as oriented S2 polygons.
			//
			// Also see https://en.wikipedia.org/wiki/Curve_orientation.
			s2Loop.Normalize()
		}
		res = append(res, s2Loop)
	}
	return res, nil
}

func parseGeoJSON(geoJSON string) (*s2.Polygon, error) {
	g := &GeoJSON{}
	if err := json.Unmarshal([]byte(geoJSON), g); err != nil {
		return nil, err
	}

	switch g.Type {
	case "Polygon":
		if err := json.Unmarshal(g.Coordinates, &g.Polygon.Loops); err != nil {
			return nil, err
		}
		s2Loops, err := buildS2Loops(g.Polygon.Loops)
		if err != nil {
			return nil, err
		}
		return s2.PolygonFromOrientedLoops(s2Loops), nil
	case "MultiPolygon":
		if err := json.Unmarshal(g.Coordinates, &g.MultiPolygon.Polygons); err != nil {
			return nil, err
		}
		s2Loops := []*s2.Loop{}
		for _, polygon := range g.MultiPolygon.Polygons {
			lps, err := buildS2Loops(polygon)
			if err != nil {
				return nil, err
			}
			s2Loops = append(s2Loops, lps...)
		}
		return s2.PolygonFromOrientedLoops(s2Loops), nil
	default:
		return nil, fmt.Errorf("unrecognized GeoJson object: %+v", g.Type)
	}
}

func isContainedIn(s2Polygon *s2.Polygon, lat float64, lng float64) (bool, error) {
	s2Point := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
	return s2Polygon.ContainsPoint(s2Point), nil
}
