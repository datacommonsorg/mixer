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

package memdb

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/datacommonsorg/mixer/internal/parser/tmcf"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/datacommonsorg/mixer/internal/util"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MemDb holds imported data in memory.
type MemDb struct {
	// statVar -> place -> []Series
	statSeries map[string]map[string][]*pb.SourceSeries
	config     *pb.MemdbConfig
	// place -> svg -> count
	placeSvExistence map[string]map[string]int32
	lock             sync.RWMutex
}

// NewMemDb initialize a MemDb instance.
func NewMemDb() *MemDb {
	return &MemDb{
		statSeries:       map[string]map[string][]*pb.SourceSeries{},
		config:           &pb.MemdbConfig{},
		placeSvExistence: map[string]map[string]int32{},
	}
}

// GetManifest get the manifest data.
func (memDb *MemDb) GetManifest() *pb.MemdbConfig {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	return memDb.config
}

// GetSvg get the svg data.
func (memDb *MemDb) GetSvg() map[string]*pb.StatVarGroupNode {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	return memDb.config.StatVarGroups
}

// GetPlaceSvExistence get the place sv existence info
func (memDb *MemDb) GetPlaceSvExistence() map[string]map[string]int32 {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	return memDb.placeSvExistence
}

// IsEmpty checks if memory database has data.
func (memDb *MemDb) IsEmpty() bool {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	return memDb.statSeries == nil || len(memDb.statSeries) == 0
}

// ReadSeries reads stat series from in-memory DB.
func (memDb *MemDb) ReadSeries(statVar, place string) []*pb.SourceSeries {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	if _, ok := memDb.statSeries[statVar]; ok {
		if series, ok := memDb.statSeries[statVar][place]; ok {
			return series
		}
	}
	return []*pb.SourceSeries{}
}

// ReadPointValue reads one observation point.
// If date is "", the latest observation is returned, otherwise, the observation
// corresponding to the given date is returned.
func (memDb *MemDb) ReadPointValue(statVar, place, date string) (
	*pb.PointStat, *pb.Facet,
) {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	placeData, ok := memDb.statSeries[statVar]
	if !ok {
		return nil, nil
	}
	seriesList, ok := placeData[place]
	if !ok {
		return nil, nil
	}
	if date != "" {
		// For private import, pick from a random series. In most cases, there should
		// be just one series.
		for _, series := range seriesList {
			if val, ok := series.Val[date]; ok {
				return &pb.PointStat{
					Date:  date,
					Value: proto.Float64(val),
				}, util.GetFacet(series)
			}
		}
	} else {
		// Get the latest date from all series
		latestDate := ""
		var latestVal float64
		var facet *pb.Facet
		for _, series := range seriesList {
			for date, val := range series.Val {
				if date > latestDate {
					latestDate = date
					latestVal = val
					facet = util.GetFacet(series)
				}
			}
		}
		if latestDate != "" {
			return &pb.PointStat{
				Date:  latestDate,
				Value: proto.Float64(latestVal),
			}, facet
		}
	}
	return nil, nil
}

// ReadStatDate reads observation date frequency for a given stat var.
func (memDb *MemDb) ReadStatDate(statVar string) *pb.StatDateList {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	result := &pb.StatDateList{}
	placeData, ok := memDb.statSeries[statVar]
	if !ok {
		return result
	}
	tmp := map[string]map[string]float64{}
	metaMap := map[string]*pb.Facet{}
	for _, seriesList := range placeData {
		for _, series := range seriesList {
			facet := util.GetFacet(series)
			facetID := util.GetFacetID(facet)
			metaMap[facetID] = facet
			if _, ok := tmp[facetID]; !ok {
				tmp[facetID] = map[string]float64{}
			}
			for date := range series.Val {
				tmp[facetID][date]++
			}
		}
	}
	for meta, val := range tmp {
		result.StatDate = append(result.StatDate, &pb.StatDate{
			DatePlaceCount: val,
			Metadata:       metaMap[meta],
		})
	}
	return result
}

// ReadObservationDates reads observation date frequency for a given stat var.
func (memDb *MemDb) ReadObservationDates(statVar string) (
	*pbv1.VariableObservationDates,
	map[string]*pb.Facet,
) {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	data := &pbv1.VariableObservationDates{
		Variable:         statVar,
		ObservationDates: []*pbv1.ObservationDates{},
	}
	placeData, ok := memDb.statSeries[statVar]
	if !ok {
		return data, nil
	}
	// keyed by date, facetID, value is the count of places
	tmp := map[string]map[string]float64{}
	metaMap := map[string]*pb.Facet{}
	for _, seriesList := range placeData {
		for _, series := range seriesList {
			facet := util.GetFacet(series)
			facetID := util.GetFacetID(facet)
			metaMap[facetID] = facet
			for date := range series.Val {
				if _, ok := tmp[date]; !ok {
					tmp[date] = map[string]float64{}
				}
				tmp[date][facetID]++
			}
		}
	}
	allDates := []string{}
	for date := range tmp {
		allDates = append(allDates, date)
	}
	sort.Strings(allDates)
	for _, date := range allDates {
		obsDates := &pbv1.ObservationDates{
			Date:        date,
			EntityCount: []*pbv1.EntityCount{},
		}
		for facetID, count := range tmp[date] {
			obsDates.EntityCount = append(obsDates.EntityCount, &pbv1.EntityCount{
				Count: count,
				Facet: facetID,
			})
		}
		sort.SliceStable(obsDates.EntityCount, func(i, j int) bool {
			return obsDates.EntityCount[i].Count > obsDates.EntityCount[j].Count
		})
		data.ObservationDates = append(
			data.ObservationDates, obsDates,
		)
	}
	return data, metaMap
}

// GetStatVars retrieves the stat vars from private import that have data for
// the given places.
func (memDb *MemDb) GetStatVars(places []string) ([]string, []string) {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	hasDataStatVars := []string{}
	noDataStatVars := []string{}
	for statVar, statVarData := range memDb.statSeries {
		valid := false
		if len(places) == 0 {
			valid = true
		} else {
			for _, place := range places {
				if _, ok := statVarData[place]; ok {
					valid = true
					break
				}
			}
		}
		if valid {
			hasDataStatVars = append(hasDataStatVars, statVar)
		} else {
			noDataStatVars = append(noDataStatVars, statVar)
		}
	}
	sort.Strings(hasDataStatVars)
	sort.Strings(noDataStatVars)
	return hasDataStatVars, noDataStatVars
}

// HasStatVar checks if a stat var exists in the memory database.
func (memDb *MemDb) HasStatVar(statVar string) bool {
	memDb.lock.RLock()
	defer memDb.lock.RUnlock()
	_, ok := memDb.statSeries[statVar]
	return ok
}

func getParentSvg(svgMap map[string]*pb.StatVarGroupNode) map[string]string {
	parentSvg := map[string]string{}
	for svg, data := range svgMap {
		for _, child := range data.ChildStatVars {
			parentSvg[child.Id] = svg
		}
		for _, child := range data.ChildStatVarGroups {
			parentSvg[child.Id] = svg
		}
	}
	return parentSvg
}

// LoadConfig loads the memdb config from file.
func (memDb *MemDb) LoadConfig(ctx context.Context, file string) error {
	bytes, err := os.ReadFile(file)
	if err != nil {
		return status.Errorf(codes.Internal, "Failed to read memdb config: %v", err)
	}
	var config pb.MemdbConfig
	err = protojson.Unmarshal(bytes, &config)
	if err != nil {
		return status.Errorf(codes.Internal, "Failed to unmarshal memdb config: %v", err)
	}
	if config.RootSvg == "" {
		return status.Errorf(codes.Internal, "Manifest missing the root SVG:\n%v", &config)
	}
	memDb.config = &config
	parentSvg := getParentSvg(config.StatVarGroups)
	allStatVars := map[string]struct{}{}
	for _, data := range memDb.config.StatVarGroups {
		for _, child := range data.ChildStatVars {
			allStatVars[child.Id] = struct{}{}
		}
	}
	for sv := range allStatVars {
		curr := sv
		for {
			parent, ok := parentSvg[curr]
			if !ok {
				break
			}
			memDb.config.StatVarGroups[parent].DescendentStatVarCount++
			curr = parent
		}
	}
	return nil
}

// LoadFromGcs loads tmcf + csv files into memory database
// This should be called after LoadConfig() so config is already set.
func (memDb *MemDb) LoadFromGcs(ctx context.Context, bucket, prefix string) error {
	memDb.lock.Lock()
	defer memDb.lock.Unlock()
	memDb.statSeries = map[string]map[string][]*pb.SourceSeries{}
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	// The bucket should contain one tmcf and multiple compatible csv files.
	bkt := gcsClient.Bucket(bucket)
	objectQuery := &storage.Query{Prefix: prefix}
	var objects []string
	it := bkt.Objects(ctx, objectQuery)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		objects = append(objects, attrs.Name)
	}
	// Read TMCF
	var schemaMapping map[string]*tmcf.TableSchema
	for _, object := range objects {
		if strings.HasSuffix(object, ".tmcf") {
			obj := bkt.Object(object)
			r, err := obj.NewReader(ctx)
			if err != nil {
				return err
			}
			defer r.Close()
			buf := new(strings.Builder)
			if _, err := io.Copy(buf, r); err != nil {
				return err
			}
			schemaMapping, err = tmcf.ParseTmcf(buf.String())
			if err != nil {
				return err
			}
			break
		}
	}
	count := 0
	for _, object := range objects {
		if strings.HasSuffix(object, ".csv") {
			obj := bkt.Object(object)
			r, err := obj.NewReader(ctx)
			if err != nil {
				return err
			}
			defer r.Close()
			tableName := strings.TrimSuffix(filepath.Base(object), ".csv")
			csvReader := csv.NewReader(r)
			header, err := csvReader.Read()
			if err != nil {
				return err
			}
			for {
				row, err := csvReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				err = memDb.addRow(header, row, schemaMapping[tableName])
				if err != nil {
					return err
				}
				count++
			}
		}
	}
	log.Printf("Number of csv rows added: %d", count)
	// Populate placeSvExistence field
	parentSvg := getParentSvg(memDb.config.StatVarGroups)
	memDb.placeSvExistence = buildMemSVExistenceCache(parentSvg, memDb.statSeries)
	return nil
}

func buildMemSVExistenceCache(
	parentSvg map[string]string,
	statSeries map[string]map[string][]*pb.SourceSeries,
) map[string]map[string]int32 {
	result := map[string]map[string]int32{}
	for sv, placeData := range statSeries {
		allParents := []string{}
		curr := sv
		for {
			if _, ok := result[curr]; !ok {
				result[curr] = map[string]int32{}
			}
			parent, ok := parentSvg[curr]
			if ok {
				allParents = append(allParents, parent)
				curr = parent
			} else {
				break
			}
		}
		for place := range placeData {
			result[sv][place] = 0
			for _, svg := range allParents {
				result[svg][place]++
			}
		}
	}
	return result
}

// nodeObs holds information for one observation
type nodeObs struct {
	statVar           string
	place             string
	date              string // Unpaired date
	value             string // Unpaired value
	provenanceUrl     string
	importName        string
	measurementMethod string
	unit              string
	scalingFactor     string
	observationPeriod string
}

func sameMetadata(s *pb.SourceSeries, n *nodeObs) bool {
	return (s.ProvenanceUrl == n.provenanceUrl &&
		s.ImportName == n.importName &&
		s.MeasurementMethod == n.measurementMethod &&
		s.Unit == n.unit &&
		s.ScalingFactor == n.scalingFactor &&
		s.ObservationPeriod == n.observationPeriod)
}

// addRow adds one csv row to memdb
func (memDb *MemDb) addRow(
	header []string,
	row []string,
	schemaMapping *tmcf.TableSchema,
) error {
	if schemaMapping == nil {
		return status.Errorf(
			codes.Internal, "No schema mapping found for row: %s", row)
	}
	// Keyed by node id like "E0"
	allNodes := map[string]*nodeObs{}
	// Initialize observation entries with the fixed schema
	for node, meta := range schemaMapping.NodeSchema {
		if typ, ok := meta["typeOf"]; ok && typ == "StatVarObservation" {
			allNodes[node] = &nodeObs{
				statVar:       meta["variableMeasured"],
				provenanceUrl: memDb.config.ProvenanceUrl,
				importName:    memDb.config.ImportName,
			}
		}
		// TODO: handle the case when meta data is specified in the column:
		// https://github.com/datacommonsorg/data/blob/master/scripts/un/energy/un_energy.tmcf#L8-L10
		if v, ok := meta["measurementMethod"]; ok {
			allNodes[node].measurementMethod = v
		}
		if v, ok := meta["unit"]; ok {
			allNodes[node].unit = v
		}
		if v, ok := meta["scalingFactor"]; ok {
			allNodes[node].scalingFactor = v
		}
		if v, ok := meta["observationPeriod"]; ok {
			allNodes[node].observationPeriod = v
		}
	}

	// Process each cell
	for idx, cell := range row {
		// Get column name from header
		colName := header[idx]
		// Format cell
		cell = strings.TrimSpace(cell)
		if cell == "" {
			continue
		}
		if cell[0] == '[' && cell[len(cell)-1] == ']' {
			cell = tmcf.ParseComplexValue(cell)
		}
		// Derive node property and value for observation.
		for _, col := range schemaMapping.ColumnInfo[colName] {
			n := col.Node
			if _, ok := allNodes[n]; !ok {
				continue
			}
			if col.Property == "value" {
				allNodes[n].value = cell
			}
			// TODO: handle the case when observationDate is a constant in the tmcf.
			if col.Property == "observationDate" {
				allNodes[n].date = cell
			}
			if col.Property == "observationAbout" {
				allNodes[n].place = cell
			}
		}
	}
	// Populate observation in the final result.
	for _, obs := range allNodes {
		if _, ok := memDb.statSeries[obs.statVar]; !ok {
			memDb.statSeries[obs.statVar] = map[string][]*pb.SourceSeries{}
		}
		if _, ok := memDb.statSeries[obs.statVar][obs.place]; !ok {
			memDb.statSeries[obs.statVar][obs.place] = []*pb.SourceSeries{}
		}
		if obs.date != "" && obs.value != "" {
			v, err := strconv.ParseFloat(obs.value, 64)
			if err != nil {
				return err
			}
			exist := false
			for _, series := range memDb.statSeries[obs.statVar][obs.place] {
				if sameMetadata(series, obs) {
					series.Val[obs.date] = v
					exist = true
				}
			}
			if !exist {
				memDb.statSeries[obs.statVar][obs.place] = append(
					memDb.statSeries[obs.statVar][obs.place],
					&pb.SourceSeries{
						Val:               map[string]float64{obs.date: v},
						ImportName:        obs.importName,
						MeasurementMethod: obs.measurementMethod,
						Unit:              obs.unit,
						ScalingFactor:     obs.scalingFactor,
						ObservationPeriod: obs.observationPeriod,
						ProvenanceUrl:     obs.provenanceUrl,
					},
				)
			}
		}
	}
	return nil
}
