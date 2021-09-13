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
	"encoding/csv"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/datacommonsorg/mixer/internal/parser"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"
)

// MemDb holds imported data in memory.
type MemDb struct {
	// statVar -> place -> []Series
	statSeries map[string]map[string][]*pb.Series
	manifest   *pb.Manifest
}

// NewMemDb initialize a MemDb instance.
func NewMemDb() *MemDb {
	return &MemDb{
		statSeries: map[string]map[string][]*pb.Series{},
		manifest:   &pb.Manifest{},
	}
}

// IsEmpty checks if memory database has data.
func (memDb *MemDb) IsEmpty() bool {
	return memDb.statSeries == nil || len(memDb.statSeries) == 0
}

// ReadSeries reads stat series from in-memory DB.
func (memDb *MemDb) ReadSeries(statVar, place string) []*pb.Series {
	if _, ok := memDb.statSeries[statVar]; ok {
		if series, ok := memDb.statSeries[statVar][place]; ok {
			return series
		}
	}
	return []*pb.Series{}
}

// ReadPointValue reads one observation point.
// If date is "", the latest observation is returned, otherwise, the observation
// corresponding to the given date is returned.
func (memDb *MemDb) ReadPointValue(statVar, place, date string) (
	*pb.PointStat, *pb.StatMetadata,
) {
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
					Value: val,
					Metadata: &pb.StatMetadata{
						ImportName: memDb.manifest.ImportName,
					}}, series.Metadata
			}
		}
	} else {
		// Get the latest date from all series
		latestDate := ""
		var latestVal float64
		var meta *pb.StatMetadata
		for _, series := range seriesList {
			for date, val := range series.Val {
				if date > latestDate {
					latestDate = date
					latestVal = val
					meta = series.Metadata
				}
			}
		}
		if latestDate != "" {
			return &pb.PointStat{
				Date:     latestDate,
				Value:    latestVal,
				Metadata: &pb.StatMetadata{ImportName: memDb.manifest.ImportName},
			}, meta
		}
	}
	return nil, nil
}

// GetStatVars retrieves the stat vars from private import that have data for
// the given places.
func (memDb *MemDb) GetStatVars(places []string) ([]string, []string) {
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
	_, ok := memDb.statSeries[statVar]
	return ok
}

// LoadFromGcs loads tmcf + csv files into memory database
func (memDb *MemDb) LoadFromGcs(ctx context.Context, bucket, prefix string) error {
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
	// Read manifest.json
	for _, object := range objects {
		if strings.HasSuffix(object, "manifest.json") {
			r, err := bkt.Object(object).NewReader(ctx)
			if err != nil {
				return err
			}
			defer r.Close()
			bytes, err := ioutil.ReadAll(r)
			if err != nil {
				return err
			}
			manifest := &pb.Manifest{}
			err = protojson.Unmarshal(bytes, manifest)
			if err != nil {
				return err
			}
			memDb.manifest = manifest
			break
		}
	}
	// Read TMCF
	var schemaMapping map[string]*parser.TableSchema
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
			schemaMapping, err = parser.ParseTmcf(buf.String())
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
				err = addRow(header, row, schemaMapping[tableName], memDb.statSeries, memDb.manifest)
				if err != nil {
					return err
				}
				count++
			}
		}
	}
	log.Printf("Number of csv rows added: %d", count)
	return nil
}

// nodeObs holds information for one observation
type nodeObs struct {
	statVar string
	place   string
	date    string // Unpaired date
	value   string // Unpaired value
	meta    *pb.StatMetadata
}

// addRow adds one csv row to memdb
func addRow(
	header []string,
	row []string,
	schemaMapping *parser.TableSchema,
	statSeries map[string]map[string][]*pb.Series,
	manifest *pb.Manifest,
) error {
	// Keyed by node id like "E0"
	allNodes := map[string]*nodeObs{}
	// Initialize observation entries with the fixed schema
	for node, meta := range schemaMapping.NodeSchema {
		if typ, ok := meta["typeOf"]; ok && typ == "StatVarObservation" {
			allNodes[node] = &nodeObs{
				statVar: meta["variableMeasured"],
				meta: &pb.StatMetadata{
					ProvenanceUrl: manifest.ProvenanceUrl,
					ImportName:    manifest.ImportName,
				},
			}
		}
		// TODO: handle the case when meta data is specified in the column:
		// https://github.com/datacommonsorg/data/blob/master/scripts/un/energy/un_energy.tmcf#L8-L10
		if v, ok := meta["measurementMethod"]; ok {
			allNodes[node].meta.MeasurementMethod = v
		}
		if v, ok := meta["unit"]; ok {
			allNodes[node].meta.Unit = v
		}
		if v, ok := meta["scalingFactor"]; ok {
			allNodes[node].meta.ScalingFactor = v
		}
		if v, ok := meta["observationPeriod"]; ok {
			allNodes[node].meta.ObservationPeriod = v
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
			cell = parser.ParseComplexValue(cell)
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
		if _, ok := statSeries[obs.statVar]; !ok {
			statSeries[obs.statVar] = map[string][]*pb.Series{}
		}
		if _, ok := statSeries[obs.statVar][obs.place]; !ok {
			statSeries[obs.statVar][obs.place] = []*pb.Series{}
		}
		if obs.date != "" && obs.value != "" {
			v, err := strconv.ParseFloat(obs.value, 64)
			if err != nil {
				return err
			}
			exist := false
			for _, series := range statSeries[obs.statVar][obs.place] {
				if series.Metadata.String() == obs.meta.String() {
					series.Val[obs.date] = v
					exist = true
				}
			}
			if !exist {
				statSeries[obs.statVar][obs.place] = append(
					statSeries[obs.statVar][obs.place],
					&pb.Series{
						Val:      map[string]float64{obs.date: v},
						Metadata: obs.meta,
					},
				)
			}
		}
	}
	return nil
}
