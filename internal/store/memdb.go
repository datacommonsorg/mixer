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
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"github.com/datacommonsorg/mixer/internal/parser"
	pb "github.com/datacommonsorg/mixer/internal/proto"
)

// MemDb holds imported data in memory.
type MemDb struct {
	// statVar -> place -> []Series
	statSeries map[string]map[string][]*pb.Series
}

// Load loads tmcf + csv files into memory database
func (memDb *MemDb) Load(tmcfFile string) error {
	tmcfBytes, err := ioutil.ReadFile(tmcfFile)
	if err != nil {
		return err
	}
	schemaMapping, err := parser.ParseTmcf(string(tmcfBytes))
	// TODO: implement csv parsing and actual data loading
	log.Printf("%v", schemaMapping)
	if err != nil {
		return err
	}
	memDb.statSeries = map[string]map[string][]*pb.Series{}
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
) error {
	// Keyed by node id like "E0"
	allNodes := map[string]*nodeObs{}
	// Initialize observation entries with the fixed schema
	for node, meta := range schemaMapping.NodeSchema {
		if typ, ok := meta["typeOf"]; ok && typ == "StatVarObservation" {
			allNodes[node] = &nodeObs{
				statVar: meta["variableMeasured"],
				meta:    &pb.StatMetadata{},
			}
		}
		if v, ok := meta["measurementMethod"]; ok {
			allNodes[node].meta.MeasurementMethod = v
		}
		if v, ok := meta["unit"]; ok {
			allNodes[node].meta.Unit = v
		}
		if v, ok := meta["scalingFactor"]; ok {
			allNodes[node].meta.ScalingFactor = v
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
			if col.Property == "observationDate" {
				allNodes[n].date = cell
			}
			if col.Property == "observationAbout" {
				allNodes[n].place = cell
			}
		}
	}
	// Population observation into the final result.
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
